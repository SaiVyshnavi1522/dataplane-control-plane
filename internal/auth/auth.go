package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const RequestIDHeader = "X-Request-ID"

type contextKey string

const (
	principalKey contextKey = "principal"
	requestIDKey contextKey = "request-id"
)

type Principal struct {
	Subject string
	Role    string
}

type credential struct {
	principal Principal
	hash      [sha256.Size]byte
}

type Authorizer struct {
	credentials []credential
	disabled    bool
	audit       AuditSink
}

type AuditSink interface {
	RecordAudit(context.Context, model.AuditEvent) error
}

// New parses comma-separated subject:role:key credentials. Keys are hashed at
// startup and are never retained in plaintext.
func New(value string) (*Authorizer, error) {
	if strings.TrimSpace(value) == "" {
		return nil, fmt.Errorf("API_KEYS must configure at least one subject:role:key credential")
	}
	entries := strings.Split(value, ",")
	authorizer := &Authorizer{credentials: make([]credential, 0, len(entries))}
	for _, entry := range entries {
		parts := strings.SplitN(strings.TrimSpace(entry), ":", 3)
		if len(parts) != 3 || parts[0] == "" || (parts[1] != "admin" && parts[1] != "viewer") || len(parts[2]) < 16 {
			return nil, fmt.Errorf("API_KEYS entries must use subject:admin|viewer:key with keys of at least 16 characters")
		}
		authorizer.credentials = append(authorizer.credentials, credential{
			principal: Principal{Subject: parts[0], Role: parts[1]},
			hash:      sha256.Sum256([]byte(parts[2])),
		})
	}
	return authorizer, nil
}

func AllowAll() *Authorizer { return &Authorizer{disabled: true} }

func (a *Authorizer) SetAuditSink(sink AuditSink) { a.audit = sink }

func (a *Authorizer) HTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requestID := validOrNewRequestID(request.Header.Get(RequestIDHeader))
		w.Header().Set(RequestIDHeader, requestID)
		ctx := context.WithValue(request.Context(), requestIDKey, requestID)
		if a.disabled || isPublicHTTP(request.URL.Path) {
			next.ServeHTTP(w, request.WithContext(ctx))
			return
		}
		principal, ok := a.authenticate(request.Header.Get("Authorization"))
		if !ok {
			writeHTTPError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "valid Bearer credentials are required")
			a.record(ctx, Principal{Subject: "anonymous", Role: "none"}, "HTTP_"+request.Method, "http_path", request.URL.Path, false, map[string]any{
				"protocol": "http", "status_code": http.StatusUnauthorized,
			})
			return
		}
		ctx = context.WithValue(ctx, principalKey, principal)
		if requiresAdminHTTP(request) && principal.Role != "admin" {
			writeHTTPError(w, http.StatusForbidden, "PERMISSION_DENIED", "admin role is required")
			a.record(ctx, principal, "HTTP_"+request.Method, "http_path", request.URL.Path, false, map[string]any{
				"protocol": "http", "status_code": http.StatusForbidden,
			})
			return
		}
		recorder := &auditResponseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, request.WithContext(ctx))
		a.record(ctx, principal, "HTTP_"+request.Method, "http_path", request.URL.Path, recorder.status < 400, map[string]any{
			"protocol": "http", "status_code": recorder.status,
		})
	})
}

func (a *Authorizer) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		requestID := validOrNewRequestID(firstMetadata(ctx, "x-request-id"))
		_ = grpc.SetHeader(ctx, metadata.Pairs("x-request-id", requestID))
		ctx = context.WithValue(ctx, requestIDKey, requestID)
		if a.disabled || isPublicGRPC(info.FullMethod) {
			return handler(ctx, request)
		}
		principal, ok := a.authenticate(firstMetadata(ctx, "authorization"))
		if !ok {
			a.record(ctx, Principal{Subject: "anonymous", Role: "none"}, info.FullMethod, "grpc_method", info.FullMethod, false, map[string]any{
				"protocol": "grpc", "status_code": codes.Unauthenticated.String(),
			})
			return nil, status.Error(codes.Unauthenticated, "valid Bearer credentials are required")
		}
		ctx = context.WithValue(ctx, principalKey, principal)
		if requiresAdminGRPC(info.FullMethod) && principal.Role != "admin" {
			a.record(ctx, principal, info.FullMethod, "grpc_method", info.FullMethod, false, map[string]any{
				"protocol": "grpc", "status_code": codes.PermissionDenied.String(),
			})
			return nil, status.Error(codes.PermissionDenied, "admin role is required")
		}
		response, err := handler(ctx, request)
		a.record(ctx, principal, info.FullMethod, "grpc_method", info.FullMethod, err == nil, map[string]any{
			"protocol": "grpc", "status_code": status.Code(err).String(),
		})
		return response, err
	}
}

func FromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalKey).(Principal)
	return principal, ok
}

func RequestID(ctx context.Context) string {
	requestID, _ := ctx.Value(requestIDKey).(string)
	return requestID
}

func (a *Authorizer) record(ctx context.Context, principal Principal, action, resourceType, resourceID string, success bool, details map[string]any) {
	if a.audit == nil {
		return
	}
	outcome := "FAILURE"
	if success {
		outcome = "SUCCESS"
	}
	auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := a.audit.RecordAudit(auditCtx, model.AuditEvent{
		RequestID: RequestID(ctx), Actor: principal.Subject, Role: principal.Role,
		Action: action, ResourceType: resourceType, ResourceID: resourceID,
		Outcome: outcome, Details: details,
	}); err != nil {
		slog.Error("record audit event", "error", err, "request_id", RequestID(ctx))
	}
}

type auditResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *auditResponseWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (a *Authorizer) authenticate(header string) (Principal, bool) {
	scheme, token, ok := strings.Cut(strings.TrimSpace(header), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return Principal{}, false
	}
	candidate := sha256.Sum256([]byte(token))
	matched := -1
	for index := range a.credentials {
		if subtle.ConstantTimeCompare(candidate[:], a.credentials[index].hash[:]) == 1 {
			matched = index
		}
	}
	if matched < 0 {
		return Principal{}, false
	}
	return a.credentials[matched].principal, true
}

func isPublicHTTP(path string) bool {
	return path == "/healthz" || path == "/readyz" || path == "/metrics"
}

func requiresAdminHTTP(request *http.Request) bool {
	return request.Method != http.MethodGet || request.URL.Path == "/v1/audit-events"
}

func isPublicGRPC(method string) bool {
	return strings.HasPrefix(method, "/grpc.health.v1.Health/") || strings.Contains(method, "ServerReflection")
}

func requiresAdminGRPC(method string) bool {
	return !strings.HasSuffix(method, "/GetCluster") && !strings.HasSuffix(method, "/ListClusters")
}

func firstMetadata(ctx context.Context, key string) string {
	values := metadata.ValueFromIncomingContext(ctx, key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func validOrNewRequestID(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= 8 && len(value) <= 128 {
		valid := true
		for _, character := range value {
			if !(character >= 'a' && character <= 'z') && !(character >= 'A' && character <= 'Z') && !(character >= '0' && character <= '9') && !strings.ContainsRune("._-", character) {
				valid = false
				break
			}
		}
		if valid {
			return value
		}
	}
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		panic("generate request ID: " + err.Error())
	}
	return hex.EncodeToString(bytes)
}

func writeHTTPError(w http.ResponseWriter, code int, errorCode, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": errorCode, "message": message}})
}
