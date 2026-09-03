package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/model"
)

type auditSinkStub struct{ events []model.AuditEvent }

func (s *auditSinkStub) RecordAudit(_ context.Context, event model.AuditEvent) error {
	s.events = append(s.events, event)
	return nil
}

func TestHTTPAuthenticationAuthorizationRequestIDAndAudit(t *testing.T) {
	authorizer, err := New("operations-admin:admin:admin-key-123456,read-operator:viewer:viewer-key-12345")
	if err != nil {
		t.Fatal(err)
	}
	sink := &auditSinkStub{}
	authorizer.SetAuditSink(sink)
	handler := authorizer.HTTP(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		principal, ok := FromContext(request.Context())
		if !ok || principal.Subject != "read-operator" || RequestID(request.Context()) != "request-12345678" {
			t.Errorf("context principal=%+v ok=%v requestID=%q", principal, ok, RequestID(request.Context()))
		}
		w.WriteHeader(http.StatusOK)
	}))

	request := httptest.NewRequest(http.MethodGet, "/v1/clusters", nil)
	request.Header.Set("Authorization", "Bearer viewer-key-12345")
	request.Header.Set(RequestIDHeader, "request-12345678")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get(RequestIDHeader) != "request-12345678" {
		t.Fatalf("status=%d requestID=%q", response.Code, response.Header().Get(RequestIDHeader))
	}
	if len(sink.events) != 1 || sink.events[0].Actor != "read-operator" || sink.events[0].Outcome != "SUCCESS" {
		t.Fatalf("audit events=%+v", sink.events)
	}
}

func TestHTTPViewerCannotMutateResources(t *testing.T) {
	authorizer, _ := New("operations-admin:admin:admin-key-123456,read-operator:viewer:viewer-key-12345")
	sink := &auditSinkStub{}
	authorizer.SetAuditSink(sink)
	handler := authorizer.HTTP(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("protected handler was called")
	}))
	request := httptest.NewRequest(http.MethodPost, "/v1/clusters", nil)
	request.Header.Set("Authorization", "Bearer viewer-key-12345")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("status=%d, want 403", response.Code)
	}
	if len(sink.events) != 1 || sink.events[0].Actor != "read-operator" || sink.events[0].Outcome != "FAILURE" {
		t.Fatalf("audit events=%+v", sink.events)
	}
}

func TestHTTPRejectsMissingCredentials(t *testing.T) {
	authorizer, _ := New("operations-admin:admin:admin-key-123456")
	sink := &auditSinkStub{}
	authorizer.SetAuditSink(sink)
	response := httptest.NewRecorder()
	authorizer.HTTP(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("protected handler was called")
	})).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/clusters", nil))
	if response.Code != http.StatusUnauthorized || response.Header().Get(RequestIDHeader) == "" {
		t.Fatalf("status=%d requestID=%q", response.Code, response.Header().Get(RequestIDHeader))
	}
	if len(sink.events) != 1 || sink.events[0].Actor != "anonymous" || sink.events[0].Outcome != "FAILURE" {
		t.Fatalf("audit events=%+v", sink.events)
	}
}
