.PHONY: fmt proto test test-integration vet build run compose-up compose-down kind-create kind-delete kind-verify run-k8s verify-local verify-retries verify-backup verify-security benchmark-load benchmark-recovery loadtest

fmt:
	go fmt ./...

proto:
	protoc -I api/proto \
		--go_out=. --go_opt=module=github.com/SaiVyshnavi1522/dataplane-control-plane \
		--go-grpc_out=. --go-grpc_opt=module=github.com/SaiVyshnavi1522/dataplane-control-plane \
		api/proto/dataplane/v1/clusters.proto

test:
	go test ./...

test-integration:
	TEST_DATABASE_URL=postgres://dataplane:dataplane@localhost:55432/dataplane?sslmode=disable go test -race -cover ./...

vet:
	go vet ./...

build:
	go build ./...

run:
	API_KEYS="$${API_KEYS:-local-admin:admin:local-admin-key-change-me}" PROVISIONER=mock go run ./cmd/api

compose-up:
	docker compose up --build -d

compose-down:
	docker compose down

kind-create:
	kind create cluster --name dataplane --config deploy/kind/kind.yaml

kind-delete:
	kind delete cluster --name dataplane

kind-verify:
	bash scripts/verify-kind.sh

run-k8s:
	API_KEYS="$${API_KEYS:-local-admin:admin:local-admin-key-change-me}" PROVISIONER=kubernetes go run ./cmd/api

verify-local:
	bash scripts/verify-local.sh

verify-retries:
	bash scripts/verify-retries.sh

verify-backup:
	bash scripts/verify-backup.sh

verify-security:
	bash scripts/verify-security.sh

benchmark-load:
	bash scripts/benchmark-load.sh

benchmark-recovery:
	bash scripts/benchmark-recovery.sh

loadtest:
	ADMIN_API_KEY="$${ADMIN_API_KEY:-local-admin-key-change-me}" go run ./cmd/loadtest -n 200 -c 20
