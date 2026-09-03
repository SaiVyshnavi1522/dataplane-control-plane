.PHONY: fmt test test-integration vet build run compose-up compose-down kind-create kind-delete run-k8s verify-local verify-retries loadtest

fmt:
	go fmt ./...

test:
	go test ./...

test-integration:
	TEST_DATABASE_URL=postgres://dataplane:dataplane@localhost:55432/dataplane?sslmode=disable go test -race -cover ./...

vet:
	go vet ./...

build:
	go build ./...

run:
	PROVISIONER=mock go run ./cmd/api

compose-up:
	docker compose up --build -d

compose-down:
	docker compose down

kind-create:
	kind create cluster --name dataplane --config deploy/kind/kind.yaml

kind-delete:
	kind delete cluster --name dataplane

run-k8s:
	PROVISIONER=kubernetes go run ./cmd/api

verify-local:
	bash scripts/verify-local.sh

verify-retries:
	bash scripts/verify-retries.sh

loadtest:
	go run ./cmd/loadtest -n 200 -c 20
