.PHONY: help build build-gateway build-control dev down migrate test tidy

BINARY_GATEWAY := bin/nexus-gateway
BINARY_CONTROL := bin/nexus-control
DOCKER_COMPOSE  := docker compose -f deploy/docker-compose.yml --env-file .env

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Build ─────────────────────────────────────────────────────────────────────

build: build-gateway build-control ## Build both binaries

build-gateway: ## Build the pgwire gateway
	CGO_ENABLED=1 go build -ldflags="-s -w" -o $(BINARY_GATEWAY) ./cmd/gateway

build-control: ## Build the control plane
	CGO_ENABLED=1 go build -ldflags="-s -w" -o $(BINARY_CONTROL) ./cmd/control

# ── Dev Stack ─────────────────────────────────────────────────────────────────

dev: ## Start the full local stack (postgres + minio + nexus)
	$(DOCKER_COMPOSE) up --build

dev-infra: ## Start only postgres and minio (run services locally)
	$(DOCKER_COMPOSE) up postgres minio minio-init

down: ## Stop and remove all containers
	$(DOCKER_COMPOSE) down -v

logs: ## Follow logs for all services
	$(DOCKER_COMPOSE) logs -f

# ── Database ──────────────────────────────────────────────────────────────────

migrate: ## Run pending migrations
	goose -dir migrations postgres "$(shell grep '^POSTGRES_DSN=' .env | cut -d= -f2-)" up

migrate-status: ## Show migration status
	goose -dir migrations postgres "$(shell grep '^POSTGRES_DSN=' .env | cut -d= -f2-)" status

# ── Development helpers ───────────────────────────────────────────────────────

run-control: ## Run control plane locally (requires dev-infra running)
	NEXUS_POSTGRES_HOST=localhost \
	NEXUS_POSTGRES_USER=nexus \
	NEXUS_POSTGRES_PASSWORD=changeme \
	NEXUS_MINIO_ENDPOINT=localhost:9000 \
	NEXUS_MINIO_ACCESS_KEY=minioadmin \
	NEXUS_MINIO_SECRET_KEY=changeme \
	NEXUS_AUTH_JWT_SECRET=supersecretkey_change_in_production \
	go run ./cmd/control

run-gateway: ## Run gateway locally (requires dev-infra + control running)
	NEXUS_POSTGRES_HOST=localhost \
	NEXUS_POSTGRES_USER=nexus \
	NEXUS_POSTGRES_PASSWORD=changeme \
	NEXUS_MINIO_ENDPOINT=localhost:9000 \
	NEXUS_MINIO_ACCESS_KEY=minioadmin \
	NEXUS_MINIO_SECRET_KEY=changeme \
	NEXUS_AUTH_JWT_SECRET=supersecretkey_change_in_production \
	go run ./cmd/gateway

# ── Quality ───────────────────────────────────────────────────────────────────

test: ## Run all tests
	go test ./...

tidy: ## Tidy go modules
	go mod tidy

lint: ## Run golangci-lint
	golangci-lint run ./...

# ── Quick demo ────────────────────────────────────────────────────────────────

demo-register: ## Register a demo tenant (requires control plane running)
	curl -s -X POST http://localhost:8080/api/v1/register \
	  -H "Content-Type: application/json" \
	  -d "{\"org_name\":\"Acme Corp\",\"email\":\"admin@acme.com\"}" | jq .

demo-health: ## Check control plane health
	curl -s http://localhost:8080/healthz | jq .
