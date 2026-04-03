APP_NAME ?= nexus
IMAGE    ?= satheeshds/$(APP_NAME)
TAG      ?= latest
DOCKER   ?= docker
COMPOSE  ?= $(DOCKER) compose
ENV_FILE ?= .env

export DOCKER_BUILDKIT ?= 1

BINARY_GATEWAY := bin/nexus-gateway
BINARY_CONTROL := bin/nexus-control
DOCKER_COMPOSE  := $(COMPOSE) -f deploy/docker-compose.yml --env-file $(ENV_FILE)

.PHONY: help image push compose-up compose-down build build-gateway build-control dev dev-infra down logs migrate migrate-status run-control run-gateway test test-integration tidy lint swagger demo-register demo-health

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Docker Image ──────────────────────────────────────────────────────────────

image: ## Build all Docker images
	$(DOCKER_COMPOSE) build

push: ## Push all Docker images
	@set -e; \
	if $(DOCKER_COMPOSE) config | grep -Eq '^[[:space:]]*image:'; then \
		$(DOCKER_COMPOSE) push; \
	else \
		echo "Error: deploy/docker-compose.yml does not define explicit image names for services."; \
		echo "docker compose push requires services to have image: entries."; \
		echo "Add image: definitions (for example using IMAGE=$(IMAGE) and TAG=$(TAG)) or push images with explicit build/tag/push commands."; \
		exit 1; \
	fi

compose-up: ## Start the full stack with docker compose
	$(DOCKER_COMPOSE) up -d --build

compose-down: ## Stop the stack
	$(DOCKER_COMPOSE) down

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

test-integration: ## Run integration tests
	go test -tags integration -v -timeout 10m ./...

tidy: ## Tidy go modules
	go mod tidy

lint: ## Run golangci-lint
	golangci-lint run ./...

swagger: ## Regenerate Swagger documentation
	swag init -g cmd/control/main.go --output docs

# ── Quick demo ────────────────────────────────────────────────────────────────

demo-register: ## Register a demo tenant (requires control plane running)
	curl -s -X POST http://localhost:8080/api/v1/register \
	  -H "Content-Type: application/json" \
	  -d "{\"org_name\":\"Acme Corp\",\"email\":\"admin@acme.com\",\"password\":\"$${DEMO_PASSWORD:-changeme}\"}" | jq .

demo-health: ## Check control plane health
	curl -s http://localhost:8080/healthz | jq .
