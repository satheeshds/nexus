# 🌊 Nexus Lakehouse

**Nexus Lakehouse** is a high-performance, cost-efficient, and fully multi-tenant analytical platform. By combining the speed of **DuckDB** with the scalability of **MinIO (S3)** and the transactional guarantees of **DuckLake**, Nexus allows you to serve thousands of isolated analytical environments on a single shared infrastructure.

Think of it as your own self-hosted, "miniature" Snowflake or Databricks, optimized for SaaS multi-tenancy.

---

## 🏗️ Architecture Overview

Nexus follows the principle of **Separation of Storage and Compute**:

- **Compute Gateway (`cmd/gateway`):** A custom Go-based server speaking the Postgres wire protocol. It routes queries to isolated, ephemeral DuckDB sessions.
- **Control Plane (`cmd/control`):** A Go-based orchestrator responsible for tenant lifecycle management, IAM provisioning on MinIO, and JWT issuance.
- **Storage Layer:** MinIO for S3-compatible object storage of Parquet/Iceberg files.
- **Metadata Catalog:** PostgreSQL backing the **DuckLake** ACID-compliant catalog, providing transactional integrity for analytical writes.

---

## 🚀 Key Features

- **True Multi-Tenancy:** Automated S3 prefix-based isolation enforced via JWT-scoped sessions.
- **ACID on S3:** Full transactional integrity for analytical workloads using the DuckLake format.
- **Compute Isolation:** Per-tenant DuckDB sessions with resource boundaries.
- **Zero-Copy Ingestion:** Analytical queries directly against S3 data without moving it.
- **Postgres Compatible:** Use any standard BI tool (DBeaver, Tableau, etc.) directly on port `5433`.

---

## 🛠️ Prerequisites

- **Docker & Docker Compose**
- **Go 1.24+** (for local development)
- **Make** (optional, for shortcuts)

---

## 🏁 Quick Start

### 1. Initialize Environment
Copy the example environment file:
```bash
cp .env.example .env
```

### 2. Boot the Entire Stack
The easiest way to start is using the provided `Makefile`:
```bash
make dev
```
This command starts PostgreSQL, MinIO, and builds/starts both the **Nexus Gateway** and **Control Plane** in Docker.

### 3. Provision a Tenant
In a new terminal, register your first tenant:
```bash
make demo-register
```
This will output a `tenant_id` and a `token` (JWT).

Admin credentials are managed as a static API key configured by the platform operator:

Generate a secure key: openssl rand -hex 32
Set ADMIN_API_KEY=<generated-key> in .env
The control plane reads it via the NEXUS_AUTH_ADMIN_API_KEY environment variable
Rotate by updating .env and restarting the control plane
Platform operators use this key in the X-Admin-API-Key header to access admin endpoints (e.g., GET /api/v1/admin/tenants/{id}/service-account). The key is never stored in the database — it lives only in environment config.

---

## 🔐 Connection Guide

Connect to the **Nexus Gateway** using any Postgres-compatible client.

#### As a Customer Tenant (JWT)
- **Host:** `localhost`
- **Port:** `5433`
- **Database:** `lake`
- **Username:** `<tenant_id>`
- **Password:** `<JWT_token>`

#### As a Service Account (API Key)
- **Host:** `localhost`
- **Port:** `5433`
- **Database:** `lake`
- **Username:** `<service_id>` (e.g., `acme_corp_xxxx_svc`)
- **Password:** `<service_api_key>` (32-byte hex string)

> [!WARNING]
> The gateway currently denies SSL/TLS (`SSLRequest` -> `N`), so credentials — including long‑lived service-account API keys — are sent in cleartext over the network. Only use API-key authentication over `localhost` during development, or over a secure channel such as a VPN/SSH tunnel or a TLS‑terminating proxy/load balancer. Do **not** expose the gateway with API-key auth directly to untrusted networks until you have end‑to‑end TLS in place.
>
> [!TIP]
> You can retrieve your service ID and rotate your API key via the Control Plane's admin endpoints. Key rotation now reuses the current key within a short TTL window; pass `{"hard_reset":true}` to the rotate endpoint to force immediate rotation if credentials are compromised.

---

## 📁 Project Structure

```text
.
├── cmd/
│   ├── gateway/          # pgwire entrypoint
│   └── control/          # Management API entrypoint
├── internal/
│   ├── gateway/          # Postgres protocol handler
│   ├── control/          # HTTP API handlers
│   ├── tenant/           # Provisioning orchestration
│   ├── pool/             # DuckDB session management
│   ├── duckdb/           # DuckDB + DuckLake integration
│   ├── catalog/          # Postgres metadata adapter
│   ├── auth/             # JWT & IAM logic
│   ├── config/           # Centralized configuration
│   └── storage/          # MinIO/S3 interface
├── migrations/           # PostgreSQL schema migrations
├── deploy/               # Dockerfiles & Compose config
├── Makefile              # Development shortcuts
└── .env.example          # Template for environment variables
```

---

## 🧪 Testing

Run the unit test suite with:

```bash
go test ./...
```

To see per-package coverage:

```bash
go test -cover ./...
```

### Test Coverage

The following packages have unit tests that can be run without any external services (no Postgres, MinIO, or DuckDB required):

| Package | Coverage | Notes |
|---|---|---|
| `internal/auth` | ~82% | JWT issue & validate, expiry, tamper, wrong-secret scenarios |
| `internal/config` | ~98% | DSN/URL formatting, default values, env-var overrides |
| `internal/control` | ~82% | All HTTP handlers, admin query endpoint, admin/JWT middleware, mock catalog & provisioner |
| `internal/gateway` | ~15% | Sequential-ID helpers: column list parsing, table name splitting, value row splitting, SQL string escaping |
| `internal/tenant` | ~16% | Pure helpers: slug generation, API key generation, password validation |

> **Note:** `internal/catalog`, `internal/storage`, `internal/duckdb`, and `internal/pool`
> require live infrastructure (Postgres, MinIO, DuckDB) and are covered by
> integration testing only. The packages above are fully unit-testable with no external dependencies.

---

## License
Distributed under the MIT License.

> [!WARNING]
> This project is currently in **Alpha**. It is intended for development and testing. Do not use in production without further security hardening.
