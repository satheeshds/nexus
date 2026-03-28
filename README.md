# 🌊 Nexus Lakehouse

Nexus Lakehouse is a high-performance, cost-efficient, and fully multi-tenant analytical platform. By combining the speed of DuckDB with the scalability of MinIO (S3) and the transactional guarantees of DuckLake, Nexus allows you to serve thousands of isolated analytical environments on a single shared infrastructure.

Think of it as your own self-hosted, "miniature" Snowflake or Databricks, optimized for SaaS multi-tenancy.

## 🏗️ Architecture Overview

Nexus follows the principle of Separation of Storage and Compute:

- **Compute Engine:** BoilStream (Rust-based) acting as a multi-tenant DuckDB gateway.
- **Storage Layer:** MinIO for S3-compatible object storage of Parquet files.
- **Metadata Catalog:** PostgreSQL for tracking DuckLake snapshots and ACID transactions.
- **Control Plane:** A Go-based orchestrator for tenant onboarding, IAM provisioning, and JWT issuance.

## 🚀 Key Features

- **True Multi-Tenancy:** Automated prefix-based isolation in S3 via JWT routing.
- **ACID on S3:** Transactional integrity for analytical writes using the DuckLake format.
- **Time Travel:** Query historical snapshots of your data effortlessly.
- **Zero-Copy Ingestion:** Stream data directly into the lake from S3 or Kafka.
- **Postgres-Compatible:** Connect any standard BI tool (DBeaver, Tableau, etc.) directly to the gateway.

## 🛠️ Prerequisites

Before you begin, ensure you have the following installed:

- Docker & Docker Compose
- Go 1.21+ (for the Control Plane)
- PostgreSQL Client (`psql`)
- MinIO Client (`mc`)

## 🏁 Quick Start

### 1. Spin up the Infrastructure

```bash
docker-compose up -d
```

This starts PostgreSQL (Catalog), MinIO (Storage), and BoilStream (Gateway).

### 2. Configure MinIO

Create the primary bucket for your lakehouse:

```bash
mc alias set myminio http://localhost:9000 minioadmin minioadmin
mc mb myminio/lakehouse
```

### 3. Run the Control Plane

```bash
cd control-plane
go run main.go
```

## 🔐 Authentication & Usage

### Tenant Registration

Tenants are provisioned via the REST API. This creates their folder in MinIO and registers their DuckLake catalog.

```bash
curl -X POST http://localhost:8080/api/v1/register \
  -H "Content-Type: application/json" \
  -d '{"email": "tenant@example.com", "organization_name": "AcmeCorp"}'
```

### Connecting to Data

Connect to the BoilStream gateway using any Postgres-compatible tool:

- **Host:** `localhost`
- **Port:** `5433`
- **Username:** `tenant_id` (from registration)
- **Password:** `<Your_JWT_Token>`

## 📁 Project Structure

```text
.
├── control-plane/          # Go source code for tenant management
│   ├── api/                # OpenAPI handlers
│   ├── internal/           # MinIO & BoilStream logic
│   └── main.go
├── docker-compose.yml      # Infrastructure stack
├── boilstream.yaml         # Gateway configuration
└── README.md
```

## 📜 License

Distributed under the MIT License. See `LICENSE` for more information.

> **Note:** This project is currently in Alpha. It is intended for development and testing environments.
