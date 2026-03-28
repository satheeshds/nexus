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

---

## 🔐 Connection Guide

Connect to the **Nexus Gateway** using any Postgres-compatible client:

- **Host:** `localhost`
- **Port:** `5433`
- **Database:** `lake`
- **Username:** `<tenant_id>`
- **Password:** `<JWT_token>`

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

## 📜 License

Distributed under the MIT License.

> [!WARNING]
> This project is currently in **Alpha**. It is intended for development and testing. Do not use in production without further security hardening.
