# Mini Snowflake Architecture: DuckDB + DuckLake + MinIO

## Goal
A self-hosted, multi-tenant analytical SaaS platform where every tenant gets isolated:
- **Storage** (Parquet files in MinIO under their own S3 prefix)
- **Catalog** (DuckLake snapshot metadata in Postgres)
- **Query session** (scoped DuckDB instance with no cross-tenant access)

---

## Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│                        CLIENTS                              │
│  DBeaver / Tableau / psql / BI Tools / REST API             │
└──────────────────────┬──────────────────────────────────────┘
                       │ Postgres Wire Protocol (port 5433)
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              QUERY GATEWAY  (Go)                            │
│  • pgproto3: speaks postgres wire protocol                  │
│  • JWT validation → tenant_id extraction                    │
│  • Spawns / reuses per-tenant DuckDB session                │
│  • Query routing & result streaming                         │
└──────────┬──────────────────────┬───────────────────────────┘
           │                      │
           ▼                      ▼
┌──────────────────┐   ┌──────────────────────────────────────┐
│  CONTROL PLANE   │   │        COMPUTE POOL                  │
│  (Go REST API)   │   │   Per-tenant DuckDB instances        │
│                  │   │   (ephemeral, spawned on demand)     │
│  • /register     │   │                                      │
│  • /login        │   │   ducklake extension attached        │
│  • /token        │   │   → reads from tenant S3 prefix      │
│  • tenant CRUD   │   │   → writes ACID snapshots to PG      │
│  • MinIO IAM     │   └──────────────┬───────────────────────┘
│  • PG schema init│                  │
└──────────────────┘                  │
                                      │
              ┌───────────────────────┼───────────────────────┐
              ▼                       ▼                       ▼
┌─────────────────────┐  ┌──────────────────┐  ┌────────────────────┐
│  POSTGRES           │  │  MINIO (S3)      │  │  METADATA CACHE    │
│  (Catalog / ACID)   │  │  (Object Store)  │  │  (Optional Redis)  │
│                     │  │                  │  │                    │
│  Per-tenant schema: │  │  Bucket layout:  │  │  • Schema cache    │
│  ducklake_acme      │  │  lakehouse/      │  │  • Token sessions  │
│  ducklake_globex    │  │  ├── acme/       │  │  • Query results   │
│  ducklake_initech   │  │  │   └── *.parq  │  └────────────────────┘
│                     │  │  ├── globex/     │
│  Stores:            │  │  └── initech/    │
│  • snapshots        │  │                  │
│  • schema versions  │  │  Parquet files   │
│  • transactions     │  │  (ZSTD compressed│
└─────────────────────┘  └──────────────────┘
```

---

## Component Deep-Dive

### 1. Storage Layer — MinIO

Each tenant gets a **prefix-isolated namespace** in MinIO:

```
s3://lakehouse/
├── acme/
│   ├── orders/
│   │   ├── year=2024/month=01/part-0001.parquet
│   │   └── year=2024/month=02/part-0001.parquet
│   └── customers/
│       └── part-0001.parquet
├── globex/
│   └── sales/
│       └── part-0001.parquet
```

**Access control**: MinIO policies restrict each service account to its prefix only:
```json
{
  "Effect": "Allow",
  "Action": ["s3:GetObject", "s3:PutObject"],
  "Resource": "arn:aws:s3:::lakehouse/acme/*"
}
```

---

### 2. Catalog Layer — DuckLake on PostgreSQL

[DuckLake](https://ducklake.select) is an Apache 2.0 open-source table format (by DuckDB Labs) that stores metadata in SQL (Postgres).

Each tenant gets their own PG schema:

```sql
-- Created at tenant registration time
CREATE SCHEMA ducklake_acme;
-- DuckLake extension manages: snapshots, schema evolution, time travel
```

Attaching from DuckDB:
```sql
ATTACH 'ducklake:postgres:host=pg user=nexus password=xxx dbname=catalog'
  AS acme_lake (
    TYPE DUCKLAKE,
    SCHEMA 'ducklake_acme',
    DATA_PATH 's3://lakehouse/acme/'
  );
```

This gives you **ACID, time travel, schema evolution** for free.

---

### 3. Compute Layer — Per-Tenant DuckDB Sessions

The gateway maintains a **session pool** per tenant:

```go
type SessionPool struct {
    mu       sync.Mutex
    sessions map[string]*TenantSession  // key: tenant_id
}

type TenantSession struct {
    db        *duckdb.Conn
    tenantID  string
    createdAt time.Time
    lastUsed  time.Time
    // DuckLake already attached for this tenant
}
```

Session lifecycle:
- **On first query**: spawn DuckDB, attach DuckLake, attach S3 secret → session cached
- **On subsequent queries**: reuse session (fast path)
- **TTL**: sessions evicted after N minutes of inactivity
- **Max sessions**: configurable cap per tenant (compute isolation)

---

### 4. Query Gateway — Postgres Wire Protocol in Go

The gateway is the critical piece that replaces BoilStream:

```
Client connects on :5433
    │
    ├─ Startup message → read username (= tenant_id)
    ├─ Auth → validate password (= JWT token)
    │         extract tenant_id from JWT claims  
    ├─ Session → get/create DuckDB session for tenant
    │
    ├─ Query message → parse SQL
    │                  execute on tenant DuckDB
    │                  stream results as DataRow messages
    └─ Terminate → connection closed, session returned to pool
```

**Key Go libraries**:
```go
import (
    "github.com/jackc/pgproto3/v2"    // Postgres wire protocol
    "github.com/marcboeker/go-duckdb" // DuckDB CGO bindings
    "github.com/golang-jwt/jwt/v5"    // JWT validation
)
```

---

### 5. Control Plane — Tenant Lifecycle API

```
POST /api/v1/register
  → Creates PG schema: ducklake_{tenant_id}
  → Creates MinIO service account with prefix policy
  → Initializes DuckLake catalog in PG schema
  → Returns: { tenant_id, access_token }

POST /api/v1/login
  → Validates credentials
  → Issues JWT: { sub: tenant_id, exp: ..., s3_prefix: "acme/" }
  → Returns: { token }  ← used as Postgres password

POST /api/v1/ingest
  → Accepts Parquet or CSV
  → Writes to tenant's S3 prefix
  → Commits DuckLake transaction

DELETE /api/v1/tenants/{id}
  → Drops PG schema
  → Revokes MinIO policy
  → (optionally) purges S3 prefix
```

---

### 6. Multi-Tenancy Isolation Matrix

| Dimension | Mechanism | Enforced By |
|---|---|---|
| **Data isolation** | S3 prefix per tenant | MinIO IAM policy |
| **Catalog isolation** | PG schema per tenant | DuckLake schema param |
| **Session isolation** | Separate DuckDB instance | Gateway session pool |
| **Compute isolation** | Max concurrent queries | Gateway rate limiter |
| **Auth isolation** | JWT scoped to tenant_id | Gateway JWT middleware |
| **Schema isolation** | DuckLake schema versioning | DuckLake extension |

---

## Deployment (Docker Compose)

```yaml
services:
  postgres:          # DuckLake catalog (ACID metadata)
  minio:             # Object storage (Parquet files)
  minio-init:        # Bucket bootstrap
  nexus-gateway:     # Go: pgwire + DuckDB session pool  ← replaces boilstream
  nexus-control:     # Go: REST API for tenant management
  redis:             # Optional: session/token cache
```

---

## Data Flow: Write Path

```
Client → POST /ingest (JSON/CSV/Parquet)
  → Control Plane validates JWT → tenant_id = "acme"
  → Converts to Parquet (using DuckDB in-process)
  → Writes to s3://lakehouse/acme/table_name/part-XXXX.parquet
  → Opens DuckLake transaction:
      BEGIN;
        INSERT INTO acme_lake.table_name SELECT * FROM parquet_file;
      COMMIT;  -- creates new snapshot in ducklake_acme PG schema
```

---

## Data Flow: Query Path

```
Client (DBeaver) → connects to :5433
  username: acme_tenant_id
  password: <JWT>

Gateway:
  1. pgproto3 handshake
  2. Validate JWT → tenant = "acme"
  3. Get DuckDB session for "acme"
     → ATTACH ducklake IF not already attached
  4. Execute: SELECT * FROM acme_lake.orders LIMIT 100
     → DuckDB reads Parquet from MinIO via httpfs
     → DuckLake resolves latest snapshot
  5. Stream rows back as Postgres DataRow messages
```

---

## Time Travel

Because DuckLake stores snapshot history in Postgres:

```sql
-- Query data as it was 7 days ago
SELECT * FROM acme_lake.orders
AT (TIMESTAMP => NOW() - INTERVAL '7 days');

-- List snapshots
SELECT * FROM ducklake_snapshots('acme_lake');
```

---

## What You DON'T Need to Build

| Feature | Already Handled By |
|---|---|
| Parquet ACID writes | DuckLake extension |
| Schema evolution | DuckLake extension |
| Time travel | DuckLake extension |
| S3 reads/writes | DuckDB `httpfs` extension |
| Parquet compression | DuckDB (ZSTD by default) |
| High-perf analytics | DuckDB vectorized engine |

---

## What You DO Need to Build

| Component | Complexity | Notes |
|---|---|---|
| pgproto3 gateway | 🔴 High | Core piece, ~500-800 LOC |
| Session pool | 🟡 Medium | ~200 LOC |
| JWT middleware | 🟢 Low | Already in control plane |
| Tenant registration | 🟢 Low | Already in control plane |
| MinIO IAM provisioning | 🟡 Medium | MinIO Go SDK |
| DuckLake schema init | 🟢 Low | One ATTACH + COMMIT |

---

## Comparison: This vs BoilStream

| | BoilStream | This Architecture |
|---|---|---|
| Open source | ❌ Closed | ✅ Fully open |
| Binary available | ✅ | ✅ (you build it) |
| Multi-tenancy | ✅ Built-in | ✅ Built-in |
| DuckLake support | ✅ | ✅ |
| Postgres wire | ✅ | ✅ via pgproto3 |
| ARM/x86 issues | ❌ Platform-locked | ✅ Go cross-compiles |
| Customizable | ❌ | ✅ 100% |
| Operational weight | Low | Low |
