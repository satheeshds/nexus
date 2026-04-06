# Plain DuckDB Backend

Nexus supports two DuckDB-based storage backends for tenant sessions. The
backend is selected globally via the `storage.backend` configuration key
(or `NEXUS_STORAGE_BACKEND` environment variable).

---

## Backends at a Glance

| Feature | `ducklake` (default) | `duckdb` (plain) |
|---|---|---|
| Parquet storage on MinIO | ✅ | ✅ |
| ACID transactions | ✅ (DuckLake snapshots) | ❌ |
| Schema evolution | ✅ | ❌ (manual) |
| Time travel / snapshots | ✅ | ❌ |
| Postgres metadata store | ✅ Required | ❌ Not needed |
| DuckLake extension required | ✅ | ❌ |
| Postgres extension required | ✅ | ❌ |
| DuckDB extension registry access | Required at startup | Required (httpfs only) |
| Tenant provisioning complexity | Higher | Lower |
| Feature compatibility | Limited by DuckLake | Full DuckDB feature set |

---

## When to Use `ducklake`

Choose the `ducklake` backend when you need:

- **ACID semantics** – Multi-statement transactions that commit or roll back
  atomically.
- **Time travel** – Query historical snapshots:
  ```sql
  SELECT * FROM lake.orders AT (TIMESTAMP => NOW() - INTERVAL '7 days');
  ```
- **Schema evolution** – Add/rename/drop columns with tracked history.
- **Managed Parquet layout** – DuckLake automatically organises data files under
  the tenant's S3 prefix; you do not need to manage file paths manually.

---

## When to Use `duckdb` (plain)

Choose the `duckdb` backend when you need:

- **Simpler infrastructure** – No Postgres metadata schema is required per
  tenant.  Postgres is still used for the Nexus control-plane catalog
  (tenants, service accounts), but not for DuckDB session state.
- **Broader DuckDB feature support** – Some DuckDB features or extensions are
  not compatible with the DuckLake attachment model. Plain DuckDB sessions give
  you the full DuckDB feature set.
- **Direct Parquet access** – You prefer to manage data files yourself using
  standard SQL:
  ```sql
  -- Write data to MinIO as Parquet
  COPY my_table TO 's3://lakehouse/tenants/acme/orders/data.parquet'
       (FORMAT parquet, COMPRESSION zstd);

  -- Read data back
  SELECT * FROM read_parquet('s3://lakehouse/tenants/acme/orders/*.parquet');
  ```
- **Lightweight evaluation** – You want to explore Nexus without setting up
  the DuckLake extension registry or Postgres schemas.

---

## Configuration

### `config/config.yaml`

```yaml
storage:
  backend: "duckdb"   # "ducklake" (default) or "duckdb"
```

### Environment variable

```bash
NEXUS_STORAGE_BACKEND=duckdb
```

---

## Session Behaviour (Plain DuckDB)

Each tenant session is an **in-memory DuckDB instance** with MinIO credentials
pre-loaded via the `httpfs` extension. Key points:

1. **In-memory tables are ephemeral** – A `CREATE TABLE t AS SELECT …` creates
   a table only in the current session's memory. It is lost when the session is
   evicted (see `pool.session_ttl`). Always persist data explicitly:

   ```sql
   COPY t TO 's3://lakehouse/<prefix>/t/part-001.parquet' (FORMAT parquet);
   ```

2. **Writes are atomic at the file level** – DuckDB writes to a local temporary
   file and uploads to S3 as a single atomic PUT. A session crash after the
   COPY completes will not corrupt the uploaded file.

3. **No catalog coordination** – Multiple concurrent sessions for the same
   tenant write to the same S3 prefix independently. You are responsible for
   avoiding conflicting file names and for managing file compaction.

4. **Session eviction** – The pool evicts sessions that have been idle longer
   than `pool.session_ttl` (default 30 minutes). Any in-memory state not yet
   COPY-ed to S3 is discarded on eviction.

---

## Tenant Provisioning Differences

When `storage.backend = "duckdb"`:

- The Postgres schema (`ducklake_<tenant_id>`) is **not created** during
  registration, since it is only needed to store DuckLake catalog metadata.
- The `initDuckLake` step is **skipped**.
- All other provisioning steps are identical: MinIO service account creation,
  catalog records (`tenants`, `service_accounts`), and IAM policy scoping.

When a tenant registered under the plain DuckDB backend is deleted, the
Postgres schema drop step is also skipped (there is no schema to drop).

---

## Example Workflow

```sql
-- Connect via psql / DBeaver to port 5433
-- username: <tenant_id>
-- password: <JWT or service account API key>

-- Create a table in memory and populate it
CREATE TABLE sales (product TEXT, amount DECIMAL, ts TIMESTAMP);
INSERT INTO sales VALUES ('widget', 9.99, NOW()), ('gadget', 49.99, NOW());

-- Persist to MinIO as Parquet (ZSTD compressed)
-- Replace <bucket>, <base_path>, and <tenant_id> with your configured values.
COPY sales TO 's3://<bucket>/<base_path>/<tenant_id>/sales/part-001.parquet'
     (FORMAT parquet, COMPRESSION zstd);

-- Read it back (works across sessions)
SELECT product, SUM(amount)
FROM read_parquet('s3://<bucket>/<base_path>/<tenant_id>/sales/*.parquet')
GROUP BY product;
```
