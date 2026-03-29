# oracle-elt-workflow

Small Oracle ELT project for portfolio and learning.

Current MVP (working):
- dated CSV + `.ok` ready file -> external table
- external table -> stage
- invalid rows -> reject
- stage -> core
- one procedure for one business date
- `MANUAL` and `AUTO` run modes
- optional scheduler job

Current environment: local `dev/sandbox`.

## Security And Production Disclaimer

This repository is a local MVP for portfolio and learning.
It is not a production deployment baseline.
Any production-like reuse should start with an independent security, infrastructure, operations, and data-governance review.
Current grants, filesystem permissions, and scheduler setup are intentionally simplified for local smoke tests.

## Quick start

1. Create Docker volume:
```bash
docker volume create oracle-data
```

2. Run Oracle Free:
```bash
ORACLE_IMAGE='gvenzl/oracle-free@sha256:62aad247879f5d4ca4a37ecc068ef6a5feb9e9bea789501b6a82d4814d14bbb3'

docker rm -f oracle-free 2>/dev/null || true

docker run -d --name oracle-free \
  -p 127.0.0.1:1521:1521 \
  -p 127.0.0.1:5500:5500 \
  -e ORACLE_PASSWORD='<ORACLE_PASSWORD>' \
  -v oracle-data:/opt/oracle/oradata \
  -v "$(pwd)/extdata:/opt/oracle/extdata" \
  -v "$(pwd):/workspace" \
  "$ORACLE_IMAGE"
```

3. Check logs:
```bash
docker logs -f oracle-free
```

4. Bootstrap the project schema:
```bash
docker exec -it oracle-free bash
sqlplus / as sysdba
```

In `SQL>`:
```sql
ALTER SESSION SET CONTAINER = FREEPDB1;
@/workspace/tests/sql/00_reset_database_to_initial_state.sql

DEFINE DWH_PASSWORD = <DWH_PASSWORD>
@/workspace/tests/sql/10_bootstrap_project_schema.sql
```

5. Follow the permissions step from `tests/SMOKE_TEST_RUNBOOK.md`, then run the manual smoke review:
```sql
CONNECT dwh/"<DWH_PASSWORD>"@//localhost:1521/FREEPDB1
ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD';
@/workspace/tests/sql/90_manual_review_pr03.sql
```

More details:
- `tests/SMOKE_TEST_RUNBOOK.md`

## Main SQL files
- `sql/01_create_stage_client_transfers.sql`
- `sql/02_create_external_client_transfers.sql`
- `sql/04_create_load_procedure.sql`
- `sql/05_create_scheduler_job.sql`
- `sql/06_create_core_client_transfers.sql`
- `sql/10_create_control_structures.sql`

## Project folders
- `sql/` SQL scripts
- `ops/` operational helpers for manual runs and validation
- `extdata/` input files
- `tests/` smoke harness and bootstrap scripts
- `docs/adr/` public decisions

## Notes
- Do not commit real passwords/secrets.
- The `extdata/*` permission setup shown in this repo is a sandbox/dev compatibility baseline for local Docker bind mounts. It is not a production security model; any non-sandbox use requires a separate security and infrastructure review.
- Any production-like reuse of this code should start with an independent security audit and environment-specific hardening review.
- Keep Oracle ports on `127.0.0.1` unless remote access is really needed.
- Keep the Oracle image pinned. Avoid `latest` in committed setup docs.
- Input files and database records in this repo are synthetic test data.
- Current input file patterns are `client_transfers_YYYYMMDD.csv` and `client_transfers_YYYYMMDD.ok`.
- The procedure supports `AUTO` status handling (`WAITING` before the cutoff, `FAILED` after the cutoff), but the bundled scheduler does not yet re-drive `WAITING` rows from `next_retry_ts`.
