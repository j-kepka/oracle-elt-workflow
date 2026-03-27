# oracle-elt-workflow

Small Oracle ELT project for portfolio and learning.

Current MVP (working):
- dated CSV -> external table
- external table -> stage
- invalid rows -> reject
- stage -> core
- one procedure for one business date
- optional scheduler job

Current environment: local `dev/sandbox`.

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

5. Follow the `extdata/work` permissions step from `tests/SMOKE_TEST_RUNBOOK.md`, then run the manual smoke review:
```sql
CONNECT dwh/"<DWH_PASSWORD>"@//localhost:1521/FREEPDB1
ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD';
@/workspace/tests/sql/90_manual_review_pr01_pr02.sql
```

More details:
- `tests/SMOKE_TEST_RUNBOOK.md`

## Main SQL files
- `sql/01_create_stage_client_transfers.sql`
- `sql/02_create_external_client_transfers.sql`
- `sql/04_create_load_procedure.sql`
- `sql/05_create_scheduler_job.sql`
- `sql/08_run_load_procedure.sql`

## Project folders
- `sql/` SQL scripts
- `extdata/` input files
- `tests/` smoke harness and bootstrap scripts
- `docs/adr/` public decisions

## Notes
- Do not commit real passwords/secrets.
- Keep Oracle ports on `127.0.0.1` unless remote access is really needed.
- Keep the Oracle image pinned. Avoid `latest` in committed setup docs.
- Input files and database records in this repo are synthetic test data.
- Current input file pattern is `client_transfers_YYYYMMDD.csv`.
- Internal notes are in `docs/internal/` (not public docs).
