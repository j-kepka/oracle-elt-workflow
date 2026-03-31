# oracle-elt-workflow

Oracle ELT workflow demo for daily client transfer and client snapshot ingestion with validation, reject handling, control-table status tracking, and scheduler-ready batch execution.

Current public state reflects `Phase-04`: two related snapshot datasets, a same-day client-to-transfer join, repeatable smoke coverage, and a local Oracle sandbox setup that is meant to be easy to review rather than production-like.

## Data Flow In One Glance

```text
clients_YYYYMMDD.csv + .ok ---------------------> ext_clients
                                                   |
                                                   v
                                            validation rules
                                                   |
                                    +--------------+--------------+
                                    |                             |
                                    v                             v
                              stg_clients                 stg_clients_reject
                                    |
                                    v
                               core_clients
                                    ^
                                    |
client_transfers_YYYYMMDD.csv + .ok -------> ext_client_transfers
                                                   |
                                                   v
                                            validation rules
                                                   |
                                    +--------------+--------------+
                                    |                             |
                                    v                             v
                         stg_client_transfers        stg_client_transfers_reject
                                    |
                                    v
                         core_client_transfers

ctl_process_run stores the current status per process_name and business_date.
Optional scheduler support currently exists for client_transfers.
```

## Current Phase-04 Scope

- dated CSV + `.ok` ready file -> external table
- parallel dataset loads for `client_transfers` and `clients`
- external table -> stage
- invalid rows -> reject
- stage -> core
- `client_status` snapshot flag on clients (`ACTIVE` / `ARCHIVED`)
- same-day FK from `core_client_transfers` to `core_clients` on `(business_date, client_id)`
- one procedure per dataset and one `business_date`
- `MANUAL` and `AUTO` run modes
- optional scheduler job for `client_transfers`

Current environment: local `dev/sandbox`.

## Expected Smoke Result

After bootstrap and manual smoke review:
- `client_transfers` `2026-03-26` -> `DONE`, `stage = 20`, `reject = 0`, `core = 20`
- `client_transfers` `2026-03-25` -> `WARNING`, `stage = 5`, `reject = 7`, `core = 5`
- `clients` `2026-03-26` -> `DONE`, `stage = 12`, `reject = 0`, `core = 12`
- `clients` `2026-03-25` -> `WARNING`, `stage = 5`, `reject = 4`, `core = 5`
- missing `.ok`, count mismatches, or a missing same-day client snapshot lead to `WAITING` or `FAILED`, depending on `MANUAL` / `AUTO` mode and the cutoff window

Full scenario coverage remains in `tests/SMOKE_TEST_RUNBOOK.md`.

## Demo Scope Boundary

Phase labels in this repository describe internal delivery stages rather than GitHub pull request numbers.
The public sequence is kept contiguous; if an earlier placeholder idea is later merged into a neighboring stage or deferred, the published phase map is normalized instead of keeping gaps.
For portfolio/demo purposes, the practical MVP scope ends before `Phase-09`.
`Phase-01` -> `Phase-08` define the demo-facing scope, while optional post-MVP work is currently grouped into `Phase-09`, `Phase-10`, `Phase-11`, and `Phase-12` rather than required delivery scope.

## Security And Production Disclaimer

This repository is a local MVP for portfolio and learning.
It is not a production deployment baseline.
Any production-like reuse should start with an independent security, infrastructure, operations, and data-governance review.
Current grants, filesystem permissions, and scheduler setup are intentionally simplified for local smoke tests.

## Quick start

1. Start Oracle Free with the local startup helper:
```bash
cd /home/kempez/projects/oracle-elt-workflow
chmod +x ops/00_start_oracle_container.sh
ORACLE_PASSWORD='<ORACLE_PASSWORD>' ./ops/00_start_oracle_container.sh
```

Current startup defaults:
- container name: `oracle-free`
- volume: `oracle-data`
- timezone: `Europe/Berlin`

Optional overrides:
```bash
ORACLE_PASSWORD='<ORACLE_PASSWORD>' \
ORACLE_TZ='Europe/Berlin' \
ORACLE_CONTAINER_NAME='oracle-free' \
ORACLE_VOLUME_NAME='oracle-data' \
./ops/00_start_oracle_container.sh
```

2. Check logs:
```bash
docker logs -f oracle-free
```

3. Bootstrap the project schema:
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

4. Follow the permissions step from `tests/SMOKE_TEST_RUNBOOK.md`, then run the manual smoke reviews:
```sql
CONNECT dwh/"<DWH_PASSWORD>"@//localhost:1521/FREEPDB1
ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD';
@/workspace/tests/sql/90_manual_review_phase03.sql
@/workspace/tests/sql/91_manual_review_clients.sql
```

More details:
- `tests/SMOKE_TEST_RUNBOOK.md`
- `docs/ROADMAP.md`

## Main SQL files
- `sql/01_create_stage_client_transfers.sql`
- `sql/02_create_external_client_transfers.sql`
- `sql/04_create_load_client_transfers_procedure.sql`
- `sql/05_create_load_client_transfers_scheduler_job.sql`
- `sql/06_create_core_client_transfers.sql`
- `sql/10_create_control_structures.sql`
- `sql/11_create_stage_clients.sql`
- `sql/12_create_external_clients.sql`
- `sql/13_create_core_clients.sql`
- `sql/14_create_load_clients_procedure.sql`

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
- The startup helper defaults to `Europe/Berlin` via `TZ` and `ORA_SDTZ`, so DST is handled by the timezone region instead of a fixed offset.
- Input files and database records in this repo are synthetic test data.
- Fields that may look like banking or customer identifiers, such as account numbers, tax IDs, document IDs, phone numbers, and emails, are demo-only placeholders and do not represent real customer data.
- Deep domain validation is intentionally limited in this demo: examples include IBAN checksum validation, document/tax identifier checksum rules, and stricter real-world phone/email validation.
- Current input file patterns are `client_transfers_YYYYMMDD.csv` / `.ok` and `clients_YYYYMMDD.csv` / `.ok`.
- Current CSV input contract uses semicolon (`;`) as the field separator.
- Client source extracts already include demo-only AML candidate columns `document_id`, `tax_id`, `phone_number`, `email`, `kyc_status`, and `risk_score`.
- The current `clients` SQL pipeline ingests those six demo AML columns into `ext`, `stage/reject`, and `core`.
- Duplicate business keys in a single snapshot are treated as input rejects before the `core` refresh for both `clients` and `client_transfers`.
- The rest of the wider AML client contract remains intentionally omitted in this demo project until later phases.
- `.ok` contains the count of data rows only, without the CSV header.
- The procedures support `AUTO` status handling (`WAITING` before the cutoff, `FAILED` after the cutoff), but the bundled scheduler does not yet re-drive `WAITING` rows from `next_retry_ts`.
