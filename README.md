# oracle-elt-workflow

Oracle ELT workflow demo for daily client transfer and client snapshot ingestion with validation, reject handling, control-table status tracking, and scheduler-ready batch execution.

Current public state reflects `Phase-05`: two related snapshot datasets, same-day transfer-to-client validation, repeatable smoke coverage, and a local Oracle sandbox setup that is meant to be easy to review rather than production-like.

## Data Flow at a Glance

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
```

`ctl_process_run` stores the current status per `process_name` and `business_date`.
`client_transfers` loads to `core` only when a matching same-day client snapshot exists.

## Current Phase-05 Scope

- dated CSV + `.ok` ready-file driven loads for `client_transfers` and `clients`
- `external -> stage/reject -> core`
- `MANUAL` and `AUTO` run modes
- same-day dependency from transfers to clients
- repeatable matrix-based smoke validation
- optional scheduler job for `client_transfers`

Current environment: local `dev/sandbox`.

## Security And Production Disclaimer

This repository is a local MVP for portfolio and learning.
It is not a production deployment baseline.
Any production-like reuse should start with an independent security, infrastructure, operations, and data-governance review.

## Quick Start

1. Start Oracle Free from the repository root:

```bash
cd <PROJECT_PATH>
./ops/00_start_oracle_container.sh
```

The helper prompts for `ORACLE_PASSWORD`.
This is the Oracle container password.

Startup defaults:
- container name: `j-kepka-oracle-elt-workflow`
- volume: `j-kepka-oracle-elt-workflow-data`
- timezone: `Europe/Berlin`

Current setup has been exercised on Linux-style environments and has not been validated on native Windows hosts yet.

2. Wait until the database is ready:

```bash
docker logs -f j-kepka-oracle-elt-workflow
```

3. Bootstrap the demo schema:

```bash
docker exec -it j-kepka-oracle-elt-workflow bash
sqlplus / as sysdba
```

In `SQL>`:

```sql
ALTER SESSION SET CONTAINER = FREEPDB1;
@/workspace/tests/sql/00_reset_database_to_initial_state.sql

DEFINE DWH_PASSWORD = <DWH_PASSWORD>
@/workspace/tests/sql/10_bootstrap_project_schema.sql
```

`DWH_PASSWORD` is the password for the demo `dwh` schema.
This bootstrap step initializes the database structures for the demo schema, but it does not execute the test scenarios yet.

4. Fix `extdata/work` permissions on the host:

```bash
cd <PROJECT_PATH>

ORACLE_UID=$(docker exec j-kepka-oracle-elt-workflow id -u oracle)
ORACLE_GID=$(docker exec j-kepka-oracle-elt-workflow id -g oracle)

sudo mkdir -p extdata/work
sudo chown "${ORACLE_UID}:${ORACLE_GID}" extdata/work
sudo chmod 770 extdata/work
sudo chmod 755 extdata
sudo find extdata -maxdepth 1 -type f \( -name 'client_transfers_*.csv' -o -name 'client_transfers_*.ok' -o -name 'clients_*.csv' -o -name 'clients_*.ok' \) -exec chmod 644 {} \;
```

5. Run the deterministic smoke flow:

```bash
docker exec -it j-kepka-oracle-elt-workflow bash
sqlplus /nolog
```

In `SQL>`:

```sql
CONNECT dwh/"<DWH_PASSWORD>"@//localhost:1521/FREEPDB1
ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD';
@/workspace/tests/sql/90_manual_smoke_run.sql
@/workspace/tests/sql/91_manual_smoke_actuals.sql
@/workspace/tests/sql/92_manual_smoke_compare.sql
```

Optional follow-up checks:

```sql
@/workspace/tests/sql/93_manual_smoke_detail_checks.sql
@/workspace/tests/sql/94_optional_auto_waiting_checks.sql
```

## Expected Smoke Result

After the deterministic smoke flow has been executed:
- `client_transfers` `2026-03-26` -> `DONE`, `stage = 20`, `reject = 0`, `core = 20`
- `client_transfers` `2026-03-25` -> `WARNING`, `stage = 5`, `reject = 7`, `core = 5`
- `clients` `2026-03-26` -> `DONE`, `stage = 12`, `reject = 0`, `core = 12`
- `clients` `2026-03-25` -> `WARNING`, `stage = 5`, `reject = 4`, `core = 5`
- missing `.ok` in `AUTO` mode before the cutoff leads to `WAITING`
- missing `.ok` in `MANUAL` mode, count mismatches, or a missing same-day client snapshot lead to `FAILED`

Full deterministic scenario coverage remains in [tests/SMOKE_TEST_RUNBOOK.md](tests/SMOKE_TEST_RUNBOOK.md) and `tests/smoke_matrix.csv`.

## Where To Go Next

- [tests/SMOKE_TEST_RUNBOOK.md](tests/SMOKE_TEST_RUNBOOK.md): full end-to-end runbook with extra detail, warnings, and output interpretation
- [docs/ROADMAP.md](docs/ROADMAP.md): public phase map and MVP boundary
- [docs/adr/](docs/adr): public architecture and delivery decisions

## Demo Scope Boundary

Phase labels in this repository describe delivery stages rather than GitHub pull request numbers.
For portfolio/demo purposes, the practical MVP scope ends before `Phase-09`.
`Phase-01` -> `Phase-08` define the demo-facing scope, while `Phase-09`, `Phase-10`, and `Phase-11` remain optional post-MVP work.

## Main SQL Areas

Transfer pipeline:
- `sql/01_create_stage_client_transfers.sql`
- `sql/02_create_external_client_transfers.sql`
- `sql/04_create_load_client_transfers_procedure.sql`
- `sql/05_create_load_client_transfers_scheduler_job.sql`
- `sql/06_create_core_client_transfers.sql`

Client pipeline:
- `sql/11_create_stage_clients.sql`
- `sql/12_create_external_clients.sql`
- `sql/13_create_core_clients.sql`
- `sql/14_create_load_clients_procedure.sql`

Shared control:
- `sql/10_create_control_structures.sql`

## Repository Structure

- `ops/`: local operational helpers
- `sql/`: schema objects and loaders
- `tests/`: bootstrap and smoke-test scripts
- `extdata/`: synthetic input files used by the demo
- `docs/adr/`: public decisions

## Notes

- Input files and database records in this repo are synthetic demo data.
- Current input contract uses `client_transfers_YYYYMMDD.csv` / `.ok` and `clients_YYYYMMDD.csv` / `.ok` with semicolon-separated CSV data.
- `.ok` contains the count of data rows only, without the CSV header.
- The bundled scheduler does not yet re-drive `WAITING` rows from `next_retry_ts`.
