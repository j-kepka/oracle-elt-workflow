# oracle-elt-workflow

Oracle ELT workflow demo for daily `clients` and `client_transfers` snapshot ingestion with validation, reject handling, control-table status tracking, an AML-oriented mart, and a GIIF-like outbound report spool.

This repository demonstrates data engineering, ELT design, data quality checks, auditability, and reporting-oriented data modeling in a realistic synthetic financial-domain scenario.

The current public scope has progressed through `Phase-07` with `aml_report_spool`, outbound CSV/OK publication, and a sequential `load -> mart -> spool` AML workflow described in [docs/ROADMAP.md](docs/ROADMAP.md).

## What The Repository Covers

- dated CSV + `.ok` ready-file driven loads for `clients` and `client_transfers`
- `external -> stage/reject -> core` flow for both datasets
- `MANUAL` and `AUTO` run modes
- same-day dependency from transfers to client snapshots
- repeatable deterministic smoke validation
- dedicated AML demo dataset with richer transfer amounts, multiple currencies, and manual FX seed support
- `mart_transfer_aml` as the first AML-oriented mart layer
- `amount_eur` normalization with FX coverage checks for currencies present on the business date
- first AML review flags, reason codes, and `report_type_candidate` inside `mart_transfer_aml`
- `aml_report_spool` as a stable GIIF-like export contract derived from the mart
- outbound `aml_report_spool_YYYYMMDD.csv` and `.ok` publication after successful spool build
- `RUN_AML_WORKFLOW` as a process-control workflow procedure for `clients -> transfers -> mart -> spool`
- disabled `DWH.JOB_RUN_AML_WORKFLOW` scheduler job as an Oracle-native runner for the workflow procedure

The documented run path targets a local Docker-based demo/sandbox environment.

## Quick Start

Replace `"<PROJECT_PATH>"` with the local path of this repository in every command below.

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

Run on the host:

```bash
docker exec -it j-kepka-oracle-elt-workflow bash
```

Then run inside the container:

```bash
sqlplus / as sysdba
```

This reset/bootstrap flow is destructive in the local sandbox.
It recreates the `dwh` schema and resets the current demo objects.

In `SQL>`:

```sql
ALTER SESSION SET CONTAINER = FREEPDB1;
@/workspace/tests/sql/00_reset_database_to_initial_state.sql

DEFINE DWH_PASSWORD = '<DWH_PASSWORD>'
@/workspace/tests/sql/10_bootstrap_project_schema.sql
```

Replace `<DWH_PASSWORD>` with the password chosen for the demo `dwh` schema.
The same password is used later when connecting as `dwh`.

4. Fix `extdata` permissions on the host:

```bash
cd <PROJECT_PATH>

ORACLE_UID=$(docker exec j-kepka-oracle-elt-workflow id -u oracle)
ORACLE_GID=$(docker exec j-kepka-oracle-elt-workflow id -g oracle)

sudo mkdir -p extdata/inbound extdata/outbound extdata/work
sudo chown "${ORACLE_UID}:${ORACLE_GID}" extdata/outbound extdata/work
sudo chmod 755 extdata extdata/inbound extdata/outbound
sudo chmod 770 extdata/work
sudo find extdata/inbound -maxdepth 1 -type f \( -name 'client_transfers_*.csv' -o -name 'client_transfers_*.ok' -o -name 'clients_*.csv' -o -name 'clients_*.ok' \) -exec chmod 644 {} \;
```

5. Run the end-to-end AML workflow smoke:

```bash
docker exec -it j-kepka-oracle-elt-workflow bash
```

Then run inside the container:

```bash
sqlplus /nolog
```

In `SQL>`:

```sql
CONNECT dwh/"<DWH_PASSWORD>"@//localhost:1521/FREEPDB1
ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD';
@/workspace/tests/sql/100_run_aml_workflow.sql
@/workspace/tests/sql/101_validate_aml_workflow.sql
@/workspace/tests/sql/96_validate_aml_demo_dataset.sql
@/workspace/tests/sql/99_validate_aml_report_spool.sql
```

The AML workflow smoke reads the bundled `clients_20260415` and `client_transfers_20260415` input files, builds the AML mart and report spool, and writes `aml_report_spool_20260415.csv/.ok` to `extdata/outbound/`.
This is the shortest end-to-end check after bootstrap and uses business date `2026-04-15`.

For the full smoke/regression matrix, optional `AUTO` timing check, FX negative coverage, component-level AML checks, and the purpose of each test script, see [tests/SMOKE_TEST_RUNBOOK.md](tests/SMOKE_TEST_RUNBOOK.md).

## Verification Notes

- The Quick Start golden path is the AML end-to-end workflow smoke after bootstrap.
- The AML workflow result is validated by `101_validate_aml_workflow.sql`, `96_validate_aml_demo_dataset.sql`, and `99_validate_aml_report_spool.sql`.
- The AML demo dataset path uses business date `2026-04-15` and is separate from the deterministic loader matrix.
- The workflow helper seeds the demo FX rows used by `mart_transfer_aml`; bootstrap creates `ref_fx_rate_daily` but leaves it empty for local data.
- Broader local regression evidence, including deterministic `MANUAL` scenarios and optional `AUTO` waiting behavior, is documented in the smoke runbook.
- The intended operational order is `LOAD_CLIENTS` before `LOAD_CLIENT_TRANSFERS` for each `business_date`.
  Transfer loads depend on the same-day client snapshot, and smoke/ops helpers follow that order unless a case explicitly tests a missing dependency.
- `aml_report_spool` publication requires `DONE` upstream statuses for client load, transfer load, and mart build.
  Upstream `WARNING` remains valid for mart review, but it is not treated as publishable for the final spool.
- `AUTO` currently uses a bounded retry loop inside the load procedure until the run-day `12:00` cutoff.
  This is a temporary demo bridge, not a full scheduler or dispatcher.
- `DWH.JOB_RUN_AML_WORKFLOW` is installed disabled by default.
  It invokes `dwh.prc_run_aml_workflow` in `AUTO` mode when manually enabled or run.
- The older single-loader scheduler object remains disabled and is not the active orchestration path for the AML workflow.

## Domain Notes

This repository uses AML as a demo domain.
AML means anti-money laundering.
Polish-domain names are used only to give the demo a realistic business context.
They do not mean that the repository implements official Polish AML reporting, legal interpretation, or production compliance requirements.
The checks, flags, report candidates, and exports in this project are simplified engineering examples, not legal, regulatory, or compliance advice.

FX reference data in the demo is modeled as manually maintained daily exchange-rate data.
The `MANUAL_NBP_TABLE_A` seed label refers to NBP, the National Bank of Poland (`Narodowy Bank Polski`), which publishes official exchange-rate tables in Poland.
The repository does not download official NBP data and does not claim regulatory-grade FX sourcing.

`GIIF` refers to the Polish Financial Intelligence Unit, formally the General Inspector of Financial Information (`Generalny Inspektor Informacji Finansowej`).
At a high level, GIIF is the Polish authority associated with receiving and processing AML-related information and notifications from obligated institutions.
In this repository, `GIIF-like` means a simplified reporting-style data shape inspired by that domain, not an official GIIF submission format and not a guarantee of meeting Polish AML reporting requirements.
Names such as `above_threshold_art72_flag`, `suspicion_art74_flag`, and `report_type_candidate` are simplified demo labels inspired by threshold-based and suspicion-based AML reporting paths.

## Input Files

- The demo uses dated `clients_YYYYMMDD.csv` / `.ok` and `client_transfers_YYYYMMDD.csv` / `.ok` files under `extdata/inbound/`.
- `.ok` stores the number of data rows only, without the CSV header.
- Bundled input files and database records are synthetic demo data.
- Detailed validation and smoke guidance remain in [tests/SMOKE_TEST_RUNBOOK.md](tests/SMOKE_TEST_RUNBOOK.md).

## Repository Map

- `ops/`: local operational helpers
- `sql/`: schema objects and loader procedures
- `tests/`: reset, bootstrap, smoke, and AML demo scripts
- `extdata/inbound/`: synthetic input CSV and `.ok` files
- `extdata/outbound/`: generated AML report spool CSV and `.ok` files
- `extdata/work/`: Oracle external-table loader artifacts
- `docs/adr/`: public architecture and delivery decisions

## Scope And Boundaries

- This repository is a local MVP/demo setup, not a production deployment baseline.
- Any production-like reuse should start with an independent security, infrastructure, operations, and data-governance review.
- Practical MVP scope ends after `Phase-07`.
- `Phase-08` and `Phase-09` remain optional post-MVP work.

Operational workflow note: the current MVP runs load, mart build, and report spool as one sequential demo workflow for a selected `business_date`.
A production-like AML process would usually separate these steps across the day: morning ingestion and mart refresh, daytime review or approval by compliance/AML users, and an incremental evening export of approved reportable records.
Approval workflow, case state, incremental export backlog, resend handling, and cancellation handling are intentionally outside this MVP.

## Further Reading

- [tests/SMOKE_TEST_RUNBOOK.md](tests/SMOKE_TEST_RUNBOOK.md): full end-to-end runbook
- [docs/ROADMAP.md](docs/ROADMAP.md): public phase map
- [docs/adr/](docs/adr): public architecture and delivery decisions
