# oracle-elt-workflow

Oracle ELT workflow demo for daily `clients` and `client_transfers` snapshot ingestion with validation, reject handling, control-table status tracking, and an AML-oriented mart foundation.

The current public scope has progressed into `Phase-06 Part 2` with the AML mart foundation described in [docs/ROADMAP.md](docs/ROADMAP.md).

## What The Repository Covers

- dated CSV + `.ok` ready-file driven loads for `clients` and `client_transfers`
- `external -> stage/reject -> core` flow for both datasets
- `MANUAL` and `AUTO` run modes
- same-day dependency from transfers to client snapshots
- repeatable deterministic smoke validation
- dedicated AML demo dataset with richer transfer amounts, multiple currencies, and manual FX seed support
- `mart_transfer_aml` as the first AML-oriented mart layer
- `amount_eur` normalization with FX coverage checks for currencies present on the business date

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

5. Run the primary deterministic smoke path:

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
@/workspace/tests/sql/90_manual_smoke_run.sql
@/workspace/tests/sql/92_manual_smoke_compare.sql
```

Optional follow-up checks:

```sql
@/workspace/tests/sql/91_manual_smoke_actuals.sql
@/workspace/tests/sql/93_manual_smoke_detail_checks.sql
@/workspace/tests/sql/94_optional_auto_waiting_checks.sql
```

`94_optional_auto_waiting_checks.sql` overrides `AUTO` to use a per-loader `now + 5 minutes` cutoff with a 1-minute retry interval.
Because it checks `LOAD_CLIENTS` and `LOAD_CLIENT_TRANSFERS` sequentially, this optional helper takes about 10 minutes.

Optional AML demo dataset checks:

```sql
@/workspace/tests/sql/95_load_aml_demo_dataset.sql
@/workspace/tests/sql/96_validate_aml_demo_dataset.sql
```

Optional mart FX coverage check:

```sql
@/workspace/tests/sql/97_optional_mart_fx_coverage_checks.sql
```

`10_bootstrap_project_schema.sql` creates `ref_fx_rate_daily`, but it does not seed FX rows for the AML demo path.
The FX seed used by the AML demo is loaded by `95_load_aml_demo_dataset.sql`.
The AML demo path builds `mart_transfer_aml`, validates `amount_eur`, and checks missing-FX behavior through `MISSING_FX_RATES`.

## Verification Notes

- The main objective smoke result is returned by `92_manual_smoke_compare.sql`.
- The deterministic `MANUAL` matrix is the default verification path for the current demo flow.
- The AML demo dataset path is separate from the legacy deterministic matrix.
- `95_load_aml_demo_dataset.sql` loads the AML demo data and builds `mart_transfer_aml`.
- `96_validate_aml_demo_dataset.sql` validates the mart row count, client-transfer join, and EUR-normalized amounts.
- `97_optional_mart_fx_coverage_checks.sql` verifies that missing FX reference data blocks the mart build before restoring the demo FX seed.
- The intended operational order is `LOAD_CLIENTS` before `LOAD_CLIENT_TRANSFERS` for each `business_date`.
  Transfer loads depend on the same-day client snapshot, and smoke/ops helpers follow that order unless a case explicitly tests a missing dependency.
- `AUTO` currently uses a bounded retry loop inside the load procedure until the run-day `12:00` cutoff.
  This is a temporary demo bridge, not a full scheduler or dispatcher.
- The disabled scheduler object in the bootstrap is not the active orchestration model.
  Scheduler or wrapper work is deferred until the full `load -> mart -> spool` workflow exists.

## Domain Notes

This repository uses AML as a demo domain.
AML means anti-money laundering.
The checks and flags in this project are simplified engineering examples, not legal, regulatory, or compliance advice.

FX reference data in the demo is modeled as manually maintained daily exchange-rate data.
The `MANUAL_NBP_TABLE_A` seed label refers to NBP, the National Bank of Poland (`Narodowy Bank Polski`), whose published exchange-rate tables are a familiar Polish reference point.
The repository does not download official NBP data or claim regulatory-grade FX sourcing.

Future AML review fields may use names such as `above_threshold_art72_flag` and `suspicion_art74_flag`.
Those names are inspired by Polish AML reporting concepts: threshold-based transaction reporting and suspicion-based reporting.
`GIIF` refers to the Polish Financial Intelligence Unit, formally the General Inspector of Financial Information (`Generalny Inspektor Informacji Finansowej`).
In this repository, `GIIF-like` means a simplified reporting-style data shape inspired by that domain, not an official GIIF submission format.

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
- `extdata/outbound/`: generated export files for later spool flows
- `extdata/work/`: Oracle external-table loader artifacts
- `docs/adr/`: public architecture and delivery decisions

## Scope And Boundaries

- This repository is a local MVP/demo setup, not a production deployment baseline.
- Any production-like reuse should start with an independent security, infrastructure, operations, and data-governance review.
- Practical MVP scope ends after `Phase-07`.
- `Phase-08` and `Phase-09` remain optional post-MVP work.

## Further Reading

- [tests/SMOKE_TEST_RUNBOOK.md](tests/SMOKE_TEST_RUNBOOK.md): full end-to-end runbook
- [docs/ROADMAP.md](docs/ROADMAP.md): public phase map
- [docs/adr/](docs/adr): public architecture and delivery decisions
