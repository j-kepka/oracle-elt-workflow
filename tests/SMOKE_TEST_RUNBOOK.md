# Smoke Test Runbook

Detailed bootstrap and validation steps for the current matrix-based smoke tests.

Expected deterministic cases live in:
- `tests/smoke_matrix.csv`

The CSV is a tester helper only.
It is not used as runtime input by SQL.

## Prerequisites

- Docker is installed and usable by the current local user.
- The repository is available in a local clone path.
- Current setup has been exercised on Linux-style environments.
- Native Windows hosts have not been validated yet.

## Warning

This smoke flow is not read-only.
`90_manual_smoke_run.sql` deletes existing `stage`, `reject`, `core`, and `CTL_PROCESS_RUN` rows for the business dates covered by the deterministic matrix before reloading them.
`94_optional_auto_waiting_checks.sql` does the same for `2026-04-07`.
`100_run_aml_workflow.sql` resets the AML demo date `2026-04-15` before running the workflow procedure.
Use this flow only in the local demo/sandbox environment.

## Process Order Contract

The default operational order is:
1. `LOAD_CLIENTS`
2. `LOAD_CLIENT_TRANSFERS`

Transfer loads depend on the same-day client snapshot.
Smoke helpers follow that order unless a case explicitly tests a missing dependency, such as `transfers_2026-04-11_manual`.
The AML workflow procedure runs the full order as:
1. `LOAD_CLIENTS`
2. `LOAD_CLIENT_TRANSFERS`
3. `BUILD_MART_TRANSFER_AML`
4. `BUILD_AML_REPORT_SPOOL`

`DWH.JOB_RUN_AML_WORKFLOW` is installed disabled by default.
The smoke helper calls `dwh.prc_run_aml_workflow` directly so the workflow can be validated without enabling a scheduled run.

## 1. Start Oracle Container

Set `<PROJECT_PATH>` to the local clone path of this repository, then start Oracle:

```bash
cd <PROJECT_PATH>
./ops/00_start_oracle_container.sh
```

The helper prompts for `ORACLE_PASSWORD`, which becomes the Oracle container password.

Startup defaults:
- container name: `j-kepka-oracle-elt-workflow`
- volume: `j-kepka-oracle-elt-workflow-data`
- timezone: `Europe/Berlin`

To override defaults for one run, prefix the start command:

```bash
ORACLE_TZ='<REGION/CITY>' \
ORACLE_CONTAINER_NAME='<CONTAINER_NAME>' \
ORACLE_VOLUME_NAME='<VOLUME_NAME>' \
./ops/00_start_oracle_container.sh
```

Wait until the database is ready:

```bash
docker logs -f j-kepka-oracle-elt-workflow
```

## 2. Reset And Bootstrap The Schema

Run on the host:

```bash
docker exec -it j-kepka-oracle-elt-workflow bash
```

Then run inside the container:

```bash
sqlplus / as sysdba
```

In `SQL>`:

```sql
ALTER SESSION SET CONTAINER = FREEPDB1;
@/workspace/tests/sql/00_reset_database_to_initial_state.sql
DEFINE DWH_PASSWORD = '<DWH_PASSWORD>'
@/workspace/tests/sql/10_bootstrap_project_schema.sql
```

This resets project-specific objects and recreates the `dwh` schema plus current demo objects.
It is destructive for the local demo schema and should be used only in the local sandbox flow.

## 3. Fix `extdata` Permissions

This permission setup is a sandbox compatibility baseline for local Docker bind mounts.
It is not a production permission model.

Run on the host:

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

Expected result:
- Oracle can read dated CSV and `.ok` files in `extdata/inbound/`.
- Oracle can write AML report spool export files into `extdata/outbound/`.
- Oracle can write loader artifacts into `extdata/work/`.
- The repository remains mounted read-only to `/workspace` inside the container.

## 4. Run The Deterministic Smoke Flow

Run on the host:

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
@/workspace/tests/sql/91_manual_smoke_actuals.sql
@/workspace/tests/sql/92_manual_smoke_compare.sql
```

Use this order for loader-level regression evidence:
- `90_manual_smoke_run.sql` executes the deterministic scenarios and resets covered dates before loading them again.
- `91_manual_smoke_actuals.sql` exposes the current database state for review.
- `92_manual_smoke_compare.sql` is the objective `PASS` / `FAIL` gate for the deterministic matrix.

Additional manual detail checks:

```sql
@/workspace/tests/sql/93_manual_smoke_detail_checks.sql
```

Optional time-based `AUTO` check:

```sql
@/workspace/tests/sql/94_optional_auto_waiting_checks.sql
```

This helper validates the `AUTO` missing-ready-file waiting path for `2026-04-07`, where the CSV files exist but the `.ok` files are missing.
For testability, each loader gets a `now + 5 minutes` cutoff and retries every 1 minute.
Because `LOAD_CLIENTS` and `LOAD_CLIENT_TRANSFERS` run sequentially, the full check takes about 10 minutes and is expected to finish with `MISSING_OK_AFTER_CUTOFF` after 5 retries per loader.

## 5. AML Demo And End-To-End Workflow Smoke

This AML-focused path is separate from the deterministic legacy smoke matrix, but it is part of the main local smoke evidence for the current MVP flow.
It covers component-level AML checks first, then leaves the database in the final end-to-end `load -> mart -> spool` workflow state through `dwh.prc_run_aml_workflow`.

AML demo data date:
- `2026-04-15`

Input files:
- `extdata/inbound/clients_20260415.csv`
- `extdata/inbound/clients_20260415.ok`
- `extdata/inbound/client_transfers_20260415.csv`
- `extdata/inbound/client_transfers_20260415.ok`

Reference seed:
- `tests/sql/11_seed_ref_fx_rate_daily.sql` seeds `ref_fx_rate_daily` for `2026-04-15`

Expected database outputs for `2026-04-15`:
- `dwh.core_clients`
- `dwh.core_client_transfers`
- `dwh.mart_transfer_aml`
- `dwh.aml_report_spool`
- `dwh.ctl_process_run`

Expected outbound files:
- `extdata/outbound/aml_report_spool_20260415.csv`
- `extdata/outbound/aml_report_spool_20260415.ok`

Component-level AML helpers can be used when validating individual layers:

```sql
@/workspace/tests/sql/95_load_aml_demo_dataset.sql
@/workspace/tests/sql/96_validate_aml_demo_dataset.sql
```

Mart FX negative coverage check:

```sql
@/workspace/tests/sql/97_optional_mart_fx_coverage_checks.sql
```

AML report spool/export checks:

```sql
@/workspace/tests/sql/98_build_aml_report_spool.sql
@/workspace/tests/sql/99_validate_aml_report_spool.sql
```

Recommended final AML workflow smoke:

```sql
@/workspace/tests/sql/100_run_aml_workflow.sql
@/workspace/tests/sql/101_validate_aml_workflow.sql
@/workspace/tests/sql/96_validate_aml_demo_dataset.sql
@/workspace/tests/sql/99_validate_aml_report_spool.sql
```

When this section is run linearly, run the component-level helpers before the final AML workflow smoke.
`95_load_aml_demo_dataset.sql` resets the AML demo date and clears the workflow control row, so `100_run_aml_workflow.sql` should be the final state-setting run.

Where to inspect results after the AML workflow smoke:

```sql
SELECT process_name,
       status,
       reason_code,
       expected_row_count,
       stage_row_count,
       reject_row_count,
       core_row_count,
       data_file_name,
       ready_file_name
FROM dwh.ctl_process_run
WHERE business_date = DATE '2026-04-15'
ORDER BY started_ts, process_name;

SELECT COUNT(*) AS mart_rows
FROM dwh.mart_transfer_aml
WHERE business_date = DATE '2026-04-15';

SELECT COUNT(*) AS spool_rows
FROM dwh.aml_report_spool
WHERE business_date = DATE '2026-04-15';
```

`10_bootstrap_project_schema.sql` creates `ref_fx_rate_daily`, but it does not seed FX rows for this AML demo path.
The FX rows used by the AML demo are inserted by `95_load_aml_demo_dataset.sql` and by `100_run_aml_workflow.sql`.

Current AML input contract notes:
- `relationship_purpose_code`: `SALARY`, `SAVINGS`, `REMITTANCE`, `INVESTMENT`, `BUSINESS_PAYMENTS`
- `expected_activity_level`: `LOW`, `MEDIUM`, `HIGH`, `VERY_HIGH`, `UNKNOWN`
- `source_of_funds_declared`, `source_of_wealth_declared`, and `transfer_title` remain trimmed descriptive fields with max length `255`

If a custom container name is used, replace `j-kepka-oracle-elt-workflow` with that name.
The expected status, reason, and row-count matrix is asserted by `92_manual_smoke_compare.sql`.

## What Each Script Proves

- `90_manual_smoke_run.sql`: proves the deterministic loader scenarios can be replayed from a clean state in `LOAD_CLIENTS` -> `LOAD_CLIENT_TRANSFERS` order, including rerun/idempotency behavior.
- `91_manual_smoke_actuals.sql`: gives a reviewable snapshot of the physical rows and control rows produced by the deterministic scenarios.
- `92_manual_smoke_compare.sql`: proves the deterministic matrix matches expected status, reason, and row-count outcomes by returning `PASS` or `FAIL`.
- `93_manual_smoke_detail_checks.sql`: proves important edge details are visible for review, especially warning, normalization, and missing-dependency cases.
- `94_optional_auto_waiting_checks.sql`: proves the `AUTO` missing-ready-file wait path by retrying each loader every 1 minute until the per-loader `now + 5 minutes` cutoff, then asserting 5 retries and `MISSING_OK_AFTER_CUTOFF`.
- `95_load_aml_demo_dataset.sql`: proves the AML demo input files and FX seed can load into core tables and build `mart_transfer_aml` at component level.
- `96_validate_aml_demo_dataset.sql`: proves the AML input extension, `transfer_title`, FX seed rows, mart row counts, EUR normalization, and first review-rule outputs.
- `97_optional_mart_fx_coverage_checks.sql`: proves the AML mart refuses to build without required FX reference data by raising `MISSING_FX_RATES`, then restores the seed and rebuilds.
- `98_build_aml_report_spool.sql`: proves the AML report spool component can build publishable rows and write `aml_report_spool_YYYYMMDD.csv` plus `.ok` to `extdata/outbound/`.
- `99_validate_aml_report_spool.sql`: proves the spool publication gate, report type candidates, report due dates, exported client context, CSV line count, and `.ok` row count.
- `100_run_aml_workflow.sql`: proves the full AML workflow procedure can run from a clean AML demo date through `LOAD_CLIENTS -> LOAD_CLIENT_TRANSFERS -> BUILD_MART_TRANSFER_AML -> BUILD_AML_REPORT_SPOOL`.
- `101_validate_aml_workflow.sql`: proves the workflow-level control row, step control rows, disabled scheduler job contract, and outbound file presence.

## Reading The Output

In `91_manual_smoke_actuals.sql`:
- `actual_stage_rows`, `actual_reject_rows`, `actual_core_rows` come from the physical tables
- `ctl_expected_rows`, `ctl_stage_rows`, `ctl_reject_rows`, `ctl_core_rows` come from `CTL_PROCESS_RUN`

For failed runs, table counts matter more than control-row counts because the control table may reflect the attempted load before rollback.

In `92_manual_smoke_compare.sql`:
- `PASS` means the checked fields match the expected matrix
- `FAIL` means at least one checked field differs
- `mismatch_fields` lists the differing columns

## Manual, AML, And AUTO Checks

The stable local smoke evidence has two main paths:
- the deterministic `MANUAL` matrix for loader behavior
- the AML end-to-end workflow smoke for `2026-04-15`

The `AUTO` checks are the optional time-based path because `2026-04-07` depends on database time relative to the cutoff.
Current `AUTO` behavior is intentionally lightweight: the procedure retries inside a bounded loop until the run-day `12:00` cutoff.
This is a temporary bridge for the demo, not a full dispatcher consuming `next_retry_ts`.
In the optional smoke helper `94_optional_auto_waiting_checks.sql`, that default is intentionally overridden to a per-loader test cutoff (`now + 5 minutes` before each loader) with retry every 1 minute, so the missing-`.ok` path can be observed for both loaders without waiting until noon.
Because the two loaders run sequentially, this helper takes about 10 minutes and asserts that both loaders record 5 retries before failing after cutoff.

## Scenario Maintenance Note

Current deterministic scenario validation is maintained in multiple places.
This is intentional for the current phase, but edits must stay synchronized.

- `tests/smoke_matrix.csv`: tester-facing matrix reference
- `tests/sql/90_manual_smoke_run.sql`: execution order and expected `SQLCODE`
- `tests/sql/91_manual_smoke_actuals.sql`: deterministic case list for actual-state output
- `tests/sql/92_manual_smoke_compare.sql`: expected status/reason/count assertions

When adding or changing a deterministic `MANUAL` scenario, update all files above in the same commit.
If one location is skipped, smoke comparison can report misleading differences even when loader logic is correct.
