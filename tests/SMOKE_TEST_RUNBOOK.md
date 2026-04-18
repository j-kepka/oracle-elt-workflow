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
Use this flow only in the local demo/sandbox environment.

## Process Order Contract

The default operational order is:
1. `LOAD_CLIENTS`
2. `LOAD_CLIENT_TRANSFERS`

Transfer loads depend on the same-day client snapshot.
Smoke helpers follow that order unless a case explicitly tests a missing dependency, such as `transfers_2026-04-11_manual`.
The current disabled scheduler object is not the active orchestration model; scheduler or wrapper work is deferred until the full `load -> mart -> spool` workflow exists.

## 1. Start Oracle Container

Run on the host from the repository root:

```bash
cd <PROJECT_PATH>
./ops/00_start_oracle_container.sh
```

The helper prompts for `ORACLE_PASSWORD`.

Replace `"<PROJECT_PATH>"` with the local path of this repository in every command below.

`<PROJECT_PATH>` is the local path where the repository was cloned, for example:
- Linux / WSL: `/home/<user>/projects/oracle-elt-workflow`
- macOS: `/Users/<user>/projects/oracle-elt-workflow`

Startup defaults:
- container name: `j-kepka-oracle-elt-workflow`
- volume: `j-kepka-oracle-elt-workflow-data`
- timezone: `Europe/Berlin`

Override defaults for one run:

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
- Oracle can write future export files into `extdata/outbound/`.
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

Optional:

```sql
@/workspace/tests/sql/93_manual_smoke_detail_checks.sql
@/workspace/tests/sql/94_optional_auto_waiting_checks.sql
```

## 5. Optional AML Demo Dataset

This AML-focused path is separate from the deterministic legacy smoke matrix.
It uses a dedicated business date with richer transfer amounts, multiple currencies, extended client AML fields, `transfer_title`, and a simple manual FX seed.

Run:

```sql
@/workspace/tests/sql/95_load_aml_demo_dataset.sql
@/workspace/tests/sql/96_validate_aml_demo_dataset.sql
```

`10_bootstrap_project_schema.sql` creates `ref_fx_rate_daily`, but it does not seed FX rows for this AML demo path.
The FX rows used by the AML demo are inserted by `95_load_aml_demo_dataset.sql`.

Current AML input contract notes:
- `relationship_purpose_code`: `SALARY`, `SAVINGS`, `REMITTANCE`, `INVESTMENT`, `BUSINESS_PAYMENTS`
- `expected_activity_level`: `LOW`, `MEDIUM`, `HIGH`, `VERY_HIGH`, `UNKNOWN`
- `source_of_funds_declared`, `source_of_wealth_declared`, and `transfer_title` remain trimmed descriptive fields with max length `255`

If a custom container name is used, replace `j-kepka-oracle-elt-workflow` with that name.
The expected status, reason, and row-count matrix is asserted by `92_manual_smoke_compare.sql`.

## Script Roles

- `90_manual_smoke_run.sql`: resets covered business dates, then runs the deterministic `MANUAL` smoke flow in `LOAD_CLIENTS` -> `LOAD_CLIENT_TRANSFERS` order, including rerun/idempotency steps
- `91_manual_smoke_actuals.sql`: shows current DB state per deterministic case
- `92_manual_smoke_compare.sql`: compares actual DB state with expected outcomes and returns `PASS` or `FAIL`
- `93_manual_smoke_detail_checks.sql`: shows focused diagnostic selects for key warning and normalization cases
- `94_optional_auto_waiting_checks.sql`: runs the optional `AUTO` checks for `2026-04-07` in `LOAD_CLIENTS` -> `LOAD_CLIENT_TRANSFERS` order with a per-loader test cutoff (`now + 5 minutes` before each loader), retry every 1 minute, and an assertion that both loaders record 5 retries before failing after cutoff
- `95_load_aml_demo_dataset.sql`: loads the dedicated AML demo dataset, seeds FX, and runs both loaders in `LOAD_CLIENTS` -> `LOAD_CLIENT_TRANSFERS` order
- `96_validate_aml_demo_dataset.sql`: validates the AML-oriented input extension, `transfer_title`, and FX seed rows

## Reading The Output

In `91_manual_smoke_actuals.sql`:
- `actual_stage_rows`, `actual_reject_rows`, `actual_core_rows` come from the physical tables
- `ctl_expected_rows`, `ctl_stage_rows`, `ctl_reject_rows`, `ctl_core_rows` come from `CTL_PROCESS_RUN`

For failed runs, table counts matter more than control-row counts because the control table may reflect the attempted load before rollback.

In `92_manual_smoke_compare.sql`:
- `PASS` means the checked fields match the expected matrix
- `FAIL` means at least one checked field differs
- `mismatch_fields` lists the differing columns

## Deterministic Vs Optional

The deterministic matrix covers only the `MANUAL` cases.
The `AUTO` checks stay separate because `2026-04-07` depends on database time relative to the cutoff.
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
