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

## 1. Start Oracle Container

Run on the host from the repository root:

```bash
cd <PROJECT_PATH>
./ops/00_start_oracle_container.sh
```

The helper prompts for `ORACLE_PASSWORD`.

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

Run:

```bash
docker exec -it j-kepka-oracle-elt-workflow bash
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

## 3. Fix `extdata/work` Permissions

This permission setup is a sandbox compatibility baseline for local Docker bind mounts.
It is not a production permission model.

Run on the host:

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

Expected result:
- Oracle can read dated CSV and `.ok` files in `extdata/`.
- Oracle can write loader artifacts into `extdata/work/`.
- The repository remains mounted read-only to `/workspace` inside the container.

## 4. Run The Deterministic Smoke Flow

Run:

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

Optional:

```sql
@/workspace/tests/sql/93_manual_smoke_detail_checks.sql
@/workspace/tests/sql/94_optional_auto_waiting_checks.sql
```

If a custom container name is used, replace `j-kepka-oracle-elt-workflow` with that name.
The expected status, reason, and row-count matrix is asserted by `92_manual_smoke_compare.sql`.

## Script Roles

- `90_manual_smoke_run.sql`: resets covered business dates, then runs the deterministic `MANUAL` smoke flow, including rerun/idempotency steps
- `91_manual_smoke_actuals.sql`: shows current DB state per deterministic case
- `92_manual_smoke_compare.sql`: compares actual DB state with expected outcomes and returns `PASS` or `FAIL`
- `93_manual_smoke_detail_checks.sql`: shows focused diagnostic selects for key warning and normalization cases
- `94_optional_auto_waiting_checks.sql`: runs the time-dependent `AUTO` checks for `2026-04-07`

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

## Scenario Maintenance Note

Current deterministic scenario validation is maintained in multiple places.
This is intentional for the current phase, but edits must stay synchronized.

- `tests/smoke_matrix.csv`: tester-facing matrix reference
- `tests/sql/90_manual_smoke_run.sql`: execution order and expected `SQLCODE`
- `tests/sql/91_manual_smoke_actuals.sql`: deterministic case list for actual-state output
- `tests/sql/92_manual_smoke_compare.sql`: expected status/reason/count assertions

When adding or changing a deterministic `MANUAL` scenario, update all files above in the same commit.
If one location is skipped, smoke comparison can report misleading differences even when loader logic is correct.
