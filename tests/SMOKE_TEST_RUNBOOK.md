# Smoke Test Runbook

Manual smoke for the current repository scope.

This runbook checks:
- Oracle container startup,
- schema reset and bootstrap,
- external table access for dated CSV files,
- `.ok` readiness checks,
- `business_date`-driven loading,
- input validation and reject handling,
- rerun safety for the same `business_date`,
- deterministic `MANUAL` failures when `.ok` is missing,
- normalization of trimmed/lowercase input into typed uppercase values,
- transfer failure when the same-day client snapshot is missing,
- current process status in `CTL_PROCESS_RUN`,
- end-to-end load into `stage` and `core`,
- parallel procedural flows for `client_transfers` and `clients`,
- a same-day foreign key from `core_client_transfers` to `core_clients` on `(business_date, client_id)`.

## Scope

Current implementation supports:
- dated `client_transfers_YYYYMMDD.csv` input files,
- matching `client_transfers_YYYYMMDD.ok` ready files,
- dated `clients_YYYYMMDD.csv` input files,
- matching `clients_YYYYMMDD.ok` ready files,
- `p_date` / `business_date` driven loads,
- raw-to-typed validation in SQL,
- reject storage in `STG_CLIENT_TRANSFERS_REJECT`,
- reject storage in `STG_CLIENTS_REJECT`,
- one current process status row in `CTL_PROCESS_RUN` per `process_name` and `business_date`,
- manual smoke coverage for the stable MVP scenarios and expected final statuses: `DONE`, `WARNING`, `FAILED`, plus a time-dependent `WAITING` check in `AUTO`,
- `MANUAL` and `AUTO` run modes,
- deterministic `MISSING_OK_MANUAL` handling in `MANUAL`,
- `WAITING` before the cutoff when `.ok` is still missing in `AUTO`,
- `FAILED` after the cutoff or for technical input problems.

Current note for the demo source contract:
- both `client_transfers_YYYYMMDD.csv` and `clients_YYYYMMDD.csv` use semicolon (`;`) as the field separator,
- `clients_YYYYMMDD.csv` already carries additional demo-only AML candidate fields: `document_id`, `tax_id`, `phone_number`, `email`, `kyc_status`, and `risk_score`,
- the current SQL load path now ingests those six demo fields into the `clients` pipeline (`ext -> stage/reject -> core`),
- the rest of the wider AML client contract remains intentionally omitted in this demo project.

Interpretation used by the repository:
- `.ok` must contain the number of data rows only, without the CSV header,
- `stage_row_count`, `reject_row_count`, and `core_row_count` count data rows only,
- `source_row_num` is a diagnostic physical file line number from `RECNUM`, so the first data row is `2` when the header is skipped,
- `client_status` in the client snapshot is a simple reporting-state flag and currently accepts `ACTIVE` or `ARCHIVED`,
- `client_transfers` can load to `core` only when the matching client snapshot exists for the same `business_date`.

Current implementation does not yet support:
- a scheduler-driven retry loop based on `next_retry_ts`,
- archive/done folders for processed inbound files,
- full attempt history or dedicated run-log tables.

## Prerequisites

- Docker is installed and usable by the current Linux user.
- The repository is available on the host in a local clone path chosen by the user.
- Oracle image `gvenzl/oracle-free@sha256:62aad247879f5d4ca4a37ecc068ef6a5feb9e9bea789501b6a82d4814d14bbb3` is available locally or can be pulled.

## Test Data Disclaimer

- Files and database records in this repo are synthetic test data.
- They may look like banking data, but they are not intended to represent real customer or account data.
- Some deeper domain validations are intentionally out of scope for this stage of the project.

## Security Disclaimer

This runbook is limited to local `dev/sandbox`.
Current grants, Docker bind mounts, and filesystem permissions are intentionally simplified for smoke tests.
This setup is not a production baseline.
Any production-like reuse should start with an independent security, infrastructure, operations, and data-governance review.

## 1. Start Oracle Container

Run on the host:

```bash
cd <PROJECT_PATH>
ORACLE_PASSWORD='<ORACLE_PASSWORD>' bash ops/00_start_oracle_container.sh
```

Run from the repository root on the host.
`<PROJECT_PATH>` is the local path where you cloned this repository, for example:
- Linux / WSL: `/home/<user>/projects/oracle-elt-workflow`
- macOS: `/Users/<user>/projects/oracle-elt-workflow`

Current setup has been exercised on Linux-style environments.
It has not been validated on native Windows hosts yet.

Default startup config:
- timezone: `Europe/Berlin`
- timezone variables passed to the container: `TZ` and `ORA_SDTZ`

Optional override:

```bash
ORACLE_PASSWORD='<ORACLE_PASSWORD>' ORACLE_TZ='Europe/Berlin' ./ops/00_start_oracle_container.sh
```

Wait until the database is ready:

```bash
docker logs -f j-kepka-oracle-elt-workflow
```

## 2. Reset Project State

Run:

```bash
docker exec -it j-kepka-oracle-elt-workflow bash
sqlplus / as sysdba
```

In `SQL>`:

```sql
ALTER SESSION SET CONTAINER = FREEPDB1;
@/workspace/tests/sql/00_reset_database_to_initial_state.sql
```

## 3. Bootstrap Project Schema

In `SQL>`:

```sql
DEFINE DWH_PASSWORD = <DWH_PASSWORD>
@/workspace/tests/sql/10_bootstrap_project_schema.sql
```

Expected result:
- user `DWH` is created,
- `EXT_DIR` and `EXT_WORK_DIR` are created,
- stage, reject, core, control table, procedures and scheduler job are created,
- final line shows `Bootstrap completed.`

## 4. Fix External Loader Permissions

This permission setup is a sandbox-only compatibility baseline for local smoke tests with a bind-mounted `extdata/` directory.
It is not a production permission model; any non-sandbox use needs a separate security and infrastructure review.

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
- Oracle can read dated CSV and `.ok` files in `extdata/`,
- Oracle can write loader artifacts into `extdata/work/`.
- Oracle system time and default SQL*Plus session timezone follow `Europe/Berlin` unless `ORACLE_TZ` is overridden at startup.

## 5. Review Input And Ready Files

Run on the host:

```bash
cd <PROJECT_PATH>

printf '\nCSV rows without header\n'
for file in \
  extdata/client_transfers_20260325.csv \
  extdata/client_transfers_20260326.csv \
  extdata/client_transfers_20260327.csv \
  extdata/client_transfers_20260328.csv \
  extdata/client_transfers_20260407.csv \
  extdata/client_transfers_20260408.csv \
  extdata/client_transfers_20260410.csv \
  extdata/client_transfers_20260411.csv \
  extdata/client_transfers_20260412.csv \
  extdata/clients_20260325.csv \
  extdata/clients_20260326.csv \
  extdata/clients_20260327.csv \
  extdata/clients_20260328.csv \
  extdata/clients_20260329.csv \
  extdata/clients_20260407.csv \
  extdata/clients_20260408.csv \
  extdata/clients_20260409.csv \
  extdata/clients_20260410.csv
do
  printf '%-36s %3d\n' "$(basename "$file")" "$(( $(wc -l < "$file") - 1 ))"
done

printf '\nReady-file values\n'
for file in \
  extdata/client_transfers_20260324.ok \
  extdata/client_transfers_20260325.ok \
  extdata/client_transfers_20260326.ok \
  extdata/client_transfers_20260327.ok \
  extdata/client_transfers_20260328.ok \
  extdata/client_transfers_20260410.ok \
  extdata/client_transfers_20260411.ok \
  extdata/client_transfers_20260412.ok \
  extdata/clients_20260324.ok \
  extdata/clients_20260325.ok \
  extdata/clients_20260326.ok \
  extdata/clients_20260327.ok \
  extdata/clients_20260328.ok \
  extdata/clients_20260329.ok \
  extdata/clients_20260409.ok \
  extdata/clients_20260410.ok
do
  printf '%-36s %s\n' "$(basename "$file")" "$(tr -d '\r\n' < "$file")"
done

printf '\nFiles expected to exist without a matching .ok\n'
ls \
  extdata/client_transfers_20260407.csv \
  extdata/client_transfers_20260408.csv \
  extdata/clients_20260407.csv \
  extdata/clients_20260408.csv
```


What to check:

Transfer baseline files:
- `client_transfers_20260326.csv`: `20` data rows, `.ok=20`, happy-path transfer load.
- `client_transfers_20260325.csv`: `12` data rows, `.ok=12`, later smoke should load `5` valid rows and reject `7`.
- `client_transfers_20260324.ok`: exists even though the matching CSV does not.
- `client_transfers_20260327.csv`: row count does not match `.ok`, used for `OK_COUNT_MISMATCH`.
- `client_transfers_20260328.csv`: `2` data rows, `.ok=2`, both rows share the same technically valid `transfer_id`, so both should later be rejected as duplicates.

Client baseline files:
- `clients_20260326.csv`: `12` data rows, `.ok=12`, happy-path client load.
- `clients_20260325.csv`: `9` data rows, `.ok=9`, later smoke should load `5` valid rows and reject `4`.
- `clients_20260324.ok`: exists even though the matching CSV does not.
- `clients_20260327.csv`: row count does not match `.ok`, used for `OK_COUNT_INCLUDES_HEADER`.
- `clients_20260328.csv`: `2` data rows, `.ok=2`, valid same-day parent clients for the transfer duplicate-reject smoke.
- `clients_20260329.csv`: `2` data rows, `.ok=2`, both rows share the same technically valid `client_id`, so both should later be rejected as duplicates.

Extended client files:
- `clients_20260407.csv`: exists without a matching `.ok`, used for `AUTO` waiting/cutoff review.
- `clients_20260408.csv`: exists without a matching `.ok`, used for deterministic `MANUAL` missing-ready-file review.
- `clients_20260409.csv`: `10` data rows, `.ok=10`, combines normalization, `risk_score` boundary values, invalid `phone_number` / `email`, unsupported `kyc_status`, and one semantic case where one invalid row shares `client_id` with one valid row.
- `clients_20260410.csv`: `2` data rows, `.ok=2`, valid same-day parents for the transfer normalization smoke.

Extended transfer files:
- `client_transfers_20260407.csv`: exists without a matching `.ok`, used for `AUTO` waiting/cutoff review.
- `client_transfers_20260408.csv`: exists without a matching `.ok`, used for deterministic `MANUAL` missing-ready-file review.
- `client_transfers_20260410.csv`: `2` data rows, `.ok=2`, contains lowercase/spaced `currency_code`, `transfer_status`, `channel`, and `country_code` values that should normalize in `stage/core`.
- `client_transfers_20260411.csv`: `1` data row, `.ok=1`, technically valid, but no same-day `clients` snapshot should be loaded for it, so the transfer load should fail with `MISSING_CLIENT_SNAPSHOT`.
- `client_transfers_20260412.csv`: `2` data rows, `.ok=2`, should drive two different reject reasons: `invalid country_code format` and `unsupported country_code`.

## 6. Run Manual Smoke Review Helpers

Run on the host:

```bash
docker exec -it j-kepka-oracle-elt-workflow bash
sqlplus /nolog
```

In `SQL>`:

```sql
CONNECT dwh/"<DWH_PASSWORD>"@//localhost:1521/FREEPDB1
ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD';
@/workspace/tests/sql/90_manual_review_phase03.sql
@/workspace/tests/sql/91_manual_review_clients.sql
@/workspace/tests/sql/92_manual_review_extended_cases.sql
```

Expected transfer result:
- `2026-03-26` finishes with `DONE`,
- `2026-03-25` finishes with `WARNING`,
- `2026-03-27` raises `ORA-20015` and stores `FAILED` with `reason_code = OK_COUNT_MISMATCH`,
- `2026-03-28` finishes with `WARNING`, because duplicate `transfer_id` rows are rejected before `core`,
- `2026-03-24` raises `ORA-20014` and stores `FAILED` with `reason_code = MISSING_DATA_FILE`.

Review the output and check if it matches:
- `2026-03-26`: `stage = 20`, `reject = 0`, `core = 20`
- `2026-03-25`: `stage = 5`, `reject = 7`, `core = 5`
- `2026-03-28`: `stage = 0`, `reject = 2`, `core = 0`
- `2026-03-26`: control row should show `status = DONE`, `reason_code = LOAD_DONE`, `expected_row_count = 20`
- `2026-03-25`: control row should show `status = WARNING`, `reason_code = INPUT_VALIDATION_WARNING`, `expected_row_count = 12`
- `2026-03-28`: control row should show `status = WARNING`, `reason_code = INPUT_VALIDATION_WARNING`, `expected_row_count = 2`
- `2026-03-28`: reject rows should show `duplicate transfer_id in snapshot`

Note for failed cases:
- `CTL_PROCESS_RUN` may store attempted `stage_row_count` / `reject_row_count` values from the last load attempt before rollback,
- `stage`, `reject`, and `core` tables should still remain empty for those `business_date` values after the failed run.

Expected client result:
- `2026-03-26` finishes with `DONE`,
- `2026-03-25` finishes with `WARNING`,
- `2026-03-27` raises `ORA-20117` and stores `FAILED` with `reason_code = OK_COUNT_INCLUDES_HEADER`,
- `2026-03-28` finishes with `DONE`,
- `2026-03-29` finishes with `WARNING`, because duplicate `client_id` rows are rejected before `core`,
- `2026-03-24` raises `ORA-20114` and stores `FAILED` with `reason_code = MISSING_DATA_FILE`.

Review the output and check if it matches:
- `2026-03-26`: `stage = 12`, `reject = 0`, `core = 12`
- `2026-03-25`: `stage = 5`, `reject = 4`, `core = 5`
- `2026-03-28`: `stage = 2`, `reject = 0`, `core = 2`
- `2026-03-29`: `stage = 0`, `reject = 2`, `core = 0`
- `2026-03-26`: control row should show `status = DONE`, `reason_code = LOAD_DONE`, `expected_row_count = 12`
- `2026-03-25`: control row should show `status = WARNING`, `reason_code = INPUT_VALIDATION_WARNING`, `expected_row_count = 9`
- `2026-03-29`: control row should show `status = WARNING`, `reason_code = INPUT_VALIDATION_WARNING`, `expected_row_count = 2`
- `2026-03-29`: reject rows should show `duplicate client_id in snapshot`
- stage and core samples for `2026-03-26` should now also expose the six demo AML fields (`document_id`, `tax_id`, `phone_number`, `email`, `kyc_status`, `risk_score`)
- reruns of `2026-03-26` and `2026-03-25` should keep the same counts for both datasets instead of growing `stage/reject/core`

Expected extended review result:
- `clients` `2026-04-09`: `WARNING`, `stage = 2`, `reject = 8`, `core = 2`
- `clients` `2026-04-09`: stage/core rows should show normalized uppercase values for `client_type`, `country_code`, `kyc_status`, `client_status`
- `clients` `2026-04-09`: reject rows should now also show `invalid email format`, `invalid phone_number format` and `unsupported kyc_status`
- `clients` `2026-04-09`: the valid row for `client_id = 8401` should still reach `stage/core`, while the second row with the same `client_id` should be rejected for invalid `risk_score`
- `clients` `2026-04-10`: `DONE`, `stage = 2`, `reject = 0`, `core = 2`
- `clients` `2026-04-08` in `MANUAL`: `ORA-20110`, `FAILED`, `reason_code = MISSING_OK_MANUAL`
- `client_transfers` `2026-04-10`: `DONE`, `stage = 2`, `reject = 0`, `core = 2`
- `client_transfers` `2026-04-10`: stage/core rows should show normalized uppercase values for `currency_code`, `transfer_status`, `channel`, `country_code`
- `client_transfers` `2026-04-08` in `MANUAL`: `ORA-20010`, `FAILED`, `reason_code = MISSING_OK_MANUAL`
- `client_transfers` `2026-04-11`: `ORA-20018`, `FAILED`, `reason_code = MISSING_CLIENT_SNAPSHOT`
- `client_transfers` `2026-04-12`: `WARNING`, `stage = 0`, `reject = 2`, `core = 0`
- `client_transfers` `2026-04-12`: reject rows should distinguish `invalid country_code format` from `unsupported country_code`

## 7. Optional AUTO Waiting Checks

These cases are time-dependent and should be treated as extra manual checks.

In `SQL>`:

```sql
BEGIN
  dwh.prc_load_client_transfers(
    p_date => DATE '2026-04-07',
    p_run_mode => 'AUTO'
  );
END;
/

BEGIN
  dwh.prc_load_clients(
    p_date => DATE '2026-04-07',
    p_run_mode => 'AUTO'
  );
END;
/
```

Expected result before `2026-04-07 12:00` database time:
- no exception is raised,
- `CTL_PROCESS_RUN` shows `status = WAITING` for both `LOAD_CLIENT_TRANSFERS` and `LOAD_CLIENTS`,
- `reason_code = WAITING_FOR_OK`,
- `next_retry_ts` is populated.

Expected result if the procedure is invoked again after `2026-04-07 12:00` database time:
- `dwh.prc_load_client_transfers` raises `ORA-20010`,
- `dwh.prc_load_clients` raises `ORA-20110`,
- `CTL_PROCESS_RUN` shows `status = FAILED`,
- `reason_code = MISSING_OK_AFTER_CUTOFF`.

This is a procedure-level behavior check.
The bundled scheduler does not yet consume `next_retry_ts` and does not automatically perform that retry.

Useful query:

```sql
SELECT
  process_name,
  business_date,
  run_mode,
  status,
  reason_code,
  retry_count,
  next_retry_ts
FROM dwh.ctl_process_run
WHERE process_name = 'LOAD_CLIENT_TRANSFERS'
  AND business_date = DATE '2026-04-07';

SELECT
  process_name,
  business_date,
  run_mode,
  status,
  reason_code,
  retry_count,
  next_retry_ts
FROM dwh.ctl_process_run
WHERE process_name = 'LOAD_CLIENTS'
  AND business_date = DATE '2026-04-07';
```

## Known Current Gaps

- `AUTO` mode stores `next_retry_ts`, but the scheduler does not yet consume it as a real retry loop,
- `CTL_PROCESS_RUN` stores the current state for one `process_name` and `business_date`, not full attempt history,
- `PROCESSING` remains a transitional internal status and is not asserted as a final smoke outcome,
- business-key duplicates for `clients` and `client_transfers` are treated as input rejects before the `core` refresh,
- the client pipeline intentionally persists only the six demo AML helper fields added in `Phase-04`; the rest of the broader AML client contract remains out of scope for this demo,
- inbound archiving and fuller operational logging stay outside the current scope.
