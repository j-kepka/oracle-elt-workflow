# Smoke Test Runbook

Manual smoke for the current PR-03 scope.

This runbook checks:
- Oracle container startup,
- schema reset and bootstrap,
- external table access for dated CSV files,
- `.ok` readiness checks,
- `business_date`-driven loading,
- input validation and reject handling,
- current process status in `CTL_PROCESS_RUN`,
- end-to-end load into `stage` and `core`.

## Scope

Current implementation supports:
- dated `client_transfers_YYYYMMDD.csv` input files,
- matching `client_transfers_YYYYMMDD.ok` ready files,
- `p_date` / `business_date` driven loads,
- raw-to-typed validation in SQL,
- reject storage in `STG_CLIENT_TRANSFERS_REJECT`,
- one current process status row in `CTL_PROCESS_RUN`,
- manual smoke coverage for the stable MVP scenarios and expected final statuses: `DONE`, `WARNING`, `FAILED`, plus a time-dependent `WAITING` check in `AUTO`,
- `MANUAL` and `AUTO` run modes,
- `WAITING` before the cutoff when `.ok` is still missing in `AUTO`,
- `FAILED` after the cutoff or for technical input problems.

Current implementation does not yet support:
- a scheduler-driven retry loop based on `next_retry_ts`,
- archive/done folders for processed inbound files,
- full attempt history or dedicated run-log tables.

## Prerequisites

- Docker is installed and usable by the current Linux user.
- The repository is available on the host at `/home/kempez/projects/oracle-elt-workflow`.
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
cd /home/kempez/projects/oracle-elt-workflow

docker volume create oracle-data
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

Wait until the database is ready:

```bash
docker logs -f oracle-free
```

## 2. Reset Project State

Run:

```bash
docker exec -it oracle-free bash
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
- stage, reject, core, control table, procedure and scheduler job are created,
- final line shows `Bootstrap completed.`

## 4. Fix External Loader Permissions

This permission setup is a sandbox-only compatibility baseline for local smoke tests with a bind-mounted `extdata/` directory.
It is not a production permission model; any non-sandbox use needs a separate security and infrastructure review.

Run on the host:

```bash
cd /home/kempez/projects/oracle-elt-workflow

ORACLE_UID=$(docker exec oracle-free id -u oracle)
ORACLE_GID=$(docker exec oracle-free id -g oracle)

sudo mkdir -p extdata/work
sudo chown "${ORACLE_UID}:${ORACLE_GID}" extdata/work
sudo chmod 770 extdata/work
sudo chmod 755 extdata
sudo find extdata -maxdepth 1 -type f \( -name 'client_transfers_*.csv' -o -name 'client_transfers_*.ok' \) -exec chmod 644 {} \;
```

Expected result:
- Oracle can read dated CSV and `.ok` files in `extdata/`,
- Oracle can write loader artifacts into `extdata/work/`.

## 5. Review Input And Ready Files

Run on the host:

```bash
cd /home/kempez/projects/oracle-elt-workflow


#cat extdata/client_transfers_20260326.ok
#cat extdata/client_transfers_20260325.ok
#cat extdata/client_transfers_20260324.ok
#cat extdata/client_transfers_20260327.ok
#cat extdata/client_transfers_20260328.ok
#sed -n '1,25p' extdata/client_transfers_20260326.csv
#sed -n '1,20p' extdata/client_transfers_20260325.csv
#sed -n '1,10p' extdata/client_transfers_20260327.csv
#sed -n '1,10p' extdata/client_transfers_20260328.csv
#sed -n '1,10p' extdata/client_transfers_20260407.csv
expr $(wc -l < extdata/client_transfers_20260326.csv) - 1
expr $(wc -l < extdata/client_transfers_20260325.csv) - 1
expr $(wc -l < extdata/client_transfers_20260327.csv) - 1
expr $(wc -l < extdata/client_transfers_20260328.csv) - 1
expr $(wc -l < extdata/client_transfers_20260407.csv) - 1
```

What to check:
- line count without the header should be `20` for `client_transfers_20260326.csv`,
- line count without the header should be `12` for `client_transfers_20260325.csv`,
- `client_transfers_20260326.ok` contains `20`,
- `client_transfers_20260325.ok` contains `12`,
- `client_transfers_20260324.ok` exists even though the matching data file does not,
- `client_transfers_20260327.ok` contains a value that does not match the data rows in `client_transfers_20260327.csv`,
- line count without the header should be `2` for `client_transfers_20260328.csv`,
- `client_transfers_20260328.ok` contains `2`,
- `client_transfers_20260328.csv` contains two valid rows for the same `transfer_id`,
- `client_transfers_20260407.csv` exists without a matching `.ok`.

## 6. Run Manual Smoke Review Helper

Run on the host:

```bash
docker exec -it oracle-free bash
sqlplus /nolog
```

In `SQL>`:

```sql
CONNECT dwh/"<DWH_PASSWORD>"@//localhost:1521/FREEPDB1
ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD';
@/workspace/tests/sql/90_manual_review_pr03.sql
```

Expected result:
- `2026-03-26` finishes with `DONE`,
- `2026-03-25` finishes with `WARNING`,
- `2026-03-27` raises `ORA-20015` and stores `FAILED` with `reason_code = OK_COUNT_MISMATCH`,
- `2026-03-28` raises `ORA-00001` during the `core` refresh and stores `FAILED` with `reason_code = UNEXPECTED_ERROR`,
- `2026-03-24` raises `ORA-20014` and stores `FAILED` with `reason_code = MISSING_DATA_FILE`.

Review the output and check if it matches:
- `2026-03-26`: `stage = 20`, `reject = 0`, `core = 20`
- `2026-03-25`: `stage = 5`, `reject = 7`, `core = 5`
- `2026-03-26`: control row should show `status = DONE`, `reason_code = LOAD_DONE`, `expected_row_count = 20`
- `2026-03-25`: control row should show `status = WARNING`, `reason_code = INPUT_VALIDATION_WARNING`, `expected_row_count = 12`
- `2026-03-28`: control row should show `status = FAILED`, `reason_code = UNEXPECTED_ERROR`, `expected_row_count = 2`

Note for failed cases:
- `CTL_PROCESS_RUN` may store attempted `stage_row_count` / `reject_row_count` values from the last load attempt before rollback,
- for the duplicate-key case on `2026-03-28`, the control row may show `stage_row_count = 2`, `reject_row_count = 0`, `core_row_count = 0`,
- `stage`, `reject`, and `core` tables should still remain empty for those `business_date` values after the failed run.

## 7. Optional AUTO Waiting Check

This case is time-dependent and should be treated as an extra manual check.

In `SQL>`:

```sql
BEGIN
  dwh.prc_load_client_transfers(
    p_date => DATE '2026-04-07',
    p_run_mode => 'AUTO'
  );
END;
/
```

Expected result before `2026-04-07 12:00` database time:
- no exception is raised,
- `CTL_PROCESS_RUN` shows `status = WAITING`,
- `reason_code = WAITING_FOR_OK`,
- `next_retry_ts` is populated.

Expected result if the procedure is invoked again after `2026-04-07 12:00` database time:
- the procedure raises `ORA-20010`,
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
```

## Known Current Gaps

- `AUTO` mode stores `next_retry_ts`, but the scheduler does not yet consume it as a real retry loop,
- `CTL_PROCESS_RUN` stores the current state for one `business_date`, not full attempt history,
- `PROCESSING` remains a transitional internal status and is not asserted as a final smoke outcome,
- inbound archiving and fuller operational logging stay outside the current PR-03 scope.
