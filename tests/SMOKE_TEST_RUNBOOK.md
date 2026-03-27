# Smoke Test Runbook

Manual smoke for the current PR-01 + PR-02 scope.

This runbook checks:
- Oracle container startup,
- schema reset and bootstrap,
- external table access for dated CSV files,
- `business_date`-driven loading,
- input validation and reject handling,
- end-to-end load into `stage` and `core`,
- manual review of input files and loaded rows.

## Scope

Now:
- dated `client_transfers_YYYYMMDD.csv` input files,
- `p_date` / `business_date` driven loads,
- raw-to-typed validation in SQL,
- reject storage in `STG_CLIENT_TRANSFERS_REJECT`,
- end-to-end load into `stage` and `core`,
- manual smoke review through `tests/sql/90_manual_review_pr01_pr02.sql`.

Not yet:
- `.ok` readiness files,
- business cutoff handling,
- process status/control tables,
- attempt history or ETL run logs.

## Prerequisites

- Docker is installed and usable by the current Linux user.
- The repository is available on the host at `/home/kempez/projects/oracle-elt-workflow`.
- Oracle image `gvenzl/oracle-free@sha256:62aad247879f5d4ca4a37ecc068ef6a5feb9e9bea789501b6a82d4814d14bbb3` is available locally or can be pulled.

## Test Data Disclaimer

- Files and database records in this repo are synthetic test data.
- They may look like banking data, but they are not intended to represent real customer or account data.
- Some deeper domain validations are intentionally out of scope for this stage of the project.

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
- tables, procedure and scheduler job are created,
- final line shows `Bootstrap completed.`

## 4. Fix External Loader Work Directory Permissions

Run on the host:

```bash
cd /home/kempez/projects/oracle-elt-workflow

ORACLE_UID=$(docker exec oracle-free id -u oracle)
ORACLE_GID=$(docker exec oracle-free id -g oracle)

sudo mkdir -p extdata/work
sudo chown "${ORACLE_UID}:${ORACLE_GID}" extdata/work
sudo chmod 770 extdata/work
sudo chmod 755 extdata
sudo find extdata -maxdepth 1 -type f -name 'client_transfers_*.csv' -exec chmod 644 {} \;
```

Expected result:
- Oracle can read dated CSV files in `extdata/`,
- Oracle can write loader artifacts into `extdata/work/`.

## 5. Review Input Files First

Run on the host:

```bash
cd /home/kempez/projects/oracle-elt-workflow

expr $(wc -l < extdata/client_transfers_20260326.csv) - 1
expr $(wc -l < extdata/client_transfers_20260325.csv) - 1

sed -n '1,25p' extdata/client_transfers_20260326.csv
sed -n '1,20p' extdata/client_transfers_20260325.csv
```

What to check:
- line count without the header should be `20` for `client_transfers_20260326.csv`
- line count without the header should be `12` for `client_transfers_20260325.csv`
- `client_transfers_20260326.csv` has 20 data rows and should load cleanly.
- `client_transfers_20260325.csv` has 12 data rows.
- For `client_transfers_20260325.csv`, 5 rows should load to `stage` and `core`, and 7 rows should go to the reject table.

This is enough for the current project stage. The main idea is to compare the input file with the rows visible later in `stage`, `reject`, and `core`.

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
@/workspace/tests/sql/90_manual_review_pr01_pr02.sql
```

Expected result:
- the process is called for `2026-03-26` and `2026-03-25`,
- counts for `stage`, `reject`, and `core` are printed,
- sample rows from `stage` are printed for the clean file,
- reject rows with `reject_reason` are printed for the invalid-row file.

Review the output and check if it matches the input files:
- `2026-03-26`: `stage = 20`, `reject = 0`, `core = 20`
- `2026-03-25`: `stage = 5`, `reject = 7`, `core = 5`

## 7. Optional Missing-File Check

In `SQL>`:

```sql
BEGIN
  dwh.prc_load_client_transfers(DATE '2026-03-24');
END;
/
```

Expected result:
- the procedure raises `ORA-20010`,
- no rows are loaded into `stage`, `reject` or `core` for `2026-03-24`.

## Known Current Gaps

- `.ok` file readiness is not implemented yet,
- there is no business cutoff window yet,
- there is no process status/control table yet,
- loader promotion is still a simple per-date refresh, without later snapshot-safety additions.
