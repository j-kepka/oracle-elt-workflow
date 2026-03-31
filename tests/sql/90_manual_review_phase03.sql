-- Manual smoke helper for Phase-03.
-- Run this as DWH after bootstrap and after extdata/work permissions are fixed.
-- First review the input files on the host:
--   note: transfer core load now expects a client snapshot for the same business_date
--   extdata/client_transfers_20260326.csv + .ok -> 20 valid and 0 reject
--   extdata/client_transfers_20260325.csv + .ok -> 5 valid and 7 reject
--   extdata/client_transfers_20260324.ok only -> missing data file case
--   extdata/client_transfers_20260327.csv + .ok -> count mismatch case
--   extdata/client_transfers_20260328.csv + .ok -> 0 valid and 2 reject due to duplicate transfer_id
--   extdata/client_transfers_20260407.csv without .ok -> AUTO waiting / cutoff review

SET SERVEROUTPUT ON;
SET LINESIZE 200;
SET PAGESIZE 100;

PROMPT ==================================================
PROMPT Manual smoke review for Phase-03
PROMPT Review the input and ready files first, then compare them with the tables.
PROMPT ==================================================

DELETE FROM dwh.ctl_process_run
WHERE process_name = 'LOAD_CLIENT_TRANSFERS'
  AND business_date IN (
    DATE '2026-03-24',
    DATE '2026-03-25',
    DATE '2026-03-26',
    DATE '2026-03-27',
    DATE '2026-03-28',
    DATE '2026-04-07'
  );

DELETE FROM dwh.stg_client_transfers
WHERE business_date IN (
    DATE '2026-03-24',
    DATE '2026-03-25',
    DATE '2026-03-26',
    DATE '2026-03-27',
    DATE '2026-03-28',
    DATE '2026-04-07'
);

DELETE FROM dwh.stg_client_transfers_reject
WHERE business_date IN (
    DATE '2026-03-24',
    DATE '2026-03-25',
    DATE '2026-03-26',
    DATE '2026-03-27',
    DATE '2026-03-28',
    DATE '2026-04-07'
);

DELETE FROM dwh.core_client_transfers
WHERE business_date IN (
    DATE '2026-03-24',
    DATE '2026-03-25',
    DATE '2026-03-26',
    DATE '2026-03-27',
    DATE '2026-03-28',
    DATE '2026-04-07'
);

COMMIT;

PROMPT
PROMPT Prepare client snapshots required by the transfer FK
PROMPT Expected result: 2026-03-26 DONE, 2026-03-25 WARNING, 2026-03-28 DONE

BEGIN
  dwh.prc_load_clients(
    p_date => DATE '2026-03-26',
    p_run_mode => 'MANUAL'
  );
  dwh.prc_load_clients(
    p_date => DATE '2026-03-25',
    p_run_mode => 'MANUAL'
  );
  dwh.prc_load_clients(
    p_date => DATE '2026-03-28',
    p_run_mode => 'MANUAL'
  );
END;
/

PROMPT
PROMPT Load business_date 2026-03-26
PROMPT Expected result: status=DONE, stage=20, reject=0, core=20

BEGIN
  dwh.prc_load_client_transfers(
    p_date => DATE '2026-03-26',
    p_run_mode => 'MANUAL'
  );
END;
/

SELECT 'stage' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_client_transfers
WHERE business_date = DATE '2026-03-26'
UNION ALL
SELECT 'reject' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_client_transfers_reject
WHERE business_date = DATE '2026-03-26'
UNION ALL
SELECT 'core' AS table_name, COUNT(*) AS row_count
FROM dwh.core_client_transfers
WHERE business_date = DATE '2026-03-26';

SELECT
  business_date,
  run_mode,
  status,
  reason_code,
  expected_row_count,
  stage_row_count,
  reject_row_count,
  core_row_count
FROM dwh.ctl_process_run
WHERE process_name = 'LOAD_CLIENT_TRANSFERS'
  AND business_date = DATE '2026-03-26';

SELECT
  business_date,
  source_row_num,
  transfer_id,
  client_id,
  amount,
  currency_code,
  transfer_status,
  channel,
  country_code
FROM dwh.stg_client_transfers
WHERE business_date = DATE '2026-03-26'
ORDER BY source_row_num
FETCH FIRST 10 ROWS ONLY;

PROMPT
PROMPT Load business_date 2026-03-25
PROMPT Expected result: status=WARNING, stage=5, reject=7, core=5

BEGIN
  dwh.prc_load_client_transfers(
    p_date => DATE '2026-03-25',
    p_run_mode => 'MANUAL'
  );
END;
/

SELECT 'stage' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_client_transfers
WHERE business_date = DATE '2026-03-25'
UNION ALL
SELECT 'reject' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_client_transfers_reject
WHERE business_date = DATE '2026-03-25'
UNION ALL
SELECT 'core' AS table_name, COUNT(*) AS row_count
FROM dwh.core_client_transfers
WHERE business_date = DATE '2026-03-25';

SELECT
  business_date,
  run_mode,
  status,
  reason_code,
  expected_row_count,
  stage_row_count,
  reject_row_count,
  core_row_count
FROM dwh.ctl_process_run
WHERE process_name = 'LOAD_CLIENT_TRANSFERS'
  AND business_date = DATE '2026-03-25';

SELECT
  business_date,
  source_row_num,
  transfer_id,
  client_id,
  amount,
  currency_code,
  transfer_status,
  channel,
  country_code
FROM dwh.stg_client_transfers
WHERE business_date = DATE '2026-03-25'
ORDER BY source_row_num;

SELECT
  business_date,
  source_file_name,
  source_row_num,
  transfer_id_raw,
  amount_raw,
  currency_code_raw,
  transfer_ts_raw,
  transfer_status_raw,
  channel_raw,
  country_code_raw,
  reject_reason
FROM dwh.stg_client_transfers_reject
WHERE business_date = DATE '2026-03-25'
ORDER BY source_row_num;

PROMPT
PROMPT Load business_date 2026-03-27
PROMPT Expected result: ORA-20015, status=FAILED, reason_code=OK_COUNT_MISMATCH

BEGIN
  BEGIN
    dwh.prc_load_client_transfers(
      p_date => DATE '2026-03-27',
      p_run_mode => 'MANUAL'
    );
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Expected error for 2026-03-27: ' || SQLCODE || ' ' || SQLERRM);
  END;
END;
/

SELECT
  business_date,
  run_mode,
  status,
  reason_code,
  expected_row_count,
  stage_row_count,
  reject_row_count,
  core_row_count
FROM dwh.ctl_process_run
WHERE process_name = 'LOAD_CLIENT_TRANSFERS'
  AND business_date = DATE '2026-03-27';

PROMPT
PROMPT Load business_date 2026-03-28
PROMPT Expected result: status=WARNING, stage=0, reject=2, core=0

BEGIN
  dwh.prc_load_client_transfers(
    p_date => DATE '2026-03-28',
    p_run_mode => 'MANUAL'
  );
END;
/

SELECT 'stage' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_client_transfers
WHERE business_date = DATE '2026-03-28'
UNION ALL
SELECT 'reject' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_client_transfers_reject
WHERE business_date = DATE '2026-03-28'
UNION ALL
SELECT 'core' AS table_name, COUNT(*) AS row_count
FROM dwh.core_client_transfers
WHERE business_date = DATE '2026-03-28';

SELECT
  business_date,
  run_mode,
  status,
  reason_code,
  expected_row_count,
  stage_row_count,
  reject_row_count,
  core_row_count
FROM dwh.ctl_process_run
WHERE process_name = 'LOAD_CLIENT_TRANSFERS'
  AND business_date = DATE '2026-03-28';

SELECT
  business_date,
  source_file_name,
  source_row_num,
  transfer_id_raw,
  client_id_raw,
  reject_reason
FROM dwh.stg_client_transfers_reject
WHERE business_date = DATE '2026-03-28'
ORDER BY source_row_num;

PROMPT
PROMPT Load business_date 2026-03-24
PROMPT Expected result: ORA-20014, status=FAILED, reason_code=MISSING_DATA_FILE

BEGIN
  BEGIN
    dwh.prc_load_client_transfers(
      p_date => DATE '2026-03-24',
      p_run_mode => 'MANUAL'
    );
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Expected error for 2026-03-24: ' || SQLCODE || ' ' || SQLERRM);
  END;
END;
/

SELECT
  business_date,
  run_mode,
  status,
  reason_code,
  expected_row_count,
  stage_row_count,
  reject_row_count,
  core_row_count
FROM dwh.ctl_process_run
WHERE process_name = 'LOAD_CLIENT_TRANSFERS'
  AND business_date = DATE '2026-03-24';

PROMPT
PROMPT Rerun business_date 2026-03-26
PROMPT Expected result: status=DONE, stage=20, reject=0, core=20 again (no growth across rerun)

BEGIN
  dwh.prc_load_client_transfers(
    p_date => DATE '2026-03-26',
    p_run_mode => 'MANUAL'
  );
END;
/

SELECT 'stage' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_client_transfers
WHERE business_date = DATE '2026-03-26'
UNION ALL
SELECT 'reject' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_client_transfers_reject
WHERE business_date = DATE '2026-03-26'
UNION ALL
SELECT 'core' AS table_name, COUNT(*) AS row_count
FROM dwh.core_client_transfers
WHERE business_date = DATE '2026-03-26';

SELECT
  business_date,
  run_mode,
  status,
  reason_code,
  expected_row_count,
  stage_row_count,
  reject_row_count,
  core_row_count
FROM dwh.ctl_process_run
WHERE process_name = 'LOAD_CLIENT_TRANSFERS'
  AND business_date = DATE '2026-03-26';

PROMPT
PROMPT Rerun business_date 2026-03-25
PROMPT Expected result: status=WARNING, stage=5, reject=7, core=5 again (no growth across rerun)

BEGIN
  dwh.prc_load_client_transfers(
    p_date => DATE '2026-03-25',
    p_run_mode => 'MANUAL'
  );
END;
/

SELECT 'stage' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_client_transfers
WHERE business_date = DATE '2026-03-25'
UNION ALL
SELECT 'reject' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_client_transfers_reject
WHERE business_date = DATE '2026-03-25'
UNION ALL
SELECT 'core' AS table_name, COUNT(*) AS row_count
FROM dwh.core_client_transfers
WHERE business_date = DATE '2026-03-25';

SELECT
  business_date,
  run_mode,
  status,
  reason_code,
  expected_row_count,
  stage_row_count,
  reject_row_count,
  core_row_count
FROM dwh.ctl_process_run
WHERE process_name = 'LOAD_CLIENT_TRANSFERS'
  AND business_date = DATE '2026-03-25';

PROMPT
PROMPT Optional AUTO status review for 2026-04-07
PROMPT Expected result before 2026-04-07 12:00 DB time: status=WAITING, reason_code=WAITING_FOR_OK
PROMPT Expected result on or after 2026-04-07 12:00 DB time when invoked again: ORA-20010, status=FAILED, reason_code=MISSING_OK_AFTER_CUTOFF

BEGIN
  BEGIN
    dwh.prc_load_client_transfers(
      p_date => DATE '2026-04-07',
      p_run_mode => 'AUTO'
    );
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('AUTO review output for 2026-04-07: ' || SQLCODE || ' ' || SQLERRM);
  END;
END;
/

SELECT
  business_date,
  run_mode,
  status,
  reason_code,
  retry_count,
  next_retry_ts
FROM dwh.ctl_process_run
WHERE process_name = 'LOAD_CLIENT_TRANSFERS'
  AND business_date = DATE '2026-04-07';
