-- Manual smoke helper for the client snapshot load.
-- Run this as DWH after bootstrap and after extdata/work permissions are fixed.
-- source_row_num reflects the physical CSV line number, so the first data row is 2.
-- The current demo client contract also carries synthetic AML helper fields:
-- document_id, tax_id, phone_number, email, kyc_status, risk_score.
-- First review the input files on the host:
--   extdata/clients_20260326.csv + .ok -> 12 valid and 0 reject
--   extdata/clients_20260325.csv + .ok -> 5 valid and 4 reject
--   extdata/clients_20260324.ok only -> missing data file case
--   extdata/clients_20260327.csv + .ok -> .ok appears to include the header row
--   extdata/clients_20260328.csv + .ok -> 2 valid parents for transfer duplicate-key smoke
--   extdata/clients_20260329.csv + .ok -> 0 valid and 2 reject due to duplicate client_id
--   extdata/clients_20260407.csv without .ok -> AUTO waiting / cutoff review

SET SERVEROUTPUT ON;
SET LINESIZE 260;
SET PAGESIZE 100;

PROMPT ==================================================
PROMPT Manual smoke review for the client snapshot load
PROMPT Review the input and ready files first, then compare them with the tables.
PROMPT ==================================================

DELETE FROM dwh.ctl_process_run
WHERE process_name = 'LOAD_CLIENTS'
  AND business_date IN (
    DATE '2026-03-24',
    DATE '2026-03-25',
    DATE '2026-03-26',
    DATE '2026-03-27',
    DATE '2026-03-28',
    DATE '2026-03-29',
    DATE '2026-04-07'
  );

DELETE FROM dwh.stg_clients
WHERE business_date IN (
    DATE '2026-03-24',
    DATE '2026-03-25',
    DATE '2026-03-26',
    DATE '2026-03-27',
    DATE '2026-03-28',
    DATE '2026-03-29',
    DATE '2026-04-07'
);

DELETE FROM dwh.stg_clients_reject
WHERE business_date IN (
    DATE '2026-03-24',
    DATE '2026-03-25',
    DATE '2026-03-26',
    DATE '2026-03-27',
    DATE '2026-03-28',
    DATE '2026-03-29',
    DATE '2026-04-07'
);

DELETE FROM dwh.core_client_transfers
WHERE business_date IN (
    DATE '2026-03-24',
    DATE '2026-03-25',
    DATE '2026-03-26',
    DATE '2026-03-27',
    DATE '2026-03-28',
    DATE '2026-03-29',
    DATE '2026-04-07'
);

DELETE FROM dwh.core_clients
WHERE business_date IN (
    DATE '2026-03-24',
    DATE '2026-03-25',
    DATE '2026-03-26',
    DATE '2026-03-27',
    DATE '2026-03-28',
    DATE '2026-03-29',
    DATE '2026-04-07'
);

COMMIT;

PROMPT
PROMPT Load business_date 2026-03-26
PROMPT Expected result: status=DONE, stage=12, reject=0, core=12

BEGIN
  dwh.prc_load_clients(
    p_date => DATE '2026-03-26',
    p_run_mode => 'MANUAL'
  );
END;
/

SELECT 'stage' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_clients
WHERE business_date = DATE '2026-03-26'
UNION ALL
SELECT 'reject' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_clients_reject
WHERE business_date = DATE '2026-03-26'
UNION ALL
SELECT 'core' AS table_name, COUNT(*) AS row_count
FROM dwh.core_clients
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
WHERE process_name = 'LOAD_CLIENTS'
  AND business_date = DATE '2026-03-26';

/*
SELECT
  business_date,
  source_row_num,
  client_id,
  client_type,
  full_name,
  document_id,
  tax_id,
  country_code,
  kyc_status,
  risk_score,
  pep_flag,
  high_risk_flag,
  client_status
FROM dwh.stg_clients
WHERE business_date = DATE '2026-03-26'
ORDER BY source_row_num
FETCH FIRST 10 ROWS ONLY;

SELECT
  business_date,
  client_id,
  document_id,
  tax_id,
  phone_number,
  email,
  kyc_status,
  risk_score
FROM dwh.core_clients
WHERE business_date = DATE '2026-03-26'
ORDER BY client_id
FETCH FIRST 10 ROWS ONLY;
*/

PROMPT
PROMPT Load business_date 2026-03-25
PROMPT Expected result: status=WARNING, stage=5, reject=4, core=5

BEGIN
  dwh.prc_load_clients(
    p_date => DATE '2026-03-25',
    p_run_mode => 'MANUAL'
  );
END;
/

SELECT 'stage' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_clients
WHERE business_date = DATE '2026-03-25'
UNION ALL
SELECT 'reject' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_clients_reject
WHERE business_date = DATE '2026-03-25'
UNION ALL
SELECT 'core' AS table_name, COUNT(*) AS row_count
FROM dwh.core_clients
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
WHERE process_name = 'LOAD_CLIENTS'
  AND business_date = DATE '2026-03-25';

/*
SELECT
  business_date,
  source_row_num,
  client_id,
  client_type,
  full_name,
  document_id,
  tax_id,
  country_code,
  kyc_status,
  risk_score,
  pep_flag,
  high_risk_flag,
  client_status
FROM dwh.stg_clients
WHERE business_date = DATE '2026-03-25'
ORDER BY source_row_num;

SELECT
  business_date,
  source_file_name,
  source_row_num,
  client_id_raw,
  client_type_raw,
  full_name_raw,
  date_of_birth_raw,
  document_id_raw,
  registration_no_raw,
  tax_id_raw,
  country_code_raw,
  phone_number_raw,
  email_raw,
  pep_flag_raw,
  high_risk_flag_raw,
  kyc_status_raw,
  risk_score_raw,
  client_status_raw,
  reject_reason
FROM dwh.stg_clients_reject
WHERE business_date = DATE '2026-03-25'
ORDER BY source_row_num;
*/

PROMPT
PROMPT Load business_date 2026-03-27
PROMPT Expected result: ORA-20117, status=FAILED, reason_code=OK_COUNT_INCLUDES_HEADER

BEGIN
  BEGIN
    dwh.prc_load_clients(
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
WHERE process_name = 'LOAD_CLIENTS'
  AND business_date = DATE '2026-03-27';

PROMPT
PROMPT Load business_date 2026-03-28
PROMPT Expected result: status=DONE, stage=2, reject=0, core=2

BEGIN
  dwh.prc_load_clients(
    p_date => DATE '2026-03-28',
    p_run_mode => 'MANUAL'
  );
END;
/

SELECT 'stage' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_clients
WHERE business_date = DATE '2026-03-28'
UNION ALL
SELECT 'reject' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_clients_reject
WHERE business_date = DATE '2026-03-28'
UNION ALL
SELECT 'core' AS table_name, COUNT(*) AS row_count
FROM dwh.core_clients
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
WHERE process_name = 'LOAD_CLIENTS'
  AND business_date = DATE '2026-03-28';

PROMPT
PROMPT Load business_date 2026-03-29
PROMPT Expected result: status=WARNING, stage=0, reject=2, core=0

BEGIN
  dwh.prc_load_clients(
    p_date => DATE '2026-03-29',
    p_run_mode => 'MANUAL'
  );
END;
/

SELECT 'stage' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_clients
WHERE business_date = DATE '2026-03-29'
UNION ALL
SELECT 'reject' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_clients_reject
WHERE business_date = DATE '2026-03-29'
UNION ALL
SELECT 'core' AS table_name, COUNT(*) AS row_count
FROM dwh.core_clients
WHERE business_date = DATE '2026-03-29';

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
WHERE process_name = 'LOAD_CLIENTS'
  AND business_date = DATE '2026-03-29';

/*
SELECT
  business_date,
  source_file_name,
  source_row_num,
  client_id_raw,
  full_name_raw,
  reject_reason
FROM dwh.stg_clients_reject
WHERE business_date = DATE '2026-03-29'
ORDER BY source_row_num;
*/

PROMPT
PROMPT Load business_date 2026-03-24
PROMPT Expected result: ORA-20114, status=FAILED, reason_code=MISSING_DATA_FILE

BEGIN
  BEGIN
    dwh.prc_load_clients(
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
WHERE process_name = 'LOAD_CLIENTS'
  AND business_date = DATE '2026-03-24';

PROMPT
PROMPT Rerun business_date 2026-03-26
PROMPT Expected result: status=DONE, stage=12, reject=0, core=12 again (no growth across rerun)

BEGIN
  dwh.prc_load_clients(
    p_date => DATE '2026-03-26',
    p_run_mode => 'MANUAL'
  );
END;
/

SELECT 'stage' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_clients
WHERE business_date = DATE '2026-03-26'
UNION ALL
SELECT 'reject' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_clients_reject
WHERE business_date = DATE '2026-03-26'
UNION ALL
SELECT 'core' AS table_name, COUNT(*) AS row_count
FROM dwh.core_clients
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
WHERE process_name = 'LOAD_CLIENTS'
  AND business_date = DATE '2026-03-26';

PROMPT
PROMPT Rerun business_date 2026-03-25
PROMPT Expected result: status=WARNING, stage=5, reject=4, core=5 again (no growth across rerun)

BEGIN
  dwh.prc_load_clients(
    p_date => DATE '2026-03-25',
    p_run_mode => 'MANUAL'
  );
END;
/

SELECT 'stage' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_clients
WHERE business_date = DATE '2026-03-25'
UNION ALL
SELECT 'reject' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_clients_reject
WHERE business_date = DATE '2026-03-25'
UNION ALL
SELECT 'core' AS table_name, COUNT(*) AS row_count
FROM dwh.core_clients
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
WHERE process_name = 'LOAD_CLIENTS'
  AND business_date = DATE '2026-03-25';

PROMPT
PROMPT Optional AUTO status review for 2026-04-07
PROMPT Expected result before 2026-04-07 12:00 DB time: status=WAITING, reason_code=WAITING_FOR_OK
PROMPT Expected result on or after 2026-04-07 12:00 DB time when invoked again: ORA-20110, status=FAILED, reason_code=MISSING_OK_AFTER_CUTOFF

BEGIN
  BEGIN
    dwh.prc_load_clients(
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
WHERE process_name = 'LOAD_CLIENTS'
  AND business_date = DATE '2026-04-07';
