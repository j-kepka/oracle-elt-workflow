-- Extended manual smoke helper for additional post-Phase-04 regression cases.
-- Run this as DWH after bootstrap and after extdata/work permissions are fixed.
-- First review the input files on the host:
--   extdata/clients_20260408.csv without .ok -> MANUAL missing ready file case
--   extdata/client_transfers_20260408.csv without .ok -> MANUAL missing ready file case
--   extdata/clients_20260409.csv + .ok -> boundary + normalization + sharper client DQ semantics
--   extdata/clients_20260410.csv + .ok -> valid parents for transfer normalization case
--   extdata/client_transfers_20260410.csv + .ok -> normalization for currency/status/channel/country
--   extdata/client_transfers_20260411.csv + .ok -> missing client snapshot case
--   extdata/client_transfers_20260412.csv + .ok -> invalid vs unsupported transfer country_code

SET SERVEROUTPUT ON;
SET LINESIZE 260;
SET PAGESIZE 100;

PROMPT ==================================================
PROMPT Extended manual smoke review
PROMPT Additional cases: rerun safety, manual missing .ok, normalization, sharper DQ rules, and missing client snapshot.
PROMPT ==================================================

DELETE FROM dwh.ctl_process_run
WHERE process_name IN ('LOAD_CLIENTS', 'LOAD_CLIENT_TRANSFERS')
  AND business_date IN (
    DATE '2026-04-08',
    DATE '2026-04-09',
    DATE '2026-04-10',
    DATE '2026-04-11',
    DATE '2026-04-12'
  );

DELETE FROM dwh.stg_client_transfers
WHERE business_date IN (
    DATE '2026-04-08',
    DATE '2026-04-10',
    DATE '2026-04-11',
    DATE '2026-04-12'
);

DELETE FROM dwh.stg_client_transfers_reject
WHERE business_date IN (
    DATE '2026-04-08',
    DATE '2026-04-10',
    DATE '2026-04-11',
    DATE '2026-04-12'
);

DELETE FROM dwh.core_client_transfers
WHERE business_date IN (
    DATE '2026-04-08',
    DATE '2026-04-10',
    DATE '2026-04-11',
    DATE '2026-04-12'
);

DELETE FROM dwh.stg_clients
WHERE business_date IN (
    DATE '2026-04-08',
    DATE '2026-04-09',
    DATE '2026-04-10',
    DATE '2026-04-11',
    DATE '2026-04-12'
);

DELETE FROM dwh.stg_clients_reject
WHERE business_date IN (
    DATE '2026-04-08',
    DATE '2026-04-09',
    DATE '2026-04-10',
    DATE '2026-04-11',
    DATE '2026-04-12'
);

DELETE FROM dwh.core_clients
WHERE business_date IN (
    DATE '2026-04-08',
    DATE '2026-04-09',
    DATE '2026-04-10',
    DATE '2026-04-11',
    DATE '2026-04-12'
);

COMMIT;

PROMPT
PROMPT Load clients business_date 2026-04-09
PROMPT Expected result: status=WARNING, stage=2, reject=8, core=2
PROMPT Normalization check: country_code/client_status/kyc_status should be uppercased and trimmed in stage/core.
PROMPT DQ check: rejects should now distinguish invalid phone/email format and unsupported kyc_status.
PROMPT Semantic check: duplicate + invalid same client_id should reject only the invalid row, while the valid row still reaches stage/core.

BEGIN
  dwh.prc_load_clients(
    p_date => DATE '2026-04-09',
    p_run_mode => 'MANUAL'
  );
END;
/

SELECT 'stage' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_clients
WHERE business_date = DATE '2026-04-09'
UNION ALL
SELECT 'reject' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_clients_reject
WHERE business_date = DATE '2026-04-09'
UNION ALL
SELECT 'core' AS table_name, COUNT(*) AS row_count
FROM dwh.core_clients
WHERE business_date = DATE '2026-04-09';

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
  AND business_date = DATE '2026-04-09';

/*
SELECT
  source_row_num,
  client_id,
  client_type,
  country_code,
  kyc_status,
  risk_score,
  client_status
FROM dwh.stg_clients
WHERE business_date = DATE '2026-04-09'
ORDER BY source_row_num;

SELECT
  client_id,
  client_type,
  country_code,
  kyc_status,
  risk_score,
  client_status
FROM dwh.core_clients
WHERE business_date = DATE '2026-04-09'
ORDER BY client_id;

SELECT
  source_row_num,
  client_id_raw,
  country_code_raw,
  kyc_status_raw,
  risk_score_raw,
  client_status_raw,
  reject_reason
FROM dwh.stg_clients_reject
WHERE business_date = DATE '2026-04-09'
ORDER BY source_row_num;
*/

PROMPT
PROMPT Load clients business_date 2026-04-10
PROMPT Expected result: status=DONE, stage=2, reject=0, core=2
PROMPT These rows serve as same-day parent snapshots for the transfer normalization case.

BEGIN
  dwh.prc_load_clients(
    p_date => DATE '2026-04-10',
    p_run_mode => 'MANUAL'
  );
END;
/

SELECT 'stage' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_clients
WHERE business_date = DATE '2026-04-10'
UNION ALL
SELECT 'reject' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_clients_reject
WHERE business_date = DATE '2026-04-10'
UNION ALL
SELECT 'core' AS table_name, COUNT(*) AS row_count
FROM dwh.core_clients
WHERE business_date = DATE '2026-04-10';

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
  AND business_date = DATE '2026-04-10';

PROMPT
PROMPT Load transfers business_date 2026-04-10
PROMPT Expected result: status=DONE, stage=2, reject=0, core=2
PROMPT Normalization check: currency_code/transfer_status/channel/country_code should be uppercased and trimmed in stage/core.

BEGIN
  dwh.prc_load_client_transfers(
    p_date => DATE '2026-04-10',
    p_run_mode => 'MANUAL'
  );
END;
/

SELECT 'stage' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_client_transfers
WHERE business_date = DATE '2026-04-10'
UNION ALL
SELECT 'reject' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_client_transfers_reject
WHERE business_date = DATE '2026-04-10'
UNION ALL
SELECT 'core' AS table_name, COUNT(*) AS row_count
FROM dwh.core_client_transfers
WHERE business_date = DATE '2026-04-10';

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
  AND business_date = DATE '2026-04-10';

/*
SELECT
  source_row_num,
  transfer_id,
  currency_code,
  transfer_status,
  channel,
  country_code
FROM dwh.stg_client_transfers
WHERE business_date = DATE '2026-04-10'
ORDER BY source_row_num;

SELECT
  transfer_id,
  currency_code,
  transfer_status,
  channel,
  country_code
FROM dwh.core_client_transfers
WHERE business_date = DATE '2026-04-10'
ORDER BY transfer_id;
*/

PROMPT
PROMPT Load clients business_date 2026-04-08 in MANUAL without .ok
PROMPT Expected result: ORA-20110, status=FAILED, reason_code=MISSING_OK_MANUAL

BEGIN
  BEGIN
    dwh.prc_load_clients(
      p_date => DATE '2026-04-08',
      p_run_mode => 'MANUAL'
    );
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Expected error for client MANUAL missing .ok on 2026-04-08: ' || SQLCODE || ' ' || SQLERRM);
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
  AND business_date = DATE '2026-04-08';

PROMPT
PROMPT Load transfers business_date 2026-04-08 in MANUAL without .ok
PROMPT Expected result: ORA-20010, status=FAILED, reason_code=MISSING_OK_MANUAL

BEGIN
  BEGIN
    dwh.prc_load_client_transfers(
      p_date => DATE '2026-04-08',
      p_run_mode => 'MANUAL'
    );
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Expected error for transfer MANUAL missing .ok on 2026-04-08: ' || SQLCODE || ' ' || SQLERRM);
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
  AND business_date = DATE '2026-04-08';

PROMPT
PROMPT Load transfers business_date 2026-04-11 without matching client snapshot
PROMPT Expected result: ORA-20018, status=FAILED, reason_code=MISSING_CLIENT_SNAPSHOT

BEGIN
  BEGIN
    dwh.prc_load_client_transfers(
      p_date => DATE '2026-04-11',
      p_run_mode => 'MANUAL'
    );
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Expected error for missing client snapshot on 2026-04-11: ' || SQLCODE || ' ' || SQLERRM);
  END;
END;
/

SELECT 'stage' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_client_transfers
WHERE business_date = DATE '2026-04-11'
UNION ALL
SELECT 'reject' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_client_transfers_reject
WHERE business_date = DATE '2026-04-11'
UNION ALL
SELECT 'core' AS table_name, COUNT(*) AS row_count
FROM dwh.core_client_transfers
WHERE business_date = DATE '2026-04-11';

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
  AND business_date = DATE '2026-04-11';

PROMPT
PROMPT Load transfers business_date 2026-04-12 with invalid vs unsupported country_code
PROMPT Expected result: status=WARNING, stage=0, reject=2, core=0
PROMPT Reject reasons should distinguish invalid country_code format from unsupported country_code.

BEGIN
  dwh.prc_load_client_transfers(
    p_date => DATE '2026-04-12',
    p_run_mode => 'MANUAL'
  );
END;
/

SELECT 'stage' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_client_transfers
WHERE business_date = DATE '2026-04-12'
UNION ALL
SELECT 'reject' AS table_name, COUNT(*) AS row_count
FROM dwh.stg_client_transfers_reject
WHERE business_date = DATE '2026-04-12'
UNION ALL
SELECT 'core' AS table_name, COUNT(*) AS row_count
FROM dwh.core_client_transfers
WHERE business_date = DATE '2026-04-12';

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
  AND business_date = DATE '2026-04-12';

/*
SELECT
  source_row_num,
  transfer_id_raw,
  country_code_raw,
  reject_reason
FROM dwh.stg_client_transfers_reject
WHERE business_date = DATE '2026-04-12'
ORDER BY source_row_num;
*/
