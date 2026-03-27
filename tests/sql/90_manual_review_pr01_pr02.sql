-- Manual smoke helper for PR-01 + PR-02.
-- Run this as DWH after bootstrap and after extdata/work permissions are fixed.
-- First review the input files on the host:
--   extdata/client_transfers_20260326.csv -> 20 data rows, expected 20 valid and 0 reject
--   extdata/client_transfers_20260325.csv -> 12 data rows, expected 5 valid and 7 reject

SET SERVEROUTPUT ON;
SET LINESIZE 200;
SET PAGESIZE 100;

PROMPT ==================================================
PROMPT Manual smoke review for PR-01 + PR-02
PROMPT Review the input files first, then compare them with the tables.
PROMPT ==================================================

DELETE FROM dwh.stg_client_transfers
WHERE business_date IN (
  DATE '2026-03-25',
  DATE '2026-03-26'
);

DELETE FROM dwh.stg_client_transfers_reject
WHERE business_date IN (
  DATE '2026-03-25',
  DATE '2026-03-26'
);

DELETE FROM dwh.core_client_transfers
WHERE business_date IN (
  DATE '2026-03-25',
  DATE '2026-03-26'
);

COMMIT;

PROMPT
PROMPT Load business_date 2026-03-26
PROMPT Expected result: stage=20, reject=0, core=20

BEGIN
  dwh.prc_load_client_transfers(
    p_date => DATE '2026-03-26'
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
PROMPT Expected result: stage=5, reject=7, core=5

BEGIN
  dwh.prc_load_client_transfers(
    p_date => DATE '2026-03-25'
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
PROMPT Optional missing-file check:
PROMPT BEGIN
PROMPT   dwh.prc_load_client_transfers(DATE '2026-03-24');
PROMPT END;
PROMPT /
PROMPT Expected result: ORA-20010 and no rows for 2026-03-24.
