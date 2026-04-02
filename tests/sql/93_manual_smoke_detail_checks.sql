-- Deterministic MANUAL smoke detail checks.
-- Focused diagnostics for reject reasons, normalization, and edge cases.

SET VERIFY OFF;
SET FEEDBACK OFF;
SET LINESIZE 260;
SET PAGESIZE 200;

COLUMN dataset FORMAT A10
COLUMN source_name FORMAT A5
COLUMN business_date FORMAT A10
COLUMN reject_reason FORMAT A70

PROMPT Reject reason counts:

SELECT
  'clients' AS dataset,
  TO_CHAR(business_date, 'YYYY-MM-DD') AS business_date,
  reject_reason,
  COUNT(*) AS row_count
FROM dwh.stg_clients_reject
WHERE business_date IN (
  DATE '2026-03-25',
  DATE '2026-03-29',
  DATE '2026-04-09'
)
GROUP BY business_date, reject_reason
UNION ALL
SELECT
  'transfers' AS dataset,
  TO_CHAR(business_date, 'YYYY-MM-DD') AS business_date,
  reject_reason,
  COUNT(*) AS row_count
FROM dwh.stg_client_transfers_reject
WHERE business_date IN (
  DATE '2026-03-25',
  DATE '2026-03-28',
  DATE '2026-04-12'
)
GROUP BY business_date, reject_reason
ORDER BY 1, 2, 3;

PROMPT
PROMPT Clients 2026-04-09 normalization:

SELECT
  'STAGE' AS source_name,
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
  'CORE' AS source_name,
  client_id,
  client_type,
  country_code,
  kyc_status,
  risk_score,
  client_status
FROM dwh.core_clients
WHERE business_date = DATE '2026-04-09'
ORDER BY client_id;

PROMPT
PROMPT Client 8401 duplicate-plus-invalid semantic check:

SELECT
  'STAGE' AS source_name,
  client_id,
  risk_score,
  country_code,
  kyc_status,
  client_status
FROM dwh.stg_clients
WHERE business_date = DATE '2026-04-09'
  AND client_id = 8401
UNION ALL
SELECT
  'CORE' AS source_name,
  client_id,
  risk_score,
  country_code,
  kyc_status,
  client_status
FROM dwh.core_clients
WHERE business_date = DATE '2026-04-09'
  AND client_id = 8401;

SELECT
  source_row_num,
  client_id_raw,
  risk_score_raw,
  reject_reason
FROM dwh.stg_clients_reject
WHERE business_date = DATE '2026-04-09'
  AND client_id_raw = '8401'
ORDER BY source_row_num;

PROMPT
PROMPT Transfers 2026-04-10 normalization:

SELECT
  'STAGE' AS source_name,
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
  'CORE' AS source_name,
  transfer_id,
  currency_code,
  transfer_status,
  channel,
  country_code
FROM dwh.core_client_transfers
WHERE business_date = DATE '2026-04-10'
ORDER BY transfer_id;

PROMPT
PROMPT Transfer 2026-04-11 missing client snapshot context:

SELECT
  process_name,
  TO_CHAR(business_date, 'YYYY-MM-DD') AS business_date,
  status,
  reason_code,
  expected_row_count,
  stage_row_count,
  reject_row_count,
  core_row_count
FROM dwh.ctl_process_run
WHERE process_name = 'LOAD_CLIENT_TRANSFERS'
  AND business_date = DATE '2026-04-11';

SELECT COUNT(*) AS same_day_client_core_rows
FROM dwh.core_clients
WHERE business_date = DATE '2026-04-11';

PROMPT
PROMPT Transfers 2026-04-12 reject reasons:

SELECT
  source_row_num,
  transfer_id_raw,
  country_code_raw,
  reject_reason
FROM dwh.stg_client_transfers_reject
WHERE business_date = DATE '2026-04-12'
ORDER BY source_row_num;
