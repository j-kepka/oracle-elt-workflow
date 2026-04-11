-- Validates the AML demo dataset.
-- Verifies the new client input fields, transfer_title, and manual FX seed path.

SET VERIFY OFF;
SET FEEDBACK OFF;
SET LINESIZE 260;
SET PAGESIZE 200;

COLUMN check_name FORMAT A32
COLUMN result FORMAT A6
COLUMN details FORMAT A120

WITH checks AS (
  SELECT
    'CLIENTS_CTL' AS check_name,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM dwh.ctl_process_run
        WHERE process_name = 'LOAD_CLIENTS'
          AND business_date = DATE '2026-04-15'
          AND run_mode = 'MANUAL'
          AND status = 'DONE'
          AND reason_code = 'LOAD_DONE'
          AND expected_row_count = 10
          AND stage_row_count = 10
          AND reject_row_count = 0
          AND core_row_count = 10
      ) THEN 'PASS'
      ELSE 'FAIL'
    END AS result,
    'expected status=DONE reason=LOAD_DONE expected=10 stage=10 reject=0 core=10' AS details
  FROM dual
  UNION ALL
  SELECT
    'TRANSFERS_CTL',
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM dwh.ctl_process_run
        WHERE process_name = 'LOAD_CLIENT_TRANSFERS'
          AND business_date = DATE '2026-04-15'
          AND run_mode = 'MANUAL'
          AND status = 'DONE'
          AND reason_code = 'LOAD_DONE'
          AND expected_row_count = 20
          AND stage_row_count = 20
          AND reject_row_count = 0
          AND core_row_count = 20
      ) THEN 'PASS'
      ELSE 'FAIL'
    END,
    'expected status=DONE reason=LOAD_DONE expected=20 stage=20 reject=0 core=20'
  FROM dual
  UNION ALL
  SELECT
    'FX_ROW_COUNT',
    CASE
      WHEN (
        SELECT COUNT(*)
        FROM dwh.ref_fx_rate_daily
        WHERE business_date = DATE '2026-04-15'
      ) = 5 THEN 'PASS'
      ELSE 'FAIL'
    END,
    'expected ref_fx_rate_daily rows=5 for 2026-04-15'
  FROM dual
  UNION ALL
  SELECT
    'TRANSFER_CURRENCY_SET',
    CASE
      WHEN (
        SELECT COUNT(DISTINCT currency_code)
        FROM dwh.core_client_transfers
        WHERE business_date = DATE '2026-04-15'
      ) = 5 THEN 'PASS'
      ELSE 'FAIL'
    END,
    'expected distinct transfer currencies=5 in core_client_transfers'
  FROM dual
  UNION ALL
  SELECT
    'CLIENT_9503_FIELDS',
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM dwh.core_clients
        WHERE business_date = DATE '2026-04-15'
          AND client_id = 9503
          AND relationship_purpose_code = 'SAVINGS'
          AND expected_activity_level = 'LOW'
          AND source_of_funds_declared = 'Cash deposits'
          AND source_of_wealth_declared = 'Undeclared'
      ) THEN 'PASS'
      ELSE 'FAIL'
    END,
    'expected normalized code fields plus preserved declared source strings for client 9503'
  FROM dual
  UNION ALL
  SELECT
    'CLIENT_9506_FIELDS',
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM dwh.core_clients
        WHERE business_date = DATE '2026-04-15'
          AND client_id = 9506
          AND relationship_purpose_code = 'BUSINESS_PAYMENTS'
          AND expected_activity_level = 'MEDIUM'
          AND source_of_funds_declared = 'Business revenue'
          AND source_of_wealth_declared = 'Business'
      ) THEN 'PASS'
      ELSE 'FAIL'
    END,
    'expected business-client AML extension fields for client 9506'
  FROM dual
  UNION ALL
  SELECT
    'TRANSFER_500110_TITLE',
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM dwh.core_client_transfers
        WHERE business_date = DATE '2026-04-15'
          AND transfer_id = 500110
          AND transfer_title = 'Dragon coin exchange topup'
      ) THEN 'PASS'
      ELSE 'FAIL'
    END,
    'expected trimmed transfer_title for transfer 500110'
  FROM dual
  UNION ALL
  SELECT
    'TRANSFER_500113_TITLE',
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM dwh.core_client_transfers
        WHERE business_date = DATE '2026-04-15'
          AND transfer_id = 500113
          AND transfer_title = 'Phoenix feather OTC settlement'
      ) THEN 'PASS'
      ELSE 'FAIL'
    END,
    'expected stored transfer_title for transfer 500113'
  FROM dual
),
output_rows AS (
  SELECT
    1 AS sort_order,
    check_name,
    result,
    details
  FROM checks
  UNION ALL
  SELECT
    2 AS sort_order,
    'SUMMARY' AS check_name,
    CASE
      WHEN SUM(CASE WHEN result = 'FAIL' THEN 1 ELSE 0 END) = 0 THEN 'PASS'
      ELSE 'FAIL'
    END AS result,
    'total_checks='
    || COUNT(*)
    || '; failed_checks='
    || SUM(CASE WHEN result = 'FAIL' THEN 1 ELSE 0 END) AS details
  FROM checks
)
SELECT
  check_name,
  result,
  details
FROM output_rows
ORDER BY
  sort_order,
  check_name;

PROMPT
PROMPT AML demo client snapshot:

SELECT
  client_id,
  client_type,
  kyc_status,
  risk_score,
  relationship_purpose_code,
  expected_activity_level,
  source_of_funds_declared,
  source_of_wealth_declared
FROM dwh.core_clients
WHERE business_date = DATE '2026-04-15'
ORDER BY client_id;

PROMPT
PROMPT AML demo transfer snapshot:

SELECT
  transfer_id,
  client_id,
  amount,
  currency_code,
  transfer_status,
  channel,
  country_code,
  transfer_title
FROM dwh.core_client_transfers
WHERE business_date = DATE '2026-04-15'
ORDER BY transfer_id;

PROMPT
PROMPT AML demo FX reference rows:

SELECT
  TO_CHAR(business_date, 'YYYY-MM-DD') AS business_date,
  currency_code,
  unit_count,
  mid_rate_pln,
  rate_source
FROM dwh.ref_fx_rate_daily
WHERE business_date = DATE '2026-04-15'
ORDER BY currency_code;
