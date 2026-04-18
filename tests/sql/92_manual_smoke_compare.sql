-- Deterministic MANUAL smoke compare.
-- Compares expected smoke outcomes with current DB state.

SET VERIFY OFF;
SET FEEDBACK OFF;
SET LINESIZE 280;
SET PAGESIZE 200;

COLUMN result FORMAT A6
COLUMN case_key FORMAT A34
COLUMN process_name FORMAT A22
COLUMN business_date FORMAT A10
COLUMN run_mode FORMAT A6
COLUMN expected_reason_code FORMAT A28
COLUMN actual_reason_code FORMAT A28
COLUMN mismatch_fields FORMAT A70

WITH expected AS (
  SELECT 10 AS case_id, 'clients_2026-03-24_manual' AS case_key, 'LOAD_CLIENTS' AS process_name, DATE '2026-03-24' AS business_date, 'MANUAL' AS run_mode, 'FAILED' AS expected_status, 'MISSING_DATA_FILE' AS expected_reason_code, 4 AS expected_row_count, 0 AS expected_stage_rows, 0 AS expected_reject_rows, 0 AS expected_core_rows FROM dual
  UNION ALL
  SELECT 20, 'transfers_2026-03-24_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-03-24', 'MANUAL', 'FAILED', 'MISSING_DATA_FILE', 13, 0, 0, 0 FROM dual
  UNION ALL
  SELECT 30, 'clients_2026-03-25_manual', 'LOAD_CLIENTS', DATE '2026-03-25', 'MANUAL', 'WARNING', 'INPUT_VALIDATION_WARNING', 9, 5, 4, 5 FROM dual
  UNION ALL
  SELECT 40, 'transfers_2026-03-25_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-03-25', 'MANUAL', 'WARNING', 'INPUT_VALIDATION_WARNING', 12, 5, 7, 5 FROM dual
  UNION ALL
  SELECT 50, 'clients_2026-03-26_manual', 'LOAD_CLIENTS', DATE '2026-03-26', 'MANUAL', 'DONE', 'LOAD_DONE', 12, 12, 0, 12 FROM dual
  UNION ALL
  SELECT 60, 'transfers_2026-03-26_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-03-26', 'MANUAL', 'DONE', 'LOAD_DONE', 20, 20, 0, 20 FROM dual
  UNION ALL
  SELECT 70, 'clients_2026-03-27_manual', 'LOAD_CLIENTS', DATE '2026-03-27', 'MANUAL', 'FAILED', 'OK_COUNT_INCLUDES_HEADER', 3, 0, 0, 0 FROM dual
  UNION ALL
  SELECT 80, 'transfers_2026-03-27_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-03-27', 'MANUAL', 'FAILED', 'OK_COUNT_MISMATCH', 20, 0, 0, 0 FROM dual
  UNION ALL
  SELECT 90, 'clients_2026-03-28_manual', 'LOAD_CLIENTS', DATE '2026-03-28', 'MANUAL', 'DONE', 'LOAD_DONE', 2, 2, 0, 2 FROM dual
  UNION ALL
  SELECT 100, 'transfers_2026-03-28_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-03-28', 'MANUAL', 'WARNING', 'INPUT_VALIDATION_WARNING', 2, 0, 2, 0 FROM dual
  UNION ALL
  SELECT 110, 'clients_2026-03-29_manual', 'LOAD_CLIENTS', DATE '2026-03-29', 'MANUAL', 'WARNING', 'INPUT_VALIDATION_WARNING', 2, 0, 2, 0 FROM dual
  UNION ALL
  SELECT 120, 'clients_2026-04-08_manual', 'LOAD_CLIENTS', DATE '2026-04-08', 'MANUAL', 'FAILED', 'MISSING_OK_MANUAL', CAST(NULL AS NUMBER), 0, 0, 0 FROM dual
  UNION ALL
  SELECT 130, 'transfers_2026-04-08_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-04-08', 'MANUAL', 'FAILED', 'MISSING_OK_MANUAL', CAST(NULL AS NUMBER), 0, 0, 0 FROM dual
  UNION ALL
  SELECT 140, 'clients_2026-04-09_manual', 'LOAD_CLIENTS', DATE '2026-04-09', 'MANUAL', 'WARNING', 'INPUT_VALIDATION_WARNING', 10, 2, 8, 2 FROM dual
  UNION ALL
  SELECT 150, 'clients_2026-04-10_manual', 'LOAD_CLIENTS', DATE '2026-04-10', 'MANUAL', 'DONE', 'LOAD_DONE', 2, 2, 0, 2 FROM dual
  UNION ALL
  SELECT 160, 'transfers_2026-04-10_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-04-10', 'MANUAL', 'DONE', 'LOAD_DONE', 2, 2, 0, 2 FROM dual
  UNION ALL
  SELECT 170, 'transfers_2026-04-11_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-04-11', 'MANUAL', 'FAILED', 'MISSING_CLIENT_SNAPSHOT', 1, 0, 0, 0 FROM dual
  UNION ALL
  SELECT 180, 'transfers_2026-04-12_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-04-12', 'MANUAL', 'WARNING', 'INPUT_VALIDATION_WARNING', 2, 0, 2, 0 FROM dual
),
ctl AS (
  SELECT
    process_name,
    business_date,
    run_mode,
    status,
    reason_code,
    expected_row_count
  FROM dwh.ctl_process_run
),
clients_stage AS (
  SELECT business_date, COUNT(*) AS row_count
  FROM dwh.stg_clients
  GROUP BY business_date
),
clients_reject AS (
  SELECT business_date, COUNT(*) AS row_count
  FROM dwh.stg_clients_reject
  GROUP BY business_date
),
clients_core AS (
  SELECT business_date, COUNT(*) AS row_count
  FROM dwh.core_clients
  GROUP BY business_date
),
transfers_stage AS (
  SELECT business_date, COUNT(*) AS row_count
  FROM dwh.stg_client_transfers
  GROUP BY business_date
),
transfers_reject AS (
  SELECT business_date, COUNT(*) AS row_count
  FROM dwh.stg_client_transfers_reject
  GROUP BY business_date
),
transfers_core AS (
  SELECT business_date, COUNT(*) AS row_count
  FROM dwh.core_client_transfers
  GROUP BY business_date
),
actuals AS (
  SELECT
    e.case_id,
    e.case_key,
    e.process_name,
    e.business_date,
    e.run_mode,
    ctl.status AS actual_status,
    ctl.reason_code AS actual_reason_code,
    ctl.expected_row_count AS actual_ctl_expected_rows,
    CASE
      WHEN e.process_name = 'LOAD_CLIENTS' THEN NVL(cs.row_count, 0)
      ELSE NVL(ts.row_count, 0)
    END AS actual_stage_rows,
    CASE
      WHEN e.process_name = 'LOAD_CLIENTS' THEN NVL(cr.row_count, 0)
      ELSE NVL(tr.row_count, 0)
    END AS actual_reject_rows,
    CASE
      WHEN e.process_name = 'LOAD_CLIENTS' THEN NVL(cc.row_count, 0)
      ELSE NVL(tc.row_count, 0)
    END AS actual_core_rows
  FROM expected e
  LEFT JOIN ctl
    ON ctl.process_name = e.process_name
   AND ctl.business_date = e.business_date
   AND ctl.run_mode = e.run_mode
  LEFT JOIN clients_stage cs
    ON cs.business_date = e.business_date
  LEFT JOIN clients_reject cr
    ON cr.business_date = e.business_date
  LEFT JOIN clients_core cc
    ON cc.business_date = e.business_date
  LEFT JOIN transfers_stage ts
    ON ts.business_date = e.business_date
  LEFT JOIN transfers_reject tr
    ON tr.business_date = e.business_date
  LEFT JOIN transfers_core tc
    ON tc.business_date = e.business_date
),
comparison AS (
  SELECT
    e.case_id,
    e.case_key,
    e.process_name,
    e.business_date,
    e.run_mode,
    e.expected_status,
    a.actual_status,
    e.expected_reason_code,
    a.actual_reason_code,
    e.expected_row_count,
    a.actual_ctl_expected_rows,
    e.expected_stage_rows,
    a.actual_stage_rows,
    e.expected_reject_rows,
    a.actual_reject_rows,
    e.expected_core_rows,
    a.actual_core_rows,
    TRIM(BOTH ';' FROM
      CASE
        WHEN NVL(a.actual_status, '#NULL#') != NVL(e.expected_status, '#NULL#') THEN 'status;'
      END ||
      CASE
        WHEN NVL(a.actual_reason_code, '#NULL#') != NVL(e.expected_reason_code, '#NULL#') THEN 'reason_code;'
      END ||
      CASE
        WHEN e.expected_row_count IS NOT NULL
         AND NVL(a.actual_ctl_expected_rows, -999999) != e.expected_row_count THEN 'expected_row_count;'
      END ||
      CASE
        WHEN NVL(a.actual_stage_rows, -999999) != e.expected_stage_rows THEN 'actual_stage_rows;'
      END ||
      CASE
        WHEN NVL(a.actual_reject_rows, -999999) != e.expected_reject_rows THEN 'actual_reject_rows;'
      END ||
      CASE
        WHEN NVL(a.actual_core_rows, -999999) != e.expected_core_rows THEN 'actual_core_rows;'
      END
    ) AS mismatch_fields
  FROM expected e
  LEFT JOIN actuals a
    ON a.case_id = e.case_id
),
scored AS (
  SELECT
    CASE
      WHEN mismatch_fields IS NULL THEN 'PASS'
      ELSE 'FAIL'
    END AS result,
    case_id,
    case_key,
    process_name,
    TO_CHAR(business_date, 'YYYY-MM-DD') AS business_date,
    run_mode,
    expected_status,
    actual_status,
    expected_reason_code,
    actual_reason_code,
    expected_row_count,
    actual_ctl_expected_rows,
    expected_stage_rows,
    actual_stage_rows,
    expected_reject_rows,
    actual_reject_rows,
    expected_core_rows,
    actual_core_rows,
    mismatch_fields
  FROM comparison
),
output_rows AS (
  SELECT
    1 AS sort_order,
    result,
    case_id,
    case_key,
    process_name,
    business_date,
    run_mode,
    expected_status,
    actual_status,
    expected_reason_code,
    actual_reason_code,
    expected_row_count,
    actual_ctl_expected_rows,
    expected_stage_rows,
    actual_stage_rows,
    expected_reject_rows,
    actual_reject_rows,
    expected_core_rows,
    actual_core_rows,
    mismatch_fields
  FROM scored
  UNION ALL
  SELECT
    2 AS sort_order,
    CASE
      WHEN SUM(CASE WHEN result = 'FAIL' THEN 1 ELSE 0 END) = 0 THEN 'PASS'
      ELSE 'FAIL'
    END AS result,
    CAST(NULL AS NUMBER) AS case_id,
    'SUMMARY' AS case_key,
    CAST(NULL AS VARCHAR2(22 CHAR)) AS process_name,
    CAST(NULL AS VARCHAR2(10 CHAR)) AS business_date,
    CAST(NULL AS VARCHAR2(6 CHAR)) AS run_mode,
    CAST(NULL AS VARCHAR2(10 CHAR)) AS expected_status,
    CAST(NULL AS VARCHAR2(10 CHAR)) AS actual_status,
    CAST(NULL AS VARCHAR2(28 CHAR)) AS expected_reason_code,
    CAST(NULL AS VARCHAR2(28 CHAR)) AS actual_reason_code,
    CAST(NULL AS NUMBER) AS expected_row_count,
    CAST(NULL AS NUMBER) AS actual_ctl_expected_rows,
    CAST(NULL AS NUMBER) AS expected_stage_rows,
    CAST(NULL AS NUMBER) AS actual_stage_rows,
    CAST(NULL AS NUMBER) AS expected_reject_rows,
    CAST(NULL AS NUMBER) AS actual_reject_rows,
    CAST(NULL AS NUMBER) AS expected_core_rows,
    CAST(NULL AS NUMBER) AS actual_core_rows,
    'total_cases='
    || COUNT(*)
    || '; passed_cases='
    || SUM(CASE WHEN result = 'PASS' THEN 1 ELSE 0 END)
    || '; failed_cases='
    || SUM(CASE WHEN result = 'FAIL' THEN 1 ELSE 0 END) AS mismatch_fields
  FROM scored
)
SELECT
  result,
  case_id,
  case_key,
  process_name,
  business_date,
  run_mode,
  expected_status,
  actual_status,
  expected_reason_code,
  actual_reason_code,
  expected_row_count,
  actual_ctl_expected_rows,
  expected_stage_rows,
  actual_stage_rows,
  expected_reject_rows,
  actual_reject_rows,
  expected_core_rows,
  actual_core_rows,
  mismatch_fields
FROM output_rows
ORDER BY sort_order, case_id;
