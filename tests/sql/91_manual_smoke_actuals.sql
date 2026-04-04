-- Deterministic MANUAL smoke actuals.
-- Pulls the current smoke state from DB tables and control rows.

SET VERIFY OFF;
SET FEEDBACK OFF;
SET LINESIZE 260;
SET PAGESIZE 200;

COLUMN case_key FORMAT A34
COLUMN process_name FORMAT A22
COLUMN business_date FORMAT A10
COLUMN run_mode FORMAT A6
COLUMN ctl_status FORMAT A10
COLUMN ctl_reason_code FORMAT A28

WITH cases AS (
  SELECT 10 AS case_id, 'clients_2026-03-24_manual' AS case_key, 'LOAD_CLIENTS' AS process_name, DATE '2026-03-24' AS business_date, 'MANUAL' AS run_mode FROM dual
  UNION ALL
  SELECT 20, 'clients_2026-03-25_manual', 'LOAD_CLIENTS', DATE '2026-03-25', 'MANUAL' FROM dual
  UNION ALL
  SELECT 30, 'clients_2026-03-26_manual', 'LOAD_CLIENTS', DATE '2026-03-26', 'MANUAL' FROM dual
  UNION ALL
  SELECT 40, 'clients_2026-03-27_manual', 'LOAD_CLIENTS', DATE '2026-03-27', 'MANUAL' FROM dual
  UNION ALL
  SELECT 50, 'clients_2026-03-28_manual', 'LOAD_CLIENTS', DATE '2026-03-28', 'MANUAL' FROM dual
  UNION ALL
  SELECT 60, 'clients_2026-03-29_manual', 'LOAD_CLIENTS', DATE '2026-03-29', 'MANUAL' FROM dual
  UNION ALL
  SELECT 70, 'clients_2026-04-08_manual', 'LOAD_CLIENTS', DATE '2026-04-08', 'MANUAL' FROM dual
  UNION ALL
  SELECT 80, 'clients_2026-04-09_manual', 'LOAD_CLIENTS', DATE '2026-04-09', 'MANUAL' FROM dual
  UNION ALL
  SELECT 90, 'clients_2026-04-10_manual', 'LOAD_CLIENTS', DATE '2026-04-10', 'MANUAL' FROM dual
  UNION ALL
  SELECT 110, 'transfers_2026-03-24_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-03-24', 'MANUAL' FROM dual
  UNION ALL
  SELECT 120, 'transfers_2026-03-25_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-03-25', 'MANUAL' FROM dual
  UNION ALL
  SELECT 130, 'transfers_2026-03-26_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-03-26', 'MANUAL' FROM dual
  UNION ALL
  SELECT 140, 'transfers_2026-03-27_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-03-27', 'MANUAL' FROM dual
  UNION ALL
  SELECT 150, 'transfers_2026-03-28_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-03-28', 'MANUAL' FROM dual
  UNION ALL
  SELECT 160, 'transfers_2026-04-08_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-04-08', 'MANUAL' FROM dual
  UNION ALL
  SELECT 170, 'transfers_2026-04-10_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-04-10', 'MANUAL' FROM dual
  UNION ALL
  SELECT 180, 'transfers_2026-04-11_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-04-11', 'MANUAL' FROM dual
  UNION ALL
  SELECT 190, 'transfers_2026-04-12_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-04-12', 'MANUAL' FROM dual
),
ctl AS (
  SELECT
    process_name,
    business_date,
    run_mode,
    status,
    reason_code,
    expected_row_count,
    stage_row_count,
    reject_row_count,
    core_row_count
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
)
SELECT
  c.case_id,
  c.case_key,
  c.process_name,
  TO_CHAR(c.business_date, 'YYYY-MM-DD') AS business_date,
  c.run_mode,
  ctl.status AS ctl_status,
  ctl.reason_code AS ctl_reason_code,
  ctl.expected_row_count AS ctl_expected_rows,
  CASE
    WHEN c.process_name = 'LOAD_CLIENTS' THEN NVL(cs.row_count, 0)
    ELSE NVL(ts.row_count, 0)
  END AS actual_stage_rows,
  CASE
    WHEN c.process_name = 'LOAD_CLIENTS' THEN NVL(cr.row_count, 0)
    ELSE NVL(tr.row_count, 0)
  END AS actual_reject_rows,
  CASE
    WHEN c.process_name = 'LOAD_CLIENTS' THEN NVL(cc.row_count, 0)
    ELSE NVL(tc.row_count, 0)
  END AS actual_core_rows,
  ctl.stage_row_count AS ctl_stage_rows,
  ctl.reject_row_count AS ctl_reject_rows,
  ctl.core_row_count AS ctl_core_rows
FROM cases c
LEFT JOIN ctl
  ON ctl.process_name = c.process_name
 AND ctl.business_date = c.business_date
 AND ctl.run_mode = c.run_mode
LEFT JOIN clients_stage cs
  ON cs.business_date = c.business_date
LEFT JOIN clients_reject cr
  ON cr.business_date = c.business_date
LEFT JOIN clients_core cc
  ON cc.business_date = c.business_date
LEFT JOIN transfers_stage ts
  ON ts.business_date = c.business_date
LEFT JOIN transfers_reject tr
  ON tr.business_date = c.business_date
LEFT JOIN transfers_core tc
  ON tc.business_date = c.business_date
ORDER BY c.case_id;

PROMPT
PROMPT Optional AUTO rows for 2026-04-07:

SELECT
  process_name,
  TO_CHAR(business_date, 'YYYY-MM-DD') AS business_date,
  run_mode,
  status,
  reason_code,
  retry_count,
  next_retry_ts
FROM dwh.ctl_process_run
WHERE process_name IN ('LOAD_CLIENTS', 'LOAD_CLIENT_TRANSFERS')
  AND business_date = DATE '2026-04-07'
  AND run_mode = 'AUTO'
ORDER BY process_name;
