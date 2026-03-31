-- Operational helper: validate the current row counts after running the client load

SELECT COUNT(*) AS external_row_count
FROM dwh.ext_clients;

SELECT business_date, COUNT(*) AS stage_row_count
FROM dwh.stg_clients
GROUP BY business_date
ORDER BY business_date;

SELECT business_date, COUNT(*) AS reject_row_count
FROM dwh.stg_clients_reject
GROUP BY business_date
ORDER BY business_date;

SELECT business_date, COUNT(*) AS core_row_count
FROM dwh.core_clients
GROUP BY business_date
ORDER BY business_date;

SELECT
  process_name,
  business_date,
  run_mode,
  status,
  reason_code,
  retry_count,
  expected_row_count,
  stage_row_count,
  reject_row_count,
  core_row_count,
  data_file_name,
  ready_file_name,
  next_retry_ts
FROM dwh.ctl_process_run
WHERE process_name = 'LOAD_CLIENTS'
ORDER BY business_date;
