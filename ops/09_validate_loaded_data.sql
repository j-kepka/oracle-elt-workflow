-- Operational helper: validate the current row counts after running the MVP load

SELECT COUNT(*) AS external_row_count
FROM dwh.ext_client_transfers;

SELECT business_date, COUNT(*) AS stage_row_count
FROM dwh.stg_client_transfers
GROUP BY business_date
ORDER BY business_date;

SELECT business_date, COUNT(*) AS reject_row_count
FROM dwh.stg_client_transfers_reject
GROUP BY business_date
ORDER BY business_date;

SELECT business_date, COUNT(*) AS core_row_count
FROM dwh.core_client_transfers
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
ORDER BY process_name, business_date;

SELECT owner, job_name, state, enabled
FROM all_scheduler_jobs
WHERE owner = 'DWH'
ORDER BY job_name;
