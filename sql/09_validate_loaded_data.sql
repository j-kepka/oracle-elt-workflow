-- Validate the current row counts after running the MVP load

SELECT COUNT(*) AS external_row_count
FROM dwh.ext_client_transfers;

SELECT COUNT(*) AS stage_row_count
FROM dwh.stg_client_transfers;

SELECT COUNT(*) AS core_row_count
FROM dwh.core_client_transfers;

SELECT owner, job_name, state, enabled
FROM all_scheduler_jobs
WHERE owner = 'DWH'
ORDER BY job_name;
