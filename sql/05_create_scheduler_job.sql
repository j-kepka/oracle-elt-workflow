-- Creates Oracle Scheduler job for the end-to-end external -> stage -> core load procedure
-- Prerequisite (run as SYSTEM once if needed):
--   GRANT CREATE JOB TO dwh;

BEGIN
  DBMS_SCHEDULER.DROP_JOB(
    job_name => 'DWH.JOB_LOAD_CLIENT_TRANSFERS',
    force    => TRUE
  );
EXCEPTION
  WHEN OTHERS THEN
    -- ORA-27475: job does not exist
    IF SQLCODE != -27475 THEN
      RAISE;
    END IF;
END;
/

BEGIN
  DBMS_SCHEDULER.DROP_JOB(
    job_name => 'DWH.JOB_LOAD_STG_CLIENT_TRANSFERS',
    force    => TRUE
  );
EXCEPTION
  WHEN OTHERS THEN
    -- ORA-27475: job does not exist
    IF SQLCODE != -27475 THEN
      RAISE;
    END IF;
END;
/

BEGIN
  DBMS_SCHEDULER.CREATE_JOB(
    job_name        => 'DWH.JOB_LOAD_CLIENT_TRANSFERS',
    job_type        => 'PLSQL_BLOCK',
    job_action      => q'[BEGIN dwh.prc_load_client_transfers(p_date => TRUNC(SYSDATE), p_run_mode => 'AUTO'); END;]',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=DAILY;BYHOUR=1;BYMINUTE=0;BYSECOND=0',
    enabled         => FALSE,
    auto_drop       => FALSE,
    comments        => 'Runs the daily client_transfers load in AUTO mode with ready-file checks.'
  );
END;
/

BEGIN
  DBMS_SCHEDULER.SET_ATTRIBUTE(
    name      => 'DWH.JOB_LOAD_CLIENT_TRANSFERS',
    attribute => 'logging_level',
    value     => DBMS_SCHEDULER.LOGGING_RUNS
  );
END;
/

-- Manual control
-- BEGIN DBMS_SCHEDULER.ENABLE('DWH.JOB_LOAD_CLIENT_TRANSFERS'); END;
-- BEGIN DBMS_SCHEDULER.DISABLE('DWH.JOB_LOAD_CLIENT_TRANSFERS'); END;

-- Useful checks
-- SELECT owner, job_name, state, repeat_interval, enabled FROM all_scheduler_jobs WHERE owner = 'DWH';
-- SELECT log_date, status, additional_info FROM all_scheduler_job_run_details WHERE owner = 'DWH' AND job_name = 'JOB_LOAD_CLIENT_TRANSFERS' ORDER BY log_id DESC;
