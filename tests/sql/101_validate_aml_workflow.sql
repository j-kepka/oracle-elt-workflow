-- Validates the AML workflow procedure status and scheduler object.

WHENEVER OSERROR EXIT FAILURE;
WHENEVER SQLERROR EXIT SQL.SQLCODE;

SET SERVEROUTPUT ON;
SET VERIFY OFF;
SET FEEDBACK OFF;
SET LINESIZE 260;
SET PAGESIZE 120;

COLUMN process_name FORMAT A28
COLUMN status FORMAT A12
COLUMN reason_code FORMAT A28
COLUMN status_message FORMAT A80

DECLARE
  c_data_file_name  CONSTANT VARCHAR2(255 CHAR) := 'aml_report_spool_20260415.csv';
  c_ready_file_name CONSTANT VARCHAR2(255 CHAR) := 'aml_report_spool_20260415.ok';
  c_export_dir      CONSTANT VARCHAR2(30 CHAR) := 'EXT_EXPORT_DIR';
  l_total_checks    NUMBER := 0;
  l_failed_checks   NUMBER := 0;
  l_count           NUMBER := 0;

  PROCEDURE add_check (
    p_check_name IN VARCHAR2,
    p_pass       IN BOOLEAN,
    p_details    IN VARCHAR2
  ) AS
  BEGIN
    l_total_checks := l_total_checks + 1;

    IF NOT p_pass THEN
      l_failed_checks := l_failed_checks + 1;
    END IF;

    DBMS_OUTPUT.PUT_LINE(
      RPAD(p_check_name, 34)
      || CASE WHEN p_pass THEN 'PASS  ' ELSE 'FAIL  ' END
      || p_details
    );
  END add_check;

  FUNCTION file_exists (
    p_filename IN VARCHAR2
  ) RETURN BOOLEAN AS
    l_exists      BOOLEAN;
    l_file_length NUMBER;
    l_block_size  BINARY_INTEGER;
  BEGIN
    UTL_FILE.FGETATTR(
      location    => c_export_dir,
      filename    => p_filename,
      fexists     => l_exists,
      file_length => l_file_length,
      block_size  => l_block_size
    );

    RETURN l_exists;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN FALSE;
  END file_exists;
BEGIN
  DBMS_OUTPUT.PUT_LINE('check_name                        result details');
  DBMS_OUTPUT.PUT_LINE('---------------------------------- ------ ------------------------------------------------------------');

  SELECT COUNT(*)
  INTO l_count
  FROM dwh.ctl_process_run
  WHERE process_name = 'RUN_AML_WORKFLOW'
    AND business_date = DATE '2026-04-15'
    AND run_mode = 'MANUAL'
    AND status = 'DONE'
    AND reason_code = 'WORKFLOW_DONE'
    AND expected_row_count IS NULL
    AND stage_row_count = 0
    AND reject_row_count = 0
    AND core_row_count = 0
    AND data_file_name IS NULL
    AND ready_file_name IS NULL;

  add_check(
    'WORKFLOW_CTL',
    l_count = 1,
    'expected DONE/WORKFLOW_DONE for the workflow header row'
  );

  SELECT COUNT(*)
  INTO l_count
  FROM dwh.ctl_process_run
  WHERE business_date = DATE '2026-04-15'
    AND (
      (
        process_name = 'LOAD_CLIENTS'
        AND run_mode = 'MANUAL'
        AND status = 'DONE'
        AND reason_code = 'LOAD_DONE'
        AND expected_row_count = 10
        AND stage_row_count = 10
        AND reject_row_count = 0
        AND core_row_count = 10
      )
      OR (
        process_name = 'LOAD_CLIENT_TRANSFERS'
        AND run_mode = 'MANUAL'
        AND status = 'DONE'
        AND reason_code = 'LOAD_DONE'
        AND expected_row_count = 20
        AND stage_row_count = 20
        AND reject_row_count = 0
        AND core_row_count = 20
      )
      OR (
        process_name = 'BUILD_MART_TRANSFER_AML'
        AND run_mode = 'MANUAL'
        AND status = 'DONE'
        AND reason_code = 'MART_BUILD_DONE'
        AND expected_row_count = 20
        AND stage_row_count = 0
        AND reject_row_count = 0
        AND core_row_count = 20
      )
      OR (
        process_name = 'BUILD_AML_REPORT_SPOOL'
        AND run_mode = 'MANUAL'
        AND status = 'DONE'
        AND reason_code = 'SPOOL_BUILD_DONE'
        AND expected_row_count = 11
        AND stage_row_count = 0
        AND reject_row_count = 0
        AND core_row_count = 11
        AND data_file_name = c_data_file_name
        AND ready_file_name = c_ready_file_name
      )
    );

  add_check(
    'WORKFLOW_STEP_CTL',
    l_count = 4,
    'expected all four workflow steps to finish with their normal DONE statuses'
  );

  SELECT COUNT(*)
  INTO l_count
  FROM all_scheduler_jobs
  WHERE owner = 'DWH'
    AND job_name = 'JOB_RUN_AML_WORKFLOW'
    AND enabled = 'FALSE'
    AND job_type = 'PLSQL_BLOCK'
    AND job_action LIKE '%prc_run_aml_workflow%';

  add_check(
    'WORKFLOW_SCHEDULER_JOB',
    l_count = 1,
    'expected disabled DWH.JOB_RUN_AML_WORKFLOW job pointing at the workflow procedure'
  );

  add_check(
    'WORKFLOW_CSV_EXISTS',
    file_exists(c_data_file_name),
    'expected outbound CSV file ' || c_data_file_name
  );

  add_check(
    'WORKFLOW_OK_EXISTS',
    file_exists(c_ready_file_name),
    'expected outbound ready file ' || c_ready_file_name
  );

  DBMS_OUTPUT.PUT_LINE('---------------------------------- ------ ------------------------------------------------------------');
  DBMS_OUTPUT.PUT_LINE(
    RPAD('SUMMARY', 34)
    || CASE WHEN l_failed_checks = 0 THEN 'PASS  ' ELSE 'FAIL  ' END
    || 'total_checks='
    || l_total_checks
    || '; failed_checks='
    || l_failed_checks
  );

  IF l_failed_checks > 0 THEN
    RAISE_APPLICATION_ERROR(
      -20993,
      'AML workflow validation failed.'
    );
  END IF;
END;
/

PROMPT
PROMPT AML workflow control snapshot:

SELECT
  process_name,
  status,
  reason_code,
  expected_row_count,
  stage_row_count,
  reject_row_count,
  core_row_count,
  status_message
FROM dwh.ctl_process_run
WHERE business_date = DATE '2026-04-15'
  AND process_name IN (
    'RUN_AML_WORKFLOW',
    'LOAD_CLIENTS',
    'LOAD_CLIENT_TRANSFERS',
    'BUILD_MART_TRANSFER_AML',
    'BUILD_AML_REPORT_SPOOL'
  )
ORDER BY
  CASE process_name
    WHEN 'RUN_AML_WORKFLOW' THEN 1
    WHEN 'LOAD_CLIENTS' THEN 2
    WHEN 'LOAD_CLIENT_TRANSFERS' THEN 3
    WHEN 'BUILD_MART_TRANSFER_AML' THEN 4
    WHEN 'BUILD_AML_REPORT_SPOOL' THEN 5
    ELSE 99
  END;
