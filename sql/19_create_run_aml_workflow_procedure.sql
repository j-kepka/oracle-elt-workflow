-- Runs the end-to-end AML workflow for one business_date.

CREATE OR REPLACE PROCEDURE dwh.prc_run_aml_workflow (
  p_date     IN DATE DEFAULT TRUNC(SYSDATE),
  p_run_mode IN VARCHAR2 DEFAULT 'MANUAL'
) AS
  c_process_name  CONSTANT VARCHAR2(100 CHAR) := 'RUN_AML_WORKFLOW';
  l_business_date DATE := TRUNC(p_date);
  l_run_mode      VARCHAR2(10 CHAR);
  l_attempt_ts    TIMESTAMP;
  l_input_valid   BOOLEAN := FALSE;
  l_final_status_set BOOLEAN := FALSE;

  PROCEDURE upsert_process_run (
    p_status         IN VARCHAR2,
    p_reason_code    IN VARCHAR2,
    p_status_message IN VARCHAR2,
    p_started_ts     IN TIMESTAMP,
    p_finished_ts    IN TIMESTAMP
  ) AS
  BEGIN
    dwh.pkg_dwh_util.upsert_process_run(
      p_process_name        => c_process_name,
      p_business_date       => l_business_date,
      p_run_mode            => l_run_mode,
      p_status              => p_status,
      p_reason_code         => p_reason_code,
      p_status_message      => p_status_message,
      p_expected_row_count  => NULL,
      p_stage_row_count     => 0,
      p_reject_row_count    => 0,
      p_core_row_count      => 0,
      p_data_file_name      => NULL,
      p_ready_file_name     => NULL,
      p_started_ts          => p_started_ts,
      p_finished_ts         => p_finished_ts,
      p_scheduled_for_ts    => l_attempt_ts,
      p_next_retry_ts       => NULL,
      p_reset_retry_count   => 1
    );
  END upsert_process_run;

  PROCEDURE read_step_state (
    p_step_process_name IN VARCHAR2,
    p_step_status       OUT VARCHAR2,
    p_step_reason_code  OUT VARCHAR2,
    p_step_message      OUT VARCHAR2
  ) AS
  BEGIN
    SELECT
      status,
      reason_code,
      status_message
    INTO
      p_step_status,
      p_step_reason_code,
      p_step_message
    FROM dwh.ctl_process_run
    WHERE process_name = p_step_process_name
      AND business_date = l_business_date;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      p_step_status := '<NO_CTL_ROW>';
      p_step_reason_code := NULL;
      p_step_message := NULL;
  END read_step_state;

  PROCEDURE mark_workflow_step_failed (
    p_step_process_name IN VARCHAR2,
    p_error_message     IN VARCHAR2
  ) AS
    l_step_status      VARCHAR2(20 CHAR);
    l_step_reason_code VARCHAR2(100 CHAR);
    l_step_message     VARCHAR2(4000 CHAR);
    l_status_message   VARCHAR2(4000 CHAR);
  BEGIN
    read_step_state(
      p_step_process_name => p_step_process_name,
      p_step_status       => l_step_status,
      p_step_reason_code  => l_step_reason_code,
      p_step_message      => l_step_message
    );

    l_status_message := SUBSTR(
      'Step '
      || p_step_process_name
      || ' failed for business_date '
      || TO_CHAR(l_business_date, 'YYYY-MM-DD')
      || '. Expected DONE, got '
      || NVL(l_step_status, '<NULL>')
      || CASE
           WHEN l_step_reason_code IS NOT NULL THEN ' / ' || l_step_reason_code
         END
      || CASE
           WHEN l_step_message IS NOT NULL THEN '. Step message: ' || SUBSTR(l_step_message, 1, 1500)
         END
      || CASE
           WHEN p_error_message IS NOT NULL THEN '. Error: ' || SUBSTR(p_error_message, 1, 1500)
         END,
      1,
      4000
    );

    upsert_process_run(
      p_status         => 'FAILED',
      p_reason_code    => 'WORKFLOW_STEP_FAILED',
      p_status_message => l_status_message,
      p_started_ts     => l_attempt_ts,
      p_finished_ts    => SYSTIMESTAMP
    );

    l_final_status_set := TRUE;
  END mark_workflow_step_failed;

  PROCEDURE assert_step_done (
    p_step_process_name IN VARCHAR2
  ) AS
    l_step_status      VARCHAR2(20 CHAR);
    l_step_reason_code VARCHAR2(100 CHAR);
    l_step_message     VARCHAR2(4000 CHAR);
  BEGIN
    read_step_state(
      p_step_process_name => p_step_process_name,
      p_step_status       => l_step_status,
      p_step_reason_code  => l_step_reason_code,
      p_step_message      => l_step_message
    );

    IF NVL(l_step_status, '<NULL>') <> 'DONE' THEN
      mark_workflow_step_failed(
        p_step_process_name => p_step_process_name,
        p_error_message     => 'Step status check failed.'
      );

      RAISE_APPLICATION_ERROR(
        -20410,
        'Workflow step '
        || p_step_process_name
        || ' did not finish with DONE for business_date '
        || TO_CHAR(l_business_date, 'YYYY-MM-DD')
        || '.'
      );
    END IF;
  END assert_step_done;

  PROCEDURE run_step (
    p_step_process_name IN VARCHAR2
  ) AS
  BEGIN
    DBMS_OUTPUT.PUT_LINE(
      'Running workflow step '
      || p_step_process_name
      || ' for business_date='
      || TO_CHAR(l_business_date, 'YYYY-MM-DD')
    );

    IF p_step_process_name = 'LOAD_CLIENTS' THEN
      dwh.prc_load_clients(
        p_date     => l_business_date,
        p_run_mode => l_run_mode
      );
    ELSIF p_step_process_name = 'LOAD_CLIENT_TRANSFERS' THEN
      dwh.prc_load_client_transfers(
        p_date     => l_business_date,
        p_run_mode => l_run_mode
      );
    ELSIF p_step_process_name = 'BUILD_MART_TRANSFER_AML' THEN
      dwh.prc_build_mart_transfer_aml(
        p_date     => l_business_date,
        p_run_mode => l_run_mode
      );
    ELSIF p_step_process_name = 'BUILD_AML_REPORT_SPOOL' THEN
      dwh.prc_build_aml_report_spool(
        p_date     => l_business_date,
        p_run_mode => l_run_mode
      );
    ELSE
      RAISE_APPLICATION_ERROR(
        -20412,
        'Unsupported AML workflow step ' || p_step_process_name || '.'
      );
    END IF;

    assert_step_done(p_step_process_name => p_step_process_name);

    DBMS_OUTPUT.PUT_LINE(
      'Workflow step '
      || p_step_process_name
      || ' finished with DONE.'
    );
  EXCEPTION
    WHEN OTHERS THEN
      IF NOT l_final_status_set THEN
        mark_workflow_step_failed(
          p_step_process_name => p_step_process_name,
          p_error_message     => SQLERRM
        );
      END IF;

      RAISE;
  END run_step;
BEGIN
  l_attempt_ts := SYSTIMESTAMP;

  l_run_mode := dwh.pkg_dwh_util.normalize_run_mode(
    p_run_mode   => p_run_mode,
    p_error_code => -20401
  );

  l_input_valid := TRUE;

  upsert_process_run(
    p_status         => 'PROCESSING',
    p_reason_code    => 'WORKFLOW_STARTED',
    p_status_message => 'AML workflow started.',
    p_started_ts     => l_attempt_ts,
    p_finished_ts    => NULL
  );

  run_step(p_step_process_name => 'LOAD_CLIENTS');
  run_step(p_step_process_name => 'LOAD_CLIENT_TRANSFERS');
  run_step(p_step_process_name => 'BUILD_MART_TRANSFER_AML');
  run_step(p_step_process_name => 'BUILD_AML_REPORT_SPOOL');

  upsert_process_run(
    p_status         => 'DONE',
    p_reason_code    => 'WORKFLOW_DONE',
    p_status_message => 'AML workflow completed successfully.',
    p_started_ts     => l_attempt_ts,
    p_finished_ts    => SYSTIMESTAMP
  );

  l_final_status_set := TRUE;

  DBMS_OUTPUT.PUT_LINE(
    'dwh.prc_run_aml_workflow business_date='
    || TO_CHAR(l_business_date, 'YYYY-MM-DD')
    || ', status=DONE'
  );
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;

    IF l_input_valid AND NOT l_final_status_set THEN
      upsert_process_run(
        p_status         => 'FAILED',
        p_reason_code    => 'UNEXPECTED_ERROR',
        p_status_message => SQLERRM,
        p_started_ts     => l_attempt_ts,
        p_finished_ts    => SYSTIMESTAMP
      );
    END IF;

    RAISE;
END;
/
