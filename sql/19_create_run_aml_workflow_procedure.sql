-- Runs the end-to-end AML workflow for one business_date.

CREATE OR REPLACE PROCEDURE dwh.prc_run_aml_workflow (
  p_date     IN DATE DEFAULT TRUNC(SYSDATE),
  p_run_mode IN VARCHAR2 DEFAULT 'MANUAL'
) AS
  c_process_name  CONSTANT VARCHAR2(100 CHAR) := 'RUN_AML_WORKFLOW';
  l_business_date DATE := TRUNC(p_date);
  l_run_mode      VARCHAR2(10 CHAR) := UPPER(TRIM(p_run_mode));
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
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    MERGE INTO dwh.ctl_process_run dst
    USING (
      SELECT
        c_process_name AS process_name,
        l_business_date AS business_date
      FROM dual
    ) src
    ON (
      dst.process_name = src.process_name
      AND dst.business_date = src.business_date
    )
    WHEN MATCHED THEN
      UPDATE SET
        dst.run_mode = l_run_mode,
        dst.status = p_status,
        dst.reason_code = p_reason_code,
        dst.retry_count = 0,
        dst.scheduled_for_ts = NVL(dst.scheduled_for_ts, l_attempt_ts),
        dst.next_retry_ts = NULL,
        dst.started_ts = p_started_ts,
        dst.finished_ts = p_finished_ts,
        dst.expected_row_count = NULL,
        dst.stage_row_count = 0,
        dst.reject_row_count = 0,
        dst.core_row_count = 0,
        dst.data_file_name = NULL,
        dst.ready_file_name = NULL,
        dst.status_message = SUBSTR(p_status_message, 1, 4000),
        dst.updated_ts = SYSTIMESTAMP
    WHEN NOT MATCHED THEN
      INSERT (
        process_name,
        business_date,
        run_mode,
        status,
        reason_code,
        retry_count,
        scheduled_for_ts,
        next_retry_ts,
        started_ts,
        finished_ts,
        expected_row_count,
        stage_row_count,
        reject_row_count,
        core_row_count,
        data_file_name,
        ready_file_name,
        status_message,
        created_ts,
        updated_ts
      )
      VALUES (
        c_process_name,
        l_business_date,
        l_run_mode,
        p_status,
        p_reason_code,
        0,
        l_attempt_ts,
        NULL,
        p_started_ts,
        p_finished_ts,
        NULL,
        0,
        0,
        0,
        NULL,
        NULL,
        SUBSTR(p_status_message, 1, 4000),
        SYSTIMESTAMP,
        SYSTIMESTAMP
      );

    COMMIT;
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

  IF l_run_mode IS NULL OR l_run_mode NOT IN ('AUTO', 'MANUAL') THEN
    RAISE_APPLICATION_ERROR(
      -20401,
      'Unsupported run mode ' || NVL(p_run_mode, '<NULL>') || '. Expected AUTO or MANUAL.'
    );
  END IF;

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
