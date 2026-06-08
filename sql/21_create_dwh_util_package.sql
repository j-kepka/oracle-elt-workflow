-- Shared process utility helpers.

CREATE OR REPLACE PACKAGE dwh.pkg_dwh_util AS
  FUNCTION normalize_run_mode (
    p_run_mode   IN VARCHAR2,
    p_error_code IN PLS_INTEGER
  ) RETURN VARCHAR2;

  FUNCTION sql_string_literal (
    p_value IN VARCHAR2
  ) RETURN VARCHAR2;

  FUNCTION sql_date_literal (
    p_value IN DATE
  ) RETURN VARCHAR2;

  FUNCTION build_loader_artifact_name (
    p_process_name  IN VARCHAR2,
    p_business_date IN DATE,
    p_attempt_ts    IN TIMESTAMP,
    p_step          IN VARCHAR2,
    p_extension     IN VARCHAR2
  ) RETURN VARCHAR2;

  FUNCTION build_external_source (
    p_table_name     IN VARCHAR2,
    p_snapshot_file  IN VARCHAR2,
    p_work_directory IN VARCHAR2,
    p_process_name   IN VARCHAR2,
    p_business_date  IN DATE,
    p_attempt_ts     IN TIMESTAMP,
    p_step_name      IN VARCHAR2
  ) RETURN VARCHAR2;

  PROCEDURE assert_file_exists (
    p_directory     IN VARCHAR2,
    p_filename      IN VARCHAR2,
    p_error_code    IN PLS_INTEGER,
    p_error_message IN VARCHAR2
  );

  FUNCTION read_ready_row_count (
    p_directory                  IN VARCHAR2,
    p_filename                   IN VARCHAR2,
    p_empty_file_error_code      IN PLS_INTEGER,
    p_invalid_content_error_code IN PLS_INTEGER,
    p_read_error_code            IN PLS_INTEGER
  ) RETURN NUMBER;

  PROCEDURE upsert_process_run (
    p_process_name        IN VARCHAR2,
    p_business_date       IN DATE,
    p_run_mode            IN VARCHAR2,
    p_status              IN VARCHAR2,
    p_reason_code         IN VARCHAR2 DEFAULT NULL,
    p_status_message      IN VARCHAR2 DEFAULT NULL,
    p_expected_row_count  IN NUMBER DEFAULT NULL,
    p_stage_row_count     IN NUMBER DEFAULT 0,
    p_reject_row_count    IN NUMBER DEFAULT 0,
    p_core_row_count      IN NUMBER DEFAULT 0,
    p_data_file_name      IN VARCHAR2 DEFAULT NULL,
    p_ready_file_name     IN VARCHAR2 DEFAULT NULL,
    p_started_ts          IN TIMESTAMP DEFAULT NULL,
    p_finished_ts         IN TIMESTAMP DEFAULT NULL,
    p_scheduled_for_ts    IN TIMESTAMP DEFAULT NULL,
    p_next_retry_ts       IN TIMESTAMP DEFAULT NULL,
    p_retry_count_delta   IN NUMBER DEFAULT 0,
    p_reset_retry_count   IN NUMBER DEFAULT 0
  );
END pkg_dwh_util;
/

CREATE OR REPLACE PACKAGE BODY dwh.pkg_dwh_util AS
  FUNCTION format_run_mode_for_error (
    p_run_mode IN VARCHAR2
  ) RETURN VARCHAR2 AS
  BEGIN
    IF p_run_mode IS NULL THEN
      RETURN '<NULL>';
    END IF;

    IF TRIM(p_run_mode) IS NULL THEN
      RETURN '<BLANK>';
    END IF;

    RETURN SUBSTR(p_run_mode, 1, 100);
  END format_run_mode_for_error;

  FUNCTION normalize_run_mode (
    p_run_mode   IN VARCHAR2,
    p_error_code IN PLS_INTEGER
  ) RETURN VARCHAR2 AS
    l_run_mode VARCHAR2(10 CHAR);
  BEGIN
    l_run_mode := UPPER(TRIM(p_run_mode));

    IF l_run_mode IS NULL OR l_run_mode NOT IN ('AUTO', 'MANUAL') THEN
      RAISE_APPLICATION_ERROR(
        p_error_code,
        'Unsupported run mode '
        || format_run_mode_for_error(p_run_mode)
        || '. Expected AUTO or MANUAL.'
      );
    END IF;

    RETURN l_run_mode;
  END normalize_run_mode;

  FUNCTION sql_string_literal (
    p_value IN VARCHAR2
  ) RETURN VARCHAR2 AS
  BEGIN
    RETURN '''' || REPLACE(p_value, '''', '''''') || '''';
  END sql_string_literal;

  FUNCTION sql_date_literal (
    p_value IN DATE
  ) RETURN VARCHAR2 AS
  BEGIN
    RETURN 'DATE ' || sql_string_literal(TO_CHAR(p_value, 'YYYY-MM-DD'));
  END sql_date_literal;

  FUNCTION build_loader_artifact_name (
    p_process_name  IN VARCHAR2,
    p_business_date IN DATE,
    p_attempt_ts    IN TIMESTAMP,
    p_step          IN VARCHAR2,
    p_extension     IN VARCHAR2
  ) RETURN VARCHAR2 AS
  BEGIN
    RETURN LOWER(p_process_name)
      || '_'
      || TO_CHAR(p_business_date, 'YYYYMMDD')
      || '_'
      || TO_CHAR(p_attempt_ts, 'YYYYMMDDHH24MISSFF6')
      || '_'
      || LOWER(p_step)
      || '.'
      || LOWER(p_extension);
  END build_loader_artifact_name;

  FUNCTION build_external_source (
    p_table_name     IN VARCHAR2,
    p_snapshot_file  IN VARCHAR2,
    p_work_directory IN VARCHAR2,
    p_process_name   IN VARCHAR2,
    p_business_date  IN DATE,
    p_attempt_ts     IN TIMESTAMP,
    p_step_name      IN VARCHAR2
  ) RETURN VARCHAR2 AS
    l_log_file          VARCHAR2(255 CHAR);
    l_bad_file          VARCHAR2(255 CHAR);
    l_discard_file      VARCHAR2(255 CHAR);
    l_access_parameters VARCHAR2(4000 CHAR);
  BEGIN
    l_log_file := build_loader_artifact_name(
      p_process_name,
      p_business_date,
      p_attempt_ts,
      p_step_name,
      'log'
    );
    l_bad_file := build_loader_artifact_name(
      p_process_name,
      p_business_date,
      p_attempt_ts,
      p_step_name,
      'bad'
    );
    l_discard_file := build_loader_artifact_name(
      p_process_name,
      p_business_date,
      p_attempt_ts,
      p_step_name,
      'dsc'
    );

    l_access_parameters := 'LOGFILE '
      || p_work_directory
      || ':'
      || sql_string_literal(l_log_file)
      || ' BADFILE '
      || p_work_directory
      || ':'
      || sql_string_literal(l_bad_file)
      || ' DISCARDFILE '
      || p_work_directory
      || ':'
      || sql_string_literal(l_discard_file);

    RETURN p_table_name
      || ' EXTERNAL MODIFY ('
      || 'ACCESS PARAMETERS ('
      || sql_string_literal(l_access_parameters)
      || ') '
      || 'LOCATION ('
      || sql_string_literal(p_snapshot_file)
      || '))';
  END build_external_source;

  PROCEDURE assert_file_exists (
    p_directory     IN VARCHAR2,
    p_filename      IN VARCHAR2,
    p_error_code    IN PLS_INTEGER,
    p_error_message IN VARCHAR2
  ) AS
    l_file_exists BOOLEAN := FALSE;
    l_file_length NUMBER := 0;
    l_block_size  BINARY_INTEGER := 0;
  BEGIN
    UTL_FILE.FGETATTR(
      location    => p_directory,
      filename    => p_filename,
      fexists     => l_file_exists,
      file_length => l_file_length,
      block_size  => l_block_size
    );

    IF NOT l_file_exists THEN
      RAISE_APPLICATION_ERROR(p_error_code, p_error_message);
    END IF;
  END assert_file_exists;

  FUNCTION read_ready_row_count (
    p_directory                  IN VARCHAR2,
    p_filename                   IN VARCHAR2,
    p_empty_file_error_code      IN PLS_INTEGER,
    p_invalid_content_error_code IN PLS_INTEGER,
    p_read_error_code            IN PLS_INTEGER
  ) RETURN NUMBER AS
    l_ready_file    UTL_FILE.FILE_TYPE;
    l_ready_line    VARCHAR2(32767 CHAR);
    l_expected_rows NUMBER := 0;
  BEGIN
    l_ready_file := UTL_FILE.FOPEN(
      location  => p_directory,
      filename  => p_filename,
      open_mode => 'R'
    );

    BEGIN
      UTL_FILE.GET_LINE(l_ready_file, l_ready_line);
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        UTL_FILE.FCLOSE(l_ready_file);
        RAISE_APPLICATION_ERROR(
          p_empty_file_error_code,
          'Ready file ' || p_filename || ' is empty.'
        );
    END;

    UTL_FILE.FCLOSE(l_ready_file);

    IF NOT REGEXP_LIKE(TRIM(l_ready_line), '^[0-9]+$') THEN
      RAISE_APPLICATION_ERROR(
        p_invalid_content_error_code,
        'Ready file ' || p_filename || ' must contain a single non-negative integer row count.'
      );
    END IF;

    l_expected_rows := TO_NUMBER(TRIM(l_ready_line));
    RETURN l_expected_rows;
  EXCEPTION
    WHEN UTL_FILE.INVALID_PATH
      OR UTL_FILE.INVALID_MODE
      OR UTL_FILE.INVALID_OPERATION
      OR UTL_FILE.READ_ERROR
      OR UTL_FILE.INTERNAL_ERROR THEN
      IF UTL_FILE.IS_OPEN(l_ready_file) THEN
        UTL_FILE.FCLOSE(l_ready_file);
      END IF;

      RAISE_APPLICATION_ERROR(
        p_read_error_code,
        'Unable to read ready file ' || p_filename || ': ' || SQLERRM
      );
  END read_ready_row_count;

  PROCEDURE upsert_process_run (
    p_process_name        IN VARCHAR2,
    p_business_date       IN DATE,
    p_run_mode            IN VARCHAR2,
    p_status              IN VARCHAR2,
    p_reason_code         IN VARCHAR2,
    p_status_message      IN VARCHAR2,
    p_expected_row_count  IN NUMBER,
    p_stage_row_count     IN NUMBER,
    p_reject_row_count    IN NUMBER,
    p_core_row_count      IN NUMBER,
    p_data_file_name      IN VARCHAR2,
    p_ready_file_name     IN VARCHAR2,
    p_started_ts          IN TIMESTAMP,
    p_finished_ts         IN TIMESTAMP,
    p_scheduled_for_ts    IN TIMESTAMP,
    p_next_retry_ts       IN TIMESTAMP,
    p_retry_count_delta   IN NUMBER,
    p_reset_retry_count   IN NUMBER
  ) AS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    MERGE INTO dwh.ctl_process_run dst
    USING (
      SELECT
        p_process_name AS process_name,
        p_business_date AS business_date
      FROM dual
    ) src
    ON (
      dst.process_name = src.process_name
      AND dst.business_date = src.business_date
    )
    WHEN MATCHED THEN
      UPDATE SET
        dst.run_mode = p_run_mode,
        dst.status = p_status,
        dst.reason_code = p_reason_code,
        dst.retry_count = CASE
          WHEN NVL(p_reset_retry_count, 0) = 1 THEN 0
          ELSE GREATEST(0, NVL(dst.retry_count, 0) + NVL(p_retry_count_delta, 0))
        END,
        dst.scheduled_for_ts = NVL(dst.scheduled_for_ts, p_scheduled_for_ts),
        dst.next_retry_ts = p_next_retry_ts,
        dst.started_ts = p_started_ts,
        dst.finished_ts = p_finished_ts,
        dst.expected_row_count = p_expected_row_count,
        dst.stage_row_count = NVL(p_stage_row_count, 0),
        dst.reject_row_count = NVL(p_reject_row_count, 0),
        dst.core_row_count = NVL(p_core_row_count, 0),
        dst.data_file_name = p_data_file_name,
        dst.ready_file_name = p_ready_file_name,
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
        p_process_name,
        p_business_date,
        p_run_mode,
        p_status,
        p_reason_code,
        CASE
          WHEN NVL(p_reset_retry_count, 0) = 1 THEN 0
          ELSE GREATEST(0, NVL(p_retry_count_delta, 0))
        END,
        p_scheduled_for_ts,
        p_next_retry_ts,
        p_started_ts,
        p_finished_ts,
        p_expected_row_count,
        NVL(p_stage_row_count, 0),
        NVL(p_reject_row_count, 0),
        NVL(p_core_row_count, 0),
        p_data_file_name,
        p_ready_file_name,
        SUBSTR(p_status_message, 1, 4000),
        SYSTIMESTAMP,
        SYSTIMESTAMP
      );

    COMMIT;
  END upsert_process_run;
END pkg_dwh_util;
/
