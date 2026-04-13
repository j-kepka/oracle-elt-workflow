-- Creates a procedure that reloads stage and core data from a dated external file snapshot

CREATE OR REPLACE PROCEDURE dwh.prc_load_client_transfers (
  p_date                      IN DATE DEFAULT TRUNC(SYSDATE),
  p_run_mode                  IN VARCHAR2 DEFAULT 'MANUAL',
  p_auto_cutoff_ts            IN TIMESTAMP DEFAULT NULL,
  p_auto_retry_sleep_minutes  IN PLS_INTEGER DEFAULT 15
) AS
  c_process_name       CONSTANT VARCHAR2(100 CHAR) := 'LOAD_CLIENT_TRANSFERS';
  c_ext_dir            CONSTANT VARCHAR2(30 CHAR) := 'EXT_DIR';
  c_ext_work_dir       CONSTANT VARCHAR2(30 CHAR) := 'EXT_WORK_DIR';
  l_business_date      DATE := TRUNC(p_date);
  l_run_mode           VARCHAR2(10 CHAR) := UPPER(TRIM(p_run_mode));
  l_snapshot_file      VARCHAR2(128 CHAR);
  l_ready_file         VARCHAR2(128 CHAR);
  l_expected_rows      NUMBER := 0;
  l_stage_rows_loaded  NUMBER := 0;
  l_reject_rows_loaded NUMBER := 0;
  l_core_rows_loaded   NUMBER := 0;
  l_missing_clients    NUMBER := 0;
  l_attempt_ts         TIMESTAMP;
  l_cutoff_ts          TIMESTAMP;
  l_next_retry_ts      TIMESTAMP;
  l_current_ts         TIMESTAMP;
  l_retry_sleep_minutes PLS_INTEGER := GREATEST(1, NVL(p_auto_retry_sleep_minutes, 15));
  l_wait_started       BOOLEAN := FALSE;
  l_final_status_set   BOOLEAN := FALSE;
  l_stage_ext_source   VARCHAR2(4000 CHAR);
  l_reject_ext_source  VARCHAR2(4000 CHAR);
  l_stage_sql          VARCHAR2(32767 CHAR);
  l_reject_sql         VARCHAR2(32767 CHAR);

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
    p_step      IN VARCHAR2,
    p_extension IN VARCHAR2
  ) RETURN VARCHAR2 AS
  BEGIN
    RETURN LOWER(c_process_name)
      || '_'
      || TO_CHAR(l_business_date, 'YYYYMMDD')
      || '_'
      || TO_CHAR(l_attempt_ts, 'YYYYMMDDHH24MISSFF6')
      || '_'
      || LOWER(p_step)
      || '.'
      || LOWER(p_extension);
  END build_loader_artifact_name;

  FUNCTION build_external_source (
    p_table_name    IN VARCHAR2,
    p_snapshot_file IN VARCHAR2,
    p_step_name     IN VARCHAR2
  ) RETURN VARCHAR2 AS
    l_log_file      VARCHAR2(255 CHAR);
    l_bad_file      VARCHAR2(255 CHAR);
    l_discard_file  VARCHAR2(255 CHAR);
  BEGIN
    l_log_file := build_loader_artifact_name(p_step_name, 'log');
    l_bad_file := build_loader_artifact_name(p_step_name, 'bad');
    l_discard_file := build_loader_artifact_name(p_step_name, 'dsc');

    RETURN p_table_name
      || ' EXTERNAL MODIFY ('
      || 'ACCESS PARAMETERS ('
      || CHR(39)
      || 'LOGFILE '
      || c_ext_work_dir
      || ':'
      || CHR(39)
      || CHR(39)
      || l_log_file
      || CHR(39)
      || CHR(39)
      || ' BADFILE '
      || c_ext_work_dir
      || ':'
      || CHR(39)
      || CHR(39)
      || l_bad_file
      || CHR(39)
      || CHR(39)
      || ' DISCARDFILE '
      || c_ext_work_dir
      || ':'
      || CHR(39)
      || CHR(39)
      || l_discard_file
      || CHR(39)
      || CHR(39)
      || CHR(39)
      || ') '
      || 'LOCATION ('
      || sql_string_literal(p_snapshot_file)
      || '))';
  END build_external_source;

  PROCEDURE assert_file_exists (
    p_directory     IN VARCHAR2,
    p_filename      IN VARCHAR2,
    p_error_code    IN NUMBER,
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

  FUNCTION read_expected_rows (
    p_directory IN VARCHAR2,
    p_filename  IN VARCHAR2
  ) RETURN NUMBER AS
    l_ok_file       UTL_FILE.FILE_TYPE;
    l_ok_line       VARCHAR2(32767 CHAR);
    l_expected_rows NUMBER := 0;
  BEGIN
    l_ok_file := UTL_FILE.FOPEN(
      location  => p_directory,
      filename  => p_filename,
      open_mode => 'R'
    );

    BEGIN
      UTL_FILE.GET_LINE(l_ok_file, l_ok_line);
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        UTL_FILE.FCLOSE(l_ok_file);
        RAISE_APPLICATION_ERROR(
          -20011,
          'Ready file ' || p_filename || ' is empty.'
        );
    END;

    UTL_FILE.FCLOSE(l_ok_file);

    IF NOT REGEXP_LIKE(TRIM(l_ok_line), '^[0-9]+$') THEN
      RAISE_APPLICATION_ERROR(
        -20012,
        'Ready file ' || p_filename || ' must contain a single non-negative integer row count.'
      );
    END IF;

    l_expected_rows := TO_NUMBER(TRIM(l_ok_line));
    RETURN l_expected_rows;
  EXCEPTION
    WHEN UTL_FILE.INVALID_PATH
      OR UTL_FILE.INVALID_MODE
      OR UTL_FILE.INVALID_OPERATION
      OR UTL_FILE.READ_ERROR
      OR UTL_FILE.INTERNAL_ERROR THEN
      IF UTL_FILE.IS_OPEN(l_ok_file) THEN
        UTL_FILE.FCLOSE(l_ok_file);
      END IF;

      RAISE_APPLICATION_ERROR(
        -20013,
        'Unable to read ready file ' || p_filename || ': ' || SQLERRM
      );
  END read_expected_rows;

  PROCEDURE upsert_process_run (
    p_status             IN VARCHAR2,
    p_reason_code        IN VARCHAR2,
    p_status_message     IN VARCHAR2,
    p_expected_row_count IN NUMBER,
    p_stage_row_count    IN NUMBER,
    p_reject_row_count   IN NUMBER,
    p_core_row_count     IN NUMBER,
    p_started_ts         IN TIMESTAMP,
    p_finished_ts        IN TIMESTAMP,
    p_next_retry_ts      IN TIMESTAMP,
    p_retry_count_delta  IN NUMBER DEFAULT 0
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
        dst.retry_count = GREATEST(0, NVL(dst.retry_count, 0) + NVL(p_retry_count_delta, 0)),
        dst.scheduled_for_ts = NVL(dst.scheduled_for_ts, l_attempt_ts),
        dst.next_retry_ts = p_next_retry_ts,
        dst.started_ts = p_started_ts,
        dst.finished_ts = p_finished_ts,
        dst.expected_row_count = p_expected_row_count,
        dst.stage_row_count = p_stage_row_count,
        dst.reject_row_count = p_reject_row_count,
        dst.core_row_count = p_core_row_count,
        dst.data_file_name = l_snapshot_file,
        dst.ready_file_name = l_ready_file,
        dst.status_message = p_status_message,
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
        GREATEST(0, NVL(p_retry_count_delta, 0)),
        l_attempt_ts,
        p_next_retry_ts,
        p_started_ts,
        p_finished_ts,
        p_expected_row_count,
        p_stage_row_count,
        p_reject_row_count,
        p_core_row_count,
        l_snapshot_file,
        l_ready_file,
        p_status_message,
        SYSTIMESTAMP,
        SYSTIMESTAMP
      );

    COMMIT;
  END upsert_process_run;
BEGIN
  l_attempt_ts := SYSTIMESTAMP;
  l_snapshot_file := 'client_transfers_' || TO_CHAR(l_business_date, 'YYYYMMDD') || '.csv';
  l_ready_file := 'client_transfers_' || TO_CHAR(l_business_date, 'YYYYMMDD') || '.ok';
  l_cutoff_ts := NVL(
    p_auto_cutoff_ts,
    CAST(TRUNC(CAST(l_attempt_ts AS DATE)) AS TIMESTAMP) + NUMTODSINTERVAL(12, 'HOUR')
  );

  IF l_run_mode NOT IN ('AUTO', 'MANUAL') THEN
    RAISE_APPLICATION_ERROR(
      -20016,
      'Unsupported run mode ' || NVL(p_run_mode, '<NULL>') || '. Expected AUTO or MANUAL.'
    );
  END IF;

  upsert_process_run(
    p_status             => 'PROCESSING',
    p_reason_code        => NULL,
    p_status_message     => 'Load attempt started.',
    p_expected_row_count => NULL,
    p_stage_row_count    => 0,
    p_reject_row_count   => 0,
    p_core_row_count     => 0,
    p_started_ts         => l_attempt_ts,
    p_finished_ts        => NULL,
    p_next_retry_ts      => NULL
  );

  LOOP
    BEGIN
      assert_file_exists(
        p_directory     => c_ext_dir,
        p_filename      => l_ready_file,
        p_error_code    => -20010,
        p_error_message => 'Ready file ' || l_ready_file || ' not found.'
      );

      EXIT;
    EXCEPTION
      WHEN OTHERS THEN
        IF SQLCODE = -20010 THEN
          l_current_ts := SYSTIMESTAMP;

          IF l_run_mode = 'AUTO' AND l_current_ts < l_cutoff_ts THEN
            l_next_retry_ts := LEAST(
              l_cutoff_ts,
              l_current_ts + NUMTODSINTERVAL(l_retry_sleep_minutes, 'MINUTE')
            );

            upsert_process_run(
              p_status             => 'WAITING',
              p_reason_code        => 'WAITING_FOR_OK',
              p_status_message     => 'Ready file not available before the run-day cutoff. Waiting inside the procedure for the next retry window.',
              p_expected_row_count => NULL,
              p_stage_row_count    => 0,
              p_reject_row_count   => 0,
              p_core_row_count     => 0,
              p_started_ts         => l_attempt_ts,
              p_finished_ts        => l_current_ts,
              p_next_retry_ts      => l_next_retry_ts,
              p_retry_count_delta  => 1
            );

            l_wait_started := TRUE;

            DBMS_OUTPUT.PUT_LINE(
              'dwh.prc_load_client_transfers business_date='
              || TO_CHAR(l_business_date, 'YYYY-MM-DD')
              || ', status=WAITING'
              || ', reason=WAITING_FOR_OK'
              || ', next_retry_ts='
              || TO_CHAR(l_next_retry_ts, 'YYYY-MM-DD HH24:MI:SS')
            );

            -- The process contract is expressed in minutes; DBMS_SESSION.SLEEP expects seconds.
            DBMS_SESSION.SLEEP(l_retry_sleep_minutes * 60);
          ELSE
            upsert_process_run(
              p_status             => 'FAILED',
              p_reason_code        => CASE
                                        WHEN l_run_mode = 'MANUAL' THEN 'MISSING_OK_MANUAL'
                                        ELSE 'MISSING_OK_AFTER_CUTOFF'
                                      END,
              p_status_message     => SQLERRM,
              p_expected_row_count => NULL,
              p_stage_row_count    => 0,
              p_reject_row_count   => 0,
              p_core_row_count     => 0,
              p_started_ts         => l_attempt_ts,
              p_finished_ts        => SYSTIMESTAMP,
              p_next_retry_ts      => NULL
            );

            l_final_status_set := TRUE;
            RAISE;
          END IF;
        ELSE
          RAISE;
        END IF;
    END;
  END LOOP;

  IF l_wait_started THEN
    upsert_process_run(
      p_status             => 'PROCESSING',
      p_reason_code        => NULL,
      p_status_message     => 'Ready file detected after WAITING. Continuing with validation and load.',
      p_expected_row_count => NULL,
      p_stage_row_count    => 0,
      p_reject_row_count   => 0,
      p_core_row_count     => 0,
      p_started_ts         => l_attempt_ts,
      p_finished_ts        => NULL,
      p_next_retry_ts      => NULL
    );
  END IF;

  BEGIN
    l_expected_rows := read_expected_rows(
      p_directory => c_ext_dir,
      p_filename  => l_ready_file
    );
  EXCEPTION
    WHEN OTHERS THEN
      upsert_process_run(
        p_status             => 'FAILED',
        p_reason_code        => CASE
                                  WHEN SQLCODE IN (-20011, -20012) THEN 'INVALID_OK_CONTENT'
                                  ELSE 'READY_FILE_READ_ERROR'
                                END,
        p_status_message     => SQLERRM,
        p_expected_row_count => NULL,
        p_stage_row_count    => 0,
        p_reject_row_count   => 0,
        p_core_row_count     => 0,
        p_started_ts         => l_attempt_ts,
        p_finished_ts        => SYSTIMESTAMP,
        p_next_retry_ts      => NULL
      );

      l_final_status_set := TRUE;
      RAISE;
  END;

  BEGIN
    assert_file_exists(
      p_directory     => c_ext_dir,
      p_filename      => l_snapshot_file,
      p_error_code    => -20014,
      p_error_message => 'Data file ' || l_snapshot_file || ' not found although ready file exists.'
    );
  EXCEPTION
    WHEN OTHERS THEN
      upsert_process_run(
        p_status             => 'FAILED',
        p_reason_code        => 'MISSING_DATA_FILE',
        p_status_message     => SQLERRM,
        p_expected_row_count => l_expected_rows,
        p_stage_row_count    => 0,
        p_reject_row_count   => 0,
        p_core_row_count     => 0,
        p_started_ts         => l_attempt_ts,
        p_finished_ts        => SYSTIMESTAMP,
        p_next_retry_ts      => NULL
      );

      l_final_status_set := TRUE;
      RAISE;
  END;

  upsert_process_run(
    p_status             => 'PROCESSING',
    p_reason_code        => NULL,
    p_status_message     => 'Validated ready file. Loading stage and reject candidates.',
    p_expected_row_count => l_expected_rows,
    p_stage_row_count    => 0,
    p_reject_row_count   => 0,
    p_core_row_count     => 0,
    p_started_ts         => l_attempt_ts,
    p_finished_ts        => NULL,
    p_next_retry_ts      => NULL
  );

  l_stage_ext_source := build_external_source(
    p_table_name    => 'dwh.ext_client_transfers',
    p_snapshot_file => l_snapshot_file,
    p_step_name     => 'stage'
  );

  l_reject_ext_source := build_external_source(
    p_table_name    => 'dwh.ext_client_transfers',
    p_snapshot_file => l_snapshot_file,
    p_step_name     => 'reject'
  );

  DELETE FROM dwh.stg_client_transfers
  WHERE business_date = l_business_date;

  DELETE FROM dwh.stg_client_transfers_reject
  WHERE business_date = l_business_date;

  l_stage_sql := REPLACE(
    REPLACE(
      q'~
  INSERT INTO dwh.stg_client_transfers (
    business_date,
    source_row_num,
    transfer_id,
    client_id,
    source_account,
    target_account,
    amount,
    currency_code,
    transfer_ts,
    transfer_status,
    channel,
    country_code,
    transfer_title
  )
  WITH normalized_data AS (
    SELECT
      __BUSINESS_DATE__ AS business_date,
      source_row_num,
      TRIM(transfer_id_raw) AS transfer_id_raw,
      TRIM(client_id_raw) AS client_id_raw,
      TRIM(source_account_raw) AS source_account_raw,
      TRIM(target_account_raw) AS target_account_raw,
      TRIM(amount_raw) AS amount_raw,
      UPPER(TRIM(currency_code_raw)) AS currency_code_raw,
      TRIM(transfer_ts_raw) AS transfer_ts_raw,
      UPPER(TRIM(transfer_status_raw)) AS transfer_status_raw,
      UPPER(TRIM(channel_raw)) AS channel_raw,
      UPPER(TRIM(country_code_raw)) AS country_code_raw,
      TRIM(transfer_title_raw) AS transfer_title_raw,
      TO_NUMBER(TRIM(transfer_id_raw) DEFAULT NULL ON CONVERSION ERROR) AS transfer_id,
      TO_NUMBER(TRIM(client_id_raw) DEFAULT NULL ON CONVERSION ERROR) AS client_id,
      TO_NUMBER(TRIM(amount_raw) DEFAULT NULL ON CONVERSION ERROR) AS amount,
      TO_TIMESTAMP(
        TRIM(transfer_ts_raw) DEFAULT NULL ON CONVERSION ERROR,
        'YYYY-MM-DD HH24:MI:SS'
      ) AS transfer_ts,
      -- Temporary business scope until reference tables are added.
      CASE
        WHEN UPPER(TRIM(currency_code_raw)) IN ('EUR', 'USD', 'PLN', 'CZK', 'GBP') THEN 1
        ELSE 0
      END AS is_supported_currency,
      CASE
        WHEN UPPER(TRIM(country_code_raw)) IN (
          'AT', 'BE', 'BG', 'CH', 'CY', 'CZ', 'DE', 'DK', 'EE', 'ES', 'FI', 'FR',
          'GB', 'GR', 'HR', 'HU', 'IE', 'IS', 'IT', 'LI', 'LT', 'LU', 'LV', 'MT',
          'NL', 'NO', 'PL', 'PT', 'RO', 'SE', 'SI', 'SK', 'US'
        ) THEN 1
        ELSE 0
      END AS is_supported_country
    FROM __EXT_SOURCE__
  ),
  valid_data AS (
    SELECT
      business_date,
      source_row_num,
      transfer_id_raw,
      client_id_raw,
      source_account_raw,
      target_account_raw,
      amount_raw,
      currency_code_raw,
      transfer_ts_raw,
      transfer_status_raw,
      channel_raw,
      country_code_raw,
      transfer_title_raw,
      transfer_id,
      client_id,
      amount,
      transfer_ts,
      COUNT(*) OVER (PARTITION BY business_date, transfer_id) AS duplicate_key_count
    FROM normalized_data
    -- Synthetic demo data only: account validation is intentionally simplified.
    -- It checks presence and max length, without IBAN checksum validation.
    WHERE transfer_id IS NOT NULL
      AND client_id IS NOT NULL
      AND source_account_raw IS NOT NULL
      AND LENGTH(source_account_raw) <= 34
      AND target_account_raw IS NOT NULL
      AND LENGTH(target_account_raw) <= 34
      AND amount IS NOT NULL
      AND amount >= 0
      AND currency_code_raw IS NOT NULL
      AND REGEXP_LIKE(currency_code_raw, '^[A-Z]{3}$')
      AND is_supported_currency = 1
      AND transfer_ts IS NOT NULL
      AND transfer_status_raw IS NOT NULL
      AND transfer_status_raw IN ('COMPLETED', 'PENDING', 'REJECTED', 'FAILED')
      AND channel_raw IS NOT NULL
      AND channel_raw IN ('API', 'BRANCH', 'MOBILE', 'WEB')
      AND country_code_raw IS NOT NULL
      AND REGEXP_LIKE(country_code_raw, '^[A-Z]{2}$')
      AND is_supported_country = 1
      AND (transfer_title_raw IS NULL OR LENGTH(transfer_title_raw) <= 255)
  )
  SELECT
    business_date,
    source_row_num,
    transfer_id,
    client_id,
    source_account_raw,
    target_account_raw,
    amount,
    currency_code_raw,
    transfer_ts,
    transfer_status_raw,
    channel_raw,
    country_code_raw,
    transfer_title_raw
  FROM valid_data
  WHERE duplicate_key_count = 1;~',
      '__EXT_SOURCE__',
      l_stage_ext_source
    ),
    '__BUSINESS_DATE__',
    sql_date_literal(l_business_date)
  );

  EXECUTE IMMEDIATE l_stage_sql;

  l_stage_rows_loaded := SQL%ROWCOUNT;

  l_reject_sql := REPLACE(
    REPLACE(
      REPLACE(
        q'~
  INSERT INTO dwh.stg_client_transfers_reject (
    business_date,
    source_file_name,
    source_row_num,
    transfer_id_raw,
    client_id_raw,
    source_account_raw,
    target_account_raw,
    amount_raw,
    currency_code_raw,
    transfer_ts_raw,
    transfer_status_raw,
    channel_raw,
    country_code_raw,
    transfer_title_raw,
    reject_reason
  )
  WITH normalized_data AS (
    SELECT
      __BUSINESS_DATE__ AS business_date,
      source_row_num,
      TRIM(transfer_id_raw) AS transfer_id_raw,
      TRIM(client_id_raw) AS client_id_raw,
      TRIM(source_account_raw) AS source_account_raw,
      TRIM(target_account_raw) AS target_account_raw,
      TRIM(amount_raw) AS amount_raw,
      UPPER(TRIM(currency_code_raw)) AS currency_code_raw,
      TRIM(transfer_ts_raw) AS transfer_ts_raw,
      UPPER(TRIM(transfer_status_raw)) AS transfer_status_raw,
      UPPER(TRIM(channel_raw)) AS channel_raw,
      UPPER(TRIM(country_code_raw)) AS country_code_raw,
      TRIM(transfer_title_raw) AS transfer_title_raw,
      TO_NUMBER(TRIM(transfer_id_raw) DEFAULT NULL ON CONVERSION ERROR) AS transfer_id,
      TO_NUMBER(TRIM(client_id_raw) DEFAULT NULL ON CONVERSION ERROR) AS client_id,
      TO_NUMBER(TRIM(amount_raw) DEFAULT NULL ON CONVERSION ERROR) AS amount,
      TO_TIMESTAMP(
        TRIM(transfer_ts_raw) DEFAULT NULL ON CONVERSION ERROR,
        'YYYY-MM-DD HH24:MI:SS'
      ) AS transfer_ts,
      -- Temporary business scope until reference tables are added.
      CASE
        WHEN UPPER(TRIM(currency_code_raw)) IN ('EUR', 'USD', 'PLN', 'CZK', 'GBP') THEN 1
        ELSE 0
      END AS is_supported_currency,
      CASE
        WHEN UPPER(TRIM(country_code_raw)) IN (
          'AT', 'BE', 'BG', 'CH', 'CY', 'CZ', 'DE', 'DK', 'EE', 'ES', 'FI', 'FR',
          'GB', 'GR', 'HR', 'HU', 'IE', 'IS', 'IT', 'LI', 'LT', 'LU', 'LV', 'MT',
          'NL', 'NO', 'PL', 'PT', 'RO', 'SE', 'SI', 'SK', 'US'
        ) THEN 1
        ELSE 0
      END AS is_supported_country
    FROM __EXT_SOURCE__
  ),
  valid_data AS (
    SELECT
      business_date,
      source_row_num,
      transfer_id_raw,
      client_id_raw,
      source_account_raw,
      target_account_raw,
      amount_raw,
      currency_code_raw,
      transfer_ts_raw,
      transfer_status_raw,
      channel_raw,
      country_code_raw,
      transfer_title_raw,
      transfer_id,
      client_id,
      amount,
      transfer_ts,
      COUNT(*) OVER (PARTITION BY business_date, transfer_id) AS duplicate_key_count
    FROM normalized_data
    -- Synthetic demo data only: account validation is intentionally simplified.
    -- It checks presence and max length, without IBAN checksum validation.
    WHERE transfer_id IS NOT NULL
      AND client_id IS NOT NULL
      AND source_account_raw IS NOT NULL
      AND LENGTH(source_account_raw) <= 34
      AND target_account_raw IS NOT NULL
      AND LENGTH(target_account_raw) <= 34
      AND amount IS NOT NULL
      AND amount >= 0
      AND currency_code_raw IS NOT NULL
      AND REGEXP_LIKE(currency_code_raw, '^[A-Z]{3}$')
      AND is_supported_currency = 1
      AND transfer_ts IS NOT NULL
      AND transfer_status_raw IS NOT NULL
      AND transfer_status_raw IN ('COMPLETED', 'PENDING', 'REJECTED', 'FAILED')
      AND channel_raw IS NOT NULL
      AND channel_raw IN ('API', 'BRANCH', 'MOBILE', 'WEB')
      AND country_code_raw IS NOT NULL
      AND REGEXP_LIKE(country_code_raw, '^[A-Z]{2}$')
      AND is_supported_country = 1
      AND (transfer_title_raw IS NULL OR LENGTH(transfer_title_raw) <= 255)
  ),
  invalid_data AS (
    SELECT
      business_date,
      __SOURCE_FILE__ AS source_file_name,
      source_row_num,
      transfer_id_raw,
      client_id_raw,
      source_account_raw,
      target_account_raw,
      amount_raw,
      currency_code_raw,
      transfer_ts_raw,
      transfer_status_raw,
      channel_raw,
      country_code_raw,
      transfer_title_raw,
      RTRIM(
        CASE WHEN transfer_id IS NULL THEN 'invalid transfer_id; ' END
        || CASE WHEN client_id IS NULL THEN 'invalid client_id; ' END
        || CASE WHEN source_account_raw IS NULL THEN 'missing source_account; ' END
        -- Synthetic demo data only: no checksum validation yet for bank accounts.
        || CASE WHEN source_account_raw IS NOT NULL AND LENGTH(source_account_raw) > 34
          THEN 'source_account too long; '
        END
        || CASE WHEN target_account_raw IS NULL THEN 'missing target_account; ' END
        || CASE WHEN target_account_raw IS NOT NULL AND LENGTH(target_account_raw) > 34
          THEN 'target_account too long; '
        END
        || CASE WHEN amount IS NULL THEN 'invalid amount; ' END
        || CASE WHEN amount IS NOT NULL AND amount < 0 THEN 'negative amount; ' END
        || CASE
          WHEN currency_code_raw IS NULL THEN 'missing currency_code; '
          WHEN NOT REGEXP_LIKE(currency_code_raw, '^[A-Z]{3}$')
            THEN 'invalid currency_code format; '
          WHEN is_supported_currency = 0 THEN 'unsupported currency_code; '
        END
        || CASE WHEN transfer_ts IS NULL THEN 'invalid transfer_ts; ' END
        || CASE
          WHEN transfer_status_raw IS NULL THEN 'missing transfer_status; '
          WHEN transfer_status_raw NOT IN ('COMPLETED', 'PENDING', 'REJECTED', 'FAILED')
            THEN 'invalid transfer_status; '
        END
        || CASE
          WHEN channel_raw IS NULL THEN 'missing channel; '
          WHEN channel_raw NOT IN ('API', 'BRANCH', 'MOBILE', 'WEB')
            THEN 'invalid channel; '
        END
        || CASE
          WHEN country_code_raw IS NULL THEN 'missing country_code; '
          WHEN NOT REGEXP_LIKE(country_code_raw, '^[A-Z]{2}$')
            THEN 'invalid country_code format; '
          WHEN is_supported_country = 0 THEN 'unsupported country_code; '
        END
        || CASE
          WHEN transfer_title_raw IS NOT NULL AND LENGTH(transfer_title_raw) > 255
            THEN 'transfer_title too long; '
        END,
        '; '
      ) AS reject_reason
    FROM normalized_data
    WHERE transfer_id IS NULL
      OR client_id IS NULL
      OR source_account_raw IS NULL
      OR LENGTH(source_account_raw) > 34
      OR target_account_raw IS NULL
      OR LENGTH(target_account_raw) > 34
      OR amount IS NULL
      OR amount < 0
      OR currency_code_raw IS NULL
      OR NOT REGEXP_LIKE(currency_code_raw, '^[A-Z]{3}$')
      OR is_supported_currency = 0
      OR transfer_ts IS NULL
      OR transfer_status_raw IS NULL
      OR transfer_status_raw NOT IN ('COMPLETED', 'PENDING', 'REJECTED', 'FAILED')
      OR channel_raw IS NULL
      OR channel_raw NOT IN ('API', 'BRANCH', 'MOBILE', 'WEB')
      OR country_code_raw IS NULL
      OR NOT REGEXP_LIKE(country_code_raw, '^[A-Z]{2}$')
      OR is_supported_country = 0
      OR (transfer_title_raw IS NOT NULL AND LENGTH(transfer_title_raw) > 255)
  ),
  duplicate_data AS (
    SELECT
      business_date,
      __SOURCE_FILE__ AS source_file_name,
      source_row_num,
      transfer_id_raw,
      client_id_raw,
      source_account_raw,
      target_account_raw,
      amount_raw,
      currency_code_raw,
      transfer_ts_raw,
      transfer_status_raw,
      channel_raw,
      country_code_raw,
      transfer_title_raw,
      'duplicate transfer_id in snapshot' AS reject_reason
    FROM valid_data
    WHERE duplicate_key_count > 1
  )
  SELECT
    business_date,
    source_file_name,
    source_row_num,
    transfer_id_raw,
    client_id_raw,
    source_account_raw,
    target_account_raw,
    amount_raw,
    currency_code_raw,
    transfer_ts_raw,
    transfer_status_raw,
    channel_raw,
    country_code_raw,
    transfer_title_raw,
    reject_reason
  FROM invalid_data
  UNION ALL
  SELECT
    business_date,
    source_file_name,
    source_row_num,
    transfer_id_raw,
    client_id_raw,
    source_account_raw,
    target_account_raw,
    amount_raw,
    currency_code_raw,
    transfer_ts_raw,
    transfer_status_raw,
    channel_raw,
    country_code_raw,
    transfer_title_raw,
    reject_reason
  FROM duplicate_data;~',
        '__EXT_SOURCE__',
        l_reject_ext_source
      ),
      '__BUSINESS_DATE__',
      sql_date_literal(l_business_date)
    ),
    '__SOURCE_FILE__',
    sql_string_literal(l_snapshot_file)
  );

  EXECUTE IMMEDIATE l_reject_sql;

  l_reject_rows_loaded := SQL%ROWCOUNT;

  IF l_expected_rows = l_stage_rows_loaded + l_reject_rows_loaded + 1 THEN
    upsert_process_run(
      p_status             => 'FAILED',
      p_reason_code        => 'OK_COUNT_INCLUDES_HEADER',
      p_status_message     => 'Ready file row count appears to include the CSV header.',
      p_expected_row_count => l_expected_rows,
      p_stage_row_count    => l_stage_rows_loaded,
      p_reject_row_count   => l_reject_rows_loaded,
      p_core_row_count     => 0,
      p_started_ts         => l_attempt_ts,
      p_finished_ts        => SYSTIMESTAMP,
      p_next_retry_ts      => NULL
    );

    l_final_status_set := TRUE;

    RAISE_APPLICATION_ERROR(
      -20017,
      'Ready file row count appears to include the CSV header. Expected data rows only: '
      || (l_stage_rows_loaded + l_reject_rows_loaded)
      || ', ready file says '
      || l_expected_rows
      || '.'
    );
  ELSIF l_expected_rows != l_stage_rows_loaded + l_reject_rows_loaded THEN
    upsert_process_run(
      p_status             => 'FAILED',
      p_reason_code        => 'OK_COUNT_MISMATCH',
      p_status_message     => 'Ready file row count mismatch.',
      p_expected_row_count => l_expected_rows,
      p_stage_row_count    => l_stage_rows_loaded,
      p_reject_row_count   => l_reject_rows_loaded,
      p_core_row_count     => 0,
      p_started_ts         => l_attempt_ts,
      p_finished_ts        => SYSTIMESTAMP,
      p_next_retry_ts      => NULL
    );

    l_final_status_set := TRUE;

    RAISE_APPLICATION_ERROR(
      -20015,
      'Ready file row count mismatch. Expected '
      || l_expected_rows
      || ' rows but got '
      || (l_stage_rows_loaded + l_reject_rows_loaded)
      || '.'
    );
  END IF;

  upsert_process_run(
    p_status             => 'PROCESSING',
    p_reason_code        => NULL,
    p_status_message     => 'Validated row counts. Refreshing core.',
    p_expected_row_count => l_expected_rows,
    p_stage_row_count    => l_stage_rows_loaded,
    p_reject_row_count   => l_reject_rows_loaded,
    p_core_row_count     => 0,
    p_started_ts         => l_attempt_ts,
    p_finished_ts        => NULL,
    p_next_retry_ts      => NULL
  );

  SELECT COUNT(*)
  INTO l_missing_clients
  FROM dwh.stg_client_transfers stg
  WHERE stg.business_date = l_business_date
    AND NOT EXISTS (
      SELECT 1
      FROM dwh.core_clients cli
      WHERE cli.business_date = stg.business_date
        AND cli.client_id = stg.client_id
    );

  IF l_missing_clients > 0 THEN
    upsert_process_run(
      p_status             => 'FAILED',
      p_reason_code        => 'MISSING_CLIENT_SNAPSHOT',
      p_status_message     => 'Some transfer rows do not have a matching client snapshot for the same business_date.',
      p_expected_row_count => l_expected_rows,
      p_stage_row_count    => l_stage_rows_loaded,
      p_reject_row_count   => l_reject_rows_loaded,
      p_core_row_count     => 0,
      p_started_ts         => l_attempt_ts,
      p_finished_ts        => SYSTIMESTAMP,
      p_next_retry_ts      => NULL
    );

    l_final_status_set := TRUE;

    RAISE_APPLICATION_ERROR(
      -20018,
      'Missing client snapshot for '
      || l_missing_clients
      || ' accepted transfer row(s) on business_date '
      || TO_CHAR(l_business_date, 'YYYY-MM-DD')
      || '.'
    );
  END IF;

  DELETE FROM dwh.core_client_transfers
  WHERE business_date = l_business_date;

  INSERT INTO dwh.core_client_transfers (
    business_date,
    transfer_id,
    client_id,
    source_account,
    target_account,
    amount,
    currency_code,
    transfer_ts,
    transfer_status,
    channel,
    country_code,
    transfer_title
  )
  SELECT
    business_date,
    transfer_id,
    client_id,
    source_account,
    target_account,
    amount,
    currency_code,
    transfer_ts,
    transfer_status,
    channel,
    country_code,
    transfer_title
  FROM dwh.stg_client_transfers
  WHERE business_date = l_business_date;

  l_core_rows_loaded := SQL%ROWCOUNT;
  COMMIT;

  IF l_reject_rows_loaded > 0 THEN
    upsert_process_run(
      p_status             => 'WARNING',
      p_reason_code        => 'INPUT_VALIDATION_WARNING',
      p_status_message     => 'Load completed with rejected input rows.',
      p_expected_row_count => l_expected_rows,
      p_stage_row_count    => l_stage_rows_loaded,
      p_reject_row_count   => l_reject_rows_loaded,
      p_core_row_count     => l_core_rows_loaded,
      p_started_ts         => l_attempt_ts,
      p_finished_ts        => SYSTIMESTAMP,
      p_next_retry_ts      => NULL
    );

    l_final_status_set := TRUE;

    DBMS_OUTPUT.PUT_LINE(
      'dwh.prc_load_client_transfers business_date='
      || TO_CHAR(l_business_date, 'YYYY-MM-DD')
      || ', status=WARNING'
      || ', file='
      || l_snapshot_file
      || ', ok='
      || l_ready_file
      || ', expected='
      || l_expected_rows
      || ', stage='
      || l_stage_rows_loaded
      || ', reject='
      || l_reject_rows_loaded
      || ', core='
      || l_core_rows_loaded
    );
  ELSE
    upsert_process_run(
      p_status             => 'DONE',
      p_reason_code        => 'LOAD_DONE',
      p_status_message     => 'Load completed successfully.',
      p_expected_row_count => l_expected_rows,
      p_stage_row_count    => l_stage_rows_loaded,
      p_reject_row_count   => l_reject_rows_loaded,
      p_core_row_count     => l_core_rows_loaded,
      p_started_ts         => l_attempt_ts,
      p_finished_ts        => SYSTIMESTAMP,
      p_next_retry_ts      => NULL
    );

    l_final_status_set := TRUE;

    DBMS_OUTPUT.PUT_LINE(
      'dwh.prc_load_client_transfers business_date='
      || TO_CHAR(l_business_date, 'YYYY-MM-DD')
      || ', status=DONE'
      || ', file='
      || l_snapshot_file
      || ', ok='
      || l_ready_file
      || ', expected='
      || l_expected_rows
      || ', stage='
      || l_stage_rows_loaded
      || ', reject='
      || l_reject_rows_loaded
      || ', core='
      || l_core_rows_loaded
    );
  END IF;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;

    IF NOT l_final_status_set THEN
      upsert_process_run(
        p_status             => 'FAILED',
        p_reason_code        => 'UNEXPECTED_ERROR',
        p_status_message     => SQLERRM,
        p_expected_row_count => l_expected_rows,
        p_stage_row_count    => l_stage_rows_loaded,
        p_reject_row_count   => l_reject_rows_loaded,
        p_core_row_count     => l_core_rows_loaded,
        p_started_ts         => l_attempt_ts,
        p_finished_ts        => SYSTIMESTAMP,
        p_next_retry_ts      => NULL
      );
    END IF;

    RAISE;
END;
/
