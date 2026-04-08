-- Creates a procedure that reloads client snapshot data from a dated external file snapshot

CREATE OR REPLACE PROCEDURE dwh.prc_load_clients (
  p_date     IN DATE DEFAULT TRUNC(SYSDATE),
  p_run_mode IN VARCHAR2 DEFAULT 'MANUAL'
) AS
  c_process_name       CONSTANT VARCHAR2(100 CHAR) := 'LOAD_CLIENTS';
  c_ext_dir            CONSTANT VARCHAR2(30 CHAR) := 'EXT_DIR';
  l_business_date      DATE := TRUNC(p_date);
  l_run_mode           VARCHAR2(10 CHAR) := UPPER(TRIM(p_run_mode));
  l_snapshot_file      VARCHAR2(128 CHAR);
  l_ready_file         VARCHAR2(128 CHAR);
  l_expected_rows      NUMBER := 0;
  l_stage_rows_loaded  NUMBER := 0;
  l_reject_rows_loaded NUMBER := 0;
  l_core_rows_loaded   NUMBER := 0;
  l_attempt_ts         TIMESTAMP := SYSTIMESTAMP;
  l_cutoff_ts          TIMESTAMP;
  l_next_retry_ts      TIMESTAMP;
  l_final_status_set   BOOLEAN := FALSE;

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
          -20111,
          'Ready file ' || p_filename || ' is empty.'
        );
    END;

    UTL_FILE.FCLOSE(l_ok_file);

    IF NOT REGEXP_LIKE(TRIM(l_ok_line), '^[0-9]+$') THEN
      RAISE_APPLICATION_ERROR(
        -20112,
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
        -20113,
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
  l_snapshot_file := 'clients_' || TO_CHAR(l_business_date, 'YYYYMMDD') || '.csv';
  l_ready_file := 'clients_' || TO_CHAR(l_business_date, 'YYYYMMDD') || '.ok';
  l_cutoff_ts := CAST(l_business_date AS TIMESTAMP) + NUMTODSINTERVAL(12, 'HOUR');

  IF l_run_mode NOT IN ('AUTO', 'MANUAL') THEN
    RAISE_APPLICATION_ERROR(
      -20116,
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

  BEGIN
    assert_file_exists(
      p_directory     => c_ext_dir,
      p_filename      => l_ready_file,
      p_error_code    => -20110,
      p_error_message => 'Ready file ' || l_ready_file || ' not found.'
    );
  EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE = -20110 THEN
        IF l_run_mode = 'AUTO' AND l_attempt_ts < l_cutoff_ts THEN
          IF l_attempt_ts + NUMTODSINTERVAL(30, 'MINUTE') < l_cutoff_ts THEN
            l_next_retry_ts := l_attempt_ts + NUMTODSINTERVAL(30, 'MINUTE');
          ELSE
            l_next_retry_ts := l_cutoff_ts;
          END IF;

          upsert_process_run(
            p_status             => 'WAITING',
            p_reason_code        => 'WAITING_FOR_OK',
            p_status_message     => 'Ready file not available before cutoff. Waiting for retry.',
            p_expected_row_count => NULL,
            p_stage_row_count    => 0,
            p_reject_row_count   => 0,
            p_core_row_count     => 0,
            p_started_ts         => l_attempt_ts,
            p_finished_ts        => SYSTIMESTAMP,
            p_next_retry_ts      => l_next_retry_ts,
            p_retry_count_delta  => 1
          );

          l_final_status_set := TRUE;

          DBMS_OUTPUT.PUT_LINE(
            'dwh.prc_load_clients business_date='
            || TO_CHAR(l_business_date, 'YYYY-MM-DD')
            || ', status=WAITING'
            || ', reason=WAITING_FOR_OK'
            || ', next_retry_ts='
            || TO_CHAR(l_next_retry_ts, 'YYYY-MM-DD HH24:MI:SS')
          );

          RETURN;
        END IF;

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
      END IF;

      RAISE;
  END;

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
                                  WHEN SQLCODE IN (-20111, -20112) THEN 'INVALID_OK_CONTENT'
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
      p_error_code    => -20114,
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

  EXECUTE IMMEDIATE
    'ALTER TABLE dwh.ext_clients LOCATION (''' || l_snapshot_file || ''')';

  DELETE FROM dwh.stg_clients
  WHERE business_date = l_business_date;

  DELETE FROM dwh.stg_clients_reject
  WHERE business_date = l_business_date;

  INSERT INTO dwh.stg_clients (
    business_date,
    source_row_num,
    client_id,
    client_type,
    full_name,
    first_name,
    last_name,
    company_name,
    date_of_birth,
    document_id,
    registration_no,
    tax_id,
    address_line_1,
    city,
    postal_code,
    country_code,
    phone_number,
    email,
    pep_flag,
    high_risk_flag,
    kyc_status,
    risk_score,
    client_status,
    relationship_purpose_code,
    expected_activity_level,
    source_of_funds_declared,
    source_of_wealth_declared
  )
  WITH normalized_data AS (
    SELECT
      source_row_num,
      TRIM(business_date_raw) AS business_date_raw,
      TRIM(client_id_raw) AS client_id_raw,
      UPPER(TRIM(client_type_raw)) AS client_type_raw,
      TRIM(full_name_raw) AS full_name_raw,
      TRIM(first_name_raw) AS first_name_raw,
      TRIM(last_name_raw) AS last_name_raw,
      TRIM(company_name_raw) AS company_name_raw,
      TRIM(date_of_birth_raw) AS date_of_birth_raw,
      TRIM(document_id_raw) AS document_id_raw,
      TRIM(registration_no_raw) AS registration_no_raw,
      TRIM(tax_id_raw) AS tax_id_raw,
      TRIM(address_line_1_raw) AS address_line_1_raw,
      TRIM(city_raw) AS city_raw,
      TRIM(postal_code_raw) AS postal_code_raw,
      UPPER(TRIM(country_code_raw)) AS country_code_raw,
      TRIM(phone_number_raw) AS phone_number_raw,
      TRIM(email_raw) AS email_raw,
      TRIM(pep_flag_raw) AS pep_flag_raw,
      TRIM(high_risk_flag_raw) AS high_risk_flag_raw,
      UPPER(TRIM(kyc_status_raw)) AS kyc_status_raw,
      TRIM(risk_score_raw) AS risk_score_raw,
      TO_NUMBER(TRIM(risk_score_raw) DEFAULT NULL ON CONVERSION ERROR) AS risk_score,
      UPPER(TRIM(client_status_raw)) AS client_status_raw,
      UPPER(TRIM(relationship_purpose_code_raw)) AS relationship_purpose_code_raw,
      UPPER(TRIM(expected_activity_level_raw)) AS expected_activity_level_raw,
      TRIM(source_of_funds_declared_raw) AS source_of_funds_declared_raw,
      TRIM(source_of_wealth_declared_raw) AS source_of_wealth_declared_raw,
      TO_DATE(
        TRIM(business_date_raw) DEFAULT NULL ON CONVERSION ERROR,
        'YYYY-MM-DD'
      ) AS business_date_from_file,
      TO_NUMBER(TRIM(client_id_raw) DEFAULT NULL ON CONVERSION ERROR) AS client_id,
      TO_DATE(
        TRIM(date_of_birth_raw) DEFAULT NULL ON CONVERSION ERROR,
        'YYYY-MM-DD'
      ) AS date_of_birth,
      TO_NUMBER(TRIM(pep_flag_raw) DEFAULT NULL ON CONVERSION ERROR) AS pep_flag,
      TO_NUMBER(TRIM(high_risk_flag_raw) DEFAULT NULL ON CONVERSION ERROR) AS high_risk_flag,
      CASE
        WHEN UPPER(TRIM(country_code_raw)) IN (
          'AT', 'BE', 'BG', 'CH', 'CY', 'CZ', 'DE', 'DK', 'EE', 'ES', 'FI', 'FR',
          'GB', 'GR', 'HR', 'HU', 'IE', 'IS', 'IT', 'LI', 'LT', 'LU', 'LV', 'MT',
          'NL', 'NO', 'PL', 'PT', 'RO', 'SE', 'SI', 'SK', 'US'
        ) THEN 1
        ELSE 0
      END AS is_supported_country,
      CASE
        WHEN UPPER(TRIM(kyc_status_raw)) IN ('PASS', 'REVIEW', 'PENDING', 'LIMITED', 'MANUAL_CHECK')
          THEN 1
        ELSE 0
      END AS is_supported_kyc_status,
      CASE
        WHEN TRIM(phone_number_raw) IS NULL THEN 1
        WHEN REGEXP_LIKE(TRIM(phone_number_raw), '^\+[0-9]{7,20}$') THEN 1
        ELSE 0
      END AS is_valid_phone_number,
      CASE
        WHEN TRIM(email_raw) IS NULL THEN 1
        WHEN REGEXP_LIKE(TRIM(email_raw), '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$') THEN 1
        ELSE 0
      END AS is_valid_email
    FROM dwh.ext_clients
  ),
  valid_data AS (
    SELECT
      source_row_num,
      business_date_raw,
      client_id_raw,
      client_type_raw,
      full_name_raw,
      first_name_raw,
      last_name_raw,
      company_name_raw,
      date_of_birth_raw,
      document_id_raw,
      registration_no_raw,
      tax_id_raw,
      address_line_1_raw,
      city_raw,
      postal_code_raw,
      country_code_raw,
      phone_number_raw,
      email_raw,
      pep_flag_raw,
      high_risk_flag_raw,
      kyc_status_raw,
      risk_score_raw,
      risk_score,
      client_status_raw,
      relationship_purpose_code_raw,
      expected_activity_level_raw,
      source_of_funds_declared_raw,
      source_of_wealth_declared_raw,
      business_date_from_file,
      client_id,
      date_of_birth,
      pep_flag,
      high_risk_flag,
      COUNT(*) OVER (PARTITION BY business_date_from_file, client_id) AS duplicate_key_count
    FROM normalized_data
    WHERE business_date_from_file = l_business_date
      AND client_id IS NOT NULL
      AND client_type_raw IN ('PRIVATE', 'BUSINESS')
      AND full_name_raw IS NOT NULL
      AND LENGTH(full_name_raw) <= 200
      AND (first_name_raw IS NULL OR LENGTH(first_name_raw) <= 100)
      AND (last_name_raw IS NULL OR LENGTH(last_name_raw) <= 100)
      AND (company_name_raw IS NULL OR LENGTH(company_name_raw) <= 200)
      AND (document_id_raw IS NULL OR LENGTH(document_id_raw) <= 100)
      AND (registration_no_raw IS NULL OR LENGTH(registration_no_raw) <= 50)
      AND (tax_id_raw IS NULL OR LENGTH(tax_id_raw) <= 100)
      AND address_line_1_raw IS NOT NULL
      AND LENGTH(address_line_1_raw) <= 200
      AND city_raw IS NOT NULL
      AND LENGTH(city_raw) <= 100
      AND postal_code_raw IS NOT NULL
      AND LENGTH(postal_code_raw) <= 20
      AND country_code_raw IS NOT NULL
      AND REGEXP_LIKE(country_code_raw, '^[A-Z]{2}$')
      AND is_supported_country = 1
      AND (phone_number_raw IS NULL OR LENGTH(phone_number_raw) <= 50)
      AND is_valid_phone_number = 1
      AND (email_raw IS NULL OR LENGTH(email_raw) <= 255)
      AND is_valid_email = 1
      AND pep_flag IN (0, 1)
      AND high_risk_flag IN (0, 1)
      AND (kyc_status_raw IS NULL OR LENGTH(kyc_status_raw) <= 30)
      AND (kyc_status_raw IS NULL OR REGEXP_LIKE(kyc_status_raw, '^[A-Z_]+$'))
      AND (kyc_status_raw IS NULL OR is_supported_kyc_status = 1)
      AND (risk_score_raw IS NULL OR risk_score IS NOT NULL)
      AND (risk_score IS NULL OR risk_score BETWEEN 0 AND 999)
      AND client_status_raw IN ('ACTIVE', 'ARCHIVED')
      AND (relationship_purpose_code_raw IS NULL OR LENGTH(relationship_purpose_code_raw) <= 50)
      AND (relationship_purpose_code_raw IS NULL OR REGEXP_LIKE(relationship_purpose_code_raw, '^[A-Z0-9_]+$'))
      AND (
        relationship_purpose_code_raw IS NULL
        OR relationship_purpose_code_raw IN (
          'SALARY',
          'SAVINGS',
          'REMITTANCE',
          'INVESTMENT',
          'BUSINESS_PAYMENTS'
        )
      )
      AND (expected_activity_level_raw IS NULL OR LENGTH(expected_activity_level_raw) <= 50)
      AND (expected_activity_level_raw IS NULL OR REGEXP_LIKE(expected_activity_level_raw, '^[A-Z0-9_]+$'))
      AND (
        expected_activity_level_raw IS NULL
        OR expected_activity_level_raw IN (
          'LOW',
          'MEDIUM',
          'HIGH',
          'VERY_HIGH',
          'UNKNOWN'
        )
      )
      AND (source_of_funds_declared_raw IS NULL OR LENGTH(source_of_funds_declared_raw) <= 255)
      AND (source_of_wealth_declared_raw IS NULL OR LENGTH(source_of_wealth_declared_raw) <= 255)
      AND (
        (client_type_raw = 'PRIVATE'
          AND first_name_raw IS NOT NULL
          AND last_name_raw IS NOT NULL
          AND date_of_birth IS NOT NULL)
        OR
        (client_type_raw = 'BUSINESS'
          AND company_name_raw IS NOT NULL
          AND registration_no_raw IS NOT NULL)
      )
  )
  SELECT
    l_business_date AS business_date,
    source_row_num,
    client_id,
    client_type_raw,
    full_name_raw,
    first_name_raw,
    last_name_raw,
    company_name_raw,
    date_of_birth,
    document_id_raw,
    registration_no_raw,
    tax_id_raw,
    address_line_1_raw,
    city_raw,
    postal_code_raw,
    country_code_raw,
    phone_number_raw,
    email_raw,
    pep_flag,
    high_risk_flag,
    kyc_status_raw,
    risk_score,
    client_status_raw,
    relationship_purpose_code_raw,
    expected_activity_level_raw,
    source_of_funds_declared_raw,
    source_of_wealth_declared_raw
  FROM valid_data
  WHERE duplicate_key_count = 1;

  l_stage_rows_loaded := SQL%ROWCOUNT;

  INSERT INTO dwh.stg_clients_reject (
    business_date,
    source_file_name,
    source_row_num,
    business_date_raw,
    client_id_raw,
    client_type_raw,
    full_name_raw,
    first_name_raw,
    last_name_raw,
    company_name_raw,
    date_of_birth_raw,
    document_id_raw,
    registration_no_raw,
    tax_id_raw,
    address_line_1_raw,
    city_raw,
    postal_code_raw,
    country_code_raw,
    phone_number_raw,
    email_raw,
    pep_flag_raw,
    high_risk_flag_raw,
    kyc_status_raw,
    risk_score_raw,
    client_status_raw,
    relationship_purpose_code_raw,
    expected_activity_level_raw,
    source_of_funds_declared_raw,
    source_of_wealth_declared_raw,
    reject_reason
  )
  WITH normalized_data AS (
    SELECT
      source_row_num,
      TRIM(business_date_raw) AS business_date_raw,
      TRIM(client_id_raw) AS client_id_raw,
      UPPER(TRIM(client_type_raw)) AS client_type_raw,
      TRIM(full_name_raw) AS full_name_raw,
      TRIM(first_name_raw) AS first_name_raw,
      TRIM(last_name_raw) AS last_name_raw,
      TRIM(company_name_raw) AS company_name_raw,
      TRIM(date_of_birth_raw) AS date_of_birth_raw,
      TRIM(document_id_raw) AS document_id_raw,
      TRIM(registration_no_raw) AS registration_no_raw,
      TRIM(tax_id_raw) AS tax_id_raw,
      TRIM(address_line_1_raw) AS address_line_1_raw,
      TRIM(city_raw) AS city_raw,
      TRIM(postal_code_raw) AS postal_code_raw,
      UPPER(TRIM(country_code_raw)) AS country_code_raw,
      TRIM(phone_number_raw) AS phone_number_raw,
      TRIM(email_raw) AS email_raw,
      TRIM(pep_flag_raw) AS pep_flag_raw,
      TRIM(high_risk_flag_raw) AS high_risk_flag_raw,
      UPPER(TRIM(kyc_status_raw)) AS kyc_status_raw,
      TRIM(risk_score_raw) AS risk_score_raw,
      UPPER(TRIM(client_status_raw)) AS client_status_raw,
      UPPER(TRIM(relationship_purpose_code_raw)) AS relationship_purpose_code_raw,
      UPPER(TRIM(expected_activity_level_raw)) AS expected_activity_level_raw,
      TRIM(source_of_funds_declared_raw) AS source_of_funds_declared_raw,
      TRIM(source_of_wealth_declared_raw) AS source_of_wealth_declared_raw,
      TO_DATE(
        TRIM(business_date_raw) DEFAULT NULL ON CONVERSION ERROR,
        'YYYY-MM-DD'
      ) AS business_date_from_file,
      TO_NUMBER(TRIM(client_id_raw) DEFAULT NULL ON CONVERSION ERROR) AS client_id,
      TO_DATE(
        TRIM(date_of_birth_raw) DEFAULT NULL ON CONVERSION ERROR,
        'YYYY-MM-DD'
      ) AS date_of_birth,
      TO_NUMBER(TRIM(pep_flag_raw) DEFAULT NULL ON CONVERSION ERROR) AS pep_flag,
      TO_NUMBER(TRIM(high_risk_flag_raw) DEFAULT NULL ON CONVERSION ERROR) AS high_risk_flag,
      TO_NUMBER(TRIM(risk_score_raw) DEFAULT NULL ON CONVERSION ERROR) AS risk_score,
      CASE
        WHEN UPPER(TRIM(country_code_raw)) IN (
          'AT', 'BE', 'BG', 'CH', 'CY', 'CZ', 'DE', 'DK', 'EE', 'ES', 'FI', 'FR',
          'GB', 'GR', 'HR', 'HU', 'IE', 'IS', 'IT', 'LI', 'LT', 'LU', 'LV', 'MT',
          'NL', 'NO', 'PL', 'PT', 'RO', 'SE', 'SI', 'SK', 'US'
        ) THEN 1
        ELSE 0
      END AS is_supported_country,
      CASE
        WHEN UPPER(TRIM(kyc_status_raw)) IN ('PASS', 'REVIEW', 'PENDING', 'LIMITED', 'MANUAL_CHECK')
          THEN 1
        ELSE 0
      END AS is_supported_kyc_status,
      CASE
        WHEN TRIM(phone_number_raw) IS NULL THEN 1
        WHEN REGEXP_LIKE(TRIM(phone_number_raw), '^\+[0-9]{7,20}$') THEN 1
        ELSE 0
      END AS is_valid_phone_number,
      CASE
        WHEN TRIM(email_raw) IS NULL THEN 1
        WHEN REGEXP_LIKE(TRIM(email_raw), '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$') THEN 1
        ELSE 0
      END AS is_valid_email
    FROM dwh.ext_clients
  ),
  valid_data AS (
    SELECT
      source_row_num,
      business_date_raw,
      client_id_raw,
      client_type_raw,
      full_name_raw,
      first_name_raw,
      last_name_raw,
      company_name_raw,
      date_of_birth_raw,
      document_id_raw,
      registration_no_raw,
      tax_id_raw,
      address_line_1_raw,
      city_raw,
      postal_code_raw,
      country_code_raw,
      phone_number_raw,
      email_raw,
      pep_flag_raw,
      high_risk_flag_raw,
      kyc_status_raw,
      risk_score_raw,
      client_status_raw,
      relationship_purpose_code_raw,
      expected_activity_level_raw,
      source_of_funds_declared_raw,
      source_of_wealth_declared_raw,
      business_date_from_file,
      client_id,
      date_of_birth,
      pep_flag,
      high_risk_flag,
      risk_score,
      COUNT(*) OVER (PARTITION BY business_date_from_file, client_id) AS duplicate_key_count
    FROM normalized_data
    WHERE business_date_from_file = l_business_date
      AND client_id IS NOT NULL
      AND client_type_raw IN ('PRIVATE', 'BUSINESS')
      AND full_name_raw IS NOT NULL
      AND LENGTH(full_name_raw) <= 200
      AND (first_name_raw IS NULL OR LENGTH(first_name_raw) <= 100)
      AND (last_name_raw IS NULL OR LENGTH(last_name_raw) <= 100)
      AND (company_name_raw IS NULL OR LENGTH(company_name_raw) <= 200)
      AND (document_id_raw IS NULL OR LENGTH(document_id_raw) <= 100)
      AND (registration_no_raw IS NULL OR LENGTH(registration_no_raw) <= 50)
      AND (tax_id_raw IS NULL OR LENGTH(tax_id_raw) <= 100)
      AND address_line_1_raw IS NOT NULL
      AND LENGTH(address_line_1_raw) <= 200
      AND city_raw IS NOT NULL
      AND LENGTH(city_raw) <= 100
      AND postal_code_raw IS NOT NULL
      AND LENGTH(postal_code_raw) <= 20
      AND country_code_raw IS NOT NULL
      AND REGEXP_LIKE(country_code_raw, '^[A-Z]{2}$')
      AND is_supported_country = 1
      AND (phone_number_raw IS NULL OR LENGTH(phone_number_raw) <= 50)
      AND is_valid_phone_number = 1
      AND (email_raw IS NULL OR LENGTH(email_raw) <= 255)
      AND is_valid_email = 1
      AND pep_flag IN (0, 1)
      AND high_risk_flag IN (0, 1)
      AND (kyc_status_raw IS NULL OR LENGTH(kyc_status_raw) <= 30)
      AND (kyc_status_raw IS NULL OR REGEXP_LIKE(kyc_status_raw, '^[A-Z_]+$'))
      AND (kyc_status_raw IS NULL OR is_supported_kyc_status = 1)
      AND (risk_score_raw IS NULL OR risk_score IS NOT NULL)
      AND (risk_score IS NULL OR risk_score BETWEEN 0 AND 999)
      AND client_status_raw IN ('ACTIVE', 'ARCHIVED')
      AND (relationship_purpose_code_raw IS NULL OR LENGTH(relationship_purpose_code_raw) <= 50)
      AND (relationship_purpose_code_raw IS NULL OR REGEXP_LIKE(relationship_purpose_code_raw, '^[A-Z0-9_]+$'))
      AND (
        relationship_purpose_code_raw IS NULL
        OR relationship_purpose_code_raw IN (
          'SALARY',
          'SAVINGS',
          'REMITTANCE',
          'INVESTMENT',
          'BUSINESS_PAYMENTS'
        )
      )
      AND (expected_activity_level_raw IS NULL OR LENGTH(expected_activity_level_raw) <= 50)
      AND (expected_activity_level_raw IS NULL OR REGEXP_LIKE(expected_activity_level_raw, '^[A-Z0-9_]+$'))
      AND (
        expected_activity_level_raw IS NULL
        OR expected_activity_level_raw IN (
          'LOW',
          'MEDIUM',
          'HIGH',
          'VERY_HIGH',
          'UNKNOWN'
        )
      )
      AND (source_of_funds_declared_raw IS NULL OR LENGTH(source_of_funds_declared_raw) <= 255)
      AND (source_of_wealth_declared_raw IS NULL OR LENGTH(source_of_wealth_declared_raw) <= 255)
      AND (
        (client_type_raw = 'PRIVATE'
          AND first_name_raw IS NOT NULL
          AND last_name_raw IS NOT NULL
          AND date_of_birth IS NOT NULL)
        OR
        (client_type_raw = 'BUSINESS'
          AND company_name_raw IS NOT NULL
          AND registration_no_raw IS NOT NULL)
      )
  ),
  invalid_data AS (
    SELECT
      l_business_date AS business_date,
      l_snapshot_file AS source_file_name,
      source_row_num,
      business_date_raw,
      client_id_raw,
      client_type_raw,
      full_name_raw,
      first_name_raw,
      last_name_raw,
      company_name_raw,
      date_of_birth_raw,
      document_id_raw,
      registration_no_raw,
      tax_id_raw,
      address_line_1_raw,
      city_raw,
      postal_code_raw,
      country_code_raw,
      phone_number_raw,
      email_raw,
      pep_flag_raw,
      high_risk_flag_raw,
      kyc_status_raw,
      risk_score_raw,
      client_status_raw,
      relationship_purpose_code_raw,
      expected_activity_level_raw,
      source_of_funds_declared_raw,
      source_of_wealth_declared_raw,
      RTRIM(
        CASE
          WHEN business_date_from_file IS NULL THEN 'invalid business_date; '
          WHEN business_date_from_file != l_business_date
            THEN 'business_date does not match requested date; '
        END
        || CASE WHEN client_id IS NULL THEN 'invalid client_id; ' END
        || CASE
          WHEN client_type_raw IS NULL THEN 'missing client_type; '
          WHEN client_type_raw NOT IN ('PRIVATE', 'BUSINESS') THEN 'invalid client_type; '
        END
        || CASE WHEN full_name_raw IS NULL THEN 'missing full_name; ' END
        || CASE
          WHEN full_name_raw IS NOT NULL AND LENGTH(full_name_raw) > 200 THEN 'full_name too long; '
        END
        || CASE
          WHEN first_name_raw IS NOT NULL AND LENGTH(first_name_raw) > 100 THEN 'first_name too long; '
        END
        || CASE
          WHEN last_name_raw IS NOT NULL AND LENGTH(last_name_raw) > 100 THEN 'last_name too long; '
        END
        || CASE
          WHEN company_name_raw IS NOT NULL AND LENGTH(company_name_raw) > 200 THEN 'company_name too long; '
        END
        || CASE
          WHEN document_id_raw IS NOT NULL AND LENGTH(document_id_raw) > 100 THEN 'document_id too long; '
        END
        || CASE
          WHEN registration_no_raw IS NOT NULL AND LENGTH(registration_no_raw) > 50
            THEN 'registration_no too long; '
        END
        || CASE
          WHEN tax_id_raw IS NOT NULL AND LENGTH(tax_id_raw) > 100 THEN 'tax_id too long; '
        END
        || CASE WHEN address_line_1_raw IS NULL THEN 'missing address_line_1; ' END
        || CASE
          WHEN address_line_1_raw IS NOT NULL AND LENGTH(address_line_1_raw) > 200
            THEN 'address_line_1 too long; '
        END
        || CASE WHEN city_raw IS NULL THEN 'missing city; ' END
        || CASE
          WHEN city_raw IS NOT NULL AND LENGTH(city_raw) > 100 THEN 'city too long; '
        END
        || CASE WHEN postal_code_raw IS NULL THEN 'missing postal_code; ' END
        || CASE
          WHEN postal_code_raw IS NOT NULL AND LENGTH(postal_code_raw) > 20
            THEN 'postal_code too long; '
        END
        || CASE
          WHEN country_code_raw IS NULL THEN 'missing country_code; '
          WHEN NOT REGEXP_LIKE(country_code_raw, '^[A-Z]{2}$') THEN 'invalid country_code format; '
          WHEN is_supported_country = 0 THEN 'unsupported country_code; '
        END
        || CASE
          WHEN phone_number_raw IS NOT NULL AND LENGTH(phone_number_raw) > 50 THEN 'phone_number too long; '
          WHEN phone_number_raw IS NOT NULL AND is_valid_phone_number = 0 THEN 'invalid phone_number format; '
        END
        || CASE
          WHEN email_raw IS NOT NULL AND LENGTH(email_raw) > 255 THEN 'email too long; '
          WHEN email_raw IS NOT NULL AND is_valid_email = 0 THEN 'invalid email format; '
        END
        || CASE
          WHEN pep_flag IS NULL THEN 'invalid pep_flag; '
          WHEN pep_flag NOT IN (0, 1) THEN 'pep_flag must be 0 or 1; '
        END
        || CASE
          WHEN high_risk_flag IS NULL THEN 'invalid high_risk_flag; '
          WHEN high_risk_flag NOT IN (0, 1) THEN 'high_risk_flag must be 0 or 1; '
        END
        || CASE
          WHEN kyc_status_raw IS NOT NULL AND LENGTH(kyc_status_raw) > 30 THEN 'kyc_status too long; '
          WHEN kyc_status_raw IS NOT NULL AND NOT REGEXP_LIKE(kyc_status_raw, '^[A-Z_]+$')
            THEN 'invalid kyc_status format; '
          WHEN kyc_status_raw IS NOT NULL AND is_supported_kyc_status = 0
            THEN 'unsupported kyc_status; '
        END
        || CASE
          WHEN risk_score_raw IS NOT NULL AND risk_score IS NULL THEN 'invalid risk_score; '
          WHEN risk_score IS NOT NULL AND risk_score NOT BETWEEN 0 AND 999
            THEN 'risk_score must be between 0 and 999; '
        END
        || CASE
          WHEN client_status_raw IS NULL THEN 'missing client_status; '
          WHEN client_status_raw NOT IN ('ACTIVE', 'ARCHIVED') THEN 'invalid client_status; '
        END
        || CASE
          WHEN relationship_purpose_code_raw IS NOT NULL
               AND LENGTH(relationship_purpose_code_raw) > 50
            THEN 'relationship_purpose_code too long; '
          WHEN relationship_purpose_code_raw IS NOT NULL
               AND NOT REGEXP_LIKE(relationship_purpose_code_raw, '^[A-Z0-9_]+$')
            THEN 'invalid relationship_purpose_code format; '
          WHEN relationship_purpose_code_raw IS NOT NULL
               AND relationship_purpose_code_raw NOT IN (
                 'SALARY',
                 'SAVINGS',
                 'REMITTANCE',
                 'INVESTMENT',
                 'BUSINESS_PAYMENTS'
               )
            THEN 'unsupported relationship_purpose_code; '
        END
        || CASE
          WHEN expected_activity_level_raw IS NOT NULL
               AND LENGTH(expected_activity_level_raw) > 50
            THEN 'expected_activity_level too long; '
          WHEN expected_activity_level_raw IS NOT NULL
               AND NOT REGEXP_LIKE(expected_activity_level_raw, '^[A-Z0-9_]+$')
            THEN 'invalid expected_activity_level format; '
          WHEN expected_activity_level_raw IS NOT NULL
               AND expected_activity_level_raw NOT IN (
                 'LOW',
                 'MEDIUM',
                 'HIGH',
                 'VERY_HIGH',
                 'UNKNOWN'
               )
            THEN 'unsupported expected_activity_level; '
        END
        || CASE
          WHEN source_of_funds_declared_raw IS NOT NULL
               AND LENGTH(source_of_funds_declared_raw) > 255
            THEN 'source_of_funds_declared too long; '
        END
        || CASE
          WHEN source_of_wealth_declared_raw IS NOT NULL
               AND LENGTH(source_of_wealth_declared_raw) > 255
            THEN 'source_of_wealth_declared too long; '
        END
        || CASE
          WHEN client_type_raw = 'PRIVATE' AND first_name_raw IS NULL THEN 'missing first_name for PRIVATE; '
          WHEN client_type_raw = 'PRIVATE' AND last_name_raw IS NULL THEN 'missing last_name for PRIVATE; '
          WHEN client_type_raw = 'PRIVATE' AND date_of_birth IS NULL THEN 'invalid date_of_birth for PRIVATE; '
          WHEN client_type_raw = 'BUSINESS' AND company_name_raw IS NULL THEN 'missing company_name for BUSINESS; '
          WHEN client_type_raw = 'BUSINESS' AND registration_no_raw IS NULL
            THEN 'missing registration_no for BUSINESS; '
        END,
        '; '
      ) AS reject_reason
    FROM normalized_data
    WHERE business_date_from_file != l_business_date
      OR business_date_from_file IS NULL
      OR client_id IS NULL
      OR client_type_raw IS NULL
      OR client_type_raw NOT IN ('PRIVATE', 'BUSINESS')
      OR full_name_raw IS NULL
      OR LENGTH(full_name_raw) > 200
      OR (first_name_raw IS NOT NULL AND LENGTH(first_name_raw) > 100)
      OR (last_name_raw IS NOT NULL AND LENGTH(last_name_raw) > 100)
      OR (company_name_raw IS NOT NULL AND LENGTH(company_name_raw) > 200)
      OR (document_id_raw IS NOT NULL AND LENGTH(document_id_raw) > 100)
      OR (registration_no_raw IS NOT NULL AND LENGTH(registration_no_raw) > 50)
      OR (tax_id_raw IS NOT NULL AND LENGTH(tax_id_raw) > 100)
      OR address_line_1_raw IS NULL
      OR LENGTH(address_line_1_raw) > 200
      OR city_raw IS NULL
      OR LENGTH(city_raw) > 100
      OR postal_code_raw IS NULL
      OR LENGTH(postal_code_raw) > 20
      OR country_code_raw IS NULL
      OR NOT REGEXP_LIKE(country_code_raw, '^[A-Z]{2}$')
      OR is_supported_country = 0
      OR (phone_number_raw IS NOT NULL AND LENGTH(phone_number_raw) > 50)
      OR (phone_number_raw IS NOT NULL AND is_valid_phone_number = 0)
      OR (email_raw IS NOT NULL AND LENGTH(email_raw) > 255)
      OR (email_raw IS NOT NULL AND is_valid_email = 0)
      OR pep_flag IS NULL
      OR pep_flag NOT IN (0, 1)
      OR high_risk_flag IS NULL
      OR high_risk_flag NOT IN (0, 1)
      OR (kyc_status_raw IS NOT NULL AND LENGTH(kyc_status_raw) > 30)
      OR (kyc_status_raw IS NOT NULL AND NOT REGEXP_LIKE(kyc_status_raw, '^[A-Z_]+$'))
      OR (kyc_status_raw IS NOT NULL AND is_supported_kyc_status = 0)
      OR (risk_score_raw IS NOT NULL AND risk_score IS NULL)
      OR (risk_score IS NOT NULL AND risk_score NOT BETWEEN 0 AND 999)
      OR client_status_raw IS NULL
      OR client_status_raw NOT IN ('ACTIVE', 'ARCHIVED')
      OR (relationship_purpose_code_raw IS NOT NULL AND LENGTH(relationship_purpose_code_raw) > 50)
      OR (relationship_purpose_code_raw IS NOT NULL
          AND NOT REGEXP_LIKE(relationship_purpose_code_raw, '^[A-Z0-9_]+$'))
      OR (relationship_purpose_code_raw IS NOT NULL
          AND relationship_purpose_code_raw NOT IN (
            'SALARY',
            'SAVINGS',
            'REMITTANCE',
            'INVESTMENT',
            'BUSINESS_PAYMENTS'
          ))
      OR (expected_activity_level_raw IS NOT NULL AND LENGTH(expected_activity_level_raw) > 50)
      OR (expected_activity_level_raw IS NOT NULL
          AND NOT REGEXP_LIKE(expected_activity_level_raw, '^[A-Z0-9_]+$'))
      OR (expected_activity_level_raw IS NOT NULL
          AND expected_activity_level_raw NOT IN (
            'LOW',
            'MEDIUM',
            'HIGH',
            'VERY_HIGH',
            'UNKNOWN'
          ))
      OR (source_of_funds_declared_raw IS NOT NULL AND LENGTH(source_of_funds_declared_raw) > 255)
      OR (source_of_wealth_declared_raw IS NOT NULL AND LENGTH(source_of_wealth_declared_raw) > 255)
      OR (client_type_raw = 'PRIVATE' AND first_name_raw IS NULL)
      OR (client_type_raw = 'PRIVATE' AND last_name_raw IS NULL)
      OR (client_type_raw = 'PRIVATE' AND date_of_birth IS NULL)
      OR (client_type_raw = 'BUSINESS' AND company_name_raw IS NULL)
      OR (client_type_raw = 'BUSINESS' AND registration_no_raw IS NULL)
  ),
  duplicate_data AS (
    SELECT
      l_business_date AS business_date,
      l_snapshot_file AS source_file_name,
      source_row_num,
      business_date_raw,
      client_id_raw,
      client_type_raw,
      full_name_raw,
      first_name_raw,
      last_name_raw,
      company_name_raw,
      date_of_birth_raw,
      document_id_raw,
      registration_no_raw,
      tax_id_raw,
      address_line_1_raw,
      city_raw,
      postal_code_raw,
      country_code_raw,
      phone_number_raw,
      email_raw,
      pep_flag_raw,
      high_risk_flag_raw,
      kyc_status_raw,
      risk_score_raw,
      client_status_raw,
      relationship_purpose_code_raw,
      expected_activity_level_raw,
      source_of_funds_declared_raw,
      source_of_wealth_declared_raw,
      'duplicate client_id in snapshot' AS reject_reason
    FROM valid_data
    WHERE duplicate_key_count > 1
  )
  SELECT
    business_date,
    source_file_name,
    source_row_num,
    business_date_raw,
    client_id_raw,
    client_type_raw,
    full_name_raw,
    first_name_raw,
    last_name_raw,
    company_name_raw,
    date_of_birth_raw,
    document_id_raw,
    registration_no_raw,
    tax_id_raw,
    address_line_1_raw,
    city_raw,
    postal_code_raw,
    country_code_raw,
    phone_number_raw,
    email_raw,
    pep_flag_raw,
    high_risk_flag_raw,
    kyc_status_raw,
    risk_score_raw,
    client_status_raw,
    relationship_purpose_code_raw,
    expected_activity_level_raw,
    source_of_funds_declared_raw,
    source_of_wealth_declared_raw,
    reject_reason
  FROM invalid_data
  UNION ALL
  SELECT
    business_date,
    source_file_name,
    source_row_num,
    business_date_raw,
    client_id_raw,
    client_type_raw,
    full_name_raw,
    first_name_raw,
    last_name_raw,
    company_name_raw,
    date_of_birth_raw,
    document_id_raw,
    registration_no_raw,
    tax_id_raw,
    address_line_1_raw,
    city_raw,
    postal_code_raw,
    country_code_raw,
    phone_number_raw,
    email_raw,
    pep_flag_raw,
    high_risk_flag_raw,
    kyc_status_raw,
    risk_score_raw,
    client_status_raw,
    relationship_purpose_code_raw,
    expected_activity_level_raw,
    source_of_funds_declared_raw,
    source_of_wealth_declared_raw,
    reject_reason
  FROM duplicate_data;

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
      -20117,
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
      -20115,
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

  EXECUTE IMMEDIATE 'SET CONSTRAINTS fk_core_client_transfers_client DEFERRED';

  DELETE FROM dwh.core_clients
  WHERE business_date = l_business_date;

  INSERT INTO dwh.core_clients (
    business_date,
    client_id,
    client_type,
    full_name,
    first_name,
    last_name,
    company_name,
    date_of_birth,
    document_id,
    registration_no,
    tax_id,
    address_line_1,
    city,
    postal_code,
    country_code,
    phone_number,
    email,
    pep_flag,
    high_risk_flag,
    kyc_status,
    risk_score,
    client_status,
    relationship_purpose_code,
    expected_activity_level,
    source_of_funds_declared,
    source_of_wealth_declared
  )
  SELECT
    business_date,
    client_id,
    client_type,
    full_name,
    first_name,
    last_name,
    company_name,
    date_of_birth,
    document_id,
    registration_no,
    tax_id,
    address_line_1,
    city,
    postal_code,
    country_code,
    phone_number,
    email,
    pep_flag,
    high_risk_flag,
    kyc_status,
    risk_score,
    client_status,
    relationship_purpose_code,
    expected_activity_level,
    source_of_funds_declared,
    source_of_wealth_declared
  FROM dwh.stg_clients
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
      'dwh.prc_load_clients business_date='
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
      'dwh.prc_load_clients business_date='
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
