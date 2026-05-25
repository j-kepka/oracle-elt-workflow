-- Builds the AML report spool and exports a dated CSV plus ready file.

CREATE OR REPLACE PROCEDURE dwh.prc_build_aml_report_spool (
  p_date     IN DATE DEFAULT TRUNC(SYSDATE),
  p_run_mode IN VARCHAR2 DEFAULT 'MANUAL'
) AS
  c_process_name      CONSTANT VARCHAR2(100 CHAR) := 'BUILD_AML_REPORT_SPOOL';
  c_export_dir        CONSTANT VARCHAR2(30 CHAR) := 'EXT_EXPORT_DIR';
  l_business_date     DATE := TRUNC(p_date);
  l_run_mode          VARCHAR2(10 CHAR);
  l_attempt_ts        TIMESTAMP;
  l_expected_rows     NUMBER := 0;
  l_spool_rows_loaded NUMBER := 0;
  l_rows_exported     NUMBER := 0;
  l_upstream_ready    NUMBER := 0;
  l_data_file_name    VARCHAR2(255 CHAR);
  l_ready_file_name   VARCHAR2(255 CHAR);
  l_input_valid       BOOLEAN := FALSE;
  l_final_status_set  BOOLEAN := FALSE;
  l_data_file         UTL_FILE.FILE_TYPE;
  l_ready_file        UTL_FILE.FILE_TYPE;

  PROCEDURE upsert_process_run (
    p_status             IN VARCHAR2,
    p_reason_code        IN VARCHAR2,
    p_status_message     IN VARCHAR2,
    p_expected_row_count IN NUMBER,
    p_core_row_count     IN NUMBER,
    p_started_ts         IN TIMESTAMP,
    p_finished_ts        IN TIMESTAMP
  ) AS
  BEGIN
    dwh.pkg_dwh_util.upsert_process_run(
      p_process_name        => c_process_name,
      p_business_date       => l_business_date,
      p_run_mode            => l_run_mode,
      p_status              => p_status,
      p_reason_code         => p_reason_code,
      p_status_message      => p_status_message,
      p_expected_row_count  => p_expected_row_count,
      p_stage_row_count     => 0,
      p_reject_row_count    => 0,
      p_core_row_count      => p_core_row_count,
      p_data_file_name      => l_data_file_name,
      p_ready_file_name     => l_ready_file_name,
      p_started_ts          => p_started_ts,
      p_finished_ts         => p_finished_ts,
      p_scheduled_for_ts    => l_attempt_ts,
      p_next_retry_ts       => NULL,
      p_reset_retry_count   => 1
    );
  END upsert_process_run;

  PROCEDURE close_file_if_open (
    p_file IN OUT UTL_FILE.FILE_TYPE
  ) AS
  BEGIN
    IF UTL_FILE.IS_OPEN(p_file) THEN
      UTL_FILE.FCLOSE(p_file);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      NULL;
  END close_file_if_open;

  PROCEDURE remove_file_if_exists (
    p_filename IN VARCHAR2
  ) AS
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

    IF l_exists THEN
      UTL_FILE.FREMOVE(
        location => c_export_dir,
        filename => p_filename
      );
    END IF;
  END remove_file_if_exists;

  FUNCTION csv_value (
    p_value IN VARCHAR2
  ) RETURN VARCHAR2 AS
  BEGIN
    RETURN '"' || REPLACE(NVL(p_value, ''), '"', '""') || '"';
  END csv_value;

  FUNCTION csv_number (
    p_value IN NUMBER
  ) RETURN VARCHAR2 AS
  BEGIN
    RETURN csv_value(TO_CHAR(p_value, 'FM9999999999999990D00', 'NLS_NUMERIC_CHARACTERS=''.,'''));
  END csv_number;

  FUNCTION csv_integer (
    p_value IN NUMBER
  ) RETURN VARCHAR2 AS
  BEGIN
    RETURN csv_value(TO_CHAR(p_value));
  END csv_integer;

  FUNCTION csv_date (
    p_value IN DATE
  ) RETURN VARCHAR2 AS
  BEGIN
    RETURN csv_value(TO_CHAR(p_value, 'YYYY-MM-DD'));
  END csv_date;

  FUNCTION csv_timestamp (
    p_value IN TIMESTAMP
  ) RETURN VARCHAR2 AS
  BEGIN
    RETURN csv_value(TO_CHAR(p_value, 'YYYY-MM-DD HH24:MI:SS'));
  END csv_timestamp;

  PROCEDURE export_spool_file AS
  BEGIN
    remove_file_if_exists(l_ready_file_name);
    remove_file_if_exists(l_data_file_name);

    l_data_file := UTL_FILE.FOPEN(
      location     => c_export_dir,
      filename     => l_data_file_name,
      open_mode    => 'W',
      max_linesize => 32767
    );

    UTL_FILE.PUT_LINE(
      l_data_file,
      'report_run_ts;business_date;report_id;report_type_candidate;report_due_date;'
      || 'transfer_id;client_id;client_type;full_name;date_of_birth;document_id;registration_no;tax_id;'
      || 'address_line_1;city;postal_code;country_code;phone_number;email;'
      || 'source_account;target_account;amount;currency_code;amount_eur;transfer_ts;transfer_status;channel;transfer_country_code;transfer_title;'
      || 'pep_flag;high_risk_flag;kyc_status;risk_score;relationship_purpose_code;source_of_funds_declared;source_of_wealth_declared;'
      || 'above_threshold_art72_flag;suspicion_art74_flag;aml_review_flag;aml_reason_code;aml_reason_details'
    );

    FOR rec IN (
      SELECT
        report_run_ts,
        business_date,
        report_id,
        report_type_candidate,
        report_due_date,
        transfer_id,
        client_id,
        client_type,
        full_name,
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
        source_account,
        target_account,
        amount,
        currency_code,
        amount_eur,
        transfer_ts,
        transfer_status,
        channel,
        transfer_country_code,
        transfer_title,
        pep_flag,
        high_risk_flag,
        kyc_status,
        risk_score,
        relationship_purpose_code,
        source_of_funds_declared,
        source_of_wealth_declared,
        above_threshold_art72_flag,
        suspicion_art74_flag,
        aml_review_flag,
        aml_reason_code,
        aml_reason_details
      FROM dwh.aml_report_spool
      WHERE business_date = l_business_date
      ORDER BY report_id
    ) LOOP
      UTL_FILE.PUT_LINE(
        l_data_file,
        csv_timestamp(rec.report_run_ts)
        || ';' || csv_date(rec.business_date)
        || ';' || csv_value(rec.report_id)
        || ';' || csv_value(rec.report_type_candidate)
        || ';' || csv_date(rec.report_due_date)
        || ';' || csv_integer(rec.transfer_id)
        || ';' || csv_integer(rec.client_id)
        || ';' || csv_value(rec.client_type)
        || ';' || csv_value(rec.full_name)
        || ';' || csv_date(rec.date_of_birth)
        || ';' || csv_value(rec.document_id)
        || ';' || csv_value(rec.registration_no)
        || ';' || csv_value(rec.tax_id)
        || ';' || csv_value(rec.address_line_1)
        || ';' || csv_value(rec.city)
        || ';' || csv_value(rec.postal_code)
        || ';' || csv_value(rec.country_code)
        || ';' || csv_value(rec.phone_number)
        || ';' || csv_value(rec.email)
        || ';' || csv_value(rec.source_account)
        || ';' || csv_value(rec.target_account)
        || ';' || csv_number(rec.amount)
        || ';' || csv_value(rec.currency_code)
        || ';' || csv_number(rec.amount_eur)
        || ';' || csv_timestamp(rec.transfer_ts)
        || ';' || csv_value(rec.transfer_status)
        || ';' || csv_value(rec.channel)
        || ';' || csv_value(rec.transfer_country_code)
        || ';' || csv_value(rec.transfer_title)
        || ';' || csv_integer(rec.pep_flag)
        || ';' || csv_integer(rec.high_risk_flag)
        || ';' || csv_value(rec.kyc_status)
        || ';' || csv_integer(rec.risk_score)
        || ';' || csv_value(rec.relationship_purpose_code)
        || ';' || csv_value(rec.source_of_funds_declared)
        || ';' || csv_value(rec.source_of_wealth_declared)
        || ';' || csv_integer(rec.above_threshold_art72_flag)
        || ';' || csv_integer(rec.suspicion_art74_flag)
        || ';' || csv_integer(rec.aml_review_flag)
        || ';' || csv_value(rec.aml_reason_code)
        || ';' || csv_value(rec.aml_reason_details)
      );

      l_rows_exported := l_rows_exported + 1;
    END LOOP;

    UTL_FILE.FCLOSE(l_data_file);

    IF l_rows_exported != l_spool_rows_loaded THEN
      RAISE_APPLICATION_ERROR(
        -20313,
        'Exported row count does not match aml_report_spool row count.'
      );
    END IF;

    l_ready_file := UTL_FILE.FOPEN(
      location     => c_export_dir,
      filename     => l_ready_file_name,
      open_mode    => 'W',
      max_linesize => 100
    );

    UTL_FILE.PUT_LINE(l_ready_file, TO_CHAR(l_rows_exported));
    UTL_FILE.FCLOSE(l_ready_file);
  EXCEPTION
    WHEN OTHERS THEN
      close_file_if_open(l_data_file);
      close_file_if_open(l_ready_file);
      RAISE;
  END export_spool_file;
BEGIN
  l_attempt_ts := SYSTIMESTAMP;
  l_data_file_name := 'aml_report_spool_' || TO_CHAR(l_business_date, 'YYYYMMDD') || '.csv';
  l_ready_file_name := 'aml_report_spool_' || TO_CHAR(l_business_date, 'YYYYMMDD') || '.ok';

  l_run_mode := dwh.pkg_dwh_util.normalize_run_mode(
    p_run_mode   => p_run_mode,
    p_error_code => -20301
  );
  l_input_valid := TRUE;

  upsert_process_run(
    p_status             => 'PROCESSING',
    p_reason_code        => NULL,
    p_status_message     => 'AML report spool build started.',
    p_expected_row_count => NULL,
    p_core_row_count     => 0,
    p_started_ts         => l_attempt_ts,
    p_finished_ts        => NULL
  );

  SELECT COUNT(*)
  INTO l_upstream_ready
  FROM dwh.ctl_process_run
  WHERE business_date = l_business_date
    AND status = 'DONE'
    AND process_name IN ('LOAD_CLIENTS', 'LOAD_CLIENT_TRANSFERS', 'BUILD_MART_TRANSFER_AML');

  IF l_upstream_ready < 3 THEN
    upsert_process_run(
      p_status             => 'FAILED',
      p_reason_code        => 'UPSTREAM_NOT_PUBLISHABLE',
      p_status_message     => 'Client load, transfer load, and AML mart build must finish with DONE before building aml_report_spool.',
      p_expected_row_count => NULL,
      p_core_row_count     => 0,
      p_started_ts         => l_attempt_ts,
      p_finished_ts        => SYSTIMESTAMP
    );

    l_final_status_set := TRUE;

    RAISE_APPLICATION_ERROR(
      -20310,
      'Upstream processes are not publishable for business_date '
      || TO_CHAR(l_business_date, 'YYYY-MM-DD')
      || '.'
    );
  END IF;

  SELECT COUNT(*)
  INTO l_expected_rows
  FROM dwh.mart_transfer_aml
  WHERE business_date = l_business_date
    AND aml_review_flag = 1
    AND report_type_candidate IS NOT NULL;

  upsert_process_run(
    p_status             => 'PROCESSING',
    p_reason_code        => NULL,
    p_status_message     => 'Publication gate passed. Building aml_report_spool.',
    p_expected_row_count => l_expected_rows,
    p_core_row_count     => 0,
    p_started_ts         => l_attempt_ts,
    p_finished_ts        => NULL
  );

  DELETE FROM dwh.aml_report_spool
  WHERE business_date = l_business_date;

  INSERT INTO dwh.aml_report_spool (
    business_date,
    report_id,
    report_run_ts,
    report_type_candidate,
    report_due_date,
    transfer_id,
    client_id,
    client_type,
    full_name,
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
    source_account,
    target_account,
    amount,
    currency_code,
    amount_eur,
    transfer_ts,
    transfer_status,
    channel,
    transfer_country_code,
    transfer_title,
    pep_flag,
    high_risk_flag,
    kyc_status,
    risk_score,
    relationship_purpose_code,
    source_of_funds_declared,
    source_of_wealth_declared,
    above_threshold_art72_flag,
    suspicion_art74_flag,
    aml_review_flag,
    aml_reason_code,
    aml_reason_details
  )
  SELECT
    mart.business_date,
    'AML-' || TO_CHAR(mart.business_date, 'YYYYMMDD') || '-' || TO_CHAR(mart.transfer_id) AS report_id,
    l_attempt_ts AS report_run_ts,
    mart.report_type_candidate,
    CASE
      WHEN mart.report_type_candidate IN ('ART74_SUSPICION', 'ART72_AND_ART74')
        THEN TRUNC(CAST(mart.transfer_ts AS DATE)) + 2
      ELSE TRUNC(CAST(mart.transfer_ts AS DATE)) + 7
    END AS report_due_date,
    mart.transfer_id,
    mart.client_id,
    cli.client_type,
    cli.full_name,
    cli.date_of_birth,
    cli.document_id,
    cli.registration_no,
    cli.tax_id,
    cli.address_line_1,
    cli.city,
    cli.postal_code,
    cli.country_code,
    cli.phone_number,
    cli.email,
    mart.source_account,
    mart.target_account,
    mart.amount,
    mart.currency_code,
    mart.amount_eur,
    mart.transfer_ts,
    mart.transfer_status,
    mart.channel,
    mart.transfer_country_code,
    mart.transfer_title,
    mart.pep_flag,
    mart.high_risk_flag,
    mart.kyc_status,
    mart.risk_score,
    mart.relationship_purpose_code,
    mart.source_of_funds_declared,
    mart.source_of_wealth_declared,
    mart.above_threshold_art72_flag,
    mart.suspicion_art74_flag,
    mart.aml_review_flag,
    mart.aml_reason_code,
    mart.aml_reason_details
  FROM dwh.mart_transfer_aml mart
  JOIN dwh.core_clients cli
    ON cli.business_date = mart.business_date
   AND cli.client_id = mart.client_id
  WHERE mart.business_date = l_business_date
    AND mart.aml_review_flag = 1
    AND mart.report_type_candidate IS NOT NULL;

  l_spool_rows_loaded := SQL%ROWCOUNT;

  IF l_spool_rows_loaded != l_expected_rows THEN
    ROLLBACK;

    upsert_process_run(
      p_status             => 'FAILED',
      p_reason_code        => 'SPOOL_ROW_COUNT_MISMATCH',
      p_status_message     => 'Spool row count does not match selected AML mart candidate count.',
      p_expected_row_count => l_expected_rows,
      p_core_row_count     => l_spool_rows_loaded,
      p_started_ts         => l_attempt_ts,
      p_finished_ts        => SYSTIMESTAMP
    );

    l_final_status_set := TRUE;

    RAISE_APPLICATION_ERROR(
      -20311,
      'Spool row count mismatch for business_date '
      || TO_CHAR(l_business_date, 'YYYY-MM-DD')
      || '. Expected '
      || l_expected_rows
      || ', inserted '
      || l_spool_rows_loaded
      || '.'
    );
  END IF;

  COMMIT;

  upsert_process_run(
    p_status             => 'PROCESSING',
    p_reason_code        => NULL,
    p_status_message     => 'AML report spool table built. Exporting outbound files.',
    p_expected_row_count => l_expected_rows,
    p_core_row_count     => l_spool_rows_loaded,
    p_started_ts         => l_attempt_ts,
    p_finished_ts        => NULL
  );

  export_spool_file;

  upsert_process_run(
    p_status             => 'DONE',
    p_reason_code        => 'SPOOL_BUILD_DONE',
    p_status_message     => 'AML report spool and outbound files completed successfully.',
    p_expected_row_count => l_expected_rows,
    p_core_row_count     => l_spool_rows_loaded,
    p_started_ts         => l_attempt_ts,
    p_finished_ts        => SYSTIMESTAMP
  );

  l_final_status_set := TRUE;

  DBMS_OUTPUT.PUT_LINE(
    'dwh.prc_build_aml_report_spool business_date='
    || TO_CHAR(l_business_date, 'YYYY-MM-DD')
    || ', status=DONE'
    || ', expected='
    || l_expected_rows
    || ', spool='
    || l_spool_rows_loaded
    || ', exported='
    || l_rows_exported
    || ', data_file='
    || l_data_file_name
    || ', ready_file='
    || l_ready_file_name
  );
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;

    IF l_input_valid THEN
      close_file_if_open(l_data_file);
      close_file_if_open(l_ready_file);

      BEGIN
        remove_file_if_exists(l_ready_file_name);
      EXCEPTION
        WHEN OTHERS THEN
          NULL;
      END;
    END IF;

    IF l_input_valid AND NOT l_final_status_set THEN
      upsert_process_run(
        p_status             => 'FAILED',
        p_reason_code        => 'UNEXPECTED_ERROR',
        p_status_message     => SQLERRM,
        p_expected_row_count => l_expected_rows,
        p_core_row_count     => l_spool_rows_loaded,
        p_started_ts         => l_attempt_ts,
        p_finished_ts        => SYSTIMESTAMP
      );
    END IF;

    RAISE;
END;
/
