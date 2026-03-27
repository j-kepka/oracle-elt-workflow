-- Creates a procedure that reloads stage and core data from a dated external file snapshot

CREATE OR REPLACE PROCEDURE dwh.prc_load_client_transfers (
  p_date IN DATE DEFAULT TRUNC(SYSDATE)
) AS
  c_ext_dir            CONSTANT VARCHAR2(30 CHAR) := 'EXT_DIR';
  l_business_date      DATE := TRUNC(p_date);
  l_snapshot_file      VARCHAR2(128 CHAR);
  l_stage_rows_loaded  NUMBER := 0;
  l_reject_rows_loaded NUMBER := 0;
  l_core_rows_loaded   NUMBER := 0;

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
BEGIN
  l_snapshot_file := 'client_transfers_' || TO_CHAR(l_business_date, 'YYYYMMDD') || '.csv';

  assert_file_exists(
    p_directory     => c_ext_dir,
    p_filename      => l_snapshot_file,
    p_error_code    => -20010,
    p_error_message => 'Data file ' || l_snapshot_file || ' not found.'
  );

  EXECUTE IMMEDIATE
    'ALTER TABLE dwh.ext_client_transfers LOCATION (''' || l_snapshot_file || ''')';

  DELETE FROM dwh.stg_client_transfers
  WHERE business_date = l_business_date;

  DELETE FROM dwh.stg_client_transfers_reject
  WHERE business_date = l_business_date;

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
    country_code
  )
  WITH normalized_data AS (
    SELECT
      l_business_date AS business_date,
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
    FROM dwh.ext_client_transfers
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
    country_code_raw
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
    AND is_supported_country = 1;

  l_stage_rows_loaded := SQL%ROWCOUNT;

  INSERT INTO dwh.stg_client_transfers_reject (
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
    reject_reason
  )
  WITH normalized_data AS (
    SELECT
      l_business_date AS business_date,
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
    FROM dwh.ext_client_transfers
  )
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
        WHEN is_supported_country = 0 THEN 'unsupported country_code; '
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
    OR is_supported_country = 0;

  l_reject_rows_loaded := SQL%ROWCOUNT;

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
    country_code
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
    country_code
  FROM dwh.stg_client_transfers
  WHERE business_date = l_business_date;

  l_core_rows_loaded := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE(
    'dwh.prc_load_client_transfers business_date='
    || TO_CHAR(l_business_date, 'YYYY-MM-DD')
    || ', file='
    || l_snapshot_file
    || ', stage='
    || l_stage_rows_loaded
    || ', reject='
    || l_reject_rows_loaded
    || ', core='
    || l_core_rows_loaded
  );
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    RAISE;
END;
/
