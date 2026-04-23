-- Builds the AML mart for one business_date from core client and transfer snapshots.

CREATE OR REPLACE PROCEDURE dwh.prc_build_mart_transfer_aml (
  p_date     IN DATE DEFAULT TRUNC(SYSDATE),
  p_run_mode IN VARCHAR2 DEFAULT 'MANUAL'
) AS
  c_process_name      CONSTANT VARCHAR2(100 CHAR) := 'BUILD_MART_TRANSFER_AML';
  l_business_date     DATE := TRUNC(p_date);
  l_run_mode          VARCHAR2(10 CHAR) := UPPER(TRIM(p_run_mode));
  l_attempt_ts        TIMESTAMP;
  l_expected_rows     NUMBER := 0;
  l_mart_rows_loaded  NUMBER := 0;
  l_upstream_ready    NUMBER := 0;
  l_missing_fx_list   VARCHAR2(1000 CHAR);
  l_final_status_set  BOOLEAN := FALSE;

  PROCEDURE upsert_process_run (
    p_status             IN VARCHAR2,
    p_reason_code        IN VARCHAR2,
    p_status_message     IN VARCHAR2,
    p_expected_row_count IN NUMBER,
    p_core_row_count     IN NUMBER,
    p_started_ts         IN TIMESTAMP,
    p_finished_ts        IN TIMESTAMP
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
        dst.expected_row_count = p_expected_row_count,
        dst.stage_row_count = 0,
        dst.reject_row_count = 0,
        dst.core_row_count = p_core_row_count,
        dst.data_file_name = NULL,
        dst.ready_file_name = NULL,
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
        0,
        l_attempt_ts,
        NULL,
        p_started_ts,
        p_finished_ts,
        p_expected_row_count,
        0,
        0,
        p_core_row_count,
        NULL,
        NULL,
        p_status_message,
        SYSTIMESTAMP,
        SYSTIMESTAMP
      );

    COMMIT;
  END upsert_process_run;
BEGIN
  l_attempt_ts := SYSTIMESTAMP;

  IF l_run_mode NOT IN ('AUTO', 'MANUAL') THEN
    RAISE_APPLICATION_ERROR(
      -20201,
      'Unsupported run mode ' || NVL(p_run_mode, '<NULL>') || '. Expected AUTO or MANUAL.'
    );
  END IF;

  upsert_process_run(
    p_status             => 'PROCESSING',
    p_reason_code        => NULL,
    p_status_message     => 'AML mart build started.',
    p_expected_row_count => NULL,
    p_core_row_count     => 0,
    p_started_ts         => l_attempt_ts,
    p_finished_ts        => NULL
  );

  SELECT COUNT(*)
  INTO l_upstream_ready
  FROM dwh.ctl_process_run
  WHERE business_date = l_business_date
    AND status IN ('DONE', 'WARNING')
    AND process_name IN ('LOAD_CLIENTS', 'LOAD_CLIENT_TRANSFERS');

  IF l_upstream_ready < 2 THEN
    upsert_process_run(
      p_status             => 'FAILED',
      p_reason_code        => 'UPSTREAM_LOAD_NOT_READY',
      p_status_message     => 'Both client and transfer loads must finish before building mart_transfer_aml.',
      p_expected_row_count => NULL,
      p_core_row_count     => 0,
      p_started_ts         => l_attempt_ts,
      p_finished_ts        => SYSTIMESTAMP
    );

    l_final_status_set := TRUE;

    RAISE_APPLICATION_ERROR(
      -20210,
      'Upstream client and transfer loads are not ready for business_date '
      || TO_CHAR(l_business_date, 'YYYY-MM-DD')
      || '.'
    );
  END IF;

  SELECT COUNT(*)
  INTO l_expected_rows
  FROM dwh.core_client_transfers
  WHERE business_date = l_business_date;

  IF l_expected_rows = 0 THEN
    upsert_process_run(
      p_status             => 'FAILED',
      p_reason_code        => 'NO_TRANSFER_CORE_ROWS',
      p_status_message     => 'No core transfer rows found for the requested business_date.',
      p_expected_row_count => 0,
      p_core_row_count     => 0,
      p_started_ts         => l_attempt_ts,
      p_finished_ts        => SYSTIMESTAMP
    );

    l_final_status_set := TRUE;

    RAISE_APPLICATION_ERROR(
      -20211,
      'No core transfer rows found for business_date '
      || TO_CHAR(l_business_date, 'YYYY-MM-DD')
      || '.'
    );
  END IF;

  WITH required_currencies AS (
    SELECT DISTINCT currency_code
    FROM dwh.core_client_transfers
    WHERE business_date = l_business_date
    UNION
    SELECT 'EUR' AS currency_code
    FROM dual
    WHERE EXISTS (
      SELECT 1
      FROM dwh.core_client_transfers
      WHERE business_date = l_business_date
        AND currency_code <> 'EUR'
    )
  ),
  missing_currencies AS (
    SELECT req.currency_code
    FROM required_currencies req
    LEFT JOIN dwh.ref_fx_rate_daily fx
      ON fx.business_date = l_business_date
     AND fx.currency_code = req.currency_code
    WHERE fx.currency_code IS NULL
  )
  SELECT LISTAGG(currency_code, ', ') WITHIN GROUP (ORDER BY currency_code)
  INTO l_missing_fx_list
  FROM missing_currencies;

  IF l_missing_fx_list IS NOT NULL THEN
    upsert_process_run(
      p_status             => 'FAILED',
      p_reason_code        => 'MISSING_FX_RATES',
      p_status_message     => 'Missing FX reference rows for currencies: ' || l_missing_fx_list,
      p_expected_row_count => l_expected_rows,
      p_core_row_count     => 0,
      p_started_ts         => l_attempt_ts,
      p_finished_ts        => SYSTIMESTAMP
    );

    l_final_status_set := TRUE;

    RAISE_APPLICATION_ERROR(
      -20212,
      'Missing FX reference rows for business_date '
      || TO_CHAR(l_business_date, 'YYYY-MM-DD')
      || ': '
      || l_missing_fx_list
      || '.'
    );
  END IF;

  DELETE FROM dwh.mart_transfer_aml
  WHERE business_date = l_business_date;

  INSERT INTO dwh.mart_transfer_aml (
    business_date,
    transfer_id,
    client_id,
    client_type,
    full_name,
    country_code,
    transfer_country_code,
    source_account,
    target_account,
    amount,
    currency_code,
    fx_rate_to_eur,
    amount_eur,
    fx_rate_source,
    fx_published_date,
    transfer_ts,
    transfer_status,
    channel,
    transfer_title,
    pep_flag,
    high_risk_flag,
    kyc_status,
    risk_score,
    relationship_purpose_code,
    expected_activity_level,
    source_of_funds_declared,
    source_of_wealth_declared
  )
  SELECT
    trn.business_date,
    trn.transfer_id,
    trn.client_id,
    cli.client_type,
    cli.full_name,
    cli.country_code,
    trn.country_code AS transfer_country_code,
    trn.source_account,
    trn.target_account,
    trn.amount,
    trn.currency_code,
    CASE
      WHEN trn.currency_code = 'EUR' THEN 1
      ELSE ROUND(
        (src_fx.mid_rate_pln / src_fx.unit_count)
        / (eur_fx.mid_rate_pln / eur_fx.unit_count),
        8
      )
    END AS fx_rate_to_eur,
    CASE
      WHEN trn.currency_code = 'EUR' THEN ROUND(trn.amount, 2)
      ELSE ROUND(
        trn.amount
        * (
          (src_fx.mid_rate_pln / src_fx.unit_count)
          / (eur_fx.mid_rate_pln / eur_fx.unit_count)
        ),
        2
      )
    END AS amount_eur,
    src_fx.rate_source,
    src_fx.published_date,
    trn.transfer_ts,
    trn.transfer_status,
    trn.channel,
    trn.transfer_title,
    cli.pep_flag,
    cli.high_risk_flag,
    cli.kyc_status,
    cli.risk_score,
    cli.relationship_purpose_code,
    cli.expected_activity_level,
    cli.source_of_funds_declared,
    cli.source_of_wealth_declared
  FROM dwh.core_client_transfers trn
  JOIN dwh.core_clients cli
    ON cli.business_date = trn.business_date
   AND cli.client_id = trn.client_id
  JOIN dwh.ref_fx_rate_daily src_fx
    ON src_fx.business_date = trn.business_date
   AND src_fx.currency_code = trn.currency_code
  JOIN dwh.ref_fx_rate_daily eur_fx
    ON eur_fx.business_date = trn.business_date
   AND eur_fx.currency_code = 'EUR'
  WHERE trn.business_date = l_business_date;

  l_mart_rows_loaded := SQL%ROWCOUNT;

  IF l_mart_rows_loaded != l_expected_rows THEN
    ROLLBACK;

    upsert_process_run(
      p_status             => 'FAILED',
      p_reason_code        => 'MART_ROW_COUNT_MISMATCH',
      p_status_message     => 'Mart row count does not match source transfer row count.',
      p_expected_row_count => l_expected_rows,
      p_core_row_count     => l_mart_rows_loaded,
      p_started_ts         => l_attempt_ts,
      p_finished_ts        => SYSTIMESTAMP
    );

    l_final_status_set := TRUE;

    RAISE_APPLICATION_ERROR(
      -20213,
      'Mart row count mismatch for business_date '
      || TO_CHAR(l_business_date, 'YYYY-MM-DD')
      || '. Expected '
      || l_expected_rows
      || ', inserted '
      || l_mart_rows_loaded
      || '.'
    );
  END IF;

  COMMIT;

  upsert_process_run(
    p_status             => 'DONE',
    p_reason_code        => 'MART_BUILD_DONE',
    p_status_message     => 'AML mart build completed successfully.',
    p_expected_row_count => l_expected_rows,
    p_core_row_count     => l_mart_rows_loaded,
    p_started_ts         => l_attempt_ts,
    p_finished_ts        => SYSTIMESTAMP
  );

  l_final_status_set := TRUE;

  DBMS_OUTPUT.PUT_LINE(
    'dwh.prc_build_mart_transfer_aml business_date='
    || TO_CHAR(l_business_date, 'YYYY-MM-DD')
    || ', status=DONE'
    || ', expected='
    || l_expected_rows
    || ', mart='
    || l_mart_rows_loaded
  );
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;

    IF NOT l_final_status_set THEN
      upsert_process_run(
        p_status             => 'FAILED',
        p_reason_code        => 'UNEXPECTED_ERROR',
        p_status_message     => SQLERRM,
        p_expected_row_count => l_expected_rows,
        p_core_row_count     => l_mart_rows_loaded,
        p_started_ts         => l_attempt_ts,
        p_finished_ts        => SYSTIMESTAMP
      );
    END IF;

    RAISE;
END;
/
