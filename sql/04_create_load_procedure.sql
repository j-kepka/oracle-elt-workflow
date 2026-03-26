-- Creates a procedure that reloads stage and core data from the current external file snapshot

CREATE OR REPLACE PROCEDURE dwh.prc_load_client_transfers AS
  l_stage_rows_loaded NUMBER := 0;
  l_core_rows_loaded  NUMBER := 0;
BEGIN
  -- Full reload pattern for the initial MVP: external -> stage -> core
  EXECUTE IMMEDIATE 'TRUNCATE TABLE dwh.stg_client_transfers';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE dwh.core_client_transfers';

  INSERT INTO dwh.stg_client_transfers (
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
  FROM dwh.ext_client_transfers;

  l_stage_rows_loaded := SQL%ROWCOUNT;

  INSERT INTO dwh.core_client_transfers (
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
  FROM dwh.stg_client_transfers;

  l_core_rows_loaded := SQL%ROWCOUNT;
  COMMIT;

  DBMS_OUTPUT.PUT_LINE(
    'dwh.prc_load_client_transfers loaded rows: stage='
    || l_stage_rows_loaded
    || ', core='
    || l_core_rows_loaded
  );
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    RAISE;
END;
