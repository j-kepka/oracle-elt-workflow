-- Load data from external table into stage table

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

COMMIT;

-- Validation
SELECT COUNT(*) AS stage_row_count
FROM dwh.stg_client_transfers;
