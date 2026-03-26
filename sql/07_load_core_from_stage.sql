-- Load data from stage table into the initial 1:1 core table

TRUNCATE TABLE dwh.core_client_transfers;

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

COMMIT;

-- Validation
SELECT COUNT(*) AS core_row_count
FROM dwh.core_client_transfers;
