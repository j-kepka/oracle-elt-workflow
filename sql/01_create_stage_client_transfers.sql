-- Stage table for client transfer records loaded from extdata/client_transfers.csv
CREATE TABLE dwh.stg_client_transfers (
  transfer_id      NUMBER(10)        NOT NULL,
  client_id        NUMBER(10)        NOT NULL,
  source_account   VARCHAR2(34 CHAR) NOT NULL,
  target_account   VARCHAR2(34 CHAR) NOT NULL,
  amount           NUMBER(14,2)      NOT NULL,
  currency_code    CHAR(3 CHAR)      NOT NULL,
  transfer_ts      TIMESTAMP         NOT NULL,
  transfer_status  VARCHAR2(20 CHAR) NOT NULL,
  channel          VARCHAR2(20 CHAR) NOT NULL,
  country_code     CHAR(2 CHAR)      NOT NULL
);
