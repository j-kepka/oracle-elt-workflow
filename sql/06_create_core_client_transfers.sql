-- Core table for the initial 1:1 curated copy of staged client transfer records.
-- Each transfer must point to the client snapshot from the same business_date.
CREATE TABLE dwh.core_client_transfers (
  business_date     DATE              NOT NULL,
  transfer_id       NUMBER(10)        NOT NULL,
  client_id         NUMBER(10)        NOT NULL,
  source_account    VARCHAR2(34 CHAR) NOT NULL,
  target_account    VARCHAR2(34 CHAR) NOT NULL,
  amount            NUMBER(14,2)      NOT NULL,
  currency_code     CHAR(3 CHAR)      NOT NULL,
  transfer_ts       TIMESTAMP         NOT NULL,
  transfer_status   VARCHAR2(20 CHAR) NOT NULL,
  channel           VARCHAR2(20 CHAR) NOT NULL,
  country_code      CHAR(2 CHAR)      NOT NULL,
  CONSTRAINT pk_core_client_transfers PRIMARY KEY (business_date, transfer_id),
  CONSTRAINT fk_core_client_transfers_client
    FOREIGN KEY (business_date, client_id)
    REFERENCES dwh.core_clients (business_date, client_id)
    DEFERRABLE INITIALLY IMMEDIATE
);

-- Supports FK checks and parent refreshes on core_clients(business_date, client_id).
CREATE INDEX dwh.ix_core_client_transfers_bd_client
  ON dwh.core_client_transfers (business_date, client_id);
