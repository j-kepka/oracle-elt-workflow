-- Stage table for validated client transfer records loaded from dated CSV snapshots
-- source_row_num keeps the physical CSV line number from ORACLE_LOADER RECNUM.
-- With SKIP 1, the header stays line 1 and the first data row becomes line 2.
CREATE TABLE dwh.stg_client_transfers (
  business_date     DATE              NOT NULL,
  source_row_num    NUMBER(10)        NOT NULL,
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
  transfer_title    VARCHAR2(255 CHAR)
);

-- Reject table for invalid records detected during raw-to-stage validation
CREATE TABLE dwh.stg_client_transfers_reject (
  business_date        DATE                NOT NULL,
  source_file_name     VARCHAR2(255 CHAR)  NOT NULL,
  source_row_num       NUMBER(10)          NOT NULL,
  transfer_id_raw      VARCHAR2(100 CHAR),
  client_id_raw        VARCHAR2(100 CHAR),
  source_account_raw   VARCHAR2(100 CHAR),
  target_account_raw   VARCHAR2(100 CHAR),
  amount_raw           VARCHAR2(100 CHAR),
  currency_code_raw    VARCHAR2(100 CHAR),
  transfer_ts_raw      VARCHAR2(100 CHAR),
  transfer_status_raw  VARCHAR2(100 CHAR),
  channel_raw          VARCHAR2(100 CHAR),
  country_code_raw     VARCHAR2(100 CHAR),
  transfer_title_raw   VARCHAR2(255 CHAR),
  reject_reason        VARCHAR2(4000 CHAR) NOT NULL,
  rejected_at          TIMESTAMP           DEFAULT SYSTIMESTAMP NOT NULL
);
