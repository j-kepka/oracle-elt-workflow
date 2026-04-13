-- External table mapped to a dated file in extdata, for example client_transfers_20260326.csv
-- RECNUM keeps the physical file line number, so the first data row is 2 when SKIP 1 is used.
-- Loader failures are treated as technical errors and must stop the run immediately.
-- Prerequisite (run as SYSTEM once):
--   CREATE OR REPLACE DIRECTORY EXT_DIR AS '/opt/oracle/extdata';
--   CREATE OR REPLACE DIRECTORY EXT_WORK_DIR AS '/opt/oracle/extdata/work';
--   GRANT READ ON DIRECTORY EXT_DIR TO dwh;
--   GRANT READ, WRITE ON DIRECTORY EXT_WORK_DIR TO dwh;

BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE dwh.ext_client_transfers';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
      RAISE;
    END IF;
END;
/

CREATE TABLE dwh.ext_client_transfers (
  source_row_num      NUMBER(10),
  transfer_id_raw     VARCHAR2(100 CHAR),
  client_id_raw       VARCHAR2(100 CHAR),
  source_account_raw  VARCHAR2(100 CHAR),
  target_account_raw  VARCHAR2(100 CHAR),
  amount_raw          VARCHAR2(100 CHAR),
  currency_code_raw   VARCHAR2(100 CHAR),
  transfer_ts_raw     VARCHAR2(100 CHAR),
  transfer_status_raw VARCHAR2(100 CHAR),
  channel_raw         VARCHAR2(100 CHAR),
  country_code_raw    VARCHAR2(100 CHAR),
  transfer_title_raw  VARCHAR2(255 CHAR)
)
ORGANIZATION EXTERNAL (
  TYPE ORACLE_LOADER
  DEFAULT DIRECTORY EXT_DIR
  ACCESS PARAMETERS (
    RECORDS DELIMITED BY NEWLINE
    LOGFILE EXT_WORK_DIR:'ext_client_transfers.log'
    BADFILE EXT_WORK_DIR:'ext_client_transfers.bad'
    DISCARDFILE EXT_WORK_DIR:'ext_client_transfers.dsc'
    SKIP 1
    FIELDS TERMINATED BY ';' OPTIONALLY ENCLOSED BY '"'
    MISSING FIELD VALUES ARE NULL
    (
      source_row_num RECNUM,
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
      transfer_title_raw
    )
  )
  -- The load procedures override LOCATION and loader artifact names per query via EXTERNAL MODIFY.
  LOCATION ('client_transfers_20260326.csv')
)
REJECT LIMIT 0;
