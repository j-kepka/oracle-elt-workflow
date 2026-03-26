-- External table mapped to extdata/client_transfers.csv via Oracle DIRECTORY EXT_DIR
-- Prerequisite (run as SYSTEM once):
--   CREATE OR REPLACE DIRECTORY EXT_DIR AS '/opt/oracle/extdata';
--   CREATE OR REPLACE DIRECTORY EXT_WORK_DIR AS '/opt/oracle/extdata/work';
--   GRANT READ, WRITE ON DIRECTORY EXT_DIR TO dwh;
--   GRANT READ, WRITE ON DIRECTORY EXT_WORK_DIR TO dwh;

BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE dwh.ext_client_transfers';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
      RAISE;
    END IF;
END;

CREATE TABLE dwh.ext_client_transfers (
  transfer_id      NUMBER(10),
  client_id        NUMBER(10),
  source_account   VARCHAR2(34 CHAR),
  target_account   VARCHAR2(34 CHAR),
  amount           NUMBER(14,2),
  currency_code    CHAR(3 CHAR),
  transfer_ts      TIMESTAMP,
  transfer_status  VARCHAR2(20 CHAR),
  channel          VARCHAR2(20 CHAR),
  country_code     CHAR(2 CHAR)
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
    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
    MISSING FIELD VALUES ARE NULL
    (
      transfer_id,
      client_id,
      source_account,
      target_account,
      amount,
      currency_code,
      transfer_ts CHAR(19) DATE_FORMAT DATE MASK "YYYY-MM-DD HH24:MI:SS",
      transfer_status,
      channel,
      country_code
    )
  )
  LOCATION ('client_transfers.csv')
)
REJECT LIMIT UNLIMITED;
