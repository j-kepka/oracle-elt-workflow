-- External table mapped to a dated file in extdata, for example clients_20260326.csv
-- RECNUM keeps the physical file line number, so the first data row is 2 when SKIP 1 is used.
-- Loader failures are treated as technical errors and must stop the run immediately.
-- Prerequisite (run as SYSTEM once):
--   CREATE OR REPLACE DIRECTORY EXT_DIR AS '/opt/oracle/extdata';
--   CREATE OR REPLACE DIRECTORY EXT_WORK_DIR AS '/opt/oracle/extdata/work';
--   GRANT READ ON DIRECTORY EXT_DIR TO dwh;
--   GRANT READ, WRITE ON DIRECTORY EXT_WORK_DIR TO dwh;

BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE dwh.ext_clients';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
      RAISE;
    END IF;
END;
/

CREATE TABLE dwh.ext_clients (
  source_row_num        NUMBER(10),
  business_date_raw     VARCHAR2(100 CHAR),
  client_id_raw         VARCHAR2(100 CHAR),
  client_type_raw       VARCHAR2(100 CHAR),
  full_name_raw         VARCHAR2(255 CHAR),
  first_name_raw        VARCHAR2(255 CHAR),
  last_name_raw         VARCHAR2(255 CHAR),
  company_name_raw      VARCHAR2(255 CHAR),
  date_of_birth_raw     VARCHAR2(100 CHAR),
  registration_no_raw   VARCHAR2(100 CHAR),
  address_line_1_raw    VARCHAR2(255 CHAR),
  city_raw              VARCHAR2(255 CHAR),
  postal_code_raw       VARCHAR2(100 CHAR),
  country_code_raw      VARCHAR2(100 CHAR),
  pep_flag_raw          VARCHAR2(100 CHAR),
  high_risk_flag_raw    VARCHAR2(100 CHAR),
  client_status_raw     VARCHAR2(100 CHAR),
  document_id_raw       VARCHAR2(255 CHAR),
  tax_id_raw            VARCHAR2(255 CHAR),
  phone_number_raw      VARCHAR2(100 CHAR),
  email_raw             VARCHAR2(255 CHAR),
  kyc_status_raw        VARCHAR2(100 CHAR),
  risk_score_raw        VARCHAR2(100 CHAR),
  relationship_purpose_code_raw  VARCHAR2(100 CHAR),
  expected_activity_level_raw    VARCHAR2(100 CHAR),
  source_of_funds_declared_raw   VARCHAR2(255 CHAR),
  source_of_wealth_declared_raw  VARCHAR2(255 CHAR)
)
ORGANIZATION EXTERNAL (
  TYPE ORACLE_LOADER
  DEFAULT DIRECTORY EXT_DIR
  ACCESS PARAMETERS (
    RECORDS DELIMITED BY NEWLINE
    LOGFILE EXT_WORK_DIR:'ext_clients.log'
    BADFILE EXT_WORK_DIR:'ext_clients.bad'
    DISCARDFILE EXT_WORK_DIR:'ext_clients.dsc'
    SKIP 1
    FIELDS TERMINATED BY ';' OPTIONALLY ENCLOSED BY '"'
    MISSING FIELD VALUES ARE NULL
    (
      source_row_num RECNUM,
      business_date_raw,
      client_id_raw,
      client_type_raw,
      full_name_raw,
      first_name_raw,
      last_name_raw,
      company_name_raw,
      date_of_birth_raw,
      registration_no_raw,
      address_line_1_raw,
      city_raw,
      postal_code_raw,
      country_code_raw,
      pep_flag_raw,
      high_risk_flag_raw,
      client_status_raw,
      document_id_raw,
      tax_id_raw,
      phone_number_raw,
      email_raw,
      kyc_status_raw,
      risk_score_raw,
      relationship_purpose_code_raw,
      expected_activity_level_raw,
      source_of_funds_declared_raw,
      source_of_wealth_declared_raw
    )
  )
  -- The load procedure changes LOCATION to the requested business date file before each run.
  LOCATION ('clients_20260326.csv')
)
REJECT LIMIT 0;
