-- Core table for the current client snapshot used for joins with transfers.
-- client_status keeps the reporting state inside the snapshot without replacing it with hard deletes.
CREATE TABLE dwh.core_clients (
  business_date    DATE               NOT NULL,
  client_id        NUMBER(10)         NOT NULL,
  client_type      VARCHAR2(20 CHAR)  NOT NULL,
  full_name        VARCHAR2(200 CHAR) NOT NULL,
  first_name       VARCHAR2(100 CHAR),
  last_name        VARCHAR2(100 CHAR),
  company_name     VARCHAR2(200 CHAR),
  date_of_birth    DATE,
  document_id      VARCHAR2(100 CHAR),
  registration_no  VARCHAR2(50 CHAR),
  tax_id           VARCHAR2(100 CHAR),
  address_line_1   VARCHAR2(200 CHAR) NOT NULL,
  city             VARCHAR2(100 CHAR) NOT NULL,
  postal_code      VARCHAR2(20 CHAR)  NOT NULL,
  country_code     CHAR(2 CHAR)       NOT NULL,
  phone_number     VARCHAR2(50 CHAR),
  email            VARCHAR2(255 CHAR),
  pep_flag         NUMBER(1)          NOT NULL,
  high_risk_flag   NUMBER(1)          NOT NULL,
  kyc_status       VARCHAR2(30 CHAR),
  risk_score       NUMBER(4),
  client_status    VARCHAR2(20 CHAR)  NOT NULL,
  CONSTRAINT pk_core_clients PRIMARY KEY (business_date, client_id),
  CONSTRAINT chk_core_clients_status CHECK (client_status IN ('ACTIVE', 'ARCHIVED')),
  CONSTRAINT chk_core_clients_risk_score CHECK (risk_score IS NULL OR risk_score BETWEEN 0 AND 999)
);
