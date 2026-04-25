-- Internal AML mart built from same-day client and transfer snapshots.
-- The table is a rerunnable derived dataset for one business_date.

CREATE TABLE dwh.mart_transfer_aml (
  business_date                 DATE               NOT NULL,
  transfer_id                   NUMBER(10)         NOT NULL,
  client_id                     NUMBER(10)         NOT NULL,
  client_type                   VARCHAR2(20 CHAR)  NOT NULL,
  full_name                     VARCHAR2(200 CHAR) NOT NULL,
  country_code                  CHAR(2 CHAR)       NOT NULL,
  transfer_country_code         CHAR(2 CHAR)       NOT NULL,
  source_account                VARCHAR2(34 CHAR)  NOT NULL,
  target_account                VARCHAR2(34 CHAR)  NOT NULL,
  amount                        NUMBER(14,2)       NOT NULL,
  currency_code                 CHAR(3 CHAR)       NOT NULL,
  fx_rate_to_eur                NUMBER(18,8)       NOT NULL,
  amount_eur                    NUMBER(18,2)       NOT NULL,
  above_threshold_art72_flag    NUMBER(1)          NOT NULL,
  suspicion_art74_flag          NUMBER(1)          NOT NULL,
  aml_review_flag               NUMBER(1)          NOT NULL,
  aml_reason_code               VARCHAR2(100 CHAR),
  aml_reason_details            VARCHAR2(1000 CHAR),
  report_type_candidate         VARCHAR2(30 CHAR),
  fx_rate_source                VARCHAR2(30 CHAR)  NOT NULL,
  fx_published_date             DATE,
  transfer_ts                   TIMESTAMP          NOT NULL,
  transfer_status               VARCHAR2(20 CHAR)  NOT NULL,
  channel                       VARCHAR2(20 CHAR)  NOT NULL,
  transfer_title                VARCHAR2(255 CHAR),
  pep_flag                      NUMBER(1)          NOT NULL,
  high_risk_flag                NUMBER(1)          NOT NULL,
  kyc_status                    VARCHAR2(30 CHAR),
  risk_score                    NUMBER(4),
  relationship_purpose_code     VARCHAR2(50 CHAR),
  expected_activity_level       VARCHAR2(50 CHAR),
  source_of_funds_declared      VARCHAR2(255 CHAR),
  source_of_wealth_declared     VARCHAR2(255 CHAR),
  created_ts                    TIMESTAMP          DEFAULT SYSTIMESTAMP NOT NULL,
  updated_ts                    TIMESTAMP          DEFAULT SYSTIMESTAMP NOT NULL,
  CONSTRAINT pk_mart_transfer_aml PRIMARY KEY (business_date, transfer_id),
  CONSTRAINT chk_mart_transfer_aml_amount CHECK (amount >= 0),
  CONSTRAINT chk_mart_transfer_aml_amount_eur CHECK (amount_eur >= 0),
  CONSTRAINT chk_mart_transfer_aml_fx_rate CHECK (fx_rate_to_eur > 0),
  CONSTRAINT chk_mart_transfer_aml_art72 CHECK (above_threshold_art72_flag IN (0, 1)),
  CONSTRAINT chk_mart_transfer_aml_art74 CHECK (suspicion_art74_flag IN (0, 1)),
  CONSTRAINT chk_mart_transfer_aml_review CHECK (aml_review_flag IN (0, 1)),
  CONSTRAINT chk_mart_transfer_aml_report_type CHECK (
    report_type_candidate IS NULL
    OR report_type_candidate IN ('ART72_THRESHOLD', 'ART74_SUSPICION', 'ART72_AND_ART74')
  )
);

CREATE INDEX dwh.ix_mart_transfer_aml_bd_client
  ON dwh.mart_transfer_aml (business_date, client_id);
