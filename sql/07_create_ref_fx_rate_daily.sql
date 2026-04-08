-- Manual daily FX reference used for controlled EUR normalization in AML demo flows.
-- The table stores NBP-style rates against PLN for a given business_date.
CREATE TABLE dwh.ref_fx_rate_daily (
  business_date   DATE               NOT NULL,
  currency_code   CHAR(3 CHAR)       NOT NULL,
  unit_count      NUMBER(6)          NOT NULL,
  mid_rate_pln    NUMBER(18,8)       NOT NULL,
  rate_source     VARCHAR2(30 CHAR)  DEFAULT 'MANUAL_NBP_TABLE_A' NOT NULL,
  published_date  DATE,
  created_ts      TIMESTAMP          DEFAULT SYSTIMESTAMP NOT NULL,
  updated_ts      TIMESTAMP          DEFAULT SYSTIMESTAMP NOT NULL,
  CONSTRAINT pk_ref_fx_rate_daily PRIMARY KEY (business_date, currency_code),
  CONSTRAINT chk_ref_fx_rate_daily_unit_count CHECK (unit_count > 0),
  CONSTRAINT chk_ref_fx_rate_daily_mid_rate CHECK (mid_rate_pln > 0),
  CONSTRAINT chk_ref_fx_rate_daily_source CHECK (rate_source = UPPER(rate_source))
);
