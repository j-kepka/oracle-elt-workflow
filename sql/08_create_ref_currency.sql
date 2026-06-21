-- Supported transfer currencies used by inbound validation.
CREATE TABLE dwh.ref_currency (
  currency_code  CHAR(3 CHAR)       NOT NULL,
  currency_name  VARCHAR2(100 CHAR) NOT NULL,
  is_active_flag NUMBER(1)          DEFAULT 1 NOT NULL,
  created_ts     TIMESTAMP          DEFAULT SYSTIMESTAMP NOT NULL,
  updated_ts     TIMESTAMP          DEFAULT SYSTIMESTAMP NOT NULL,
  CONSTRAINT pk_ref_currency PRIMARY KEY (currency_code),
  CONSTRAINT chk_ref_currency_code CHECK (currency_code = UPPER(currency_code)),
  CONSTRAINT chk_ref_currency_active CHECK (is_active_flag IN (0, 1))
);

INSERT ALL
  INTO dwh.ref_currency (currency_code, currency_name) VALUES ('EUR', 'Euro')
  INTO dwh.ref_currency (currency_code, currency_name) VALUES ('USD', 'US Dollar')
  INTO dwh.ref_currency (currency_code, currency_name) VALUES ('PLN', 'Polish Zloty')
  INTO dwh.ref_currency (currency_code, currency_name) VALUES ('CZK', 'Czech Koruna')
  INTO dwh.ref_currency (currency_code, currency_name) VALUES ('GBP', 'Pound Sterling')
SELECT 1 FROM dual;

COMMIT;
