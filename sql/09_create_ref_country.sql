-- Supported transfer countries used by inbound validation.
CREATE TABLE dwh.ref_country (
  country_code   CHAR(2 CHAR)       NOT NULL,
  country_name   VARCHAR2(100 CHAR) NOT NULL,
  is_active_flag NUMBER(1)          DEFAULT 1 NOT NULL,
  created_ts     TIMESTAMP          DEFAULT SYSTIMESTAMP NOT NULL,
  updated_ts     TIMESTAMP          DEFAULT SYSTIMESTAMP NOT NULL,
  CONSTRAINT pk_ref_country PRIMARY KEY (country_code),
  CONSTRAINT chk_ref_country_code CHECK (country_code = UPPER(country_code)),
  CONSTRAINT chk_ref_country_active CHECK (is_active_flag IN (0, 1))
);

INSERT ALL
  INTO dwh.ref_country (country_code, country_name) VALUES ('AT', 'Austria')
  INTO dwh.ref_country (country_code, country_name) VALUES ('BE', 'Belgium')
  INTO dwh.ref_country (country_code, country_name) VALUES ('BG', 'Bulgaria')
  INTO dwh.ref_country (country_code, country_name) VALUES ('CH', 'Switzerland')
  INTO dwh.ref_country (country_code, country_name) VALUES ('CY', 'Cyprus')
  INTO dwh.ref_country (country_code, country_name) VALUES ('CZ', 'Czechia')
  INTO dwh.ref_country (country_code, country_name) VALUES ('DE', 'Germany')
  INTO dwh.ref_country (country_code, country_name) VALUES ('DK', 'Denmark')
  INTO dwh.ref_country (country_code, country_name) VALUES ('EE', 'Estonia')
  INTO dwh.ref_country (country_code, country_name) VALUES ('ES', 'Spain')
  INTO dwh.ref_country (country_code, country_name) VALUES ('FI', 'Finland')
  INTO dwh.ref_country (country_code, country_name) VALUES ('FR', 'France')
  INTO dwh.ref_country (country_code, country_name) VALUES ('GB', 'United Kingdom')
  INTO dwh.ref_country (country_code, country_name) VALUES ('GR', 'Greece')
  INTO dwh.ref_country (country_code, country_name) VALUES ('HR', 'Croatia')
  INTO dwh.ref_country (country_code, country_name) VALUES ('HU', 'Hungary')
  INTO dwh.ref_country (country_code, country_name) VALUES ('IE', 'Ireland')
  INTO dwh.ref_country (country_code, country_name) VALUES ('IS', 'Iceland')
  INTO dwh.ref_country (country_code, country_name) VALUES ('IT', 'Italy')
  INTO dwh.ref_country (country_code, country_name) VALUES ('LI', 'Liechtenstein')
  INTO dwh.ref_country (country_code, country_name) VALUES ('LT', 'Lithuania')
  INTO dwh.ref_country (country_code, country_name) VALUES ('LU', 'Luxembourg')
  INTO dwh.ref_country (country_code, country_name) VALUES ('LV', 'Latvia')
  INTO dwh.ref_country (country_code, country_name) VALUES ('MT', 'Malta')
  INTO dwh.ref_country (country_code, country_name) VALUES ('NL', 'Netherlands')
  INTO dwh.ref_country (country_code, country_name) VALUES ('NO', 'Norway')
  INTO dwh.ref_country (country_code, country_name) VALUES ('PL', 'Poland')
  INTO dwh.ref_country (country_code, country_name) VALUES ('PT', 'Portugal')
  INTO dwh.ref_country (country_code, country_name) VALUES ('RO', 'Romania')
  INTO dwh.ref_country (country_code, country_name) VALUES ('SE', 'Sweden')
  INTO dwh.ref_country (country_code, country_name) VALUES ('SI', 'Slovenia')
  INTO dwh.ref_country (country_code, country_name) VALUES ('SK', 'Slovakia')
  INTO dwh.ref_country (country_code, country_name) VALUES ('US', 'United States')
SELECT 1 FROM dual;

COMMIT;
