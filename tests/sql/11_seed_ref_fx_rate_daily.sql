-- Simple deterministic FX seed for the AML-focused Phase-06 fixture date.
-- This helper is rerunnable because it clears the seeded business_date first.

WHENEVER OSERROR EXIT FAILURE;
WHENEVER SQLERROR EXIT SQL.SQLCODE;

SET VERIFY OFF;
SET FEEDBACK OFF;
SET SERVEROUTPUT ON;

PROMPT Seeding ref_fx_rate_daily for 2026-04-15...

DELETE FROM dwh.ref_fx_rate_daily
WHERE business_date = DATE '2026-04-15';

INSERT INTO dwh.ref_fx_rate_daily (
  business_date,
  currency_code,
  unit_count,
  mid_rate_pln,
  rate_source,
  published_date
) VALUES (
  DATE '2026-04-15',
  'EUR',
  1,
  4.30500000,
  'MANUAL_NBP_TABLE_A',
  DATE '2026-04-15'
);

INSERT INTO dwh.ref_fx_rate_daily (
  business_date,
  currency_code,
  unit_count,
  mid_rate_pln,
  rate_source,
  published_date
) VALUES (
  DATE '2026-04-15',
  'USD',
  1,
  3.96500000,
  'MANUAL_NBP_TABLE_A',
  DATE '2026-04-15'
);

INSERT INTO dwh.ref_fx_rate_daily (
  business_date,
  currency_code,
  unit_count,
  mid_rate_pln,
  rate_source,
  published_date
) VALUES (
  DATE '2026-04-15',
  'GBP',
  1,
  5.02000000,
  'MANUAL_NBP_TABLE_A',
  DATE '2026-04-15'
);

INSERT INTO dwh.ref_fx_rate_daily (
  business_date,
  currency_code,
  unit_count,
  mid_rate_pln,
  rate_source,
  published_date
) VALUES (
  DATE '2026-04-15',
  'PLN',
  1,
  1.00000000,
  'MANUAL_NBP_TABLE_A',
  DATE '2026-04-15'
);

INSERT INTO dwh.ref_fx_rate_daily (
  business_date,
  currency_code,
  unit_count,
  mid_rate_pln,
  rate_source,
  published_date
) VALUES (
  DATE '2026-04-15',
  'CZK',
  10,
  1.72500000,
  'MANUAL_NBP_TABLE_A',
  DATE '2026-04-15'
);

COMMIT;

PROMPT FX seed completed.
