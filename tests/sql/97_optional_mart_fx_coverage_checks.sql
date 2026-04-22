-- Optional negative check for mart FX coverage.
-- Run after 95_load_aml_demo_dataset.sql has prepared the AML demo date.

WHENEVER OSERROR EXIT FAILURE;
WHENEVER SQLERROR EXIT SQL.SQLCODE;

SET SERVEROUTPUT ON;
SET VERIFY OFF;
SET FEEDBACK OFF;
SET LINESIZE 220;
SET PAGESIZE 100;

PROMPT Running optional mart FX coverage checks for 2026-04-15...

DELETE FROM dwh.mart_transfer_aml
WHERE business_date = DATE '2026-04-15';

DELETE FROM dwh.ref_fx_rate_daily
WHERE business_date = DATE '2026-04-15'
  AND currency_code = 'USD';

COMMIT;

DECLARE
  l_mismatch_count NUMBER;
BEGIN
  BEGIN
    dwh.prc_build_mart_transfer_aml(
      p_date     => DATE '2026-04-15',
      p_run_mode => 'MANUAL'
    );

    RAISE_APPLICATION_ERROR(
      -20997,
      'Expected mart build to fail when the USD FX reference row is missing.'
    );
  EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE = -20212 THEN
        DBMS_OUTPUT.PUT_LINE('Expected MISSING_FX_RATES failure observed.');
      ELSE
        RAISE;
      END IF;
  END;

  SELECT COUNT(*)
  INTO l_mismatch_count
  FROM dwh.ctl_process_run
  WHERE process_name = 'BUILD_MART_TRANSFER_AML'
    AND business_date = DATE '2026-04-15'
    AND status = 'FAILED'
    AND reason_code = 'MISSING_FX_RATES'
    AND status_message LIKE '%USD%';

  IF l_mismatch_count <> 1 THEN
    RAISE_APPLICATION_ERROR(
      -20996,
      'Expected BUILD_MART_TRANSFER_AML to record FAILED / MISSING_FX_RATES for USD.'
    );
  END IF;
END;
/

PROMPT Restoring FX seed and rebuilding mart...

@/workspace/tests/sql/11_seed_ref_fx_rate_daily.sql

BEGIN
  dwh.prc_build_mart_transfer_aml(
    p_date     => DATE '2026-04-15',
    p_run_mode => 'MANUAL'
  );
END;
/

DECLARE
  l_mismatch_count NUMBER;
BEGIN
  SELECT COUNT(*)
  INTO l_mismatch_count
  FROM dwh.ctl_process_run
  WHERE process_name = 'BUILD_MART_TRANSFER_AML'
    AND business_date = DATE '2026-04-15'
    AND status = 'DONE'
    AND reason_code = 'MART_BUILD_DONE'
    AND expected_row_count = 20
    AND core_row_count = 20;

  IF l_mismatch_count <> 1 THEN
    RAISE_APPLICATION_ERROR(
      -20995,
      'Expected mart rebuild to pass after restoring the FX seed.'
    );
  END IF;

  DBMS_OUTPUT.PUT_LINE('Optional mart FX coverage checks passed.');
END;
/
