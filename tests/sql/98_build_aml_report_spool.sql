-- Builds the dedicated AML report spool and outbound files for the AML demo date.
-- Run after 95_load_aml_demo_dataset.sql and 96_validate_aml_demo_dataset.sql.

WHENEVER OSERROR EXIT FAILURE;
WHENEVER SQLERROR EXIT SQL.SQLCODE;

SET SERVEROUTPUT ON;
SET VERIFY OFF;
SET FEEDBACK OFF;
SET LINESIZE 260;
SET PAGESIZE 120;

PROMPT Building AML report spool for 2026-04-15...

DELETE FROM dwh.aml_report_spool
WHERE business_date = DATE '2026-04-15';

DELETE FROM dwh.ctl_process_run
WHERE process_name = 'BUILD_AML_REPORT_SPOOL'
  AND business_date = DATE '2026-04-15';

COMMIT;

BEGIN
  dwh.prc_build_aml_report_spool(
    p_date     => DATE '2026-04-15',
    p_run_mode => 'MANUAL'
  );
END;
/

PROMPT AML report spool build completed.
