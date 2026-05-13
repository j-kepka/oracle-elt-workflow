-- Runs the dedicated AML workflow procedure for the AML demo date.
-- This helper starts from a clean 2026-04-15 demo state.

WHENEVER OSERROR EXIT FAILURE;
WHENEVER SQLERROR EXIT SQL.SQLCODE;

SET SERVEROUTPUT ON;
SET VERIFY OFF;
SET FEEDBACK OFF;
SET LINESIZE 260;
SET PAGESIZE 120;

PROMPT Running AML workflow for 2026-04-15...

DELETE FROM dwh.aml_report_spool
WHERE business_date = DATE '2026-04-15';

DELETE FROM dwh.mart_transfer_aml
WHERE business_date = DATE '2026-04-15';

DELETE FROM dwh.core_client_transfers
WHERE business_date = DATE '2026-04-15';

DELETE FROM dwh.stg_client_transfers_reject
WHERE business_date = DATE '2026-04-15';

DELETE FROM dwh.stg_client_transfers
WHERE business_date = DATE '2026-04-15';

DELETE FROM dwh.core_clients
WHERE business_date = DATE '2026-04-15';

DELETE FROM dwh.stg_clients_reject
WHERE business_date = DATE '2026-04-15';

DELETE FROM dwh.stg_clients
WHERE business_date = DATE '2026-04-15';

DELETE FROM dwh.ctl_process_run
WHERE process_name IN (
    'RUN_AML_WORKFLOW',
    'LOAD_CLIENTS',
    'LOAD_CLIENT_TRANSFERS',
    'BUILD_MART_TRANSFER_AML',
    'BUILD_AML_REPORT_SPOOL'
  )
  AND business_date = DATE '2026-04-15';

COMMIT;

@/workspace/tests/sql/11_seed_ref_fx_rate_daily.sql

BEGIN
  dwh.prc_run_aml_workflow(
    p_date     => DATE '2026-04-15',
    p_run_mode => 'MANUAL'
  );
END;
/

PROMPT AML workflow completed.
