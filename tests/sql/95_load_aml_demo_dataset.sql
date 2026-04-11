-- Loads the dedicated AML demo dataset.
-- Uses a dedicated AML date instead of the legacy deterministic smoke matrix.

WHENEVER OSERROR EXIT FAILURE;
WHENEVER SQLERROR EXIT SQL.SQLCODE;

SET SERVEROUTPUT ON;
SET VERIFY OFF;
SET FEEDBACK OFF;
SET LINESIZE 220;
SET PAGESIZE 100;

PROMPT Loading AML demo dataset for 2026-04-15...

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
WHERE process_name IN ('LOAD_CLIENTS', 'LOAD_CLIENT_TRANSFERS')
  AND business_date = DATE '2026-04-15';

COMMIT;

@/workspace/tests/sql/11_seed_ref_fx_rate_daily.sql

BEGIN
  dwh.prc_load_clients(
    p_date     => DATE '2026-04-15',
    p_run_mode => 'MANUAL'
  );

  dwh.prc_load_client_transfers(
    p_date     => DATE '2026-04-15',
    p_run_mode => 'MANUAL'
  );
END;
/

PROMPT AML demo dataset load completed.
