-- Operational helper: execute the client transfers load procedure manually after the client snapshot load.
-- Expected SQL*Plus/SQLcl variable:
--   DEFINE P_DATE = '2026-03-26'
--   DEFINE RUN_MODE = 'MANUAL'

BEGIN
  dwh.prc_load_client_transfers(
    p_date => TO_DATE('&P_DATE', 'YYYY-MM-DD'),
    p_run_mode => UPPER('&RUN_MODE')
  );
END;
/
