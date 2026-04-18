-- Operational helper: execute the client snapshot load procedure manually before dependent transfer loads.
-- Expected SQL*Plus/SQLcl variable:
--   DEFINE P_DATE = '2026-03-26'
--   DEFINE RUN_MODE = 'MANUAL'

BEGIN
  dwh.prc_load_clients(
    p_date => TO_DATE('&P_DATE', 'YYYY-MM-DD'),
    p_run_mode => UPPER('&RUN_MODE')
  );
END;
/
