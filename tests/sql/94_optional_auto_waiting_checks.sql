-- Optional AUTO checks for 2026-04-07 with a test-specific dynamic cutoff.
-- The production/default AUTO cutoff is based on the current run day, not on business_date.
-- This helper overrides the cutoff to "now + 5 minutes" and retries every 1 minute.
-- A single shared cutoff keeps the whole helper close to a 5-minute wait window.
-- so the WAITING path can be observed at any time of day without relying on 12:00.
-- Not included in the deterministic compare matrix.

WHENEVER OSERROR EXIT FAILURE;
WHENEVER SQLERROR EXIT SQL.SQLCODE;

SET SERVEROUTPUT ON;
SET VERIFY OFF;
SET FEEDBACK OFF;
SET LINESIZE 220;
SET PAGESIZE 100;

PROMPT Running optional AUTO checks for 2026-04-07...

COLUMN test_cutoff_ts NEW_VALUE TEST_CUTOFF_TS NOPRINT;

SELECT TO_CHAR(
         CAST(SYSTIMESTAMP AS TIMESTAMP) + NUMTODSINTERVAL(5, 'MINUTE'),
         'YYYY-MM-DD HH24:MI:SS'
       ) AS test_cutoff_ts
FROM dual;

DELETE FROM dwh.core_client_transfers
WHERE business_date = DATE '2026-04-07';

DELETE FROM dwh.stg_client_transfers_reject
WHERE business_date = DATE '2026-04-07';

DELETE FROM dwh.stg_client_transfers
WHERE business_date = DATE '2026-04-07';

DELETE FROM dwh.core_clients
WHERE business_date = DATE '2026-04-07';

DELETE FROM dwh.stg_clients_reject
WHERE business_date = DATE '2026-04-07';

DELETE FROM dwh.stg_clients
WHERE business_date = DATE '2026-04-07';

DELETE FROM dwh.ctl_process_run
WHERE process_name IN ('LOAD_CLIENTS', 'LOAD_CLIENT_TRANSFERS')
  AND business_date = DATE '2026-04-07';

COMMIT;

SELECT
  TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS TZH:TZM') AS db_now,
  '&&TEST_CUTOFF_TS' AS test_cutoff_ts,
  1 AS retry_sleep_minutes,
  CASE
    WHEN CAST(SYSTIMESTAMP AS TIMESTAMP) <
         TO_TIMESTAMP('&&TEST_CUTOFF_TS', 'YYYY-MM-DD HH24:MI:SS')
      THEN 'BEFORE_CUTOFF'
    ELSE 'AT_OR_AFTER_CUTOFF'
  END AS cutoff_state
FROM dual;

BEGIN
  BEGIN
    dwh.prc_load_client_transfers(
      p_date                     => DATE '2026-04-07',
      p_run_mode                 => 'AUTO',
      p_auto_cutoff_ts           => TO_TIMESTAMP('&&TEST_CUTOFF_TS', 'YYYY-MM-DD HH24:MI:SS'),
      p_auto_retry_sleep_minutes => 1
    );
    DBMS_OUTPUT.PUT_LINE('LOAD_CLIENT_TRANSFERS 2026-04-07 AUTO completed without exception.');
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE(
        'LOAD_CLIENT_TRANSFERS 2026-04-07 AUTO -> ' || SQLCODE || ' ' || SQLERRM
      );
  END;

  BEGIN
    dwh.prc_load_clients(
      p_date                     => DATE '2026-04-07',
      p_run_mode                 => 'AUTO',
      p_auto_cutoff_ts           => TO_TIMESTAMP('&&TEST_CUTOFF_TS', 'YYYY-MM-DD HH24:MI:SS'),
      p_auto_retry_sleep_minutes => 1
    );
    DBMS_OUTPUT.PUT_LINE('LOAD_CLIENTS 2026-04-07 AUTO completed without exception.');
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE(
        'LOAD_CLIENTS 2026-04-07 AUTO -> ' || SQLCODE || ' ' || SQLERRM
      );
  END;
END;
/

SELECT
  process_name,
  TO_CHAR(business_date, 'YYYY-MM-DD') AS business_date,
  run_mode,
  status,
  reason_code,
  retry_count,
  next_retry_ts
FROM dwh.ctl_process_run
WHERE process_name IN ('LOAD_CLIENTS', 'LOAD_CLIENT_TRANSFERS')
  AND business_date = DATE '2026-04-07'
  AND run_mode = 'AUTO'
ORDER BY process_name;
