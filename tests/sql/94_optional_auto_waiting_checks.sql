-- Optional AUTO checks for 2026-04-07 with a test-specific dynamic cutoff.
-- The production/default AUTO cutoff is based on the current run day, not on business_date.
-- This helper gives each loader its own "now + 5 minutes" cutoff and retries every 1 minute.
-- That keeps the WAITING path observable for both loaders without relying on 12:00.
-- The helper takes about 10 minutes because the two AUTO windows run sequentially.
-- Not included in the deterministic compare matrix.

WHENEVER OSERROR EXIT FAILURE;
WHENEVER SQLERROR EXIT SQL.SQLCODE;

SET SERVEROUTPUT ON;
SET VERIFY OFF;
SET FEEDBACK OFF;
SET LINESIZE 220;
SET PAGESIZE 100;

PROMPT Running optional AUTO checks for 2026-04-07...

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
  'per-loader now + 5 minutes' AS test_cutoff,
  1 AS retry_sleep_minutes,
  5 AS expected_retry_count
FROM dual;

DECLARE
  c_business_date       CONSTANT DATE := DATE '2026-04-07';
  c_wait_window_minutes CONSTANT PLS_INTEGER := 5;
  c_retry_sleep_minutes CONSTANT PLS_INTEGER := 1;

  l_cutoff_ts TIMESTAMP;

  FUNCTION next_cutoff_ts RETURN TIMESTAMP AS
  BEGIN
    RETURN CAST(SYSTIMESTAMP AS TIMESTAMP)
      + NUMTODSINTERVAL(c_wait_window_minutes, 'MINUTE');
  END next_cutoff_ts;

  PROCEDURE print_window (
    p_process_name IN VARCHAR2,
    p_cutoff_ts    IN TIMESTAMP
  ) AS
  BEGIN
    DBMS_OUTPUT.PUT_LINE(
      p_process_name
      || ' AUTO window: db_now='
      || TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS TZH:TZM')
      || ', cutoff='
      || TO_CHAR(p_cutoff_ts, 'YYYY-MM-DD HH24:MI:SS')
      || ', retry_sleep_minutes='
      || c_retry_sleep_minutes
    );
  END print_window;
BEGIN
  l_cutoff_ts := next_cutoff_ts;
  print_window('LOAD_CLIENTS', l_cutoff_ts);

  BEGIN
    dwh.prc_load_clients(
      p_date                     => c_business_date,
      p_run_mode                 => 'AUTO',
      p_auto_cutoff_ts           => l_cutoff_ts,
      p_auto_retry_sleep_minutes => c_retry_sleep_minutes
    );
    DBMS_OUTPUT.PUT_LINE('LOAD_CLIENTS 2026-04-07 AUTO completed without exception.');
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE(
        'LOAD_CLIENTS 2026-04-07 AUTO -> ' || SQLCODE || ' ' || SQLERRM
      );
  END;

  l_cutoff_ts := next_cutoff_ts;
  print_window('LOAD_CLIENT_TRANSFERS', l_cutoff_ts);

  BEGIN
    dwh.prc_load_client_transfers(
      p_date                     => c_business_date,
      p_run_mode                 => 'AUTO',
      p_auto_cutoff_ts           => l_cutoff_ts,
      p_auto_retry_sleep_minutes => c_retry_sleep_minutes
    );
    DBMS_OUTPUT.PUT_LINE('LOAD_CLIENT_TRANSFERS 2026-04-07 AUTO completed without exception.');
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE(
        'LOAD_CLIENT_TRANSFERS 2026-04-07 AUTO -> ' || SQLCODE || ' ' || SQLERRM
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

DECLARE
  l_mismatch_count NUMBER;
BEGIN
  SELECT COUNT(*)
  INTO l_mismatch_count
  FROM (
    SELECT 'LOAD_CLIENTS' AS process_name FROM dual
    UNION ALL
    SELECT 'LOAD_CLIENT_TRANSFERS' AS process_name FROM dual
  ) expected
  LEFT JOIN dwh.ctl_process_run actual
    ON actual.process_name = expected.process_name
   AND actual.business_date = DATE '2026-04-07'
   AND actual.run_mode = 'AUTO'
  WHERE actual.process_name IS NULL
     OR NVL(actual.status, '<NULL>') <> 'FAILED'
     OR NVL(actual.reason_code, '<NULL>') <> 'MISSING_OK_AFTER_CUTOFF'
     OR NVL(actual.retry_count, -1) <> 5;

  IF l_mismatch_count > 0 THEN
    RAISE_APPLICATION_ERROR(
      -20994,
      'Optional AUTO waiting checks failed: expected both loaders to fail after 5 retries.'
    );
  END IF;

  DBMS_OUTPUT.PUT_LINE('Optional AUTO waiting checks passed: both loaders failed after 5 retries.');
END;
/
