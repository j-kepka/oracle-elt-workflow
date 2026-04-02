-- Optional time-dependent AUTO checks for 2026-04-07.
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
  '2026-04-07 12:00:00' AS cutoff_ts,
  CASE
    WHEN CAST(SYSTIMESTAMP AS TIMESTAMP) < TIMESTAMP '2026-04-07 12:00:00'
      THEN 'BEFORE_CUTOFF'
    ELSE 'AT_OR_AFTER_CUTOFF'
  END AS cutoff_state
FROM dual;

BEGIN
  BEGIN
    dwh.prc_load_client_transfers(
      p_date     => DATE '2026-04-07',
      p_run_mode => 'AUTO'
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
      p_date     => DATE '2026-04-07',
      p_run_mode => 'AUTO'
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
