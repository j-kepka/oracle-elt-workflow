-- Deterministic MANUAL smoke runner.
-- The execution list is kept in a local matrix-style block and includes
-- the rerun steps used for idempotency coverage.

WHENEVER OSERROR EXIT FAILURE;
WHENEVER SQLERROR EXIT SQL.SQLCODE;

SET SERVEROUTPUT ON;
SET VERIFY OFF;
SET FEEDBACK OFF;
SET LINESIZE 220;
SET PAGESIZE 100;

PROMPT Running deterministic MANUAL smoke cases...

DECLARE
  TYPE t_case_rec IS RECORD (
    run_order         PLS_INTEGER,
    case_key          VARCHAR2(64 CHAR),
    process_name      VARCHAR2(100 CHAR),
    business_date     DATE,
    run_mode          VARCHAR2(10 CHAR),
    expected_sqlcode  NUMBER
  );

  TYPE t_case_tab IS TABLE OF t_case_rec;
  TYPE t_seen_map IS TABLE OF PLS_INTEGER INDEX BY VARCHAR2(128 CHAR);

  l_cases           t_case_tab;
  l_transfer_dates  t_seen_map;
  l_client_dates    t_seen_map;
  l_ctl_keys        t_seen_map;
  l_key             VARCHAR2(128 CHAR);
  l_process_name    VARCHAR2(100 CHAR);
  l_business_date   DATE;

  PROCEDURE execute_case (
    p_run_order        IN PLS_INTEGER,
    p_case_key         IN VARCHAR2,
    p_process_name     IN VARCHAR2,
    p_business_date    IN DATE,
    p_run_mode         IN VARCHAR2,
    p_expected_sqlcode IN NUMBER
  ) IS
  BEGIN
    BEGIN
      IF p_process_name = 'LOAD_CLIENTS' THEN
        dwh.prc_load_clients(
          p_date     => p_business_date,
          p_run_mode => p_run_mode
        );
      ELSIF p_process_name = 'LOAD_CLIENT_TRANSFERS' THEN
        dwh.prc_load_client_transfers(
          p_date     => p_business_date,
          p_run_mode => p_run_mode
        );
      ELSE
        RAISE_APPLICATION_ERROR(
          -20996,
          'Unsupported process_name ' || p_process_name || ' for case ' || p_case_key || '.'
        );
      END IF;

      IF p_expected_sqlcode IS NOT NULL THEN
        RAISE_APPLICATION_ERROR(
          -20995,
          'Expected SQLCODE ' || p_expected_sqlcode || ' was not raised for ' || p_case_key || '.'
        );
      END IF;

      DBMS_OUTPUT.PUT_LINE(
        LPAD(TO_CHAR(p_run_order), 3, '0')
        || ' '
        || RPAD(p_case_key, 36)
        || ' OK'
      );
    EXCEPTION
      WHEN OTHERS THEN
        IF p_expected_sqlcode IS NOT NULL AND SQLCODE = p_expected_sqlcode THEN
          DBMS_OUTPUT.PUT_LINE(
            LPAD(TO_CHAR(p_run_order), 3, '0')
            || ' '
            || RPAD(p_case_key, 36)
            || ' OK expected '
            || SQLCODE
          );
        ELSE
          DBMS_OUTPUT.PUT_LINE(
            LPAD(TO_CHAR(p_run_order), 3, '0')
            || ' '
            || RPAD(p_case_key, 36)
            || ' FAIL '
            || SQLCODE
            || ' '
            || SUBSTR(SQLERRM, 1, 160)
          );
          RAISE;
        END IF;
    END;
  END execute_case;
BEGIN
  SELECT
    run_order,
    case_key,
    process_name,
    business_date,
    run_mode,
    expected_sqlcode
  BULK COLLECT INTO l_cases
  FROM (
    WITH run_cases AS (
      SELECT 10 AS run_order, 'clients_2026-03-24_manual' AS case_key, 'LOAD_CLIENTS' AS process_name, DATE '2026-03-24' AS business_date, 'MANUAL' AS run_mode, -20114 AS expected_sqlcode FROM dual
      UNION ALL
      SELECT 20, 'transfers_2026-03-24_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-03-24', 'MANUAL', -20014 FROM dual
      UNION ALL
      SELECT 30, 'clients_2026-03-25_manual', 'LOAD_CLIENTS', DATE '2026-03-25', 'MANUAL', CAST(NULL AS NUMBER) FROM dual
      UNION ALL
      SELECT 40, 'transfers_2026-03-25_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-03-25', 'MANUAL', CAST(NULL AS NUMBER) FROM dual
      UNION ALL
      SELECT 50, 'clients_2026-03-25_manual_rerun', 'LOAD_CLIENTS', DATE '2026-03-25', 'MANUAL', CAST(NULL AS NUMBER) FROM dual
      UNION ALL
      SELECT 60, 'transfers_2026-03-25_manual_rerun', 'LOAD_CLIENT_TRANSFERS', DATE '2026-03-25', 'MANUAL', CAST(NULL AS NUMBER) FROM dual
      UNION ALL
      SELECT 70, 'clients_2026-03-26_manual', 'LOAD_CLIENTS', DATE '2026-03-26', 'MANUAL', CAST(NULL AS NUMBER) FROM dual
      UNION ALL
      SELECT 80, 'transfers_2026-03-26_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-03-26', 'MANUAL', CAST(NULL AS NUMBER) FROM dual
      UNION ALL
      SELECT 90, 'clients_2026-03-26_manual_rerun', 'LOAD_CLIENTS', DATE '2026-03-26', 'MANUAL', CAST(NULL AS NUMBER) FROM dual
      UNION ALL
      SELECT 100, 'transfers_2026-03-26_manual_rerun', 'LOAD_CLIENT_TRANSFERS', DATE '2026-03-26', 'MANUAL', CAST(NULL AS NUMBER) FROM dual
      UNION ALL
      SELECT 110, 'clients_2026-03-27_manual', 'LOAD_CLIENTS', DATE '2026-03-27', 'MANUAL', -20117 FROM dual
      UNION ALL
      SELECT 120, 'transfers_2026-03-27_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-03-27', 'MANUAL', -20015 FROM dual
      UNION ALL
      SELECT 130, 'clients_2026-03-28_manual', 'LOAD_CLIENTS', DATE '2026-03-28', 'MANUAL', CAST(NULL AS NUMBER) FROM dual
      UNION ALL
      SELECT 140, 'transfers_2026-03-28_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-03-28', 'MANUAL', CAST(NULL AS NUMBER) FROM dual
      UNION ALL
      SELECT 150, 'clients_2026-03-29_manual', 'LOAD_CLIENTS', DATE '2026-03-29', 'MANUAL', CAST(NULL AS NUMBER) FROM dual
      UNION ALL
      SELECT 160, 'clients_2026-04-08_manual', 'LOAD_CLIENTS', DATE '2026-04-08', 'MANUAL', -20110 FROM dual
      UNION ALL
      SELECT 170, 'transfers_2026-04-08_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-04-08', 'MANUAL', -20010 FROM dual
      UNION ALL
      SELECT 180, 'clients_2026-04-09_manual', 'LOAD_CLIENTS', DATE '2026-04-09', 'MANUAL', CAST(NULL AS NUMBER) FROM dual
      UNION ALL
      SELECT 190, 'clients_2026-04-10_manual', 'LOAD_CLIENTS', DATE '2026-04-10', 'MANUAL', CAST(NULL AS NUMBER) FROM dual
      UNION ALL
      SELECT 200, 'transfers_2026-04-10_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-04-10', 'MANUAL', CAST(NULL AS NUMBER) FROM dual
      UNION ALL
      SELECT 210, 'transfers_2026-04-11_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-04-11', 'MANUAL', -20018 FROM dual
      UNION ALL
      SELECT 220, 'transfers_2026-04-12_manual', 'LOAD_CLIENT_TRANSFERS', DATE '2026-04-12', 'MANUAL', CAST(NULL AS NUMBER) FROM dual
    )
    SELECT
      run_order,
      case_key,
      process_name,
      business_date,
      run_mode,
      expected_sqlcode
    FROM run_cases
    ORDER BY run_order
  );

  FOR i IN 1 .. l_cases.COUNT LOOP
    IF l_cases(i).process_name = 'LOAD_CLIENT_TRANSFERS' THEN
      l_transfer_dates(TO_CHAR(l_cases(i).business_date, 'YYYY-MM-DD')) := 1;
      -- Transfer cases depend on a same-day client snapshot. Clean the parent date too
      -- so dependency-negative smoke cases remain deterministic on a reused sandbox.
      l_client_dates(TO_CHAR(l_cases(i).business_date, 'YYYY-MM-DD')) := 1;
      l_ctl_keys('LOAD_CLIENTS|' || TO_CHAR(l_cases(i).business_date, 'YYYY-MM-DD')) := 1;
    ELSIF l_cases(i).process_name = 'LOAD_CLIENTS' THEN
      l_client_dates(TO_CHAR(l_cases(i).business_date, 'YYYY-MM-DD')) := 1;
    END IF;

    l_ctl_keys(
      l_cases(i).process_name || '|' || TO_CHAR(l_cases(i).business_date, 'YYYY-MM-DD')
    ) := 1;
  END LOOP;

  l_key := l_transfer_dates.FIRST;
  WHILE l_key IS NOT NULL LOOP
    l_business_date := TO_DATE(l_key, 'YYYY-MM-DD');

    DELETE FROM dwh.core_client_transfers
    WHERE business_date = l_business_date;

    DELETE FROM dwh.stg_client_transfers_reject
    WHERE business_date = l_business_date;

    DELETE FROM dwh.stg_client_transfers
    WHERE business_date = l_business_date;

    l_key := l_transfer_dates.NEXT(l_key);
  END LOOP;

  l_key := l_client_dates.FIRST;
  WHILE l_key IS NOT NULL LOOP
    l_business_date := TO_DATE(l_key, 'YYYY-MM-DD');

    DELETE FROM dwh.core_clients
    WHERE business_date = l_business_date;

    DELETE FROM dwh.stg_clients_reject
    WHERE business_date = l_business_date;

    DELETE FROM dwh.stg_clients
    WHERE business_date = l_business_date;

    l_key := l_client_dates.NEXT(l_key);
  END LOOP;

  l_key := l_ctl_keys.FIRST;
  WHILE l_key IS NOT NULL LOOP
    l_process_name := SUBSTR(l_key, 1, INSTR(l_key, '|') - 1);
    l_business_date := TO_DATE(SUBSTR(l_key, INSTR(l_key, '|') + 1), 'YYYY-MM-DD');

    DELETE FROM dwh.ctl_process_run
    WHERE process_name = l_process_name
      AND business_date = l_business_date;

    l_key := l_ctl_keys.NEXT(l_key);
  END LOOP;

  COMMIT;

  FOR i IN 1 .. l_cases.COUNT LOOP
    execute_case(
      p_run_order        => l_cases(i).run_order,
      p_case_key         => l_cases(i).case_key,
      p_process_name     => l_cases(i).process_name,
      p_business_date    => l_cases(i).business_date,
      p_run_mode         => l_cases(i).run_mode,
      p_expected_sqlcode => l_cases(i).expected_sqlcode
    );
  END LOOP;

  DBMS_OUTPUT.PUT_LINE('Deterministic MANUAL smoke execution completed.');
END;
/
