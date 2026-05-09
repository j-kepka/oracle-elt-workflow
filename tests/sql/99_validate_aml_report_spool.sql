-- Validates the AML report spool table and outbound file contract.

WHENEVER OSERROR EXIT FAILURE;
WHENEVER SQLERROR EXIT SQL.SQLCODE;

SET SERVEROUTPUT ON;
SET VERIFY OFF;
SET FEEDBACK OFF;
SET LINESIZE 260;
SET PAGESIZE 120;

COLUMN report_id FORMAT A22
COLUMN report_type_candidate FORMAT A20
COLUMN aml_reason_code FORMAT A24
COLUMN aml_reason_details FORMAT A80

DECLARE
  c_data_file_name  CONSTANT VARCHAR2(255 CHAR) := 'aml_report_spool_20260415.csv';
  c_ready_file_name CONSTANT VARCHAR2(255 CHAR) := 'aml_report_spool_20260415.ok';
  c_export_dir      CONSTANT VARCHAR2(30 CHAR) := 'EXT_EXPORT_DIR';
  l_total_checks    NUMBER := 0;
  l_failed_checks   NUMBER := 0;
  l_count           NUMBER := 0;

  PROCEDURE add_check (
    p_check_name IN VARCHAR2,
    p_pass       IN BOOLEAN,
    p_details    IN VARCHAR2
  ) AS
  BEGIN
    l_total_checks := l_total_checks + 1;

    IF NOT p_pass THEN
      l_failed_checks := l_failed_checks + 1;
    END IF;

    DBMS_OUTPUT.PUT_LINE(
      RPAD(p_check_name, 34)
      || CASE WHEN p_pass THEN 'PASS  ' ELSE 'FAIL  ' END
      || p_details
    );
  END add_check;

  FUNCTION file_exists (
    p_filename IN VARCHAR2
  ) RETURN BOOLEAN AS
    l_exists      BOOLEAN;
    l_file_length NUMBER;
    l_block_size  BINARY_INTEGER;
  BEGIN
    UTL_FILE.FGETATTR(
      location    => c_export_dir,
      filename    => p_filename,
      fexists     => l_exists,
      file_length => l_file_length,
      block_size  => l_block_size
    );

    RETURN l_exists;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN FALSE;
  END file_exists;

  FUNCTION first_file_line (
    p_filename IN VARCHAR2
  ) RETURN VARCHAR2 AS
    l_file UTL_FILE.FILE_TYPE;
    l_line VARCHAR2(32767);
  BEGIN
    l_file := UTL_FILE.FOPEN(
      location     => c_export_dir,
      filename     => p_filename,
      open_mode    => 'R',
      max_linesize => 32767
    );
    UTL_FILE.GET_LINE(l_file, l_line);
    UTL_FILE.FCLOSE(l_file);
    RETURN l_line;
  EXCEPTION
    WHEN OTHERS THEN
      BEGIN
        IF UTL_FILE.IS_OPEN(l_file) THEN
          UTL_FILE.FCLOSE(l_file);
        END IF;
      EXCEPTION
        WHEN OTHERS THEN
          NULL;
      END;

      RETURN NULL;
  END first_file_line;

  FUNCTION file_line_count (
    p_filename IN VARCHAR2
  ) RETURN NUMBER AS
    l_file       UTL_FILE.FILE_TYPE;
    l_line       VARCHAR2(32767);
    l_line_count NUMBER := 0;
  BEGIN
    l_file := UTL_FILE.FOPEN(
      location     => c_export_dir,
      filename     => p_filename,
      open_mode    => 'R',
      max_linesize => 32767
    );

    LOOP
      BEGIN
        UTL_FILE.GET_LINE(l_file, l_line);
        l_line_count := l_line_count + 1;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          EXIT;
      END;
    END LOOP;

    UTL_FILE.FCLOSE(l_file);
    RETURN l_line_count;
  EXCEPTION
    WHEN OTHERS THEN
      BEGIN
        IF UTL_FILE.IS_OPEN(l_file) THEN
          UTL_FILE.FCLOSE(l_file);
        END IF;
      EXCEPTION
        WHEN OTHERS THEN
          NULL;
      END;

      RETURN -1;
  END file_line_count;
BEGIN
  DBMS_OUTPUT.PUT_LINE('check_name                        result details');
  DBMS_OUTPUT.PUT_LINE('---------------------------------- ------ ------------------------------------------------------------');

  SELECT COUNT(*)
  INTO l_count
  FROM dwh.ctl_process_run
  WHERE process_name = 'BUILD_AML_REPORT_SPOOL'
    AND business_date = DATE '2026-04-15'
    AND run_mode = 'MANUAL'
    AND status = 'DONE'
    AND reason_code = 'SPOOL_BUILD_DONE'
    AND expected_row_count = 11
    AND stage_row_count = 0
    AND reject_row_count = 0
    AND core_row_count = 11
    AND data_file_name = c_data_file_name
    AND ready_file_name = c_ready_file_name;

  add_check(
    'SPOOL_CTL',
    l_count = 1,
    'expected DONE/SPOOL_BUILD_DONE with 11 spool rows and outbound file names'
  );

  SELECT COUNT(*)
  INTO l_count
  FROM dwh.aml_report_spool
  WHERE business_date = DATE '2026-04-15';

  add_check(
    'SPOOL_ROW_COUNT',
    l_count = 11,
    'expected aml_report_spool rows=11 for 2026-04-15'
  );

  SELECT COUNT(*)
  INTO l_count
  FROM dwh.aml_report_spool
  WHERE business_date = DATE '2026-04-15'
    AND transfer_id = 500101;

  add_check(
    'SPOOL_PUBLICATION_GATE',
    l_count = 0,
    'expected non-review transfer 500101 to stay out of the spool'
  );

  SELECT COUNT(*)
  INTO l_count
  FROM dwh.aml_report_spool
  WHERE business_date = DATE '2026-04-15'
    AND transfer_id = 500107
    AND report_id = 'AML-20260415-500107'
    AND report_type_candidate = 'ART72_THRESHOLD'
    AND report_due_date = DATE '2026-04-22'
    AND above_threshold_art72_flag = 1
    AND suspicion_art74_flag = 0
    AND aml_review_flag = 1
    AND aml_reason_code = 'ART72_THRESHOLD';

  add_check(
    'SPOOL_ART72_THRESHOLD',
    l_count = 1,
    'expected transfer 500107 as ART72_THRESHOLD with seven-day due date'
  );

  SELECT COUNT(*)
  INTO l_count
  FROM dwh.aml_report_spool
  WHERE business_date = DATE '2026-04-15'
    AND transfer_id = 500103
    AND report_type_candidate = 'ART74_SUSPICION'
    AND report_due_date = DATE '2026-04-17'
    AND above_threshold_art72_flag = 0
    AND suspicion_art74_flag = 1
    AND aml_reason_code = 'ART74_PEP'
    AND aml_reason_details LIKE '%pep_flag=1%';

  add_check(
    'SPOOL_ART74_PEP',
    l_count = 1,
    'expected transfer 500103 as ART74_SUSPICION with two-day due date'
  );

  SELECT COUNT(*)
  INTO l_count
  FROM dwh.aml_report_spool
  WHERE business_date = DATE '2026-04-15'
    AND transfer_id = 500113
    AND report_type_candidate = 'ART72_AND_ART74'
    AND report_due_date = DATE '2026-04-17'
    AND above_threshold_art72_flag = 1
    AND suspicion_art74_flag = 1
    AND aml_reason_code = 'ART72_AND_ART74'
    AND aml_reason_details LIKE '%risk_score=910 >= 900%';

  add_check(
    'SPOOL_ART72_AND_ART74',
    l_count = 1,
    'expected transfer 500113 as ART72_AND_ART74 with the earlier two-day due date'
  );

  SELECT COUNT(*)
  INTO l_count
  FROM dwh.aml_report_spool spool
  JOIN dwh.core_clients cli
    ON cli.business_date = spool.business_date
   AND cli.client_id = spool.client_id
  WHERE spool.business_date = DATE '2026-04-15'
    AND spool.transfer_id = 500113
    AND spool.full_name = cli.full_name
    AND spool.registration_no = cli.registration_no
    AND spool.tax_id = cli.tax_id
    AND spool.address_line_1 = cli.address_line_1
    AND spool.relationship_purpose_code = cli.relationship_purpose_code
    AND spool.source_of_funds_declared = cli.source_of_funds_declared
    AND spool.source_of_wealth_declared = cli.source_of_wealth_declared;

  add_check(
    'SPOOL_CLIENT_FIELDS',
    l_count = 1,
    'expected spool to carry export-oriented client fields from the same-day snapshot'
  );

  add_check(
    'SPOOL_CSV_EXISTS',
    file_exists(c_data_file_name),
    'expected outbound CSV file ' || c_data_file_name
  );

  add_check(
    'SPOOL_OK_EXISTS',
    file_exists(c_ready_file_name),
    'expected outbound ready file ' || c_ready_file_name
  );

  add_check(
    'SPOOL_OK_CONTENT',
    first_file_line(c_ready_file_name) = '11',
    'expected ready file to contain the data row count only'
  );

  add_check(
    'SPOOL_CSV_LINE_COUNT',
    file_line_count(c_data_file_name) = 12,
    'expected CSV header plus 11 data rows'
  );

  DBMS_OUTPUT.PUT_LINE('---------------------------------- ------ ------------------------------------------------------------');
  DBMS_OUTPUT.PUT_LINE(
    RPAD('SUMMARY', 34)
    || CASE WHEN l_failed_checks = 0 THEN 'PASS  ' ELSE 'FAIL  ' END
    || 'total_checks='
    || l_total_checks
    || '; failed_checks='
    || l_failed_checks
  );

  IF l_failed_checks > 0 THEN
    RAISE_APPLICATION_ERROR(
      -20994,
      'AML report spool validation failed.'
    );
  END IF;
END;
/

PROMPT
PROMPT AML report spool snapshot:

SELECT
  report_id,
  transfer_id,
  client_id,
  report_type_candidate,
  TO_CHAR(report_due_date, 'YYYY-MM-DD') AS report_due_date,
  amount_eur,
  aml_reason_code,
  aml_reason_details
FROM dwh.aml_report_spool
WHERE business_date = DATE '2026-04-15'
ORDER BY report_id;
