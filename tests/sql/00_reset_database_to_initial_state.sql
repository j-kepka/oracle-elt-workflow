-- Reset project-specific database objects to the pre-bootstrap state.
-- Run in the target PDB as SYSTEM or another DBA-capable account.

SET SERVEROUTPUT ON;

DECLARE
  l_user_exists NUMBER := 0;
BEGIN
  SELECT COUNT(*)
  INTO l_user_exists
  FROM dba_users
  WHERE username = 'DWH';

  IF l_user_exists = 1 THEN
    EXECUTE IMMEDIATE 'DROP USER dwh CASCADE';
    DBMS_OUTPUT.PUT_LINE('Dropped user DWH.');
  ELSE
    DBMS_OUTPUT.PUT_LINE('User DWH does not exist. Nothing to drop.');
  END IF;
END;
/

DECLARE
  l_dir_exists NUMBER := 0;
BEGIN
  SELECT COUNT(*)
  INTO l_dir_exists
  FROM dba_directories
  WHERE directory_name = 'EXT_WORK_DIR';

  IF l_dir_exists = 1 THEN
    EXECUTE IMMEDIATE 'DROP DIRECTORY EXT_WORK_DIR';
    DBMS_OUTPUT.PUT_LINE('Dropped directory EXT_WORK_DIR.');
  ELSE
    DBMS_OUTPUT.PUT_LINE('Directory EXT_WORK_DIR does not exist. Nothing to drop.');
  END IF;
END;
/

DECLARE
  l_dir_exists NUMBER := 0;
BEGIN
  SELECT COUNT(*)
  INTO l_dir_exists
  FROM dba_directories
  WHERE directory_name = 'EXT_DIR';

  IF l_dir_exists = 1 THEN
    EXECUTE IMMEDIATE 'DROP DIRECTORY EXT_DIR';
    DBMS_OUTPUT.PUT_LINE('Dropped directory EXT_DIR.');
  ELSE
    DBMS_OUTPUT.PUT_LINE('Directory EXT_DIR does not exist. Nothing to drop.');
  END IF;
END;
/
