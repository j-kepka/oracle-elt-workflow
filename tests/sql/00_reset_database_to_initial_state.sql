-- Reset project-specific database objects to the pre-bootstrap state.
-- Run in the target PDB as SYSTEM or another DBA-capable account.

SET SERVEROUTPUT ON;

DECLARE
  l_user_exists      NUMBER := 0;
  l_sessions_killed NUMBER := 0;
BEGIN
  SELECT COUNT(*)
  INTO l_user_exists
  FROM dba_users
  WHERE username = 'DWH';

  IF l_user_exists = 1 THEN
    EXECUTE IMMEDIATE 'ALTER USER dwh ACCOUNT LOCK';
    DBMS_OUTPUT.PUT_LINE('Locked user DWH before session cleanup.');
  END IF;

  FOR rec IN (
    SELECT sid, serial#
    FROM v$session
    WHERE username = 'DWH'
      AND type <> 'BACKGROUND'
  ) LOOP
    EXECUTE IMMEDIATE
      'ALTER SYSTEM KILL SESSION '''
      || rec.sid
      || ','
      || rec.serial#
      || ''' IMMEDIATE';
    l_sessions_killed := l_sessions_killed + 1;
  END LOOP;

  IF l_sessions_killed > 0 THEN
    DBMS_OUTPUT.PUT_LINE('Killed ' || l_sessions_killed || ' active DWH session(s).');
  ELSE
    DBMS_OUTPUT.PUT_LINE('No active DWH sessions found.');
  END IF;

  IF l_sessions_killed > 0 THEN
    DBMS_LOCK.SLEEP(1);
  END IF;
END;
/

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
