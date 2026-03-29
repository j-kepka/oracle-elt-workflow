-- Bootstrap the current project schema after the reset step.
-- Run in the target PDB as SYSTEM or another DBA-capable account.
-- Expected SQL*Plus/SQLcl variable:
--   DEFINE DWH_PASSWORD = '<DWH_PASSWORD>'
-- Optional overrides:
--   DEFINE WORKSPACE_ROOT = '/workspace'
--   DEFINE PDB_CONNECT_STRING = 'localhost:1521/FREEPDB1'
--   DEFINE EXT_DIR_PATH = '/opt/oracle/extdata'
--   DEFINE EXT_WORK_DIR_PATH = '/opt/oracle/extdata/work'

WHENEVER OSERROR EXIT FAILURE;
WHENEVER SQLERROR EXIT SQL.SQLCODE;

SET ECHO ON;
SET VERIFY OFF;
SET SERVEROUTPUT ON;

DEFINE WORKSPACE_ROOT = '/workspace'
DEFINE PDB_CONNECT_STRING = 'localhost:1521/FREEPDB1'
DEFINE EXT_DIR_PATH = '/opt/oracle/extdata'
DEFINE EXT_WORK_DIR_PATH = '/opt/oracle/extdata/work'

PROMPT Creating schema user DWH...
CREATE USER dwh IDENTIFIED BY "&&DWH_PASSWORD";
GRANT CREATE SESSION TO dwh;
GRANT CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE PROCEDURE TO dwh;
GRANT CREATE JOB TO dwh;
GRANT EXECUTE ON SYS.UTL_FILE TO dwh;
ALTER USER dwh QUOTA 500M ON USERS;

PROMPT Creating Oracle DIRECTORY objects...
CREATE OR REPLACE DIRECTORY EXT_DIR AS '&&EXT_DIR_PATH';
CREATE OR REPLACE DIRECTORY EXT_WORK_DIR AS '&&EXT_WORK_DIR_PATH';
GRANT READ ON DIRECTORY EXT_DIR TO dwh;
GRANT READ, WRITE ON DIRECTORY EXT_WORK_DIR TO dwh;

PROMPT Connecting as DWH and creating project objects...
CONNECT dwh/"&&DWH_PASSWORD"@&&PDB_CONNECT_STRING

@&&WORKSPACE_ROOT/sql/01_create_stage_client_transfers.sql
@&&WORKSPACE_ROOT/sql/02_create_external_client_transfers.sql
@&&WORKSPACE_ROOT/sql/06_create_core_client_transfers.sql
@&&WORKSPACE_ROOT/sql/10_create_control_structures.sql
@&&WORKSPACE_ROOT/sql/04_create_load_procedure.sql
@&&WORKSPACE_ROOT/sql/05_create_scheduler_job.sql

WHENEVER OSERROR CONTINUE NONE;
WHENEVER SQLERROR CONTINUE NONE;

PROMPT Bootstrap completed.
