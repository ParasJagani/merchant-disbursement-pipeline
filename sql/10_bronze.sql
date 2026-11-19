-- Bronze objects. Safe to run repeatedly. Applied by scripts/deploy_sql.py as the admin role (ACCOUNTADMIN on the trial).
-- Placeholders: {db} is the database name.

USE DATABASE {db};
USE SCHEMA BRONZE;

CREATE FILE FORMAT IF NOT EXISTS ndjson_gz
  TYPE = JSON
  COMPRESSION = GZIP;

-- Internal stage. The file writer PUTs files here; the load task reads them.
CREATE STAGE IF NOT EXISTS landing
  FILE_FORMAT = ndjson_gz
  COMMENT = 'Landing zone for gzip NDJSON files, one folder per topic';

-- One append-only table per topic. The whole message is kept as VARIANT, plus where it came from.
-- CHANGE_TRACKING lets streams (reference data) and dynamic tables (events) read these tables.
CREATE TABLE IF NOT EXISTS ref_merchants            (raw VARIANT, source_file STRING, source_row NUMBER, loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()) CHANGE_TRACKING = TRUE;
CREATE TABLE IF NOT EXISTS ref_accounts             (raw VARIANT, source_file STRING, source_row NUMBER, loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()) CHANGE_TRACKING = TRUE;
CREATE TABLE IF NOT EXISTS payments_transactions    (raw VARIANT, source_file STRING, source_row NUMBER, loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()) CHANGE_TRACKING = TRUE;
CREATE TABLE IF NOT EXISTS payments_refunds         (raw VARIANT, source_file STRING, source_row NUMBER, loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()) CHANGE_TRACKING = TRUE;
CREATE TABLE IF NOT EXISTS payments_disputes        (raw VARIANT, source_file STRING, source_row NUMBER, loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()) CHANGE_TRACKING = TRUE;
CREATE TABLE IF NOT EXISTS billing_fees             (raw VARIANT, source_file STRING, source_row NUMBER, loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()) CHANGE_TRACKING = TRUE;
CREATE TABLE IF NOT EXISTS ledger_adjustments       (raw VARIANT, source_file STRING, source_row NUMBER, loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()) CHANGE_TRACKING = TRUE;

-- Roles. The file writer needs to PUT; the task and downstream roles need to read and load.
GRANT READ, WRITE ON STAGE landing TO ROLE LOADER;
GRANT USAGE ON FILE FORMAT ndjson_gz TO ROLE LOADER;
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA BRONZE TO ROLE LOADER;
GRANT INSERT, SELECT ON FUTURE TABLES IN SCHEMA BRONZE TO ROLE LOADER;
GRANT SELECT ON ALL TABLES IN SCHEMA BRONZE TO ROLE TRANSFORMER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA BRONZE TO ROLE TRANSFORMER;
