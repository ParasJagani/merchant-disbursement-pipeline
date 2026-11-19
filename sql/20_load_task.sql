-- Load procedure and scheduled task. Safe to run repeatedly. Applied as the admin role (ACCOUNTADMIN on the trial).
-- COPY INTO remembers which files it has loaded (by path and name), so re-staging a file with the
-- same name, or running the task twice, loads nothing new. That is the "replay loads once" rule.
-- {db} is the database name.

USE DATABASE {db};

CREATE OR REPLACE PROCEDURE {db}.OPS.LOAD_BRONZE()
  RETURNS STRING
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
BEGIN
    COPY INTO {db}.BRONZE.ref_merchants (raw, source_file, source_row)
      FROM (SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER FROM @{db}.BRONZE.LANDING/ref_merchants/)
      PATTERN = '.*[.]ndjson[.]gz'
      ON_ERROR = SKIP_FILE;
    COPY INTO {db}.BRONZE.ref_accounts (raw, source_file, source_row)
      FROM (SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER FROM @{db}.BRONZE.LANDING/ref_accounts/)
      PATTERN = '.*[.]ndjson[.]gz'
      ON_ERROR = SKIP_FILE;
    COPY INTO {db}.BRONZE.payments_transactions (raw, source_file, source_row)
      FROM (SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER FROM @{db}.BRONZE.LANDING/payments_transactions/)
      PATTERN = '.*[.]ndjson[.]gz'
      ON_ERROR = SKIP_FILE;
    COPY INTO {db}.BRONZE.payments_refunds (raw, source_file, source_row)
      FROM (SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER FROM @{db}.BRONZE.LANDING/payments_refunds/)
      PATTERN = '.*[.]ndjson[.]gz'
      ON_ERROR = SKIP_FILE;
    COPY INTO {db}.BRONZE.payments_disputes (raw, source_file, source_row)
      FROM (SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER FROM @{db}.BRONZE.LANDING/payments_disputes/)
      PATTERN = '.*[.]ndjson[.]gz'
      ON_ERROR = SKIP_FILE;
    COPY INTO {db}.BRONZE.billing_fees (raw, source_file, source_row)
      FROM (SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER FROM @{db}.BRONZE.LANDING/billing_fees/)
      PATTERN = '.*[.]ndjson[.]gz'
      ON_ERROR = SKIP_FILE;
    COPY INTO {db}.BRONZE.ledger_adjustments (raw, source_file, source_row)
      FROM (SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER FROM @{db}.BRONZE.LANDING/ledger_adjustments/)
      PATTERN = '.*[.]ndjson[.]gz'
      ON_ERROR = SKIP_FILE;
  RETURN 'ok';
END;
$$;

-- Created suspended. Demo mode resumes it (make demo-up) and suspends it again (make demo-down).
CREATE OR REPLACE TASK {db}.OPS.LOAD_BRONZE_TASK
  WAREHOUSE = INGEST_WH
  SCHEDULE = '5 MINUTE'
AS
  CALL {db}.OPS.LOAD_BRONZE();
