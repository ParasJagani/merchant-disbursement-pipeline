-- Phase 6: health signals, alert log and one scheduled alert. Safe to run repeatedly. Applied as the admin role.
--
-- How it fits together:
--   OPS.alert_rules      one row per signal: severity, threshold, window and an on/off switch (edit these, not the SQL)
--   OPS.health_signals   a view that measures every signal now and says which ones are firing
--   OPS.alert_log        one row per incident: OPEN while the signal fires, RESOLVED when it clears
--   OPS.RAISE_ALERTS()   opens new incidents, refreshes open ones, resolves cleared ones, then sends emails
--   OPS.HEALTH_ALERT     the scheduled Snowflake ALERT that calls RAISE_ALERTS() when something needs attention
-- {db} is the database name.

-- ---- configuration ----
CREATE TABLE IF NOT EXISTS {db}.OPS.alert_rules (
  signal STRING,
  severity STRING,
  threshold NUMBER,       -- the signal fires when the measured value is ABOVE this
  window_minutes NUMBER,  -- look-back window for data signals (platform signals always look back 60 minutes)
  enabled BOOLEAN,
  description STRING
);

-- Inserts only the rules that are missing, so a threshold you changed is never overwritten by a deploy.
INSERT INTO {db}.OPS.alert_rules (signal, severity, threshold, window_minutes, enabled, description)
SELECT s.signal, s.severity, s.threshold, s.window_minutes, s.enabled, s.description
FROM (
  SELECT 'quarantine_recent' AS signal, 'warning' AS severity, 0 AS threshold, 60 AS window_minutes, TRUE AS enabled,
         'messages silver rejected into quarantine in the window; run SELECT * FROM QUARANTINE.reject_summary' AS description
  UNION ALL
  SELECT 'reconciliation_gap', 'critical', 0, NULL, TRUE,
         'rows or entities that are in bronze but not accounted for in silver; run SELECT * FROM OPS.silver_reconciliation'
  UNION ALL
  SELECT 'task_failures_recent', 'critical', 0, 60, TRUE,
         'failed task runs in the last 60 minutes; run SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) WHERE state = ''FAILED'''
  UNION ALL
  SELECT 'dynamic_table_failures_recent', 'critical', 0, 60, TRUE,
         'failed dynamic table refreshes in the last 60 minutes; run SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY()) WHERE state = ''FAILED'''
  UNION ALL
  -- Off by default: the demo data is loaded once, so it would be "stale" all the time. Turn it on when the live feed runs.
  SELECT 'bronze_stale_minutes', 'warning', 180, NULL, FALSE,
         'minutes since the newest bronze row was loaded; enable it when the live feed is running'
) s
WHERE s.signal NOT IN (SELECT signal FROM {db}.OPS.alert_rules);

-- Where emails go. Written by `python -m ops.setup_alerts`; nothing here is committed to git.
CREATE TABLE IF NOT EXISTS {db}.OPS.alert_settings (
  setting STRING,
  setting_value STRING
);

-- Measured by RAISE_ALERTS(), because the INFORMATION_SCHEMA history functions cannot be read from a view.
CREATE TABLE IF NOT EXISTS {db}.OPS.platform_signals (
  signal STRING,
  observed NUMBER,
  measured_at TIMESTAMP_LTZ
);

-- One row per incident. A signal has at most one OPEN row.
CREATE TABLE IF NOT EXISTS {db}.OPS.alert_log (
  alert_id STRING,
  signal STRING,
  severity STRING,
  status STRING,          -- OPEN or RESOLVED
  opened_at TIMESTAMP_LTZ,
  last_seen_at TIMESTAMP_LTZ,
  resolved_at TIMESTAMP_LTZ,
  observed NUMBER,
  threshold NUMBER,
  detail STRING,
  email_status STRING     -- NULL (not tried yet), sent, not configured, or failed: <reason>
);

-- ---- measuring ----
CREATE OR REPLACE VIEW {db}.OPS.health_signals AS
WITH measures AS (
  SELECT 'quarantine_recent' AS signal, COUNT(*) AS observed
  FROM (
    SELECT loaded_at FROM {db}.QUARANTINE.rejected_merchants
    UNION ALL SELECT loaded_at FROM {db}.QUARANTINE.rejected_accounts
    UNION ALL SELECT loaded_at FROM {db}.QUARANTINE.rejected_transactions
    UNION ALL SELECT loaded_at FROM {db}.QUARANTINE.rejected_refunds
    UNION ALL SELECT loaded_at FROM {db}.QUARANTINE.rejected_disputes
    UNION ALL SELECT loaded_at FROM {db}.QUARANTINE.rejected_fees
    UNION ALL SELECT loaded_at FROM {db}.QUARANTINE.rejected_adjustments
  )
  WHERE loaded_at >= DATEADD('minute', -(SELECT MAX(window_minutes) FROM {db}.OPS.alert_rules WHERE signal = 'quarantine_recent'), CURRENT_TIMESTAMP())
  UNION ALL
  SELECT 'reconciliation_gap',
         COALESCE(SUM(ABS(unaccounted_rows)), 0) + COALESCE(SUM(ABS(missing_entities)), 0)
  FROM {db}.OPS.silver_reconciliation
  UNION ALL
  SELECT 'bronze_stale_minutes', DATEDIFF('minute', MAX(loaded_at), CURRENT_TIMESTAMP())
  FROM (
    SELECT loaded_at FROM {db}.BRONZE.ref_merchants
    UNION ALL SELECT loaded_at FROM {db}.BRONZE.ref_accounts
    UNION ALL SELECT loaded_at FROM {db}.BRONZE.payments_transactions
    UNION ALL SELECT loaded_at FROM {db}.BRONZE.payments_refunds
    UNION ALL SELECT loaded_at FROM {db}.BRONZE.payments_disputes
    UNION ALL SELECT loaded_at FROM {db}.BRONZE.billing_fees
    UNION ALL SELECT loaded_at FROM {db}.BRONZE.ledger_adjustments
  )
  UNION ALL
  SELECT signal, observed FROM {db}.OPS.platform_signals
)
SELECT r.signal,
       r.severity,
       m.observed,
       r.threshold,
       r.window_minutes,
       r.enabled,
       COALESCE(r.enabled AND m.observed > r.threshold, FALSE) AS firing,
       r.signal || ' is ' || COALESCE(m.observed::STRING, 'unknown') || ' (alerts above ' || r.threshold::STRING || '): ' || r.description AS detail
FROM {db}.OPS.alert_rules r
LEFT JOIN measures m ON m.signal = r.signal;

-- ---- raising ----
-- One transaction for the log, so a half-updated log is never visible. Emails go after the commit and
-- never fail the run: the log is the record, the email is a convenience.
CREATE OR REPLACE PROCEDURE {db}.OPS.RAISE_ALERTS()
  RETURNS STRING
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
BEGIN
  BEGIN TRANSACTION;

  DELETE FROM {db}.OPS.platform_signals;
  INSERT INTO {db}.OPS.platform_signals (signal, observed, measured_at)
  SELECT 'task_failures_recent', COUNT(*), CURRENT_TIMESTAMP()
  FROM TABLE({db}.INFORMATION_SCHEMA.TASK_HISTORY(SCHEDULED_TIME_RANGE_START => DATEADD('minute', -60, CURRENT_TIMESTAMP()), RESULT_LIMIT => 1000))
  WHERE state = 'FAILED'
  UNION ALL
  SELECT 'dynamic_table_failures_recent', COUNT(*), CURRENT_TIMESTAMP()
  FROM TABLE({db}.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(DATA_TIMESTAMP_START => DATEADD('minute', -60, CURRENT_TIMESTAMP())))
  WHERE state = 'FAILED';

  INSERT INTO {db}.OPS.alert_log (alert_id, signal, severity, status, opened_at, last_seen_at, observed, threshold, detail)
  SELECT UUID_STRING(), s.signal, s.severity, 'OPEN', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), s.observed, s.threshold, s.detail
  FROM {db}.OPS.health_signals s
  WHERE s.firing
    AND NOT EXISTS (SELECT 1 FROM {db}.OPS.alert_log a WHERE a.signal = s.signal AND a.status = 'OPEN');

  UPDATE {db}.OPS.alert_log a
  SET last_seen_at = CURRENT_TIMESTAMP(), observed = s.observed, detail = s.detail
  FROM {db}.OPS.health_signals s
  WHERE a.signal = s.signal AND a.status = 'OPEN' AND s.firing;

  UPDATE {db}.OPS.alert_log
  SET status = 'RESOLVED', resolved_at = CURRENT_TIMESTAMP()
  WHERE status = 'OPEN'
    AND signal NOT IN (SELECT signal FROM {db}.OPS.health_signals WHERE firing);

  COMMIT;

  CALL {db}.OPS.SEND_ALERT_EMAILS();
  RETURN 'ok';
EXCEPTION
  WHEN OTHER THEN
    ROLLBACK;
    RAISE;
END;
$$;

-- Sends one email per new incident. If SYSTEM$SEND_EMAIL fails (integration missing, address not verified)
-- the reason is written to alert_log.email_status and the run carries on.
CREATE OR REPLACE PROCEDURE {db}.OPS.SEND_ALERT_EMAILS()
  RETURNS STRING
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  pending CURSOR FOR
    SELECT alert_id, signal, severity, detail
    FROM {db}.OPS.alert_log
    WHERE status = 'OPEN' AND email_status IS NULL;
  recipient STRING;
  integration STRING;
  incident_id STRING;
  subject STRING;
  body STRING;
  problem STRING;
  sent NUMBER DEFAULT 0;
BEGIN
  SELECT MAX(setting_value) INTO :recipient FROM {db}.OPS.alert_settings WHERE setting = 'email_recipient';
  SELECT MAX(setting_value) INTO :integration FROM {db}.OPS.alert_settings WHERE setting = 'email_integration';
  FOR r IN pending DO
    incident_id := r.alert_id;
    IF (recipient IS NULL OR integration IS NULL) THEN
      UPDATE {db}.OPS.alert_log SET email_status = 'not configured' WHERE alert_id = :incident_id;
    ELSE
      subject := '[' || UPPER(r.severity) || '] merchant pipeline: ' || r.signal;
      body := r.detail || ' Open incidents: SELECT * FROM {db}.OPS.alert_log WHERE status = ''OPEN''';
      BEGIN
        CALL SYSTEM$SEND_EMAIL(:integration, :recipient, :subject, :body);
        UPDATE {db}.OPS.alert_log SET email_status = 'sent' WHERE alert_id = :incident_id;
        sent := sent + 1;
      EXCEPTION
        WHEN OTHER THEN
          problem := 'failed: ' || LEFT(SQLERRM, 200);
          UPDATE {db}.OPS.alert_log SET email_status = :problem WHERE alert_id = :incident_id;
      END;
    END IF;
  END FOR;
  RETURN 'emails sent: ' || TO_VARCHAR(sent);
END;
$$;

-- ---- scheduling ----
-- Created suspended, like the tasks. Demo mode resumes it (make demo-up) and suspends it again (make demo-down).
-- No WAREHOUSE: a serverless alert bills only the seconds it runs. The condition is deliberately wide (anything
-- firing, anything still open, any recent failure) and RAISE_ALERTS() decides what is actually new.
CREATE OR REPLACE ALERT {db}.OPS.HEALTH_ALERT
  SCHEDULE = '10 MINUTE'
  IF (EXISTS (
    SELECT 1 FROM {db}.OPS.health_signals WHERE firing
    UNION ALL
    SELECT 1 FROM {db}.OPS.alert_log WHERE status = 'OPEN'
    UNION ALL
    SELECT 1 FROM TABLE({db}.INFORMATION_SCHEMA.TASK_HISTORY(SCHEDULED_TIME_RANGE_START => DATEADD('minute', -60, CURRENT_TIMESTAMP()), RESULT_LIMIT => 1000)) WHERE state = 'FAILED'
    UNION ALL
    SELECT 1 FROM TABLE({db}.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(DATA_TIMESTAMP_START => DATEADD('minute', -60, CURRENT_TIMESTAMP()))) WHERE state = 'FAILED'
  ))
  THEN CALL {db}.OPS.RAISE_ALERTS();
