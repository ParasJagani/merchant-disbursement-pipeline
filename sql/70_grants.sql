-- Phase 8: least privilege. Runs last, so it also re-applies the grants that CREATE OR REPLACE
-- drops from views. Every statement is safe to repeat.
--
--   TRANSFORMER  builds gold (dbt). Reads silver, owns and writes gold. Nothing else.
--   REPORTER     the dashboard and the statements. Reads gold plus a few named views. Writes nothing.
--
-- Both roles roll up to ACCOUNTADMIN, so the admin service user can switch to either one and
-- every connection names the role it runs as.
GRANT ROLE TRANSFORMER TO ROLE ACCOUNTADMIN;
GRANT ROLE REPORTER TO ROLE ACCOUNTADMIN;

-- A login also gets the privileges of every other role granted to the user ("secondary roles").
-- Switch that off for the service user, or a connection running as REPORTER would still hold the
-- admin's rights and the roles below would limit nothing. (ops/connection.py does the same per
-- session; this covers clients that do not, such as dbt.)
ALTER USER IF EXISTS TF_SERVICE_USER SET DEFAULT_SECONDARY_ROLES = ();

-- ---- TRANSFORMER: read silver, own gold -----------------------------------------------------
GRANT USAGE ON SCHEMA {db}.SILVER TO ROLE TRANSFORMER;
GRANT SELECT ON ALL TABLES IN SCHEMA {db}.SILVER TO ROLE TRANSFORMER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA {db}.SILVER TO ROLE TRANSFORMER;
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA {db}.SILVER TO ROLE TRANSFORMER;
GRANT SELECT ON FUTURE DYNAMIC TABLES IN SCHEMA {db}.SILVER TO ROLE TRANSFORMER;

GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA {db}.GOLD TO ROLE TRANSFORMER;
-- Gold tables made while dbt ran as the admin belong to the admin; dbt can only replace what it
-- owns. Tables dbt makes from now on belong to TRANSFORMER already.
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA {db}.GOLD TO ROLE TRANSFORMER COPY CURRENT GRANTS;

-- ---- REPORTER: read gold and a few named views ----------------------------------------------
GRANT USAGE ON SCHEMA {db}.GOLD TO ROLE REPORTER;
GRANT SELECT ON ALL TABLES IN SCHEMA {db}.GOLD TO ROLE REPORTER;
-- dbt replaces gold tables on every build, which drops their grants; future grants restore them.
GRANT SELECT ON FUTURE TABLES IN SCHEMA {db}.GOLD TO ROLE REPORTER;

GRANT USAGE ON SCHEMA {db}.SILVER TO ROLE REPORTER;
GRANT SELECT ON TABLE {db}.SILVER.silver_merchants TO ROLE REPORTER;
GRANT SELECT ON TABLE {db}.SILVER.silver_accounts TO ROLE REPORTER;

GRANT USAGE ON SCHEMA {db}.QUARANTINE TO ROLE REPORTER;
GRANT SELECT ON VIEW {db}.QUARANTINE.reject_summary TO ROLE REPORTER;

-- Views run with their owner's rights, so the reporter sees these views and not the tables under
-- them (the alert settings, which hold the notification address, stay out of reach).
GRANT USAGE ON SCHEMA {db}.OPS TO ROLE REPORTER;
GRANT SELECT ON VIEW {db}.OPS.health_signals TO ROLE REPORTER;
GRANT SELECT ON VIEW {db}.OPS.silver_reconciliation TO ROLE REPORTER;
GRANT SELECT ON TABLE {db}.OPS.alert_log TO ROLE REPORTER;
