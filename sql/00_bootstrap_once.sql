-- Run ONCE in a Snowsight worksheet as ACCOUNTADMIN, after you create the trial account.
-- Replace the two public keys (the text between the BEGIN/END lines of each .pub file, one line,
-- no header or footer). Never paste a PRIVATE key anywhere.

USE ROLE ACCOUNTADMIN;

-- 1. A service user for Terraform and the deploy script.
--    TRIAL SIMPLIFICATION: it holds ACCOUNTADMIN, because creating resource monitors, roles and
--    grants needs it. On a real account, split this into narrower roles (a Phase 8 task).
CREATE USER IF NOT EXISTS TF_SERVICE_USER
  TYPE = SERVICE
  DEFAULT_ROLE = ACCOUNTADMIN
  COMMENT = 'Terraform and SQL deployments';
ALTER USER TF_SERVICE_USER SET RSA_PUBLIC_KEY = '<PASTE_TF_PUBLIC_KEY_HERE>';
GRANT ROLE ACCOUNTADMIN TO USER TF_SERVICE_USER;

-- 2. A service user for the file writer (it only PUTs files). Its role is created by Terraform;
--    the grant of that role to this user is in step 3 below, after `terraform apply`.
CREATE USER IF NOT EXISTS LOADER_SERVICE_USER
  TYPE = SERVICE
  DEFAULT_ROLE = PUBLIC
  COMMENT = 'File writer: stages files into BRONZE.LANDING';
ALTER USER LOADER_SERVICE_USER SET RSA_PUBLIC_KEY = '<PASTE_LOADER_PUBLIC_KEY_HERE>';

-- 3. After `terraform apply` has created the roles, run:
--    GRANT ROLE LOADER TO USER LOADER_SERVICE_USER;
--    ALTER USER LOADER_SERVICE_USER SET DEFAULT_ROLE = LOADER;
