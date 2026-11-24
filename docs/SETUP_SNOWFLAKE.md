# Snowflake setup and the Phase 2 run

Do these steps in order. Each is one copy-paste. The 30-day trial clock starts at step 1, so do not
start until you have an uninterrupted block of time and everything below is installed.

## Before you start (no Snowflake needed)

- Python 3.10 or newer, Terraform 1.5 or newer, Docker (only for the Kafka path), OpenSSL.
- `pip install -r requirements.txt`
- `make check` passes.

## 1. Create the trial account

Sign up at the Snowflake trial page. Choose **AWS** and a region near you. Standard edition is
enough for this project (the trial may give you Enterprise, which also works, at a higher credit price); check the credit price of your edition before choosing.

In Snowsight, open the account menu and view the account details. Note two values: the
**organization name** and the **account name** (the account identifier is `ORG-ACCOUNT`).

## 2. Create the keys

```bash
./scripts/make_keys.sh
```

This writes two private keys into `~/.ssh` and prints two one-line **public** keys. Never share a
private key (`*.p8`) with anyone, including in chat.

## 3. One-time bootstrap

Open `sql/00_bootstrap_once.sql`, replace the two placeholders with the public keys from step 2, and
run it in a Snowsight worksheet as ACCOUNTADMIN. It creates two service users.

## 4. Configure your environment

```bash
cp .env.example .env        # fill in the organization and account names
cp terraform/terraform.tfvars.example terraform/terraform.tfvars   # same values
set -a; source .env; set +a
```

## 5. Create the infrastructure

```bash
cd terraform && terraform init && terraform apply
```

Then, in Snowsight as ACCOUNTADMIN, run the last two lines of the bootstrap file:

```sql
GRANT ROLE LOADER TO USER LOADER_SERVICE_USER;
ALTER USER LOADER_SERVICE_USER SET DEFAULT_ROLE = LOADER;
```

If `terraform apply` reports an unknown resource or argument, the Snowflake provider has renamed
something. Paste the error back and it is a quick fix.

## 6. Deploy the bronze objects

```bash
python -m ops.deploy --dry-run     # read what will run
python -m ops.deploy
```

This creates the stage, file format, seven bronze tables, the load procedure and the load task
(suspended).

## 7. Generate, stage and load

```bash
make golden                         # 30-day dataset in data/golden
make stage                          # validates, then PUTs gzip files into the stage
```

In Snowsight as ACCOUNTADMIN, run the load once:

```sql
CALL MERCHANT_DISBURSEMENT.OPS.LOAD_BRONZE();
```

## 8. Check the gate: a replayed file loads exactly once

```bash
make verify                         # every topic should say PASS
make stage                          # stage everything again; it should report 0 new files
```

Run the `CALL` again in Snowsight, then `make verify` again. Every topic must still say PASS with the
same counts. That is the Phase 2 gate.

## 9. Optional: the live Kafka path

```bash
make up
make live                           # publish the dataset to Redpanda
python -m file_writer kafka --stager snowflake --exit-when-idle 30
```

## Saving credits

Leave the task suspended except while demoing: `make demo-up` and `make demo-down`. The ingest
warehouse suspends itself after 60 seconds and is capped by a resource monitor.

## Where things can go wrong

| Symptom | Likely cause |
| --- | --- |
| `JWT token is invalid` | The public key in the bootstrap SQL does not match the private key file, or the account identifier is wrong |
| `Insufficient privileges` on the stage | The LOADER grant in step 5 was not run, or `ops.deploy` has not run since Terraform |
| `verify` shows fewer rows than expected | The `CALL` has not run yet, or `ON_ERROR = SKIP_FILE` skipped a file; check `COPY_HISTORY` |
| `verify` shows more rows after a second run | Files were re-staged under a different name; check the file names in the stage |
