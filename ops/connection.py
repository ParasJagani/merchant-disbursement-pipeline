from __future__ import annotations

import os
from pathlib import Path


def connect(
    role_env: str = "SNOWFLAKE_ADMIN_ROLE",
    default_role: str = "ACCOUNTADMIN",
    user_env: str = "SNOWFLAKE_ADMIN_USER",
    key_env: str = "SNOWFLAKE_ADMIN_PRIVATE_KEY_PATH",
    default_user: str = "TF_SERVICE_USER",
    warehouse: str | None = None,
):
    """Key-pair connection from environment variables. Imported lazily; tests use fakes."""
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization

    key = serialization.load_pem_private_key(
        Path(os.path.expanduser(os.environ[key_env])).read_bytes(), password=None
    )
    der = key.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    conn = snowflake.connector.connect(
        account=f"{os.environ['SNOWFLAKE_ORGANIZATION']}-{os.environ['SNOWFLAKE_ACCOUNT']}",
        user=os.environ.get(user_env, default_user),
        private_key=der,
        role=os.environ.get(role_env, default_role),
        warehouse=warehouse or os.environ.get("SNOWFLAKE_INGEST_WAREHOUSE", "INGEST_WH"),
    )
    # A session also gets the privileges of every other role granted to the user ("secondary
    # roles") unless told otherwise. Without this, a connection that says it is REPORTER could
    # still do whatever the admin role can, and the role would limit nothing.
    cursor = conn.cursor()
    try:
        cursor.execute("USE SECONDARY ROLES NONE")
    finally:
        cursor.close()
    return conn
