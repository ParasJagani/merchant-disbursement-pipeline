from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Protocol

from .naming import StagedFile


class Stager(Protocol):
    def stage(self, file: StagedFile) -> bool:
        """Land the file. Returns True if newly written, False if a file with that name existed."""


class LocalStager:
    """Writes into a folder; an existing name is left alone, like PUT ... OVERWRITE=FALSE."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def stage(self, file: StagedFile) -> bool:
        target = self.root / file.name
        if target.exists():
            return False
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(".tmp")
        tmp.write_bytes(file.data)
        tmp.rename(target)  # atomic, so a half-written file is never visible
        return True


class SnowflakeStager:
    """PUTs files into an internal stage. ``connection`` is a snowflake.connector connection."""

    def __init__(self, connection, stage: str = "@BRONZE.LANDING") -> None:
        self.connection = connection
        self.stage_name = stage

    def stage(self, file: StagedFile) -> bool:
        directory, _, base = file.name.rpartition("/")
        with tempfile.TemporaryDirectory() as tmp:
            local = Path(tmp) / base
            local.write_bytes(file.data)
            sql = (
                f"PUT 'file://{local.as_posix()}' {self.stage_name}/{directory} "
                "AUTO_COMPRESS=FALSE OVERWRITE=FALSE"
            )
            cursor = self.connection.cursor()
            try:
                cursor.execute(sql)
                row = cursor.fetchone()
            finally:
                cursor.close()
        status = str(row[6]).upper() if row and len(row) > 6 else "UPLOADED"
        return status == "UPLOADED"


def connect_from_env():
    """Key-pair connection from environment variables (see .env.example)."""
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization

    key_path = Path(os.path.expanduser(os.environ["SNOWFLAKE_LOADER_PRIVATE_KEY_PATH"]))
    key = serialization.load_pem_private_key(key_path.read_bytes(), password=None)
    der = key.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=f"{os.environ['SNOWFLAKE_ORGANIZATION']}-{os.environ['SNOWFLAKE_ACCOUNT']}",
        user=os.environ.get("SNOWFLAKE_LOADER_USER", "LOADER_SERVICE_USER"),
        private_key=der,
        role=os.environ.get("SNOWFLAKE_LOADER_ROLE", "LOADER"),
        warehouse=os.environ.get("SNOWFLAKE_INGEST_WAREHOUSE", "INGEST_WH"),
    )
