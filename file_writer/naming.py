"""Deterministic file names, so a replay re-creates the same name and Snowflake skips it."""

from __future__ import annotations

import gzip
import io
from dataclasses import dataclass
from datetime import datetime, timezone


def topic_path(topic: str) -> str:
    """ref.merchants -> ref_merchants (safe for stage paths and table names)."""
    return topic.replace(".", "_")


def day_of(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def kafka_file_name(
    topic: str, partition: int, first_offset: int, last_offset: int, first_ts_ms: int
) -> str:
    path = topic_path(topic)
    return (
        f"{path}/dt={day_of(first_ts_ms)}/"
        f"{path}__p{partition:02d}__{first_offset:012d}-{last_offset:012d}.ndjson.gz"
    )


def backfill_file_name(topic: str, day: str, part: int) -> str:
    path = topic_path(topic)
    return f"{path}/dt={day}/{path}__{day}__part-{part:04d}.ndjson.gz"


def gzip_lines(lines: list[str]) -> bytes:
    """Same input always gives the same bytes (no timestamp in the gzip header)."""
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as handle:
        handle.write(("\n".join(lines) + "\n").encode())
    return buffer.getvalue()


@dataclass(frozen=True)
class StagedFile:
    name: str  # path inside the stage
    data: bytes
    count: int
    topic: str
