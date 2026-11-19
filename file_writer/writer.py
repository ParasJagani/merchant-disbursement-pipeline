from __future__ import annotations

import time
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from generator.schema import validate_message

from .dlq import DeadLetters
from .naming import StagedFile, backfill_file_name, gzip_lines, kafka_file_name
from .stagers import Stager


@dataclass(frozen=True)
class Record:
    """What the writer needs from a Kafka message."""

    topic: str
    partition: int
    offset: int
    key: str | None
    value: str
    timestamp_ms: int


@dataclass
class Stats:
    consumed: int = 0
    valid: int = 0
    rejected: int = 0
    files_new: int = 0
    files_skipped: int = 0
    rejected_by_reason: dict[str, int] = field(default_factory=dict)


@dataclass
class _Batch:
    first_offset: int
    first_ts_ms: int
    last_offset: int = -1
    lines: list[str] = field(default_factory=list)


class FileWriter:
    """Validate, batch per topic and partition, stage the file, and only then commit offsets.

    Committing after staging means a crash can only cause a replay of already staged data, never
    a loss. Replays recreate the same file name (offsets are in it), so the stage skips them.
    """

    def __init__(
        self,
        stager: Stager,
        dead_letters: DeadLetters,
        commit: Callable[[dict[tuple[str, int], int]], None] = lambda offsets: None,
        max_messages: int = 5000,
        max_age_seconds: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.stager, self.dead_letters, self.commit = stager, dead_letters, commit
        self.max_messages, self.max_age_seconds, self.clock = max_messages, max_age_seconds, clock
        self.stats = Stats()
        self._batches: dict[tuple[str, int], _Batch] = {}
        self._opened_at: dict[tuple[str, int], float] = {}
        self._pending_commit: dict[tuple[str, int], int] = {}

    def handle(self, record: Record) -> None:
        self.stats.consumed += 1
        ok, reason = validate_message(record.topic, record.value)
        key = (record.topic, record.partition)
        if not ok:
            self.stats.rejected += 1
            self.stats.rejected_by_reason[reason] = self.stats.rejected_by_reason.get(reason, 0) + 1
            self.dead_letters.send(record.topic, record.key, record.value, reason)
            # The offset still has to advance, so a poison message is not read again.
            self._pending_commit[key] = max(self._pending_commit.get(key, -1), record.offset)
            return
        self.stats.valid += 1
        batch = self._batches.get(key)
        if batch is None:
            batch = self._batches[key] = _Batch(record.offset, record.timestamp_ms)
            self._opened_at[key] = self.clock()
        batch.lines.append(record.value)
        batch.last_offset = record.offset
        if len(batch.lines) >= self.max_messages:
            self._flush(key)

    def tick(self) -> None:
        """Call regularly: flush batches that have been open too long."""
        now = self.clock()
        for key in [k for k, t in self._opened_at.items() if now - t >= self.max_age_seconds]:
            self._flush(key)
        self._commit_pending()

    def close(self) -> None:
        for key in list(self._batches):
            self._flush(key)
        self._commit_pending()

    def _flush(self, key: tuple[str, int]) -> None:
        batch = self._batches.pop(key)
        self._opened_at.pop(key, None)
        topic, partition = key
        name = kafka_file_name(
            topic, partition, batch.first_offset, batch.last_offset, batch.first_ts_ms
        )
        staged = StagedFile(name, gzip_lines(batch.lines), len(batch.lines), topic)
        if self.stager.stage(staged):
            self.stats.files_new += 1
        else:
            self.stats.files_skipped += 1
        self._pending_commit[key] = max(self._pending_commit.get(key, -1), batch.last_offset)
        self._commit_pending()

    def _commit_pending(self) -> None:
        # Commit only offsets whose data is staged, so skip partitions with an open batch.
        committable = {k: v + 1 for k, v in self._pending_commit.items() if k not in self._batches}
        if committable:
            self.commit(committable)
            for k in committable:
                self._pending_commit.pop(k, None)


def stage_backfill(
    source: Path, stager: Stager, dead_letters: DeadLetters, lines_per_file: int = 50_000
) -> Stats:
    """Stage the files written by ``python -m generator backfill``, rejecting bad lines first."""
    stats = Stats()
    for topic_dir in sorted(p for p in Path(source).iterdir() if p.is_dir() and p.name != "oracle"):
        topic = topic_dir.name
        for path in sorted(topic_dir.glob("*.ndjson")):
            day = path.stem
            valid: list[str] = []
            for line in path.read_text().splitlines():
                if not line:
                    continue
                stats.consumed += 1
                ok, reason = validate_message(topic, line)
                if ok:
                    stats.valid += 1
                    valid.append(line)
                else:
                    stats.rejected += 1
                    stats.rejected_by_reason[reason] = stats.rejected_by_reason.get(reason, 0) + 1
                    dead_letters.send(topic, None, line, reason)
            by_part: dict[int, list[str]] = defaultdict(list)
            for i in range(0, len(valid), lines_per_file):
                by_part[i // lines_per_file] = valid[i : i + lines_per_file]
            for part, lines in by_part.items():
                staged = StagedFile(
                    backfill_file_name(topic, day, part), gzip_lines(lines), len(lines), topic
                )
                if stager.stage(staged):
                    stats.files_new += 1
                else:
                    stats.files_skipped += 1
    return stats
