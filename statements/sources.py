"""Read-only access to the gold report, from a local DuckDB file or from Snowflake.

Both sources answer the same questions with the same SQL, so the dashboard and the statement
builder do not care which one they have. DuckDB is the default (free, works offline); Snowflake
is the real pipeline. Amounts are integer minor units everywhere.

    source = open_source("duckdb", path="dbt/target/local.duckdb")
    source = open_source("snowflake", database="MERCHANT_DISBURSEMENT")
"""

from __future__ import annotations

import os
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

AMOUNTS = (
    "sales_amount",
    "refund_amount",
    "dispute_amount",
    "platform_fee_amount",
    "pass_through_fee_amount",
    "adjustment_amount",
    "disbursed_total",
)
DEFAULT_LOCAL = Path("dbt/target/local.duckdb")


def month_start(month: str | date) -> date:
    """'2026-01' or a date in January 2026 -> date(2026, 1, 1)."""
    if isinstance(month, date):  # also a datetime: DuckDB's date_trunc can return one
        return date(month.year, month.month, 1)
    year, number = month.split("-")
    return date(int(year), int(number), 1)


def next_month(first: date) -> date:
    return date(first.year + (first.month == 12), first.month % 12 + 1, 1)


def _plain(value: Any) -> Any:
    """Snowflake returns Decimal for NUMBER; the rest of the code wants int."""
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else value
    return value


class Source:
    """The questions the dashboard and the statements ask. Subclasses supply ``_query``."""

    kind = "base"

    def _table(self, schema: str, name: str) -> str:
        raise NotImplementedError

    def _query(self, sql: str, params: tuple = ()) -> list[dict]:
        raise NotImplementedError

    def query(self, sql: str, params: tuple = ()) -> list[dict]:
        return [{k: _plain(v) for k, v in row.items()} for row in self._query(sql, params)]

    def close(self) -> None:
        pass

    # -- who ------------------------------------------------------------------------------------
    def merchants(self) -> list[dict]:
        """Every merchant gold may mention, including closed ones: they were still paid."""
        t = self._table("silver", "silver_merchants")
        return self.query(
            f"SELECT merchant_id, name, country, is_deleted AS closed FROM {t} ORDER BY merchant_id"
        )

    def accounts(self, merchant_id: str) -> list[dict]:
        t = self._table("silver", "silver_accounts")
        return self.query(
            f"SELECT account_id, merchant_id, label, currency FROM {t} "
            "WHERE merchant_id = ? ORDER BY account_id",
            (merchant_id,),
        )

    # -- the report -----------------------------------------------------------------------------
    def months(self) -> list[date]:
        t = self._table("gold", "gold_monthly_disbursement")
        rows = self.query(f"SELECT DISTINCT posting_month FROM {t} ORDER BY posting_month")
        return [month_start(r["posting_month"]) for r in rows]

    def merchant_months(self, merchant_id: str) -> list[date]:
        t = self._table("gold", "gold_monthly_disbursement")
        rows = self.query(
            f"SELECT DISTINCT posting_month FROM {t} WHERE merchant_id = ? ORDER BY posting_month",
            (merchant_id,),
        )
        return [month_start(r["posting_month"]) for r in rows]

    def daily(self, merchant_id: str, month: str | date) -> list[dict]:
        """Daily rows of one merchant for one calendar month (UTC posting dates)."""
        first = month_start(month)
        t = self._table("gold", "gold_daily_disbursement")
        return self.query(
            "SELECT merchant_id, account_id, currency, posting_date, "
            + ", ".join(AMOUNTS)
            + f" FROM {t} WHERE merchant_id = ? AND posting_date >= ? AND posting_date < ? "
            "ORDER BY account_id, posting_date",
            (merchant_id, first, next_month(first)),
        )

    def monthly(
        self, merchant_id: str | None = None, month: str | date | None = None
    ) -> list[dict]:
        """gold_monthly_disbursement rows, optionally for one merchant and month."""
        t = self._table("gold", "gold_monthly_disbursement")
        where, params = [], []
        if merchant_id:
            where.append("merchant_id = ?")
            params.append(merchant_id)
        if month:
            where.append("posting_month = ?")
            params.append(month_start(month))
        clause = f" WHERE {' AND '.join(where)}" if where else ""
        rows = self.query(
            "SELECT merchant_id, account_id, currency, posting_month, active_days, "
            + ", ".join(AMOUNTS)
            + f" FROM {t}{clause} ORDER BY merchant_id, account_id, posting_month",
            tuple(params),
        )
        for row in rows:
            row["posting_month"] = month_start(row["posting_month"])
        return rows

    # -- pipeline health (Phase 5 and 6) ---------------------------------------------------------
    def health_signals(self) -> list[dict]:
        t = self._table("ops", "health_signals")
        return self.query(
            f"SELECT signal, severity, observed, threshold, enabled, firing, detail FROM {t} "
            "ORDER BY signal"
        )

    def incidents(self, limit: int = 50) -> list[dict]:
        t = self._table("ops", "alert_log")
        return self.query(
            "SELECT signal, severity, status, opened_at, resolved_at, observed, email_status "
            f"FROM {t} ORDER BY opened_at DESC LIMIT {int(limit)}"
        )

    def quarantine(self) -> list[dict]:
        t = self._table("quarantine", "reject_summary")
        return self.query(
            f"SELECT topic, reject_reason, messages, last_loaded FROM {t} "
            "ORDER BY messages DESC, topic, reject_reason"
        )

    def reconciliation(self) -> list[dict]:
        t = self._table("ops", "silver_reconciliation")
        return self.query(
            "SELECT topic, bronze_rows, rejected_rows, duplicate_rows, unique_valid_events, "
            f"unaccounted_rows, entities_expected, entities_actual, missing_entities FROM {t} "
            "ORDER BY topic"
        )


class DuckDBSource(Source):
    kind = "duckdb"

    def __init__(self, path: str | Path = DEFAULT_LOCAL, connection=None) -> None:
        import duckdb

        self.path = str(path)
        if connection is None and not Path(self.path).exists():
            raise FileNotFoundError(
                f"{self.path} does not exist. Build it with `make dbt-local` (it needs "
                "`make golden` first)."
            )
        self.con = connection or duckdb.connect(self.path, read_only=True)

    def _table(self, schema: str, name: str) -> str:
        return f"{schema}.{name}"

    def _query(self, sql: str, params: tuple = ()) -> list[dict]:
        cursor = self.con.cursor()  # one cursor per query: the dashboard shares this connection
        try:
            cursor.execute(sql, list(params))
            names = [d[0] for d in cursor.description]
            return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def close(self) -> None:
        self.con.close()


class SnowflakeSource(Source):
    kind = "snowflake"

    def __init__(self, connection, database: str) -> None:
        self.con, self.database = connection, database

    def _table(self, schema: str, name: str) -> str:
        return f"{self.database}.{schema.upper()}.{name}"

    def _query(self, sql: str, params: tuple = ()) -> list[dict]:
        cursor = self.con.cursor()
        try:
            cursor.execute(sql.replace("?", "%s"), params)
            names = [d[0].lower() for d in cursor.description]
            return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def close(self) -> None:
        self.con.close()


def open_source(
    kind: str | None = None, path: str | Path | None = None, database: str | None = None
):
    """``kind`` is 'duckdb' or 'snowflake'; default from $DASHBOARD_SOURCE, else duckdb."""
    kind = (kind or os.environ.get("DASHBOARD_SOURCE", "duckdb")).lower()
    if kind == "duckdb":
        return DuckDBSource(path or os.environ.get("DBT_DUCKDB_PATH", DEFAULT_LOCAL))
    if kind == "snowflake":
        from ops.connection import connect

        warehouse = os.environ.get("SNOWFLAKE_TRANSFORM_WAREHOUSE", "TRANSFORM_WH")
        return SnowflakeSource(
            connect(
                role_env="SNOWFLAKE_REPORTER_ROLE", default_role="REPORTER", warehouse=warehouse
            ),
            database or os.environ.get("SNOWFLAKE_DATABASE", "MERCHANT_DISBURSEMENT"),
        )
    raise ValueError(f"unknown source {kind!r}: use 'duckdb' or 'snowflake'")
