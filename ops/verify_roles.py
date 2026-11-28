"""Phase 8 gate: each role can do its job and nothing more.

Connects as TRANSFORMER and as REPORTER (the same service user, switching role) and runs probes
that must succeed and probes that must be refused. A refusal only counts if Snowflake says it is
a privilege problem, so a typo cannot pass as "denied".

    python -m ops.verify_roles
"""

from __future__ import annotations

import argparse
import json
import os

DENIED = ("insufficient privileges", "not authorized", "does not exist")

# (role, what is checked, statements to run, must it work?)
# Probes that write use WHERE 1=0 or a table that is dropped again, so a wrong grant cannot
# change the data.
PROBES: list[tuple[str, str, list[str], bool]] = [
    ("TRANSFORMER", "reads silver", ["SELECT COUNT(*) FROM {db}.SILVER.silver_accounts"], True),
    (
        "TRANSFORMER",
        "reads silver dynamic tables",
        ["SELECT COUNT(*) FROM {db}.SILVER.silver_transactions"],
        True,
    ),
    (
        "TRANSFORMER",
        "builds and drops a gold table",
        [
            "CREATE OR REPLACE TABLE {db}.GOLD._role_probe (x INT)",
            "DROP TABLE {db}.GOLD._role_probe",
        ],
        True,
    ),
    (
        "TRANSFORMER",
        "cannot change silver",
        ["DELETE FROM {db}.SILVER.silver_accounts WHERE 1=0"],
        False,
    ),
    (
        "TRANSFORMER",
        "cannot create objects in silver",
        ["CREATE TABLE {db}.SILVER._role_probe (x INT)"],
        False,
    ),
    (
        "TRANSFORMER",
        "cannot read the alert settings",
        ["SELECT COUNT(*) FROM {db}.OPS.alert_settings"],
        False,
    ),
    (
        "REPORTER",
        "reads daily gold",
        ["SELECT COUNT(*) FROM {db}.GOLD.gold_daily_disbursement"],
        True,
    ),
    (
        "REPORTER",
        "reads monthly gold",
        ["SELECT COUNT(*) FROM {db}.GOLD.gold_monthly_disbursement"],
        True,
    ),
    (
        "REPORTER",
        "reads merchants and accounts",
        [
            "SELECT COUNT(*) FROM {db}.SILVER.silver_merchants",
            "SELECT COUNT(*) FROM {db}.SILVER.silver_accounts",
        ],
        True,
    ),
    (
        "REPORTER",
        "reads the health views and the incident log",
        [
            "SELECT COUNT(*) FROM {db}.OPS.health_signals",
            "SELECT COUNT(*) FROM {db}.OPS.alert_log",
            "SELECT COUNT(*) FROM {db}.OPS.silver_reconciliation",
            "SELECT COUNT(*) FROM {db}.QUARANTINE.reject_summary",
        ],
        True,
    ),
    (
        "REPORTER",
        "cannot change gold",
        ["DELETE FROM {db}.GOLD.gold_daily_disbursement WHERE 1=0"],
        False,
    ),
    (
        "REPORTER",
        "cannot create objects in gold",
        ["CREATE TABLE {db}.GOLD._role_probe (x INT)"],
        False,
    ),
    (
        "REPORTER",
        "cannot read bronze",
        ["SELECT COUNT(*) FROM {db}.BRONZE.ref_merchants"],
        False,
    ),
    (
        "REPORTER",
        "cannot read the alert settings",
        ["SELECT COUNT(*) FROM {db}.OPS.alert_settings"],
        False,
    ),
]


def _run(connection, statements: list[str]) -> str | None:
    """Run the statements; return None if all worked, else the error text."""
    cursor = connection.cursor()
    try:
        for sql in statements:
            cursor.execute(sql)
        return None
    except Exception as exc:  # noqa: BLE001 - the text of the error is the result
        return str(exc)
    finally:
        cursor.close()


def _is_denied(message: str) -> bool:
    return any(word in message.lower() for word in DENIED)


def check(connect_as, database: str) -> list[dict]:
    """``connect_as(role)`` returns a connection that runs as that role."""
    results, connections = [], {}
    try:
        for role, name, statements, must_work in PROBES:
            if role not in connections:
                connections[role] = connect_as(role)
            conn = connections[role]
            if not any(r["name"] == f"{role}: is running as that role" for r in results):
                cursor = conn.cursor()
                try:
                    cursor.execute("SELECT CURRENT_ROLE()")
                    actual = str(cursor.fetchone()[0])
                finally:
                    cursor.close()
                results.append(
                    {"name": f"{role}: is running as that role", "expected": role,
                     "actual": actual, "ok": actual.upper() == role}
                )  # fmt: skip
                cursor = conn.cursor()
                try:
                    cursor.execute("SELECT CURRENT_SECONDARY_ROLES()")
                    extra = json.loads(str(cursor.fetchone()[0])).get("roles", "")
                finally:
                    cursor.close()
                results.append(
                    {"name": f"{role}: has no secondary roles", "expected": "none",
                     "actual": extra or "none", "ok": not extra}
                )  # fmt: skip
            error = _run(conn, [s.replace("{db}", database) for s in statements])
            if must_work:
                ok, actual = error is None, "worked" if error is None else error[:90]
            else:
                ok = error is not None and _is_denied(error)
                actual = "refused" if ok else ("worked" if error is None else error[:90])
            results.append(
                {"name": f"{role}: {name}", "expected": "worked" if must_work else "refused",
                 "actual": actual, "ok": ok}
            )  # fmt: skip
    finally:
        for conn in connections.values():
            conn.close()
    return results


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ops.verify_roles")
    parser.add_argument(
        "--database", default=os.environ.get("SNOWFLAKE_DATABASE", "MERCHANT_DISBURSEMENT")
    )
    args = parser.parse_args(argv)
    from .connection import connect

    warehouse = os.environ.get("SNOWFLAKE_TRANSFORM_WAREHOUSE", "TRANSFORM_WH")

    def connect_as(role: str):
        return connect(role_env=f"SNOWFLAKE_{role}_ROLE", default_role=role, warehouse=warehouse)

    results = check(connect_as, args.database)
    for r in results:
        verdict = "PASS" if r["ok"] else "FAIL"
        print(f"{verdict}  {r['name']:<62} expected {r['expected']:<8} got {r['actual']}")
    return 0 if all(r["ok"] for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
