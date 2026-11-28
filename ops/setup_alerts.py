"""One-time email set-up for alerts: create the email integration and tell silver where to send.

    python -m ops.setup_alerts --email you@example.com

Snowflake only sends email to a verified address of a user in the same account, so use the address
you signed up with (or verify another in Snowsight: your name, then Profile). The address is stored
in ``OPS.alert_settings`` in Snowflake and is never written to the repository.
"""

from __future__ import annotations

import argparse
import os
import re

INTEGRATION = "MERCHANT_EMAIL_INT"
_EMAIL = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}$")


def validate_email(address: str) -> str:
    """The address goes into DDL (which cannot take bind variables), so be strict."""
    address = address.strip()
    if not _EMAIL.match(address) or len(address) > 254:
        raise ValueError(f"{address!r} does not look like an email address")
    return address


def statements(database: str, address: str, send_test: bool = True) -> list[str]:
    address = validate_email(address)
    out = [
        f"CREATE OR REPLACE NOTIFICATION INTEGRATION {INTEGRATION} "
        f"TYPE = EMAIL ENABLED = TRUE ALLOWED_RECIPIENTS = ('{address}')",
        f"DELETE FROM {database}.OPS.alert_settings",
        f"INSERT INTO {database}.OPS.alert_settings (setting, setting_value) VALUES "
        f"('email_recipient', '{address}'), ('email_integration', '{INTEGRATION}')",
    ]
    if send_test:
        out.append(
            f"CALL SYSTEM$SEND_EMAIL('{INTEGRATION}', '{address}', "
            "'Merchant pipeline: alert email is set up', "
            "'This is a test. Alerts for the merchant disbursement pipeline will arrive here.')"
        )
    return out


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ops.setup_alerts")
    parser.add_argument("--email", default=os.environ.get("ALERT_EMAIL_TO"), required=False)
    parser.add_argument("--no-test", action="store_true", help="do not send the test email")
    parser.add_argument(
        "--database", default=os.environ.get("SNOWFLAKE_DATABASE", "MERCHANT_DISBURSEMENT")
    )
    args = parser.parse_args(argv)
    if not args.email:
        parser.error("give --email you@example.com (or set ALERT_EMAIL_TO)")
    todo = statements(args.database, args.email, send_test=not args.no_test)
    from .connection import connect

    conn = connect()
    try:
        cursor = conn.cursor()
        for sql in todo:
            try:
                cursor.execute(sql)
            except Exception as error:
                print(f"FAILED: {sql[:90]}...\n  {error}")
                print(
                    "If the address is not verified: Snowsight, your name, Profile, "
                    "verify the email, then run this again."
                )
                return 1
        cursor.close()
    finally:
        conn.close()
    print(f"Alert email goes to {args.email}. Check the inbox for the test message.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
