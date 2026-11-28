"""Statements: the PDF is laid out from integer minor units, and its printed totals equal gold's."""

from datetime import date

import pytest

from statements import verify
from statements.build import main as build_main
from statements.model import StatementError, build_statement, disbursed, money
from statements.pdf import build_pdf
from statements.sources import DuckDBSource, month_start, next_month, open_source


@pytest.fixture()
def source(reporting_world):
    src = DuckDBSource(reporting_world[0])
    yield src
    src.close()


@pytest.fixture()
def oracle(reporting_world):
    return reporting_world[1]


# --- the small pieces -----------------------------------------------------------------------


@pytest.mark.parametrize(
    "minor, text",
    [(0, "0.00"), (5, "0.05"), (-5, "-0.05"), (12345, "123.45"), (123456789, "1,234,567.89"),
     (-100, "-1.00")],
)  # fmt: skip
def test_money_is_formatted_from_integers_with_two_decimals(minor, text):
    assert money(minor, "EUR") == text


def test_money_refuses_a_currency_it_has_no_rule_for():
    with pytest.raises(StatementError):
        money(100, "JPY")


def test_printed_amounts_read_back_exactly():
    for minor in (0, 1, 99, 100, 101, -1, 999_999_999_99, -123_456):
        assert verify.parse_amount(money(minor, "USD")) == minor


def test_a_timestamp_is_reduced_to_a_plain_first_of_month_date():
    from datetime import datetime

    result = month_start(datetime(2026, 3, 17, 5, 30))
    assert result == date(2026, 3, 1) and type(result) is date


def test_month_helpers():
    assert month_start("2026-01") == date(2026, 1, 1)
    assert month_start(date(2026, 3, 17)) == date(2026, 3, 1)
    assert next_month(date(2026, 12, 1)) == date(2027, 1, 1)


def test_the_disbursed_rule():
    row = dict(sales_amount=1000, refund_amount=100, dispute_amount=50, platform_fee_amount=10,
               pass_through_fee_amount=5, adjustment_amount=20)  # fmt: skip
    assert disbursed(row) == 855


# --- the data -------------------------------------------------------------------------------


def test_closed_merchants_are_listed_because_they_were_still_paid(source):
    closed = [m for m in source.merchants() if m["closed"]]
    assert closed, "the test world should contain closed merchants"
    paid = {r["merchant_id"] for r in source.monthly()}
    assert {m["merchant_id"] for m in closed} & paid


def test_a_statement_is_built_from_one_merchants_month(source):
    st = build_statement(source, "mch_001", "2026-01")
    assert st.name and st.month == date(2026, 1, 1) and st.accounts
    for account in st.accounts:
        assert all(d["posting_date"].month == 1 for d in account.days)
        assert account.totals["disbursed_total"] == sum(d["disbursed_total"] for d in account.days)
        assert account.totals["disbursed_total"] == disbursed(account.totals)


def test_a_statement_is_refused_for_an_unknown_merchant_or_an_empty_month(source):
    with pytest.raises(StatementError):
        build_statement(source, "mch_nope", "2026-01")
    with pytest.raises(StatementError):
        build_statement(source, "mch_001", "2020-01")


def test_a_statement_is_refused_when_gold_rows_do_not_add_up(reporting_world, tmp_path):
    import shutil

    import duckdb

    copy = tmp_path / "bad.duckdb"
    shutil.copy(reporting_world[0], copy)
    con = duckdb.connect(str(copy))
    con.execute("UPDATE gold.gold_daily_disbursement SET disbursed_total = disbursed_total + 1 "
                "WHERE merchant_id = 'mch_001' AND posting_date = '2026-01-02'")  # fmt: skip
    con.close()
    bad = DuckDBSource(copy)
    try:
        with pytest.raises(StatementError, match="add up"):
            build_statement(bad, "mch_001", "2026-01")
    finally:
        bad.close()


# --- the PDF and the gate -------------------------------------------------------------------


def test_the_pdf_is_a_real_multi_page_pdf_with_the_merchant_on_it(source):
    st = build_statement(source, "mch_001", "2026-01")
    data = build_pdf(st, generated_on=date(2026, 2, 1))
    assert data.startswith(b"%PDF") and len(data) > 5000
    text = "\n".join(verify.pdf_lines(data))
    assert st.name in text and "mch_001" in text and "January 2026" in text
    for account in st.accounts:
        assert account.account_id in text


def test_the_pdf_is_the_same_every_time_for_the_same_input(source):
    st = build_statement(source, "mch_001", "2026-01")
    assert build_pdf(st, date(2026, 2, 1)) == build_pdf(st, date(2026, 2, 1))


def test_printed_totals_equal_gold_for_every_merchant_and_month(source, oracle):
    counts, problems = verify.verify_all(source, oracle)
    assert counts["statements"] >= 50
    assert dict(problems) == {}
    assert counts["printed"] == counts["gold"]


def test_every_gold_merchant_gets_a_statement(source, oracle):
    counts, _ = verify.verify_all(source, oracle)
    expected = len({(r["merchant_id"], r["posting_month"]) for r in source.monthly()})
    assert counts["statements"] == expected


def test_the_gate_notices_a_total_that_is_one_cent_off(source, oracle, monkeypatch):
    real = verify.build_statement

    def off_by_a_cent(src, merchant_id, month):
        st = real(src, merchant_id, month)
        if merchant_id == "mch_001" and month == date(2026, 1, 1):
            first = st.accounts[0]
            first.totals["disbursed_total"] += 1  # the printed total drifts from its days
        return st

    monkeypatch.setattr(verify, "build_statement", off_by_a_cent)
    _, problems = verify.verify_all(source, oracle)
    assert problems["account totals equal gold monthly"]
    assert problems["daily rows add up to the total"]


def test_the_gate_notices_a_wrong_answer_key(source, oracle):
    wrong = [dict(r) for r in oracle]
    wrong[0]["sales_amount"] += 1
    _, problems = verify.verify_all(source, wrong)
    assert problems["account totals equal the answer key"]


def test_the_gate_notices_a_missing_merchant(source, oracle, monkeypatch):
    real = source.merchants()
    monkeypatch.setattr(source, "merchants", lambda: [m for m in real if not m["closed"]])
    _, problems = verify.verify_all(source, oracle)
    assert problems["grand total per currency equals gold"]


def test_a_pdf_that_is_not_laid_out_like_ours_is_rejected():
    with pytest.raises(verify.ReadError):
        verify.parse_amount("12.5")


# --- the commands ---------------------------------------------------------------------------


def test_build_writes_one_pdf(reporting_world, tmp_path, capsys):
    code = build_main(["--path", str(reporting_world[0]), "--merchant", "mch_001",
                       "--month", "2026-01", "--out", str(tmp_path)])  # fmt: skip
    assert code == 0 and (tmp_path / "mch_001_2026-01.pdf").read_bytes().startswith(b"%PDF")


def test_verify_command_passes_on_the_test_world(reporting_world, capsys, tmp_path):
    csv = tmp_path / "oracle" / "daily_disbursement.csv"
    csv.parent.mkdir()
    from generator.oracle import write_oracle

    write_oracle(csv, reporting_world[1])
    code = verify.main(["--path", str(reporting_world[0]), "--golden", str(tmp_path)])
    out = capsys.readouterr().out
    assert code == 0 and "FAIL" not in out and "answer key" in out


def test_a_missing_local_file_explains_how_to_build_it(tmp_path):
    with pytest.raises(FileNotFoundError, match="make dbt-local"):
        open_source("duckdb", path=tmp_path / "nope.duckdb")


def test_an_unknown_source_is_refused():
    with pytest.raises(ValueError):
        open_source("oracle")


# --- the Snowflake source, with a scripted connection ----------------------------------------


class FakeSnowflake:
    def __init__(self, rows, names):
        self.rows, self.names, self.sql, self.params = rows, names, [], []

    def cursor(self):
        return self

    def execute(self, sql, params=None):
        self.sql.append(sql)
        self.params.append(params)
        self.description = [(n,) for n in self.names]

    def fetchall(self):
        return self.rows

    def close(self):
        pass


def test_the_snowflake_source_qualifies_tables_binds_parameters_and_normalises_values():
    from decimal import Decimal

    from statements.sources import SnowflakeSource

    conn = FakeSnowflake([(Decimal("1250"), date(2026, 1, 1))], ["DISBURSED_TOTAL", "POSTING_DATE"])
    source = SnowflakeSource(conn, "MERCHANT_DISBURSEMENT")
    rows = source.daily("mch_001", "2026-01")
    assert rows == [{"disbursed_total": 1250, "posting_date": date(2026, 1, 1)}]
    assert isinstance(rows[0]["disbursed_total"], int)
    sql = conn.sql[0]
    assert "MERCHANT_DISBURSEMENT.GOLD.gold_daily_disbursement" in sql
    assert "?" not in sql and sql.count("%s") == 3
    assert conn.params[0] == ("mch_001", date(2026, 1, 1), date(2026, 2, 1))


def test_every_dashboard_query_is_qualified_for_snowflake():
    from statements.sources import SnowflakeSource

    conn = FakeSnowflake([], ["X"])
    source = SnowflakeSource(conn, "DB")
    source.merchants()
    source.accounts("mch_001")
    source.months()
    source.merchant_months("mch_001")
    source.monthly("mch_001", "2026-01")
    source.health_signals()
    source.incidents(5)
    source.quarantine()
    source.reconciliation()
    for sql in conn.sql:
        assert " DB." in sql or "FROM DB." in sql, sql
        assert "FROM gold." not in sql and "FROM silver." not in sql and "FROM ops." not in sql
