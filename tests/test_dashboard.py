"""The dashboard, run headless with Streamlit's own test harness against the local test world."""

from pathlib import Path

import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

APP = str(Path(__file__).resolve().parents[1] / "dashboard" / "app.py")


@pytest.fixture()
def app(reporting_world, monkeypatch):
    monkeypatch.setenv("DASHBOARD_SOURCE", "duckdb")
    monkeypatch.setenv("DBT_DUCKDB_PATH", str(reporting_world[0]))
    import streamlit as st

    st.cache_data.clear()
    st.cache_resource.clear()
    at = AppTest.from_file(APP, default_timeout=60)
    at.run()
    return at


def texts(at):
    return " ".join(
        [m.value for m in at.markdown]
        + [t.value for t in at.title]
        + [s.value for s in at.subheader]
    )


def test_the_disbursements_page_renders_without_error(app):
    assert not app.exception, app.exception
    assert "Merchant disbursements" in texts(app)
    assert any("Disbursed," in m.label for m in app.metric)
    assert app.dataframe  # the by-account table


def test_the_merchant_picker_lists_closed_merchants_too(app):
    options = app.selectbox(key="merchant").options
    assert any(o.endswith("(closed)") for o in options)
    assert len(options) >= 28


def test_picking_another_merchant_changes_the_numbers(app):
    first = [m.value for m in app.metric]
    other = app.selectbox(key="merchant").options[3]
    app.selectbox(key="merchant").select(other).run()
    assert not app.exception, app.exception
    assert [m.value for m in app.metric] != first


def test_picking_one_account_shows_one_currency(app):
    accounts = app.selectbox(key="account").options
    assert accounts[0] == "All accounts" and len(accounts) > 1
    app.selectbox(key="account").select(accounts[1]).run()
    assert not app.exception, app.exception
    assert len(app.metric) == 1


def test_the_metric_equals_gold_for_that_merchant_and_month(app, reporting_world):
    from statements.model import money
    from statements.sources import DuckDBSource

    merchant = app.selectbox(key="merchant").value.split(" · ")[0]
    month = app.selectbox(key="month").value
    src = DuckDBSource(reporting_world[0])
    try:
        rows = src.monthly(merchant, month)
    finally:
        src.close()
    expected = {}
    for r in rows:
        expected[r["currency"]] = expected.get(r["currency"], 0) + r["disbursed_total"]
    shown = {m.label.split(", ")[1]: m.value for m in app.metric}
    assert shown == {c: money(v, c) for c, v in expected.items()}


def test_the_health_page_shows_signals_incidents_and_reconciliation(app):
    app.radio(key="page").set_value("Pipeline health").run()
    assert not app.exception, app.exception
    assert "Pipeline health" in texts(app)
    labels = [m.label for m in app.metric]
    assert labels == [
        "Open incidents",
        "Signals firing",
        "Quarantined messages",
        "Unaccounted rows",
    ]
    assert len(app.dataframe) >= 3
    # The test world has an open quarantine incident, so the page must say so.
    assert app.error and "open incident" in app.error[0].value


def test_a_missing_local_file_is_explained_not_a_traceback(monkeypatch, tmp_path):
    monkeypatch.setenv("DASHBOARD_SOURCE", "duckdb")
    monkeypatch.setenv("DBT_DUCKDB_PATH", str(tmp_path / "nope.duckdb"))
    import streamlit as st

    st.cache_data.clear()
    st.cache_resource.clear()
    at = AppTest.from_file(APP, default_timeout=60)
    at.run()
    assert not at.exception
    assert at.error and "make dbt-local" in at.error[0].value


def test_chart_helpers_keep_account_colours_stable_when_filtered():
    from dashboard import charts

    slots = charts.account_slots(["acc_b", "acc_a", "acc_c"])
    assert slots == {"acc_a": 0, "acc_b": 1, "acc_c": 2}
    assert charts.colour(0, False) != charts.colour(0, True)
    assert charts.colour(8, False) == charts.colour(0, False)  # never invents a ninth hue


def test_component_totals_signs_deductions_negative():
    import pandas as pd

    from dashboard import charts

    frame = pd.DataFrame(
        [
            dict(
                sales_amount=1000,
                refund_amount=100,
                dispute_amount=0,
                platform_fee_amount=10,
                pass_through_fee_amount=5,
                adjustment_amount=-20,
            )
        ]  # fmt: skip
    )
    totals = charts.component_totals(frame).set_index("component")["effect"]
    assert totals["Sales"] == 10.0 and totals["Refunds"] == -1.0 and totals["Adjustments"] == -0.2
