"""Cached access to the data for the dashboard. Streamlit reruns the script on every click, so
queries are cached for a few minutes (this also keeps Snowflake credit use small)."""

from __future__ import annotations

import streamlit as st

from statements.model import build_statement
from statements.pdf import build_pdf
from statements.sources import open_source

TTL = 300  # seconds


@st.cache_resource(show_spinner=False)
def get_source(kind: str, path: str, database: str):
    return open_source(kind, path=path or None, database=database or None)


@st.cache_data(ttl=TTL, show_spinner=False)
def fetch(kind: str, path: str, database: str, what: str, *args):
    """Call ``Source.<what>(*args)``. The arguments must be hashable (strings, dates, numbers)."""
    return getattr(get_source(kind, path, database), what)(*args)


@st.cache_data(ttl=TTL, show_spinner=False)
def statement_pdf(kind: str, path: str, database: str, merchant_id: str, month) -> bytes:
    from datetime import date

    statement = build_statement(get_source(kind, path, database), merchant_id, month)
    return build_pdf(statement, generated_on=date.today())


def clear() -> None:
    st.cache_data.clear()
    st.cache_resource.clear()
