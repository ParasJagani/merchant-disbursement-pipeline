"""The Phase 7 gate: the totals printed in the PDFs equal gold's totals (and the answer key).

The PDF is read back as text, the way a person would read it, and compared with gold:

* each account's printed monthly total equals ``gold_monthly_disbursement``
* each printed daily row equals ``gold_daily_disbursement``, and the daily rows add up to the total
* the printed currency summary equals the sum of its accounts
* every printed account total equals the generator's answer key, which gold never touched
* across every statement, the grand total per currency equals gold's grand total per currency

    python -m statements.verify                      # local DuckDB file
    python -m statements.verify --source snowflake   # the real gold tables
"""

from __future__ import annotations

import argparse
import io
import re
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

from .model import build_statement
from .pdf import HEADINGS, build_pdf
from .sources import AMOUNTS, Source, month_start, open_source

_AMOUNT = re.compile(r"^-?\d{1,3}(,\d{3})*\.\d{2}$")
_PAGE = re.compile(r"\bPage \d+ of \d+\s*$")
_META = re.compile(r"^Statement (?P<merchant>\S+) (?P<month>\d{4}-\d{2})$")
_CURRENCY = re.compile(r"^[A-Z]{3}$")
_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_COLUMNS = len(AMOUNTS)


class ReadError(ValueError):
    """The PDF is not laid out the way ``statements.pdf`` writes it."""


def parse_amount(text: str) -> int:
    if not _AMOUNT.match(text):
        raise ReadError(f"{text!r} is not an amount")
    sign = -1 if text.startswith("-") else 1
    whole, cents = text.replace(",", "").lstrip("-").split(".")
    return sign * (int(whole) * 100 + int(cents))


def _amounts(tokens: list[str]) -> dict[str, int]:
    return {name: parse_amount(text) for name, text in zip(AMOUNTS, tokens, strict=True)}


def _page_texts(data: bytes) -> tuple[str, list[str]]:
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(data))
    title = (reader.metadata.title if reader.metadata else None) or ""
    return title, [page.extract_text() or "" for page in reader.pages]


def pdf_lines(data: bytes) -> list[str]:
    """The text of the PDF, one entry per line, without the page footers.

    Different versions of pypdf cut the same page into lines differently (one cell per line, or one
    table row per line), so nothing here may depend on where the line breaks fall.
    """
    lines: list[str] = []
    for text in _page_texts(data)[1]:
        lines += [ln.strip() for ln in text.splitlines() if ln.strip()]
    # The footer can fall in the middle of a table that runs onto a new page: drop it.
    return [ln for ln in lines if " · generated " not in ln and not _PAGE.search(ln)]


def read_statement(data: bytes) -> dict:
    """Parse a statement PDF into numbers: header, summaries and the per-account detail.

    Works on the stream of words, not on lines. Who and when come from the PDF's title, and the
    visible text is checked separately by ``check_statement``.
    """
    raw_title = _page_texts(data)[0]
    title = _META.match(raw_title)
    if title is None:
        raise ReadError(
            f"the PDF has no 'Statement <merchant> <YYYY-MM>' title (it says {raw_title!r})"
        )
    tokens = " ".join(pdf_lines(data)).split()
    out = {
        "merchant_id": title["merchant"],
        "month": datetime.strptime(title["month"], "%Y-%m").date(),
        "by_currency": {},
        "by_account": {},
        "detail": {},
    }
    words = [w for a in AMOUNTS for w in HEADINGS[a].split()]

    def find(*phrase: str, start: int = 0) -> int:
        for i in range(start, len(tokens) - len(phrase) + 1):
            if tokens[i : i + len(phrase)] == list(phrase):
                return i
        raise ReadError(f"{' '.join(phrase)!r} not found; the text begins {tokens[:12]}")

    def expect(i: int, expected: list[str], where: str) -> int:
        if tokens[i : i + len(expected)] != expected:
            raise ReadError(f"unexpected columns under {where}: {tokens[i : i + len(expected)]}")
        return i + len(expected)

    def amount(i: int) -> bool:
        return i < len(tokens) and bool(_AMOUNT.match(tokens[i]))

    i = expect(find("Summary", "by", "currency") + 3, ["Currency", "Accounts", *words], "currency")
    while _CURRENCY.match(tokens[i]) and tokens[i + 1].isdigit():
        out["by_currency"][tokens[i]] = {
            "accounts": int(tokens[i + 1]),
            "amounts": _amounts(tokens[i + 2 : i + 2 + _COLUMNS]),
        }
        i += 2 + _COLUMNS
    i = expect(find("Summary", "by", "account") + 3, ["Account", "Currency", *words], "account")
    while _CURRENCY.match(tokens[i + 1]) and amount(i + 2):
        out["by_account"][tokens[i]] = {
            "currency": tokens[i + 1],
            "amounts": _amounts(tokens[i + 2 : i + 2 + _COLUMNS]),
        }
        i += 2 + _COLUMNS
    start = 0
    while True:
        try:
            i = find("Account", "detail:", start=start)
        except ReadError:
            break
        j = i + 2
        while not (tokens[j] == "·" and _CURRENCY.match(tokens[j + 1])):
            j += 1  # the label can be several words; the account id is the last one before "·"
        account, currency = tokens[j - 1].strip("()"), tokens[j + 1]
        j = expect(j + 2, ["Date", *words], account)
        days = []
        while _DAY.match(tokens[j]) or tokens[j] == "Date":
            if tokens[j] == "Date":  # the header repeats when a table runs onto a new page
                j = expect(j, ["Date", *words], account)
                continue
            days.append((date.fromisoformat(tokens[j]), _amounts(tokens[j + 1 : j + 1 + _COLUMNS])))
            j += 1 + _COLUMNS
        if tokens[j] != "Total":
            raise ReadError(f"no total row under {account}")
        out["detail"][account] = {
            "currency": currency,
            "days": days,
            "total": _amounts(tokens[j + 1 : j + 1 + _COLUMNS]),
        }
        start = j + 1 + _COLUMNS
    return out


def _different(printed: dict[str, int], expected: dict) -> list[str]:
    return [name for name in AMOUNTS if int(printed[name]) != int(expected[name])]


def check_statement(
    data: bytes, source: Source, merchant_id: str, month: date, oracle: dict | None = None
) -> dict[str, list[str]]:
    """Problems found in one PDF, grouped by what was compared. Empty lists mean it agrees."""
    parsed = read_statement(data)
    problems: dict[str, list[str]] = defaultdict(list)
    shown = " ".join(pdf_lines(data))
    if merchant_id not in shown or f"{month:%B %Y}" not in shown:
        problems["merchant and month are printed"].append(f"{merchant_id} {month:%B %Y}")
    if (parsed["merchant_id"], parsed["month"]) != (merchant_id, month):
        problems["right merchant and month"].append(f"{parsed['merchant_id']} {parsed['month']}")
    monthly = {r["account_id"]: r for r in source.monthly(merchant_id, month)}
    if set(parsed["by_account"]) != set(monthly):
        problems["same accounts as gold"].append(
            f"printed {sorted(parsed['by_account'])} gold {sorted(monthly)}"
        )
    for account, row in monthly.items():
        printed = parsed["by_account"].get(account)
        if printed is None:
            continue
        if printed["currency"] != row["currency"]:
            problems["same currency as gold"].append(account)
        bad = _different(printed["amounts"], row)
        if bad:
            problems["account totals equal gold monthly"].append(f"{account}: {bad}")
        if oracle is not None:
            key = (merchant_id, account, month)
            expected = oracle.get(key)
            if expected is None or _different(printed["amounts"], expected):
                problems["account totals equal the answer key"].append(account)
    gold_days = defaultdict(list)
    for row in source.daily(merchant_id, month):
        gold_days[row["account_id"]].append(row)
    for account, detail in parsed["detail"].items():
        rows = gold_days.get(account, [])
        printed_days = [d for d, _ in detail["days"]]
        if printed_days != [r["posting_date"] for r in rows] or any(
            _different(a, r) for (_, a), r in zip(detail["days"], rows, strict=False)
        ):
            problems["daily rows equal gold daily"].append(account)
        summed = {n: sum(a[n] for _, a in detail["days"]) for n in AMOUNTS}
        if _different(summed, detail["total"]):
            problems["daily rows add up to the total"].append(account)
        summary = parsed["by_account"].get(account)
        if summary is None or _different(detail["total"], summary["amounts"]):
            problems["detail total equals summary"].append(account)
    if set(parsed["detail"]) != set(parsed["by_account"]):
        problems["every account has its detail"].append(merchant_id)
    for currency, summary in parsed["by_currency"].items():
        members = [a["amounts"] for a in parsed["by_account"].values() if a["currency"] == currency]
        total = {n: sum(m[n] for m in members) for n in AMOUNTS}
        if _different(summary["amounts"], total) or summary["accounts"] != len(members):
            problems["currency summary equals its accounts"].append(currency)
    return problems


def oracle_by_month(rows: list[dict]) -> dict:
    out: dict[tuple, dict[str, int]] = defaultdict(lambda: dict.fromkeys(AMOUNTS, 0))
    for r in rows:
        posting = r["posting_date"]
        day = date.fromisoformat(posting) if isinstance(posting, str) else posting
        key = (r["merchant_id"], r["account_id"], month_start(day))
        for name in AMOUNTS:
            out[key][name] += int(r[name])
    return dict(out)


def verify_all(source: Source, oracle_rows: list[dict] | None = None, out_dir: Path | None = None):
    """Build and check every merchant-month statement. Returns (counts, problems)."""
    oracle = oracle_by_month(oracle_rows) if oracle_rows else None
    problems: dict[str, list[str]] = defaultdict(list)
    built = 0
    printed_by_currency: dict[str, int] = defaultdict(int)
    for merchant in source.merchants():
        for month in source.merchant_months(merchant["merchant_id"]):
            statement = build_statement(source, merchant["merchant_id"], month)
            data = build_pdf(statement, generated_on=date.today())
            if out_dir is not None:
                out_dir.mkdir(parents=True, exist_ok=True)
                (out_dir / f"{merchant['merchant_id']}_{month:%Y-%m}.pdf").write_bytes(data)
            built += 1
            found = check_statement(data, source, merchant["merchant_id"], month, oracle)
            for check, items in found.items():
                problems[check] += [f"{merchant['merchant_id']} {month:%Y-%m} {i}" for i in items]
            for currency, summary in read_statement(data)["by_currency"].items():
                printed_by_currency[currency] += summary["amounts"]["disbursed_total"]
    gold_by_currency: dict[str, int] = defaultdict(int)
    for row in source.monthly():
        gold_by_currency[row["currency"]] += row["disbursed_total"]
    counts = {
        "statements": built,
        "printed": dict(printed_by_currency),
        "gold": dict(gold_by_currency),
    }
    diffs = [c for c in set(printed_by_currency) | set(gold_by_currency)
             if printed_by_currency[c] != gold_by_currency[c]]  # fmt: skip
    if diffs:
        problems["grand total per currency equals gold"] += sorted(diffs)
    return counts, problems


CHECKS = [
    "right merchant and month",
    "merchant and month are printed",
    "same accounts as gold",
    "same currency as gold",
    "account totals equal gold monthly",
    "daily rows equal gold daily",
    "daily rows add up to the total",
    "detail total equals summary",
    "every account has its detail",
    "currency summary equals its accounts",
    "account totals equal the answer key",
    "grand total per currency equals gold",
]


def report(counts: dict, problems: dict, with_oracle: bool) -> list[tuple[str, int, int, bool]]:
    lines = [
        ("statements built", counts["statements"], counts["statements"], counts["statements"] > 0)
    ]
    for check in CHECKS:
        if check == "account totals equal the answer key" and not with_oracle:
            continue
        lines.append((check, 0, len(problems.get(check, [])), not problems.get(check)))
    return lines


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m statements.verify")
    parser.add_argument("--source", choices=["duckdb", "snowflake"], default=None)
    parser.add_argument(
        "--path", default=None, help="DuckDB file (default dbt/target/local.duckdb)"
    )
    parser.add_argument("--golden", type=Path, default=Path("data/golden"))
    parser.add_argument("--out", type=Path, default=None, help="also keep the PDFs in this folder")
    args = parser.parse_args(argv)
    oracle_rows = None
    csv_path = args.golden / "oracle" / "daily_disbursement.csv"
    if csv_path.exists():
        from silver.check import oracle_from_csv

        oracle_rows = oracle_from_csv(csv_path)
    else:
        print(f"note: {csv_path} not found, skipping the answer-key comparison")
    source = open_source(args.source, path=args.path)
    try:
        counts, problems = verify_all(source, oracle_rows, args.out)
    finally:
        source.close()
    ok = True
    for name, want, actual, passed in report(counts, problems, oracle_rows is not None):
        ok &= passed
        print(f"{'PASS' if passed else 'FAIL'}  {name:<46} expected {want:>6}  actual {actual:>6}")
        for item in problems.get(name, [])[:3]:
            print(f"        {item}")
    for currency in sorted(counts["gold"]):
        print(f"      {currency} grand total disbursed: printed {counts['printed'].get(currency)} "
              f"gold {counts['gold'][currency]} minor units")  # fmt: skip
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
