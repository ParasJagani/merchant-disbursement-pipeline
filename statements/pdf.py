"""Lay a statement out as a PDF (A4 landscape): a summary page, then one section per account.

Every number on the page is formatted from the integer minor units in the statement model, so the
PDF cannot hold a total that the model does not. ``statements.verify`` reads the PDF text back and
compares it with gold to prove that.
"""

from __future__ import annotations

import io
from datetime import date

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from .model import COMPONENTS, MerchantStatement, money

INK = colors.HexColor("#191919")
MUTED = colors.HexColor("#666666")
RULE = colors.HexColor("#d9d9d9")
ACCENT = colors.HexColor("#257de0")
HEADINGS = {
    "sales_amount": "Sales",
    "refund_amount": "Refunds",
    "dispute_amount": "Disputes",
    "platform_fee_amount": "Platform fees",
    "pass_through_fee_amount": "Pass-through fees",
    "adjustment_amount": "Adjustments",
    "disbursed_total": "Total disbursed",
}
PAGE = landscape(A4)
WIDTH = PAGE[0] - 30 * mm


def _styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()["Normal"]
    return {
        "title": ParagraphStyle(
            "title", parent=base, fontName="Helvetica-Bold", fontSize=20, leading=24, textColor=INK
        ),  # fmt: skip
        "sub": ParagraphStyle("sub", parent=base, fontSize=10, leading=14, textColor=MUTED),
        "h2": ParagraphStyle(
            "h2",
            parent=base,
            fontName="Helvetica-Bold",
            fontSize=12,
            leading=16,
            textColor=INK,
            spaceBefore=10,
            spaceAfter=4,
        ),  # fmt: skip
        "note": ParagraphStyle("note", parent=base, fontSize=8, leading=11, textColor=MUTED),
    }


def _table(
    rows: list[list[str]], first_widths: list[float], total_last_row: bool, pad: float = 2.5
) -> Table:
    count = len(rows[0])
    rest = (WIDTH - sum(first_widths)) / (count - len(first_widths))
    table = Table(rows, colWidths=[*first_widths, *[rest] * (count - len(first_widths))],
                  repeatRows=1)  # fmt: skip
    style = [
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("TEXTCOLOR", (0, 0), (-1, -1), INK),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (len(first_widths), 0), (-1, -1), "RIGHT"),
        ("LINEBELOW", (0, 0), (-1, 0), 0.75, INK),
        ("LINEBELOW", (0, 1), (-1, -1), 0.25, RULE),
        ("FONTNAME", (-1, 0), (-1, -1), "Helvetica-Bold"),
        ("TOPPADDING", (0, 0), (-1, -1), pad),
        ("BOTTOMPADDING", (0, 0), (-1, -1), pad),
    ]
    if total_last_row:
        style += [
            ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
            ("LINEABOVE", (0, -1), (-1, -1), 0.75, INK),
        ]
    table.setStyle(TableStyle(style))
    return table


def _header(first: list[str]) -> list[str]:
    return [*first, *[HEADINGS[name] for name in (*COMPONENTS, "disbursed_total")]]


def _amounts(totals: dict[str, int], currency: str) -> list[str]:
    return [money(totals[name], currency) for name in (*COMPONENTS, "disbursed_total")]


def _numbered(statement: MerchantStatement, generated_on: date):
    class Numbered(canvas.Canvas):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._saved = []

        def showPage(self):
            self._saved.append(dict(self.__dict__))
            self._startPage()

        def save(self):
            total = len(self._saved)
            for state in self._saved:
                self.__dict__.update(state)
                self.setFont("Helvetica", 7.5)
                self.setFillColor(MUTED)
                self.drawString(
                    15 * mm, 9 * mm,
                    f"{statement.name} ({statement.merchant_id}) · {statement.month:%B %Y}"
                    f" · generated {generated_on:%Y-%m-%d}",
                )  # fmt: skip
                self.drawRightString(
                    PAGE[0] - 15 * mm, 9 * mm, f"Page {self._pageNumber} of {total}"
                )
                super().showPage()
            super().save()

    return Numbered


def build_pdf(statement: MerchantStatement, generated_on: date | None = None) -> bytes:
    """Return the statement as PDF bytes. ``generated_on`` is printed in the footer."""
    generated_on = generated_on or date.today()
    s = _styles()
    story = [
        Paragraph("Merchant statement", s["sub"]),
        Paragraph(f"{statement.name}", s["title"]),
        Paragraph(
            f"{statement.merchant_id} · {statement.country} · "
            f"{statement.month:%B %Y} (UTC posting dates)",
            s["sub"],
        ),
        Spacer(1, 6),
        Paragraph("Summary by currency", s["h2"]),
    ]
    rows = [_header(["Currency", "Accounts"])]
    for c in statement.currencies:
        rows.append([c.currency, str(c.accounts), *_amounts(c.totals, c.currency)])
    story.append(_table(rows, [28 * mm, 20 * mm], total_last_row=False))
    story += [Spacer(1, 4), Paragraph("Summary by account", s["h2"])]
    rows = [_header(["Account", "Currency"])]
    for a in statement.accounts:
        rows.append([a.account_id, a.currency, *_amounts(a.totals, a.currency)])
    story.append(_table(rows, [28 * mm, 20 * mm], total_last_row=False))
    story += [
        Spacer(1, 6),
        Paragraph(
            "Total disbursed = sales − refunds − disputes − platform fees − pass-through fees + "
            "adjustments. Deductions are shown as positive amounts. Amounts are in each "
            "account's own currency and are never added across currencies. Dates are UTC.",
            s["note"],
        ),
    ]
    for a in statement.accounts:
        story += [
            PageBreak(),
            Paragraph(f"Account detail: {a.label} ({a.account_id}) · {a.currency}", s["h2"]),
        ]
        rows = [_header(["Date", ""])]
        for day in a.days:
            rows.append([f"{day['posting_date']:%Y-%m-%d}", "", *_amounts(day, a.currency)])
        rows.append(["Total", "", *_amounts(a.totals, a.currency)])
        # Compact rows so a 31-day month and its total fit on one page.
        story.append(_table(rows, [24 * mm, 4 * mm], total_last_row=True, pad=1.4))
    buffer = io.BytesIO()
    SimpleDocTemplate(
        buffer, pagesize=PAGE, leftMargin=15 * mm, rightMargin=15 * mm,
        topMargin=14 * mm, bottomMargin=16 * mm, invariant=1,
        title=f"Statement {statement.merchant_id} {statement.month:%Y-%m}",
        author="Merchant reporting pipeline",
    ).build(story, canvasmaker=_numbered(statement, generated_on))  # fmt: skip
    return buffer.getvalue()
