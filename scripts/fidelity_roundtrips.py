#!/usr/bin/env python3
"""Render Fidelity VOO/UPRO traffic as ledger-style round-trip tables.

Reads one or more concatenated Fidelity Accounts_History CSV exports from stdin.
Duplicate rows from overlapping downloads are ignored.  Unrelated symbols are
ignored; only VOO/UPRO stock rows and VOO/UPRO option rows are rendered.

Example:
  cat /home/alan/fin/transactions/Accounts_History*.csv \
      | scripts/fidelity_roundtrips.py \
          --symbol UPRO \
          --from-date 20260626 \
          --positions /home/alan/fin/transactions/Portfolio_Positions_Aug-08-2026.csv

Use --from-date/--to-date to isolate the strategy traffic for a known round
trip.  The optional --positions CSV is only needed to mark open positions in the
final "current" row; closed trips can be rendered from transactions alone.

Output columns are signed balance-sheet buckets: Stock is normally positive,
short Option liabilities are normally negative, Cash is positive, and Total is
the row sum.  Stock/option buys and sells are printed as dated rows.  Market
change rows are inserted where value changes without a cash transaction.
"""

import argparse
import csv
import sys
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from io import StringIO
from pathlib import Path


SYMBOLS = ("VOO", "UPRO")
ZERO = Decimal("0.00")


def dec(value):
    text = (
        str(value or "")
        .replace(",", "")
        .replace('"', "")
        .replace("$", "")
        .replace("%", "")
        .replace("+", "")
        .strip()
    )
    if text in {"", "--"}:
        return Decimal("0")
    return Decimal(text)


def money(value):
    return Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def fmt(value):
    value = money(value)
    return f"{value:,.2f}"


def date_key(value):
    return datetime.strptime(value, "%m/%d/%Y").strftime("%Y%m%d")


def normalize_date(value):
    if not value:
        return None
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return text
    return date_key(text)


def account_name(value):
    if value == "Alan IRA":
        return "401k"
    if value == "Individual - TOD":
        return "indiv"
    return (value or "unknown").replace(" ", "_")


def row_key(row, fieldnames):
    return (
        row.get("Run Date", ""),
        account_name(row.get("Account", "")),
        row.get("Action", ""),
        row.get("Symbol", ""),
        row.get("Description", ""),
        row.get("Type", ""),
        row.get("Price ($)", ""),
        row.get("Quantity", ""),
        row.get("Amount ($)", ""),
        row.get("Settlement Date", ""),
    )


def csv_blocks(text):
    lines = text.splitlines()
    starts = [index for index, line in enumerate(lines) if line.startswith("Run Date,")]
    for block_index, start in enumerate(starts):
        end = starts[block_index + 1] if block_index + 1 < len(starts) else len(lines)
        block = "\n".join(lines[start:end])
        if block.strip():
            yield block


def read_transaction_rows(handle):
    rows = []
    seen = set()
    for block in csv_blocks(handle.read().lstrip("\ufeff")):
        reader = csv.DictReader(StringIO(block))
        if not reader.fieldnames:
            continue
        for row in reader:
            if not row.get("Run Date") or not row.get("Action"):
                continue
            if row["Run Date"].startswith("The data"):
                continue
            if "Account" not in row:
                row["Account"] = "Individual - TOD"
            key = row_key(row, reader.fieldnames)
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)
    return rows


def read_positions(paths):
    if not paths:
        return {}
    account_values = {}
    for path in paths:
        for account, symbol_values in read_position_file(path).items():
            account_values[account] = symbol_values
    values = {symbol: {"stock": Decimal("0"), "option": Decimal("0")} for symbol in SYMBOLS}
    for symbol_values in account_values.values():
        for symbol in SYMBOLS:
            values[symbol]["stock"] += symbol_values[symbol]["stock"]
            values[symbol]["option"] += symbol_values[symbol]["option"]
    return values


def read_position_file(path):
    text = Path(path).read_text(encoding="utf-8-sig")
    lines = text.splitlines()
    start = next((index for index, line in enumerate(lines) if line.startswith("Account number,")), None)
    if start is None:
        return {}
    reader = csv.DictReader(lines[start:])
    values = defaultdict(lambda: {symbol: {"stock": Decimal("0"), "option": Decimal("0")} for symbol in SYMBOLS})
    for row in reader:
        raw_symbol = row.get("Symbol") or ""
        raw_description = row.get("Description") or ""
        if not raw_symbol and not raw_description:
            continue
        account = account_name(row.get("Account name", ""))
        haystack = " ".join([raw_symbol, raw_description]).upper()
        for symbol in SYMBOLS:
            if symbol not in haystack:
                continue
            current = money(dec(row.get("Current value", "0")))
            if is_option_text(haystack):
                values[account][symbol]["option"] += current
            elif row.get("Symbol", "").strip().upper() == symbol:
                values[account][symbol]["stock"] += current
    return values


def is_option_text(text):
    return " CALL " in f" {text} " or " PUT " in f" {text} "


def row_symbol(row):
    haystack = " ".join([row.get("Symbol", ""), row.get("Action", ""), row.get("Description", "")]).upper()
    for symbol in SYMBOLS:
        if symbol in haystack:
            return symbol
    return None


def is_option_row(row):
    haystack = " ".join([row.get("Symbol", ""), row.get("Action", ""), row.get("Description", "")]).upper()
    return is_option_text(haystack)


def is_assignment(row):
    action = row.get("Action", "").upper()
    return "ASSIGNED" in action or "ASSIGNMENT" in action or "EXERCISED" in action


def action_kind(row):
    action = row.get("Action", "").upper()
    amount = money(dec(row.get("Amount ($)", "0")))
    quantity = dec(row.get("Quantity", "0"))
    if is_assignment(row):
        return "assignment"
    if is_option_row(row):
        if "SOLD" in action:
            return "option_sale"
        if "BOUGHT" in action:
            return "option_buyback"
        return "option"
    if "DIVIDEND" in action:
        return "cash_income"
    if "YOU BOUGHT" in action or amount < 0 or quantity > 0:
        return "stock_buy"
    if "YOU SOLD" in action or amount > 0 or quantity < 0:
        return "stock_sale"
    return None


def sort_order(row):
    order = {
        "cash_income": 0,
        "stock_buy": 1,
        "option_sale": 2,
        "option_buyback": 3,
        "assignment": 4,
        "stock_sale": 5,
    }
    return order.get(action_kind(row), 9)


def merge_split_stock_rows(rows):
    merged = []
    pending = {}
    for row in rows:
        kind = action_kind(row)
        if kind not in {"stock_buy", "stock_sale"}:
            merged.append(row)
            continue
        key = (
            date_key(row["Run Date"]),
            row_symbol(row),
            account_name(row.get("Account", "")),
            kind,
        )
        if key not in pending:
            pending[key] = len(merged)
            merged.append(row.copy())
            continue
        existing = merged[pending[key]]
        existing["Amount ($)"] = str(money(dec(existing.get("Amount ($)", "0")) + dec(row.get("Amount ($)", "0"))))
        existing["Quantity"] = str(dec(existing.get("Quantity", "0")) + dec(row.get("Quantity", "0")))
    return merged


class Ledger:
    def __init__(self, symbol, current_values):
        self.symbol = symbol
        self.rows = []
        self.stock = ZERO
        self.option = ZERO
        self.cash = ZERO
        self.shares = Decimal("0")
        self.current_values = current_values or {"stock": ZERO, "option": ZERO}
        self.in_trip = False
        self.trip_no = 0
        self.begin_date = None
        self.last_date = None
        self.open_option_cost = defaultdict(Decimal)
        self.option_values = defaultdict(Decimal)

    def add(self, label, stock=ZERO, option=ZERO, cash=ZERO):
        stock = money(stock)
        option = money(option)
        cash = money(cash)
        total = money(stock + option + cash)
        self.rows.append((label, stock, option, cash, total))
        self.stock = money(self.stock + stock)
        self.option = money(self.option + option)
        self.cash = money(self.cash + cash)

    def begin(self, date, starting_cash):
        self.in_trip = True
        self.trip_no += 1
        self.begin_date = date
        self.rows = []
        self.stock = ZERO
        self.option = ZERO
        self.cash = ZERO
        self.shares = Decimal("0")
        self.option_values = defaultdict(Decimal)
        self.add("begin", cash=starting_cash)

    def option_symbol(self, row):
        return account_name(row.get("Account", "")) + ":" + row.get("Symbol", "").strip()

    def market_option_to(self, date, target_value):
        change = money(target_value - self.option)
        if change:
            self.add(f"{date} option market", option=change)

    def market_one_option_to(self, date, option_key, target_value):
        current = money(self.option_values[option_key])
        target_value = money(target_value)
        change = money(target_value - current)
        if change:
            self.add(f"{date} option market", option=change)
            self.option_values[option_key] = target_value

    def fund_if_needed(self, date, amount):
        amount = money(amount)
        if self.cash >= amount:
            return
        needed = money(amount - self.cash)
        self.add(f"{date} cash contribution", cash=needed)

    def process(self, row):
        symbol = row_symbol(row)
        if symbol != self.symbol:
            return []
        kind = action_kind(row)
        if kind is None:
            return []
        date = date_key(row["Run Date"])
        acct = account_name(row.get("Account", ""))
        amount = money(dec(row.get("Amount ($)", "0")))
        abs_amount = money(abs(amount))
        completed = []

        quantity = dec(row.get("Quantity", "0"))

        if not self.in_trip and kind not in {"stock_buy", "cash_income"}:
            return completed
        if not self.in_trip:
            self.begin(date, abs_amount if kind == "stock_buy" else ZERO)
        elif kind == "stock_buy" and self.stock == 0 and self.option == 0 and self.cash == 0:
            self.begin(date, abs_amount)

        if kind == "cash_income":
            self.add(f"{date} cash income {acct}", cash=amount)
        elif kind == "stock_buy":
            self.fund_if_needed(date, abs_amount)
            self.add(f"{date} stock buy {acct}", stock=abs_amount, cash=-abs_amount)
            if quantity > 0:
                self.shares += quantity
        elif kind == "stock_sale":
            sold_shares = abs(quantity)
            if self.shares > 0 and sold_shares > 0:
                ratio = min(Decimal("1"), sold_shares / self.shares)
                carrying = money(self.stock * ratio)
                market_change = money(abs_amount - carrying)
                if market_change:
                    self.add(f"{date} stock market", stock=market_change)
            self.add(f"{date} stock sale {acct}", stock=-abs_amount, cash=abs_amount)
            if sold_shares > 0:
                self.shares = max(Decimal("0"), self.shares - sold_shares)
        elif kind == "option_sale":
            option_key = self.option_symbol(row)
            self.open_option_cost[option_key] += abs_amount
            self.option_values[option_key] -= abs_amount
            self.add(f"{date} option sale {acct}", option=-abs_amount, cash=abs_amount)
        elif kind == "option_buyback":
            option_key = self.option_symbol(row)
            target = -abs_amount
            self.market_one_option_to(date, option_key, target)
            self.open_option_cost[option_key] = Decimal("0")
            self.option_values[option_key] = Decimal("0")
            self.add(f"{date} option buyback {acct}", option=abs_amount, cash=-abs_amount)
        elif kind == "assignment":
            option_clear = -self.option
            cash = abs_amount
            stock = -cash if cash else ZERO
            self.add(f"{date} assigned call {acct}", stock=stock, option=option_clear, cash=cash)

        self.last_date = date
        if self.in_trip and self.shares == 0 and self.option == 0 and kind == "stock_sale":
            completed.append(self.finish(open_trip=False, end_date=date))
        return completed

    def finish(self, open_trip, end_date=None):
        label = "current" if open_trip else "final"
        if open_trip:
            target_stock = money(self.current_values.get("stock", ZERO))
            target_option = money(self.current_values.get("option", ZERO))
            stock_change = money(target_stock - self.stock)
            option_change = money(target_option - self.option)
            if stock_change or option_change:
                self.add(f"{end_date or self.last_date} market current", stock=stock_change, option=option_change)
        title_end = end_date or self.last_date or self.begin_date
        title = (
            f"{self.symbol} round trip {self.trip_no}: {self.begin_date} through "
            f"{title_end}, {'open' if open_trip else 'closed'}"
        )
        rendered = render_table(title, self.rows, label, self.stock, self.option, self.cash)
        self.in_trip = False
        self.rows = []
        self.stock = ZERO
        self.option = ZERO
        self.cash = ZERO
        self.shares = Decimal("0")
        self.begin_date = None
        return rendered


def render_table(title, rows, final_label, stock, option, cash):
    rows = coalesce_rows(rows)
    lines = [
        title,
        "",
        f"{'':28s}{'Stock':>12s}{'Option':>14s}{'Cash':>14s}{'Total':>14s}",
    ]
    for label, row_stock, row_option, row_cash, row_total in rows:
        lines.append(
            f"{label[:28]:28s}{fmt(row_stock):>12s}{fmt(row_option):>14s}"
            f"{fmt(row_cash):>14s}{fmt(row_total):>14s}"
        )
    total = money(stock + option + cash)
    lines.append(
        f"{final_label:28s}{fmt(stock):>12s}{fmt(option):>14s}"
        f"{fmt(cash):>14s}{fmt(total):>14s}"
    )
    contributions = money(
        sum(row_total for label, _stock, _option, _cash, row_total in rows if label == "begin" or "cash contribution" in label)
    )
    profit = money(total - contributions)
    lines.append(
        f"{'net contributions':28s}{fmt(ZERO):>12s}{fmt(ZERO):>14s}"
        f"{fmt(contributions):>14s}{fmt(contributions):>14s}"
    )
    lines.append(
        f"{'profit':28s}{fmt(stock):>12s}{fmt(option):>14s}"
        f"{fmt(money(cash - contributions)):>14s}{fmt(profit):>14s}"
    )
    return "\n".join(lines)


def coalesce_rows(rows):
    combined = []
    for label, stock, option, cash, total in rows:
        if combined and combined[-1][0] == label:
            prior_label, prior_stock, prior_option, prior_cash, prior_total = combined[-1]
            combined[-1] = (
                prior_label,
                money(prior_stock + stock),
                money(prior_option + option),
                money(prior_cash + cash),
                money(prior_total + total),
            )
        else:
            combined.append((label, stock, option, cash, total))
    return combined


def main():
    parser = argparse.ArgumentParser(
        description="Render VOO/UPRO Fidelity transactions as ledger-style round trips."
    )
    parser.add_argument("--positions", action="append", help="optional Fidelity portfolio positions CSV for open current marks; may be repeated")
    parser.add_argument("--symbol", action="append", choices=SYMBOLS, help="symbol to include; defaults to VOO and UPRO")
    parser.add_argument("--from-date", help="first Run Date to include, YYYYMMDD or MM/DD/YYYY")
    parser.add_argument("--to-date", help="last Run Date to include, YYYYMMDD or MM/DD/YYYY")
    parser.add_argument("--as-of", help="date to use for open-position current marks, YYYYMMDD or MM/DD/YYYY")
    args = parser.parse_args()

    symbols = tuple(args.symbol or SYMBOLS)
    from_date = normalize_date(args.from_date)
    to_date = normalize_date(args.to_date)
    as_of = normalize_date(args.as_of)
    positions = read_positions(args.positions)
    rows = [
        row for row in read_transaction_rows(sys.stdin)
        if row_symbol(row) in symbols and action_kind(row) is not None
    ]
    if from_date:
        rows = [row for row in rows if date_key(row["Run Date"]) >= from_date]
    if to_date:
        rows = [row for row in rows if date_key(row["Run Date"]) <= to_date]
    rows = merge_split_stock_rows(rows)
    rows.sort(key=lambda row: (date_key(row["Run Date"]), row_symbol(row), sort_order(row), row.get("Account", ""), row.get("Action", ""), row.get("Amount ($)", "")))

    ledgers = {symbol: Ledger(symbol, positions.get(symbol, {})) for symbol in symbols}
    rendered = []
    for row in rows:
        symbol = row_symbol(row)
        rendered.extend(ledgers[symbol].process(row))
    latest_date = as_of or max((date_key(row["Run Date"]) for row in rows), default=None)
    for symbol in symbols:
        ledger = ledgers[symbol]
        if ledger.in_trip:
            rendered.append(ledger.finish(open_trip=True, end_date=latest_date))

    print("\n\n".join(rendered))


if __name__ == "__main__":
    main()
