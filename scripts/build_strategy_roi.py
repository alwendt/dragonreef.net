#!/usr/bin/env python3
import argparse
import csv
import glob
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path


CASH_RATE = Decimal("0.04")
DAYS_PER_YEAR = Decimal("252")


def money(value):
    return Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def dec(text):
    text = (
        str(text or "")
        .replace(",", "")
        .replace('"', "")
        .replace("$", "")
        .replace("%", "")
        .replace("+", "")
        .strip()
    )
    if text in {"", "--"}:
        return Decimal("0")
    return Decimal(text) if text else Decimal("0")


def date_key(text):
    return datetime.strptime(text, "%m/%d/%Y").strftime("%Y%m%d")


def fmt(value):
    value = Decimal(value)
    if value == value.to_integral_value():
        return str(value.quantize(Decimal("1")))
    return format(value.normalize(), "f")


def read_fidelity_rows(directory):
    rows = []
    seen = set()
    for path in sorted(glob.glob(str(Path(directory) / "*.csv"))):
        lines = Path(path).read_text(encoding="utf-8-sig").splitlines()
        start = next((i for i, line in enumerate(lines) if line.startswith("Run Date,")), None)
        if start is None:
            continue
        reader = csv.DictReader(lines[start:])
        for row in reader:
            if not row.get("Run Date") or not row.get("Action"):
                continue
            if row["Run Date"].startswith("The data"):
                continue
            key = tuple(row.get(field, "") for field in reader.fieldnames)
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)
    return rows


def read_price_cache(path):
    rows = []
    with Path(path).open(newline="") as handle:
        for row in csv.DictReader(handle):
            rows.append((row["date"].replace("-", ""), dec(row["close"])))
    if not rows:
        raise RuntimeError(f"no price rows in {path}")
    return rows


def read_positions(path):
    if not path:
        return []
    position_path = Path(path)
    if not position_path.exists():
        return []
    lines = position_path.read_text(encoding="utf-8-sig").splitlines()
    start = next((i for i, line in enumerate(lines) if line.startswith("Account number,")), None)
    if start is None:
        return []
    rows = []
    reader = csv.DictReader(lines[start:])
    for row in reader:
        if not row.get("Account name") or not row.get("Symbol"):
            continue
        rows.append(row)
    return rows


def position_values(rows, symbol):
    values = defaultdict(lambda: {"current": Decimal("0"), "cost": Decimal("0")})
    for row in rows:
        haystack = " ".join([row.get("Symbol", ""), row.get("Description", "")]).upper()
        if symbol.upper() not in haystack:
            continue
        acct = account_name(row.get("Account name", ""))
        values[acct]["current"] += money(dec(row.get("Current value", "0")))
        if row.get("Cost basis total"):
            sign = Decimal("-1") if dec(row.get("Quantity", "0")) < 0 else Decimal("1")
            values[acct]["cost"] += sign * money(dec(row.get("Cost basis total", "0")))
    return values


def account_name(fidelity_account):
    if fidelity_account == "Alan IRA":
        return "401k"
    if fidelity_account == "Individual - TOD":
        return "indiv"
    return fidelity_account.replace(" ", "_")


def relevant_symbol(row, symbol):
    haystack = " ".join([
        row.get("Symbol", ""),
        row.get("Action", ""),
        row.get("Description", ""),
    ]).upper()
    return symbol.upper() in haystack


def is_option_row(row):
    haystack = " ".join([
        row.get("Symbol", ""),
        row.get("Action", ""),
        row.get("Description", ""),
    ]).upper()
    return " CALL " in f" {haystack} " or " PUT " in f" {haystack} "


def trade_action(row):
    action = row.get("Action", "").upper()
    amount = dec(row.get("Amount ($)", "0"))
    quantity = dec(row.get("Quantity", "0"))
    if is_option_row(row) or "DIVIDEND" in action:
        return "div"
    if "YOU BOUGHT" in action or amount < 0 or quantity > 0:
        return "buy"
    if "YOU SOLD" in action or amount > 0 or quantity < 0:
        return "sell"
    return None


def trade_sort_order(row):
    action = trade_action(row)
    if action == "sell":
        return 0
    if action == "buy":
        return 1
    if action == "div":
        return 2
    return 2


class StrategyBuilder:
    def __init__(self, symbol, price_rows, final_values):
        self.symbol = f"{symbol}STRAT"
        self.price_rows = price_rows
        self.trading_dates = [date for date, _ in price_rows]
        self.latest_date, self.latest_price = price_rows[-1]
        self.lines_by_date = defaultdict(list)
        self.has_transactions = defaultdict(bool)
        self.cash = defaultdict(lambda: Decimal("0"))
        self.last_accrual_date = {}
        self.final_values = final_values

    def add_line(self, date, action, stock, acct, shares, amount):
        self.lines_by_date[date].append(
            f"{action}\t{stock}\t{acct}\t{fmt(shares)}\t{fmt(amount)}"
        )

    def trading_days_between(self, start, end):
        if start is None:
            return 0
        return sum(1 for date in self.trading_dates if start < date <= end)

    def accrue_cash(self, acct, date):
        prior = self.last_accrual_date.get(acct)
        days = self.trading_days_between(prior, date)
        if days > 0 and self.cash[acct] > 0:
            factor = (Decimal("1") + (CASH_RATE / DAYS_PER_YEAR)) ** days
            before = self.cash[acct]
            self.cash[acct] = money(self.cash[acct] * factor)
            interest = money(self.cash[acct] - before)
            if interest > 0:
                self.add_line(date, "div", self.symbol, acct, interest, interest)
        self.last_accrual_date[acct] = date

    def process_trade(self, row):
        acct = account_name(row.get("Account", ""))
        date = date_key(row["Run Date"])
        self.accrue_cash(acct, date)

        action = trade_action(row)
        if action is None:
            return

        amount = money(abs(dec(row.get("Amount ($)", "0"))))
        if amount <= 0:
            return

        if action == "div":
            raw_amount = money(dec(row.get("Amount ($)", "0")))
            if raw_amount != 0:
                self.add_line(date, "div", self.symbol, acct, raw_amount, raw_amount)
            return

        if action == "buy":
            self.add_line(date, "buy", self.symbol, acct, amount, amount)
            self.cash[acct] = max(Decimal("0"), money(self.cash[acct] - amount))
        else:
            self.add_line(date, "sell", self.symbol, acct, amount, amount)
            self.cash[acct] += amount
        self.has_transactions[acct] = True

    def finish(self):
        for acct, values in self.final_values.items():
            if not self.has_transactions[acct] and values["cost"] > 0:
                deposit = money(values["cost"])
                self.add_line(self.latest_date, "buy", self.symbol, acct, deposit, deposit)
                self.has_transactions[acct] = True
        for acct in sorted(set(self.has_transactions) | set(self.final_values)):
            self.accrue_cash(acct, self.latest_date)
            final_value = self.final_values.get(acct, {}).get("current", Decimal("0"))
            if final_value != 0:
                self.add_line(self.latest_date, "bal", self.symbol, acct, final_value, final_value)

    def render(self):
        chunks = []
        for date in sorted(self.lines_by_date):
            chunks.append(f"date\t{date}")
            chunks.extend(self.lines_by_date[date])
        return "\n".join(chunks) + ("\n" if chunks else "")


def build(symbol, rows, price_cache, positions):
    builder = StrategyBuilder(symbol, read_price_cache(price_cache), position_values(positions, symbol))
    for row in sorted(rows, key=lambda item: (date_key(item["Run Date"]), item.get("Account", ""), trade_sort_order(item), item.get("Action", ""), item.get("Amount ($)", ""))):
        if relevant_symbol(row, symbol):
            builder.process_trade(row)
    builder.finish()
    return builder.render(), builder


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--transactions", default="/home/alan/fin/transactions")
    parser.add_argument("--outdir", default="/tmp")
    parser.add_argument("--voo-cache", default="/home/alan/investing/chart-cache/VOO.csv")
    parser.add_argument("--upro-cache", default="/home/alan/investing/chart-cache/UPRO.csv")
    parser.add_argument("--positions", default="/home/alan/fin/transactions/Portfolio_Positions_Aug-08-2026.csv")
    args = parser.parse_args()

    rows = read_fidelity_rows(args.transactions)
    positions = read_positions(args.positions)
    outputs = {
        "voostrat": build("VOO", rows, args.voo_cache, positions)[0],
        "uprostrat": build("UPRO", rows, args.upro_cache, positions)[0],
    }
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    for name, text in outputs.items():
        (outdir / name).write_text(text, encoding="utf-8")
        print(outdir / name)


if __name__ == "__main__":
    main()
