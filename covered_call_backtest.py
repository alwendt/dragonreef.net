#!/usr/bin/env python3

#
# Examples:
#
# for delta in 0.10 0.11 0.12 0.13 0.14 0.15 0.16 0.17 0.18 0.19 0.20 0.21
# do
#     python3 covered_call_backtest.py --ticker=VOO --ma=200 --option-days=10 --delta=$delta --delay-days=10
# done
#
# --ticker       underlying ticker, e.g. VOO, TSLA, QQQ
# --ma           moving-average window in trading days
# --option-days  selling call options expiring in this many days
# --delay-days   when moving average takes you out of market, wait this long before coming back
# --delta        sell options at this delta
#

import argparse
import io
from pathlib import Path
from math import log, sqrt, exp, erf

import numpy as np
import pandas as pd
import requests

try:
    import yfinance as yf
except ImportError:
    yf = None


RISK_FREE = 0.035
DAYS_PER_YEAR = 252


def norm_cdf(x):
    return 0.5 * (1.0 + erf(x / sqrt(2.0)))


def load_from_stooq(ticker):
    symbol = ticker.lower() + ".us"
    url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "https://stooq.com/",
        "Accept": "text/csv,text/plain,application/octet-stream,*/*",
    }

    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()

    text = resp.text.strip()
    if not text:
        raise RuntimeError(f"Empty response from Stooq for {ticker}")

    if "<html" in text.lower():
        raise RuntimeError(f"Got HTML instead of CSV from Stooq for {ticker}")

    df = pd.read_csv(io.StringIO(text))
    if df.empty:
        raise RuntimeError(f"No rows returned from Stooq for {ticker}")

    df.columns = [str(c).lower() for c in df.columns]

    required = {"date", "open", "high", "low", "close"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(
            f"Missing columns from Stooq for {ticker}: {sorted(missing)}"
        )

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


def load_from_yahoo(ticker):
    if yf is None:
        raise RuntimeError(
            "yfinance is not installed. Install it with: pip install yfinance"
        )

    df = yf.download(
        ticker,
        period="max",
        interval="1d",
        auto_adjust=False,
        progress=False,
        actions=False,
        threads=False,
    )

    if df is None or df.empty:
        raise RuntimeError(f"No rows returned from Yahoo for {ticker}")

    df = df.reset_index()

    # Flatten MultiIndex columns if yfinance returns them
    flat_cols = []
    for col in df.columns:
        if isinstance(col, tuple):
            parts = [str(x) for x in col if str(x) != ""]
            flat_cols.append("_".join(parts))
        else:
            flat_cols.append(str(col))
    df.columns = flat_cols

    colmap = {}
    for col in df.columns:
        low = col.lower()
        if low == "date":
            colmap[col] = "date"
        elif low.startswith("open"):
            colmap[col] = "open"
        elif low.startswith("high"):
            colmap[col] = "high"
        elif low.startswith("low"):
            colmap[col] = "low"
        elif low.startswith("close"):
            colmap[col] = "close"
        elif low.startswith("adj close"):
            colmap[col] = "adj_close"
        elif low.startswith("volume"):
            colmap[col] = "volume"

    df = df.rename(columns=colmap)

    required = {"date", "open", "high", "low", "close"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(
            f"Missing columns from Yahoo for {ticker}: {sorted(missing)}"
        )

    keep = [c for c in ["date", "open", "high", "low", "close", "volume"] if c in df.columns]
    df = df[keep].copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


def load_data(ticker):
    stooq_err = None
    try:
        df = load_from_stooq(ticker)
        print(f"data_source=stooq")
        return df
    except Exception as e:
        stooq_err = e
        print(f"warning: stooq failed for {ticker}: {e}")

    try:
        df = load_from_yahoo(ticker)
        print(f"data_source=yahoo")
        return df
    except Exception as yahoo_err:
        raise RuntimeError(
            f"Both sources failed for {ticker}. "
            f"Stooq error: {stooq_err}. "
            f"Yahoo error: {yahoo_err}"
        )


def load_from_csv(csv_path):
    path = Path(csv_path)
    df = pd.read_csv(path)
    if df.empty:
        raise RuntimeError(f"No rows found in CSV: {path}")

    df.columns = [str(c).lower().strip() for c in df.columns]

    required = {"date", "open", "high", "low", "close"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(
            f"Missing columns in CSV {path}: {sorted(missing)}"
        )

    keep = [c for c in ["date", "open", "high", "low", "close", "volume"] if c in df.columns]
    df = df[keep].copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


def call_delta(S, K, T, r, sigma):
    if sigma <= 0 or T <= 0:
        return 0.0
    d1 = (log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    return norm_cdf(d1)


def find_strike(S, T, r, sigma, target_delta):
    best_k = S
    best_err = 999.0

    for k_mult in np.linspace(0.90, 1.20, 400):
        K = S * k_mult
        d = call_delta(S, K, T, r, sigma)
        err = abs(d - target_delta)
        if err < best_err:
            best_err = err
            best_k = K

    return best_k


def call_price(S, K, T, r, sigma):
    if sigma <= 0 or T <= 0:
        return max(0.0, S - K)

    d1 = (log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)

    return S * norm_cdf(d1) - K * exp(-r * T) * norm_cdf(d2)


def stats(df):
    total = df["equity"].iloc[-1]
    years = len(df) / DAYS_PER_YEAR
    cagr = total ** (1.0 / years) - 1.0

    peak = df["equity"].cummax()
    dd = df["equity"] / peak - 1.0

    return {
        "CAGR": cagr,
        "MaxDD": dd.min(),
        "Final": total,
    }


def simulate(df, delay_days, target_delta, option_days, ma_window):
    df = df.copy()
    df["sma"] = df["close"].rolling(ma_window).mean()
    df["daily_ret"] = df["close"].pct_change()

    in_market = True
    days_above = 0
    equity = 1.0
    equity_curve = [equity]

    i = 1
    while i < len(df):
        row = df.iloc[i]

        if pd.isna(row["sma"]):
            equity_curve.append(equity)
            i += 1
            continue

        price = float(row["close"])
        sma = float(row["sma"])

        if in_market and price < sma:
            in_market = False
            days_above = 0

        if price > sma:
            days_above += 1
        else:
            days_above = 0

        if not in_market and days_above >= delay_days:
            in_market = True

        if in_market:
            start_idx = i
            end_idx = min(i + option_days, len(df) - 1)

            S_start = float(df.iloc[start_idx]["close"])
            S_end = float(df.iloc[end_idx]["close"])

            hist = df["daily_ret"].iloc[max(1, start_idx - 20):start_idx].dropna()
            sigma = float(hist.std() * sqrt(DAYS_PER_YEAR)) if len(hist) >= 2 else 0.20

            T = (end_idx - start_idx) / DAYS_PER_YEAR
            if T <= 0:
                T = 1.0 / DAYS_PER_YEAR

            K = find_strike(S_start, T, RISK_FREE, sigma, target_delta)
            premium = call_price(S_start, K, T, RISK_FREE, sigma)

            period_ret = (min(S_end, K) + premium) / S_start

            for _ in range(start_idx, end_idx):
                equity_curve.append(equity)

            equity *= period_ret
            equity_curve.append(equity)

            i = end_idx + 1
        else:
            equity *= (1.0 + RISK_FREE / DAYS_PER_YEAR)
            equity_curve.append(equity)
            i += 1

    out = df.iloc[:len(equity_curve)].copy()
    out["equity"] = equity_curve
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", type=str, default="VOO")
    parser.add_argument("--csv", type=str, default="")
    parser.add_argument("--ma", type=int, default=200)
    parser.add_argument("--option-days", type=int, default=10)
    parser.add_argument("--delta", type=float, default=0.30)
    parser.add_argument("--delay-days", type=int, default=5)

    args = parser.parse_args()

    if args.csv:
        df = load_from_csv(args.csv)
        print(f"data_source=csv path={args.csv}")
    else:
        df = load_data(args.ticker)

    bt = simulate(
        df,
        delay_days=args.delay_days,
        target_delta=args.delta,
        option_days=args.option_days,
        ma_window=args.ma,
    )

    s = stats(bt)

    print(
        f"ticker={args.ticker} "
        f"ma={args.ma} "
        f"option_days={args.option_days} "
        f"delay_days={args.delay_days} "
        f"delta={args.delta:.2f} "
        f"CAGR={s['CAGR']:.4%} "
        f"MaxDD={s['MaxDD']:.4%} "
        f"Final={s['Final']:.4f}"
    )


if __name__ == "__main__":
    main()
