#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { chromium } from "playwright-core";

const DEFAULT_OUTPUT_DIR = path.resolve(process.env.HOME || ".", "investing", "chart-cache");
const YAHOO_RANGE = "3mo";
const CSV_HEADER = "date,open,high,low,close,volume";
const CHROME_PATHS = [
  "/usr/bin/google-chrome",
  "/usr/bin/google-chrome-stable",
  "/usr/bin/chromium-browser",
  "/usr/bin/chromium"
];

function usage() {
  console.error("usage: refresh_chart_cache.mjs [--output-dir DIR] [--source stooq|yahoo] TICKER [TICKER ...]");
  process.exit(2);
}

function normalizeTicker(raw) {
  const cleaned = String(raw || "").toUpperCase().trim().replace(/[^A-Z0-9.\-]/g, "");
  return cleaned || "VOO";
}

function stooqSymbol(ticker) {
  return ticker.includes(".") ? ticker.toLowerCase() : ticker.toLowerCase() + ".us";
}

function toCsvNumber(value) {
  return Number.isFinite(value) ? String(value) : "";
}

function parseArgs(argv) {
  let outputDir = DEFAULT_OUTPUT_DIR;
  let source = "stooq";
  const tickers = [];

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--output-dir") {
      if (i + 1 >= argv.length) {
        usage();
      }
      outputDir = path.resolve(argv[i + 1]);
      i += 1;
      continue;
    }
    if (arg === "--source") {
      if (i + 1 >= argv.length) {
        usage();
      }
      source = String(argv[i + 1]).toLowerCase();
      i += 1;
      continue;
    }
    if (arg.startsWith("--source=")) {
      source = String(arg.slice("--source=".length)).toLowerCase();
      continue;
    }
    if (arg.startsWith("-")) {
      usage();
    }
    tickers.push(normalizeTicker(arg));
  }

  if (tickers.length === 0) {
    usage();
  }

  if (!["stooq", "yahoo"].includes(source)) {
    usage();
  }

  return { outputDir, tickers, source };
}

async function findChromePath() {
  for (const candidate of CHROME_PATHS) {
    try {
      await fs.access(candidate);
      return candidate;
    } catch {
      continue;
    }
  }
  throw new Error("Could not find a Chrome/Chromium executable.");
}

function summarizeText(text, limit = 240) {
  return String(text || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, limit);
}

function csvLinesToObjects(text) {
  const trimmed = String(text || "").trim();
  if (!trimmed) {
    return [];
  }

  const lines = trimmed.split("\n");
  if (lines.length <= 1) {
    return [];
  }

  const out = [];
  for (let i = 1; i < lines.length; i += 1) {
    const line = lines[i].trim();
    if (!line) {
      continue;
    }
    const parts = line.split(",");
    if (parts.length < 6) {
      continue;
    }
    out.push({
      date: parts[0],
      open: parts[1],
      high: parts[2],
      low: parts[3],
      close: parts[4],
      volume: parts[5]
    });
  }
  return out;
}

function mergeCsv(existingText, incomingText) {
  const merged = new Map();

  for (const row of csvLinesToObjects(existingText)) {
    merged.set(row.date, row);
  }
  for (const row of csvLinesToObjects(incomingText)) {
    merged.set(row.date, row);
  }

  const rows = Array.from(merged.values()).sort((a, b) => a.date.localeCompare(b.date));
  const lines = [CSV_HEADER];
  for (const row of rows) {
    lines.push([row.date, row.open, row.high, row.low, row.close, row.volume].join(","));
  }
  return lines.join("\n") + "\n";
}

function validateCsv(text, ticker, meta = {}) {
  const trimmed = String(text || "").trim();
  if (!trimmed) {
    throw new Error(
      `Empty CSV body for ${ticker}` +
      (meta.status ? ` status=${meta.status}` : "") +
      (meta.contentType ? ` content_type=${meta.contentType}` : "")
    );
  }
  const lines = trimmed.split("\n");
  if (lines.length < 2) {
    throw new Error(
      `Not enough CSV lines for ${ticker}` +
      (meta.status ? ` status=${meta.status}` : "") +
      (meta.contentType ? ` content_type=${meta.contentType}` : "") +
      ` body_len=${trimmed.length} preview="${summarizeText(trimmed)}"`
    );
  }
  if (!new RegExp(`^${CSV_HEADER}$`, "i").test(lines[0].trim())) {
    throw new Error(
      `Unexpected CSV header for ${ticker}` +
      (meta.status ? ` status=${meta.status}` : "") +
      (meta.contentType ? ` content_type=${meta.contentType}` : "") +
      ` header="${lines[0].trim()}" preview="${summarizeText(trimmed)}"`
    );
  }
  return trimmed + "\n";
}

async function fetchTickerFromStooq(page, ticker) {
  const symbol = stooqSymbol(ticker);
  const rootUrl = "https://stooq.com/";
  const quoteUrl = `https://stooq.com/q/?s=${symbol}`;
  const historyUrl = `https://stooq.com/q/d/?s=${symbol}`;

  await page.goto(rootUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
  await page.waitForTimeout(1500);

  await page.goto(quoteUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
  await page.waitForTimeout(1500);

  await page.goto(historyUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
  await page.waitForTimeout(2500);

  const result = await page.evaluate(async (symbolValue) => {
    const response = await fetch(`/q/d/l/?s=${symbolValue}&i=d`, {
      method: "GET",
      credentials: "include",
      cache: "no-store"
    });
    return {
      status: response.status,
      contentType: response.headers.get("content-type") || "",
      text: await response.text()
    };
  }, symbol);

  if (result.status !== 200) {
    throw new Error(
      `HTTP ${result.status} for ${ticker}` +
      (result.contentType ? ` content_type=${result.contentType}` : "") +
      ` body_len=${result.text.length} preview="${summarizeText(result.text)}"`
    );
  }

  return validateCsv(result.text, ticker, {
    status: result.status,
    contentType: result.contentType
  });
}

function validateYahooChartJson(payload, ticker) {
  const chart = payload?.chart;
  const result = chart?.result?.[0];
  const quote = result?.indicators?.quote?.[0];
  const timestamps = result?.timestamp;

  if (!result || !quote || !Array.isArray(timestamps)) {
    const message = chart?.error?.description || "missing chart result";
    throw new Error(`Yahoo chart response invalid for ${ticker}: ${message}`);
  }

  const opens = quote.open || [];
  const highs = quote.high || [];
  const lows = quote.low || [];
  const closes = quote.close || [];
  const volumes = quote.volume || [];

  const lines = ["date,open,high,low,close,volume"];
  for (let i = 0; i < timestamps.length; i += 1) {
    const ts = timestamps[i];
    const open = opens[i];
    const high = highs[i];
    const low = lows[i];
    const close = closes[i];
    const volume = volumes[i];
    if (![open, high, low, close].every(Number.isFinite)) {
      continue;
    }
    const date = new Date(ts * 1000).toISOString().slice(0, 10);
    lines.push([
      date,
      toCsvNumber(open),
      toCsvNumber(high),
      toCsvNumber(low),
      toCsvNumber(close),
      toCsvNumber(volume)
    ].join(","));
  }

  return validateCsv(lines.join("\n"), ticker, {
    status: 200,
    contentType: "text/csv; charset=UTF-8"
  });
}

async function fetchTickerFromYahoo(page, ticker) {
  const historyUrl = `https://finance.yahoo.com/quote/${encodeURIComponent(ticker)}/history?p=${encodeURIComponent(ticker)}`;

  await page.goto(historyUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
  await page.waitForTimeout(2500);

  const result = await page.evaluate(async ({ symbolValue, rangeValue }) => {
    const url =
      `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbolValue)}` +
      `?range=${encodeURIComponent(rangeValue)}&interval=1d&includePrePost=false&events=div%2Csplits`;
    const response = await fetch(url, {
      method: "GET",
      credentials: "include",
      cache: "no-store"
    });
    return {
      status: response.status,
      contentType: response.headers.get("content-type") || "",
      text: await response.text()
    };
  }, { symbolValue: ticker, rangeValue: YAHOO_RANGE });

  if (result.status !== 200) {
    throw new Error(
      `HTTP ${result.status} for ${ticker}` +
      (result.contentType ? ` content_type=${result.contentType}` : "") +
      ` body_len=${result.text.length} preview="${summarizeText(result.text)}"`
    );
  }

  let payload;
  try {
    payload = JSON.parse(result.text);
  } catch (error) {
    throw new Error(
      `Yahoo response was not JSON for ${ticker}` +
      (result.contentType ? ` content_type=${result.contentType}` : "") +
      ` body_len=${result.text.length} preview="${summarizeText(result.text)}"`
    );
  }

  return validateYahooChartJson(payload, ticker);
}

async function fetchTicker(page, ticker, source) {
  if (source === "yahoo") {
    return fetchTickerFromYahoo(page, ticker);
  }
  return fetchTickerFromStooq(page, ticker);
}

async function main() {
  const { outputDir, tickers, source } = parseArgs(process.argv.slice(2));
  const executablePath = await findChromePath();
  await fs.mkdir(outputDir, { recursive: true });

  const browser = await chromium.launch({
    headless: true,
    executablePath,
    args: ["--disable-blink-features=AutomationControlled"]
  });

  const context = await browser.newContext({
    viewport: { width: 1440, height: 1100 },
    locale: "en-US",
    userAgent: "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    acceptDownloads: true
  });

  const page = await context.newPage();

  try {
    for (const ticker of tickers) {
      const csv = await fetchTicker(page, ticker, source);
      const outputPath = path.join(outputDir, `${ticker}.csv`);
      let mergedCsv = csv;
      let mergeNote = "new";

      try {
        const existingCsv = await fs.readFile(outputPath, "utf8");
        mergedCsv = mergeCsv(existingCsv, csv);
        mergeNote = "merged";
      } catch (error) {
        if (error?.code !== "ENOENT") {
          throw error;
        }
      }

      await fs.writeFile(outputPath, mergedCsv, "utf8");
      console.log(`${ticker} (${source}, ${mergeNote}) -> ${outputPath}`);
    }
  } finally {
    await context.close();
    await browser.close();
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
