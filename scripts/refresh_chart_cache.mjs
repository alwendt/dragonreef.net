#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { chromium } from "playwright-core";

const DEFAULT_OUTPUT_DIR = path.resolve(process.env.HOME || ".", "investing", "chart-cache");
const CHROME_PATHS = [
  "/usr/bin/google-chrome",
  "/usr/bin/google-chrome-stable",
  "/usr/bin/chromium-browser",
  "/usr/bin/chromium"
];

function usage() {
  console.error("usage: refresh_chart_cache.mjs [--output-dir DIR] TICKER [TICKER ...]");
  process.exit(2);
}

function normalizeTicker(raw) {
  const cleaned = String(raw || "").toUpperCase().trim().replace(/[^A-Z0-9.\-]/g, "");
  return cleaned || "VOO";
}

function stooqSymbol(ticker) {
  return ticker.includes(".") ? ticker.toLowerCase() : ticker.toLowerCase() + ".us";
}

function parseArgs(argv) {
  let outputDir = DEFAULT_OUTPUT_DIR;
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
    if (arg.startsWith("-")) {
      usage();
    }
    tickers.push(normalizeTicker(arg));
  }

  if (tickers.length === 0) {
    usage();
  }

  return { outputDir, tickers };
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

function validateCsv(text, ticker) {
  const trimmed = String(text || "").trim();
  if (!trimmed) {
    throw new Error(`Empty CSV body for ${ticker}`);
  }
  const lines = trimmed.split("\n");
  if (lines.length < 2) {
    throw new Error(`Not enough CSV lines for ${ticker}`);
  }
  if (!/^date,open,high,low,close,volume$/i.test(lines[0].trim())) {
    throw new Error(`Unexpected CSV header for ${ticker}: ${lines[0]}`);
  }
  return trimmed + "\n";
}

async function fetchTicker(page, ticker) {
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
      text: await response.text()
    };
  }, symbol);

  if (result.status !== 200) {
    throw new Error(`HTTP ${result.status} for ${ticker}`);
  }

  return validateCsv(result.text, ticker);
}

async function main() {
  const { outputDir, tickers } = parseArgs(process.argv.slice(2));
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
      const csv = await fetchTicker(page, ticker);
      const outputPath = path.join(outputDir, `${ticker}.csv`);
      await fs.writeFile(outputPath, csv, "utf8");
      console.log(`${ticker} -> ${outputPath}`);
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
