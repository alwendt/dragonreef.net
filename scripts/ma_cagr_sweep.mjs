#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";

const DAYS_PER_YEAR = 252;
const CASH_RATE = 0.04;
const DEFAULT_DELTA = 0.20;
const DEFAULT_DELAY_DAYS = 10;
const DEFAULT_SURVIVAL_THRESHOLD = 0.50;
const DEFAULT_STARTING_CAPITAL = 10000;

function usage() {
  console.error(
    "usage: ma_cagr_sweep.mjs --input FILE --output FILE " +
    "[--ticker TICKER] [--min-days N] [--max-days N] " +
    "[--delta X] [--delay-days N] [--survival-prob X] [--starting-capital X]"
  );
  process.exit(2);
}

function parseArgs(argv) {
  const args = {
    ticker: "VOO",
    minDays: 1,
    maxDays: 210,
    delta: DEFAULT_DELTA,
    delayDays: DEFAULT_DELAY_DAYS,
    survivalProb: DEFAULT_SURVIVAL_THRESHOLD,
    startingCapital: DEFAULT_STARTING_CAPITAL,
    input: "",
    output: ""
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = argv[i + 1];
    if (arg === "--ticker" && next) {
      args.ticker = next;
      i += 1;
    } else if (arg === "--input" && next) {
      args.input = next;
      i += 1;
    } else if (arg === "--output" && next) {
      args.output = next;
      i += 1;
    } else if (arg === "--min-days" && next) {
      args.minDays = Number.parseInt(next, 10);
      i += 1;
    } else if (arg === "--max-days" && next) {
      args.maxDays = Number.parseInt(next, 10);
      i += 1;
    } else if (arg === "--delta" && next) {
      args.delta = Number.parseFloat(next);
      i += 1;
    } else if (arg === "--delay-days" && next) {
      args.delayDays = Number.parseInt(next, 10);
      i += 1;
    } else if (arg === "--survival-prob" && next) {
      args.survivalProb = Number.parseFloat(next);
      i += 1;
    } else if (arg === "--starting-capital" && next) {
      args.startingCapital = Number.parseFloat(next);
      i += 1;
    } else {
      usage();
    }
  }

  if (!args.input || !args.output) {
    usage();
  }
  if (!Number.isFinite(args.minDays) || !Number.isFinite(args.maxDays) || args.minDays < 1 || args.maxDays < args.minDays) {
    throw new Error("invalid day range");
  }
  return args;
}

function parseCsv(text) {
  const lines = text.trim().split(/\r?\n/);
  if (lines.length < 2) {
    throw new Error("CSV has no data rows");
  }

  const header = lines[0].toLowerCase();
  const columns = header.split(",");
  const dateIndex = columns.indexOf("date");
  const closeIndex = columns.indexOf("close");
  if (dateIndex === -1 || closeIndex === -1) {
    throw new Error("CSV must contain date and close columns");
  }

  const points = [];
  for (let i = 1; i < lines.length; i += 1) {
    const row = lines[i].trim();
    if (!row) {
      continue;
    }
    const parts = row.split(",");
    const dateText = parts[dateIndex];
    const closeText = parts[closeIndex];
    if (!dateText || closeText === undefined) {
      continue;
    }
    const [yearText, monthText, dayText] = dateText.split("-");
    const year = Number(yearText);
    const month = Number(monthText) - 1;
    const day = Number(dayText);
    const close = Number(closeText);
    if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day) || !Number.isFinite(close)) {
      continue;
    }
    points.push({
      date: new Date(year, month, day),
      close
    });
  }

  points.sort((left, right) => left.date - right.date);
  return points;
}

function computeSma(points, windowSize) {
  const sma = new Array(points.length).fill(null);
  let sum = 0;
  for (let i = 0; i < points.length; i += 1) {
    sum += points[i].close;
    if (i >= windowSize) {
      sum -= points[i - windowSize].close;
    }
    if (i >= windowSize - 1) {
      sma[i] = sum / windowSize;
    }
  }
  return sma;
}

function computeEma(points, windowSize) {
  const ema = new Array(points.length).fill(null);
  let sum = 0;
  const multiplier = 2 / (windowSize + 1);
  for (let i = 0; i < points.length; i += 1) {
    const close = points[i].close;
    if (i < windowSize) {
      sum += close;
      if (i === windowSize - 1) {
        ema[i] = sum / windowSize;
      }
      continue;
    }
    ema[i] = ((close - ema[i - 1]) * multiplier) + ema[i - 1];
  }
  return ema;
}

function computeMa(points, windowSize, type) {
  return type === "EMA" ? computeEma(points, windowSize) : computeSma(points, windowSize);
}

function erfApprox(x) {
  const sign = x < 0 ? -1 : 1;
  const absX = Math.abs(x);
  const a1 = 0.254829592;
  const a2 = -0.284496736;
  const a3 = 1.421413741;
  const a4 = -1.453152027;
  const a5 = 1.061405429;
  const p = 0.3275911;
  const t = 1 / (1 + p * absX);
  const y = 1 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * Math.exp(-absX * absX);
  return sign * y;
}

function normCdf(x) {
  return 0.5 * (1 + erfApprox(x / Math.sqrt(2)));
}

function callDelta(spot, strike, timeToExpiry, rate, sigma) {
  if (sigma <= 0 || timeToExpiry <= 0 || spot <= 0 || strike <= 0) {
    return 0;
  }
  const d1 = (Math.log(spot / strike) + (rate + 0.5 * sigma * sigma) * timeToExpiry) / (sigma * Math.sqrt(timeToExpiry));
  return normCdf(d1);
}

function callPrice(spot, strike, timeToExpiry, rate, sigma) {
  if (timeToExpiry <= 0 || sigma <= 0 || spot <= 0 || strike <= 0) {
    return Math.max(0, spot - strike);
  }
  const sqrtT = Math.sqrt(timeToExpiry);
  const d1 = (Math.log(spot / strike) + (rate + 0.5 * sigma * sigma) * timeToExpiry) / (sigma * sqrtT);
  const d2 = d1 - sigma * sqrtT;
  return spot * normCdf(d1) - strike * Math.exp(-rate * timeToExpiry) * normCdf(d2);
}

function findStrike(spot, timeToExpiry, rate, sigma, targetDelta) {
  let bestStrike = spot;
  let bestError = Number.POSITIVE_INFINITY;
  for (let i = 0; i < 400; i += 1) {
    const strike = spot * (0.90 + (0.30 * i / 399));
    const error = Math.abs(callDelta(spot, strike, timeToExpiry, rate, sigma) - targetDelta);
    if (error < bestError) {
      bestError = error;
      bestStrike = strike;
    }
  }
  return bestStrike;
}

function sampleStdDev(values) {
  if (values.length < 2) {
    return 0;
  }
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  let squared = 0;
  for (const value of values) {
    const diff = value - mean;
    squared += diff * diff;
  }
  return Math.sqrt(squared / (values.length - 1));
}

function estimateSigma(points, endIndexExclusive) {
  const start = Math.max(1, endIndexExclusive - 20);
  const returns = [];
  for (let i = start; i < endIndexExclusive; i += 1) {
    const prior = points[i - 1].close;
    const current = points[i].close;
    if (prior > 0 && current > 0) {
      returns.push(current / prior - 1);
    }
  }
  if (returns.length < 2) {
    return 0.20;
  }
  return sampleStdDev(returns) * Math.sqrt(DAYS_PER_YEAR);
}

function estimateDrift(points, endIndexExclusive) {
  const start = Math.max(1, endIndexExclusive - 20);
  const logReturns = [];
  for (let i = start; i < endIndexExclusive; i += 1) {
    const prior = points[i - 1].close;
    const current = points[i].close;
    if (prior > 0 && current > 0) {
      logReturns.push(Math.log(current / prior));
    }
  }
  if (logReturns.length < 2) {
    return 0;
  }
  const mean = logReturns.reduce((sum, value) => sum + value, 0) / logReturns.length;
  return mean * DAYS_PER_YEAR;
}

function forcedSaleProbability(price, movingAverage, sigmaAnn, muAnn, days) {
  if (days <= 0) {
    return 0;
  }
  if (!(price > 0) || !(movingAverage > 0)) {
    return 1;
  }
  if (price <= movingAverage) {
    return 1;
  }

  const time = days / DAYS_PER_YEAR;
  const distance = Math.log(price / movingAverage);

  if (sigmaAnn < 1e-12) {
    if (muAnn >= 0) {
      return 0;
    }
    return distance + muAnn * time <= 0 ? 1 : 0;
  }

  const sigmaSquared = sigmaAnn * sigmaAnn;
  const denominator = sigmaAnn * Math.sqrt(time);
  const z1 = (-distance - muAnn * time) / denominator;
  const z2 = (-distance + muAnn * time) / denominator;
  let probability = normCdf(z1) + Math.exp((-2 * muAnn * distance) / sigmaSquared) * normCdf(z2);
  probability = Math.max(0, Math.min(1, probability));
  return probability;
}

function solveLinearSystem3x3(matrix, vector) {
  const a = matrix.map((row) => row.slice());
  const b = vector.slice();
  for (let pivot = 0; pivot < 3; pivot += 1) {
    let maxRow = pivot;
    for (let row = pivot + 1; row < 3; row += 1) {
      if (Math.abs(a[row][pivot]) > Math.abs(a[maxRow][pivot])) {
        maxRow = row;
      }
    }
    if (Math.abs(a[maxRow][pivot]) < 1e-12) {
      return null;
    }
    if (maxRow !== pivot) {
      [a[pivot], a[maxRow]] = [a[maxRow], a[pivot]];
      [b[pivot], b[maxRow]] = [b[maxRow], b[pivot]];
    }
    const pivotValue = a[pivot][pivot];
    for (let col = pivot; col < 3; col += 1) {
      a[pivot][col] /= pivotValue;
    }
    b[pivot] /= pivotValue;
    for (let row = 0; row < 3; row += 1) {
      if (row === pivot) {
        continue;
      }
      const factor = a[row][pivot];
      for (let col = pivot; col < 3; col += 1) {
        a[row][col] -= factor * a[pivot][col];
      }
      b[row] -= factor * b[pivot];
    }
  }
  return b;
}

function fitQuadraticCalibration(samples) {
  if (!samples || samples.length < 12) {
    return null;
  }
  let n = 0;
  let sumX = 0;
  let sumX2 = 0;
  let sumX3 = 0;
  let sumX4 = 0;
  let sumY = 0;
  let sumXY = 0;
  let sumX2Y = 0;
  for (const sample of samples) {
    const x = sample.raw;
    const y = sample.outcome;
    const x2 = x * x;
    n += 1;
    sumX += x;
    sumX2 += x2;
    sumX3 += x2 * x;
    sumX4 += x2 * x2;
    sumY += y;
    sumXY += x * y;
    sumX2Y += x2 * y;
  }
  const coeffs = solveLinearSystem3x3(
    [
      [n, sumX, sumX2],
      [sumX, sumX2, sumX3],
      [sumX2, sumX3, sumX4]
    ],
    [sumY, sumXY, sumX2Y]
  );
  if (!coeffs || coeffs.some((value) => !Number.isFinite(value))) {
    return null;
  }
  return {
    intercept: coeffs[0],
    linear: coeffs[1],
    quadratic: coeffs[2]
  };
}

function calibratedProbability(model, rawValue) {
  if (!model) {
    return Math.max(0, Math.min(1, rawValue));
  }
  const x = Math.max(0, Math.min(1, rawValue));
  const fitted = model.intercept + (model.linear * x) + (model.quadratic * x * x);
  return Math.max(0, Math.min(1, fitted));
}

function buildSurvivalCalibration(points, maSeries, delta) {
  if (!(delta > 0)) {
    return null;
  }
  const models = {};
  for (const termDays of [20, 15, 10, 5]) {
    const samples = [];
    for (let index = 0; index + termDays < points.length; index += 1) {
      const movingAverage = maSeries[index];
      const price = points[index].close;
      if (movingAverage === null || !(price > movingAverage)) {
        continue;
      }
      const sigma = estimateSigma(points, index);
      const drift = estimateDrift(points, index);
      const rawSurvival = 1 - forcedSaleProbability(price, movingAverage, sigma, drift, termDays);
      const expirationIndex = Math.min(index + termDays, points.length - 1);
      const timeToExpiry = Math.max((expirationIndex - index) / DAYS_PER_YEAR, 1 / DAYS_PER_YEAR);
      const strike = findStrike(price, timeToExpiry, CASH_RATE, sigma, delta);
      let forcedBuyback = false;
      for (let future = index + 1; future < expirationIndex; future += 1) {
        const futureMa = maSeries[future];
        if (futureMa !== null && points[future].close < futureMa) {
          forcedBuyback = true;
          break;
        }
      }
      const expiredOtm = !forcedBuyback && points[expirationIndex].close <= strike;
      samples.push({
        raw: rawSurvival,
        outcome: expiredOtm ? 1 : 0
      });
    }
    models[termDays] = fitQuadraticCalibration(samples);
  }
  return models;
}

function chooseSuggestedTerm(points, maSeries, index, targetSurvival, delta, calibration) {
  if (!(delta > 0)) {
    return { termDays: 0 };
  }

  const price = points[index].close;
  const movingAverage = maSeries[index];
  if (movingAverage === null) {
    return { termDays: 20 };
  }
  if (!(price > movingAverage)) {
    return { termDays: 0 };
  }

  const sigma = estimateSigma(points, index);
  const drift = estimateDrift(points, index);
  for (const termDays of [20, 15, 10, 5]) {
    const rawSurvival = 1 - forcedSaleProbability(price, movingAverage, sigma, drift, termDays);
    const survival = calibratedProbability(calibration ? calibration[termDays] : null, rawSurvival);
    if (survival >= targetSurvival) {
      return { termDays };
    }
  }
  return { termDays: 0 };
}

function optionMarketValue(activeOption, spot, index) {
  if (!activeOption) {
    return 0;
  }
  const remainingDays = Math.max(activeOption.expirationIndex - index, 0);
  const timeToExpiry = remainingDays > 0 ? remainingDays / DAYS_PER_YEAR : 0;
  return callPrice(spot, activeOption.strike, timeToExpiry, CASH_RATE, activeOption.sigma);
}

function simulateStrategy(points, config) {
  const maSeries = computeMa(points, config.smaWindow, config.averageType);
  const survivalCalibration = buildSurvivalCalibration(points, maSeries, config.delta);
  let invested = true;
  let shares = config.startingCapital / points[0].close;
  let cash = 0;
  let activeOption = null;
  let reentryIndex = -1;
  const equitySeries = [];

  for (let i = 0; i < points.length; i += 1) {
    const price = points[i].close;
    const movingAverage = maSeries[i];
    const hasMa = movingAverage !== null;
    let justExited = false;

    if (invested && activeOption && activeOption.expirationIndex === i) {
      const settledEquity = cash + shares * price - shares * Math.max(0, price - activeOption.strike);
      shares = settledEquity / price;
      cash = 0;
      activeOption = null;
    }

    if (invested && hasMa && price < movingAverage) {
      const optionValue = optionMarketValue(activeOption, price, i);
      cash = cash + shares * price - shares * optionValue;
      shares = 0;
      activeOption = null;
      invested = false;
      reentryIndex = i + config.delayDays;
      justExited = true;
    }

    if (!invested) {
      if (!justExited) {
        cash *= 1 + CASH_RATE / DAYS_PER_YEAR;
      }
      if (hasMa && i >= reentryIndex && price > movingAverage) {
        invested = true;
        shares = cash / price;
        cash = 0;
      }
    }

    if (invested && !activeOption) {
      const suggestion = chooseSuggestedTerm(
        points,
        maSeries,
        i,
        config.survivalThreshold,
        config.delta,
        survivalCalibration
      );
      if (suggestion.termDays > 0) {
        const expirationIndex = Math.min(i + suggestion.termDays, points.length - 1);
        const timeToExpiry = Math.max((expirationIndex - i) / DAYS_PER_YEAR, 1 / DAYS_PER_YEAR);
        const sigma = estimateSigma(points, i);
        const strike = findStrike(price, timeToExpiry, CASH_RATE, sigma, config.delta);
        const premium = callPrice(price, strike, timeToExpiry, CASH_RATE, sigma);
        cash += shares * premium;
        activeOption = {
          strike,
          sigma,
          expirationIndex
        };
      }
    }

    const optionValue = invested ? optionMarketValue(activeOption, price, i) : 0;
    const equity = invested ? cash + shares * price - shares * optionValue : cash;
    equitySeries.push(equity);
  }

  return {
    equitySeries,
    startingEquity: config.startingCapital
  };
}

function computeStats(equitySeries, startingEquity) {
  const values = equitySeries.filter((value) => value !== null);
  const finalEquity = values[values.length - 1];
  const years = Math.max((values.length - 1) / DAYS_PER_YEAR, 1 / DAYS_PER_YEAR);
  const cagr = Math.pow(finalEquity / startingEquity, 1 / years) - 1;
  return {
    finalEquity,
    cagr
  };
}

function runSweep(points, args) {
  const rows = [];
  for (const averageType of ["SMA", "EMA"]) {
    for (let days = args.minDays; days <= args.maxDays; days += 1) {
      const simulation = simulateStrategy(points, {
        averageType,
        smaWindow: days,
        delta: args.delta,
        delayDays: args.delayDays,
        survivalThreshold: args.survivalProb,
        startingCapital: args.startingCapital
      });
      const stats = computeStats(simulation.equitySeries, simulation.startingEquity);
      rows.push({
        averageType,
        days,
        cagr: stats.cagr * 100
      });
    }
  }
  return rows;
}

function toCsv(rows) {
  const lines = ["average_type,days,cagr"];
  for (const row of rows) {
    lines.push(`${row.averageType},${row.days},${row.cagr.toFixed(6)}`);
  }
  return lines.join("\n") + "\n";
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const inputPath = path.resolve(args.input);
  const outputPath = path.resolve(args.output);
  const text = fs.readFileSync(inputPath, "utf8");
  const points = parseCsv(text);
  if (points.length < args.maxDays + 20) {
    throw new Error(`not enough rows for max window ${args.maxDays}`);
  }
  const rows = runSweep(points, args);
  fs.writeFileSync(outputPath, toCsv(rows), "utf8");
  console.log(`wrote ${rows.length} rows to ${outputPath}`);
}

main();
