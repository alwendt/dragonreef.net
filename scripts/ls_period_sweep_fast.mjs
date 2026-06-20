#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";

const CASH_RATE = 0.04;
const DAYS_PER_YEAR = 252;
const QCHART_START_INDEX = 300;

const DEFAULTS = {
  csv: "/home/alan/investing/chart-cache/QQQ.csv",
  output: "qqq_ls_period_sweep.csv",
  mode: "least_squares",
  minPeriod: 10,
  maxPeriod: 200,
  step: 2,
  delta: 0.26,
  delayDays: 6,
  survivalProb: 0.47,
  startingCapital: 12494.00,
  buckets: 2
};

function parseArgs(argv) {
  const args = { ...DEFAULTS };
  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (!key.startsWith("--")) {
      continue;
    }
    if (next === undefined || next.startsWith("--")) {
      throw new Error(`missing value for ${key}`);
    }
    i += 1;
    switch (key) {
      case "--csv":
        args.csv = next;
        break;
      case "--output":
        args.output = next;
        break;
      case "--mode":
        args.mode = next;
        break;
      case "--min-period":
        args.minPeriod = Number(next);
        break;
      case "--max-period":
        args.maxPeriod = Number(next);
        break;
      case "--step":
        args.step = Number(next);
        break;
      case "--delta":
        args.delta = Number(next);
        break;
      case "--delay-days":
        args.delayDays = Number(next);
        break;
      case "--survival-prob":
        args.survivalProb = Number(next);
        break;
      case "--starting-capital":
        args.startingCapital = Number(next);
        break;
      case "--buckets":
        args.buckets = Number(next);
        break;
      default:
        throw new Error(`unknown option ${key}`);
    }
  }
  return args;
}

function splitCsvLine(line) {
  const fields = [];
  let value = "";
  let quoted = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      if (quoted && line[i + 1] === '"') {
        value += '"';
        i += 1;
      } else {
        quoted = !quoted;
      }
    } else if (ch === "," && !quoted) {
      fields.push(value);
      value = "";
    } else {
      value += ch;
    }
  }
  fields.push(value);
  return fields;
}

function readPoints(csvPath) {
  const text = fs.readFileSync(csvPath, "utf8").trim();
  const lines = text.split(/\r?\n/).filter(Boolean);
  const header = splitCsvLine(lines[0]).map((field) => field.trim());
  const dateCol = header.findIndex((field) => field.toLowerCase() === "date");
  const closeCol = header.findIndex((field) => field.toLowerCase() === "close");
  if (dateCol < 0 || closeCol < 0) {
    throw new Error("CSV must contain Date and Close columns");
  }
  const points = [];
  for (let i = 1; i < lines.length; i += 1) {
    const fields = splitCsvLine(lines[i]);
    const date = fields[dateCol];
    const close = Number(fields[closeCol]);
    if (date && Number.isFinite(close)) {
      points.push({ date, close });
    }
  }
  points.sort((a, b) => a.date.localeCompare(b.date));
  return points;
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

function precomputeRisk(points) {
  const sigma = new Array(points.length).fill(0.20);
  const drift = new Array(points.length).fill(0);
  for (let end = 0; end < points.length; end += 1) {
    const start = Math.max(1, end - 20);
    let returnsCount = 0;
    let sumReturns = 0;
    let sumReturnsSquared = 0;
    let logCount = 0;
    let sumLogs = 0;
    for (let i = start; i < end; i += 1) {
      const prior = points[i - 1].close;
      const current = points[i].close;
      if (prior > 0 && current > 0) {
        const ret = current / prior - 1;
        returnsCount += 1;
        sumReturns += ret;
        sumReturnsSquared += ret * ret;
        logCount += 1;
        sumLogs += Math.log(current / prior);
      }
    }
    if (returnsCount >= 2) {
      const variance = (sumReturnsSquared - ((sumReturns * sumReturns) / returnsCount)) / (returnsCount - 1);
      sigma[end] = Math.sqrt(Math.max(0, variance)) * Math.sqrt(DAYS_PER_YEAR);
    }
    if (logCount >= 2) {
      drift[end] = (sumLogs / logCount) * DAYS_PER_YEAR;
    }
  }
  return { sigma, drift };
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
  const probability = normCdf(z1) + Math.exp((-2 * muAnn * distance) / sigmaSquared) * normCdf(z2);
  return Math.max(0, Math.min(1, probability));
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
  if (samples.length < 12) {
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
  return coeffs;
}

function calibratedProbability(model, rawValue) {
  if (!model) {
    return Math.max(0, Math.min(1, rawValue));
  }
  const x = Math.max(0, Math.min(1, rawValue));
  const fitted = model[0] + (model[1] * x) + (model[2] * x * x);
  return Math.max(0, Math.min(1, fitted));
}

function computeLeastSquares(points, windowSize) {
  const fitted = new Array(points.length).fill(null);
  const slopes = new Array(points.length).fill(null);
  const n = windowSize;
  const sumX = n * (n - 1) / 2;
  const sumX2 = (n - 1) * n * ((2 * n) - 1) / 6;
  const denominator = n * sumX2 - sumX * sumX;
  if (denominator === 0) {
    return { fitted, slopes };
  }
  for (let i = windowSize - 1; i < points.length; i += 1) {
    let sumY = 0;
    let sumXY = 0;
    for (let offset = 0; offset < windowSize; offset += 1) {
      const y = points[i - windowSize + 1 + offset].close;
      sumY += y;
      sumXY += offset * y;
    }
    if (i >= windowSize - 1) {
      const slope = (n * sumXY - sumX * sumY) / denominator;
      const intercept = (sumY - slope * sumX) / n;
      slopes[i] = slope;
      fitted[i] = intercept + slope * (windowSize - 1);
    }
  }
  return { fitted, slopes };
}

function computeEma(points, windowSize) {
  const ema = new Array(points.length).fill(null);
  if (!points.length) {
    return ema;
  }
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

function computeEmaCrossover(points, windowSize) {
  const fastSeries = computeEma(points, windowSize);
  const slowSeries = computeEma(points, windowSize * 2);
  const spreadSeries = new Array(points.length).fill(null);
  for (let i = 0; i < points.length; i += 1) {
    if (fastSeries[i] !== null && slowSeries[i] !== null) {
      spreadSeries[i] = fastSeries[i] - slowSeries[i];
    }
  }
  return { fastSeries, slowSeries, spreadSeries };
}

function isBuySignal(signal) {
  return signal !== null && Number.isFinite(signal) && signal > 0;
}

function isSellSignal(signal) {
  return signal !== null && Number.isFinite(signal) && signal < 0;
}

function computeCloseStd(points, windowSize) {
  const stdSeries = new Array(points.length).fill(null);
  let sum = 0;
  let sumSquares = 0;
  for (let i = 0; i < points.length; i += 1) {
    const value = points[i].close;
    sum += value;
    sumSquares += value * value;
    if (i >= windowSize) {
      const outgoing = points[i - windowSize].close;
      sum -= outgoing;
      sumSquares -= outgoing * outgoing;
    }
    if (i >= windowSize - 1) {
      const mean = sum / windowSize;
      const variance = Math.max(0, (sumSquares / windowSize) - (mean * mean));
      stdSeries[i] = Math.sqrt(variance);
    }
  }
  return stdSeries;
}

function bucketFractions(bucketCount) {
  if (bucketCount <= 2) {
    return [0, 1];
  }
  return Array.from({ length: bucketCount }, (_, index) => index / (bucketCount - 1));
}

function quantizeFraction(fraction, buckets) {
  let best = buckets[0];
  let bestError = Number.POSITIVE_INFINITY;
  for (const bucket of buckets) {
    const error = Math.abs(bucket - fraction);
    if (error < bestError) {
      bestError = error;
      best = bucket;
    }
  }
  return best;
}

function targetFractionForSignal(signalValue, currentFraction) {
  if (signalValue === null || !Number.isFinite(signalValue)) {
    return currentFraction === null ? 0 : currentFraction;
  }
  return isBuySignal(signalValue) ? 1 : 0;
}

function buildSurvivalCalibration(points, maSeries, signalSeries, risk, delta) {
  if (!(delta > 0)) {
    return null;
  }
  const models = {};
  for (const termDays of [20, 15, 10, 5]) {
    const samples = [];
    for (let index = 0; index + termDays < points.length; index += 1) {
      const movingAverage = maSeries[index];
      const signal = signalSeries[index];
      const price = points[index].close;
      if (movingAverage === null || !isBuySignal(signal)) {
        continue;
      }
      const rawSurvival = 1 - forcedSaleProbability(price, movingAverage, risk.sigma[index], risk.drift[index], termDays);
      const expirationIndex = Math.min(index + termDays, points.length - 1);
      const timeToExpiry = Math.max((expirationIndex - index) / DAYS_PER_YEAR, 1 / DAYS_PER_YEAR);
      const strike = findStrike(price, timeToExpiry, CASH_RATE, risk.sigma[index], delta);
      let forcedBuyback = false;
      for (let future = index + 1; future < expirationIndex; future += 1) {
        if (maSeries[future] !== null && isSellSignal(signalSeries[future])) {
          forcedBuyback = true;
          break;
        }
      }
      const expiredOtm = !forcedBuyback && points[expirationIndex].close <= strike;
      samples.push({ raw: rawSurvival, outcome: expiredOtm ? 1 : 0 });
    }
    models[termDays] = fitQuadraticCalibration(samples);
  }
  return models;
}

function chooseSuggestedTerm(points, maSeries, signalSeries, risk, index, targetSurvival, delta, calibration) {
  if (!(delta > 0)) {
    return 0;
  }
  const movingAverage = maSeries[index];
  const signal = signalSeries[index];
  if (movingAverage === null || !isBuySignal(signal)) {
    return 0;
  }
  const price = points[index].close;
  for (const termDays of [20, 15, 10, 5]) {
    const rawSurvival = 1 - forcedSaleProbability(price, movingAverage, risk.sigma[index], risk.drift[index], termDays);
    const survival = calibratedProbability(calibration ? calibration[termDays] : null, rawSurvival);
    if (survival >= targetSurvival) {
      return termDays;
    }
  }
  return 0;
}

function optionMarketValue(activeOption, spot, index) {
  if (!activeOption) {
    return 0;
  }
  const remainingDays = Math.max(activeOption.expirationIndex - index, 0);
  const timeToExpiry = remainingDays > 0 ? remainingDays / DAYS_PER_YEAR : 0;
  return callPrice(spot, activeOption.strike, timeToExpiry, CASH_RATE, activeOption.sigma);
}

function computeStrategySeries(points, period, config) {
  if (config.mode === "ema_crossover") {
    const crossover = computeEmaCrossover(points, period);
    return {
      maSeries: crossover.slowSeries,
      signalSeries: crossover.spreadSeries
    };
  }
  const leastSquares = computeLeastSquares(points, period);
  return {
    maSeries: leastSquares.fitted,
    signalSeries: leastSquares.slopes
  };
}

function simulateBinary(points, period, config, risk) {
  const { maSeries, signalSeries } = computeStrategySeries(points, period, config);
  const survivalCalibration = buildSurvivalCalibration(points, maSeries, signalSeries, risk, config.delta);
  let invested = true;
  let shares = config.startingCapital / points[0].close;
  let cash = 0;
  let activeOption = null;
  let reentryIndex = -1;
  const equitySeries = [];
  let openRoundTripPurchasePrice = points.length ? points[0].close : null;
  let lastCompletedRoundTripSalePrice = null;
  let lastCompletedRoundTripSaleIndex = null;
  let lastCompletedRoundTripLostMoney = false;

  for (let i = 0; i < points.length; i += 1) {
    const price = points[i].close;
    const signal = signalSeries[i];
    let justExited = false;

    if (invested && activeOption && activeOption.expirationIndex === i) {
      const settledEquity = cash + shares * price - shares * Math.max(0, price - activeOption.strike);
      shares = settledEquity / price;
      cash = 0;
      activeOption = null;
    }

    const suppressSaleAfterLoss = lastCompletedRoundTripLostMoney &&
      lastCompletedRoundTripSalePrice !== null &&
      lastCompletedRoundTripSaleIndex !== null &&
      i - lastCompletedRoundTripSaleIndex <= period * 2 &&
      price > lastCompletedRoundTripSalePrice;

    if (invested && isSellSignal(signal) && !suppressSaleAfterLoss) {
      const optionValue = optionMarketValue(activeOption, price, i);
      lastCompletedRoundTripSalePrice = price;
      lastCompletedRoundTripSaleIndex = i;
      lastCompletedRoundTripLostMoney = openRoundTripPurchasePrice !== null && price < openRoundTripPurchasePrice;
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
      if (i >= reentryIndex && isBuySignal(signal)) {
        invested = true;
        openRoundTripPurchasePrice = price;
        shares = cash / price;
        cash = 0;
      }
    }

    if (invested && !activeOption) {
      const termDays = config.delta <= 0 ? 0 : chooseSuggestedTerm(points, maSeries, signalSeries, risk, i, config.survivalProb, config.delta, survivalCalibration);
      if (termDays > 0) {
        const expirationIndex = Math.min(i + termDays, points.length - 1);
        const timeToExpiry = Math.max((expirationIndex - i) / DAYS_PER_YEAR, 1 / DAYS_PER_YEAR);
        const sigma = risk.sigma[i];
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
    equitySeries.push(invested ? cash + shares * price - shares * optionValue : cash);
  }

  return computeStats(equitySeries, config.startingCapital);
}

function simulateQuantized(points, period, config, risk) {
  const { maSeries, signalSeries } = computeStrategySeries(points, period, config);
  const survivalCalibration = buildSurvivalCalibration(points, maSeries, signalSeries, risk, config.delta);
  const stdSeries = computeCloseStd(points, period);
  const buckets = bucketFractions(config.buckets);
  const equitySeries = new Array(points.length).fill(null);
  let cash = config.startingCapital;
  let shares = 0;
  let activeOption = null;
  let actualFraction = null;
  let cooldownUntil = -1;
  let openRoundTripPurchasePrice = null;
  let lastCompletedRoundTripSalePrice = null;
  let lastCompletedRoundTripSaleIndex = null;
  let lastCompletedRoundTripLostMoney = false;

  for (let i = QCHART_START_INDEX; i < points.length; i += 1) {
    const price = points[i].close;

    if (activeOption && activeOption.expirationIndex === i) {
      const itmShares = activeOption.contracts * 100;
      cash -= itmShares * Math.max(0, price - activeOption.strike);
      activeOption = null;
    }

    const rawFraction = quantizeFraction(targetFractionForSignal(signalSeries[i], actualFraction), buckets);
    const suppressSaleAfterLoss = lastCompletedRoundTripLostMoney &&
      lastCompletedRoundTripSalePrice !== null &&
      lastCompletedRoundTripSaleIndex !== null &&
      i - lastCompletedRoundTripSaleIndex <= period * 2 &&
      price > lastCompletedRoundTripSalePrice;

    if (actualFraction === null) {
      actualFraction = rawFraction;
      if (actualFraction > 0) {
        openRoundTripPurchasePrice = price;
      }
    } else if (rawFraction < actualFraction && !suppressSaleAfterLoss) {
      actualFraction = rawFraction;
      lastCompletedRoundTripSalePrice = price;
      lastCompletedRoundTripSaleIndex = i;
      lastCompletedRoundTripLostMoney = openRoundTripPurchasePrice !== null && price < openRoundTripPurchasePrice;
      cooldownUntil = i + config.delayDays;
    } else if (rawFraction > actualFraction && i >= cooldownUntil) {
      actualFraction = rawFraction;
      openRoundTripPurchasePrice = price;
    }

    const currentOptionValue = activeOption ? optionMarketValue(activeOption, price, i) : 0;
    const currentOptionLiability = activeOption ? (activeOption.contracts * 100 * currentOptionValue) : 0;
    const equityBefore = cash + shares * price - currentOptionLiability;
    const targetShares = price > 0 ? ((equityBefore * actualFraction) / price) : 0;
    const shareDelta = targetShares - shares;
    cash -= shareDelta * price;
    shares = targetShares;

    if (activeOption) {
      const optionValue = optionMarketValue(activeOption, price, i);
      const maxContracts = Math.floor(shares / 100);
      if (activeOption.contracts > maxContracts) {
        const reducedContracts = activeOption.contracts - maxContracts;
        cash -= reducedContracts * 100 * optionValue;
        activeOption.contracts = maxContracts;
        if (activeOption.contracts <= 0) {
          activeOption = null;
        }
      }
    }

    if (cash > 0) {
      cash *= 1 + CASH_RATE / DAYS_PER_YEAR;
    }

    const coveredContracts = Math.floor(shares / 100);
    if (!activeOption) {
      const termDays = config.delta <= 0 ? 0 : chooseSuggestedTerm(points, maSeries, signalSeries, risk, i, config.survivalProb, config.delta, survivalCalibration);
      if (termDays > 0 && coveredContracts > 0) {
        const expirationIndex = Math.min(i + termDays, points.length - 1);
        const timeToExpiry = Math.max((expirationIndex - i) / DAYS_PER_YEAR, 1 / DAYS_PER_YEAR);
        const sigma = risk.sigma[i];
        const strike = findStrike(price, timeToExpiry, CASH_RATE, sigma, config.delta);
        const premium = callPrice(price, strike, timeToExpiry, CASH_RATE, sigma);
        cash += coveredContracts * 100 * premium;
        activeOption = {
          contracts: coveredContracts,
          strike,
          sigma,
          expirationIndex
        };
      }
    }

    const liveOptionValue = activeOption ? optionMarketValue(activeOption, price, i) : 0;
    const liveLiability = activeOption ? (activeOption.contracts * 100 * liveOptionValue) : 0;
    equitySeries[i] = cash + shares * price - liveLiability;
  }

  return computeStats(equitySeries, config.startingCapital);
}

function computeStats(equitySeries, startingEquity) {
  const values = equitySeries.filter((value) => value !== null);
  const finalEquity = values.at(-1);
  const years = Math.max((values.length - 1) / DAYS_PER_YEAR, 1 / DAYS_PER_YEAR);
  const cagr = Math.pow(finalEquity / startingEquity, 1 / years) - 1;
  let peak = values[0];
  let maxDrawdown = 0;
  for (const value of values) {
    if (value > peak) {
      peak = value;
    }
    const drawdown = value / peak - 1;
    if (drawdown < maxDrawdown) {
      maxDrawdown = drawdown;
    }
  }
  return { finalEquity, cagr, maxDrawdown };
}

function main() {
  const args = parseArgs(process.argv);
  if (!["least_squares", "ema_crossover"].includes(args.mode)) {
    throw new Error("--mode must be least_squares or ema_crossover");
  }
  const points = readPoints(args.csv);
  if (!points.length) {
    throw new Error("no price data loaded");
  }
  const risk = precomputeRisk(points);
  const rows = [["period", "cagr", "max_drawdown", "cagr_pct", "max_drawdown_pct"]];
  const results = [];
  for (let period = args.minPeriod; period <= args.maxPeriod; period += args.step) {
    const stats = args.buckets > 2
      ? simulateQuantized(points, period, args, risk)
      : simulateBinary(points, period, args, risk);
    results.push({ period, ...stats });
    rows.push([
      period,
      stats.cagr.toFixed(8),
      stats.maxDrawdown.toFixed(8),
      (stats.cagr * 100).toFixed(4) + "%",
      (stats.maxDrawdown * 100).toFixed(4) + "%"
    ]);
    process.stderr.write(`${period},${(stats.cagr * 100).toFixed(4)}%,${(stats.maxDrawdown * 100).toFixed(4)}%\n`);
  }
  fs.writeFileSync(args.output, rows.map((row) => row.join(",")).join("\n") + "\n");
  const bestCagr = results.reduce((best, row) => row.cagr > best.cagr ? row : best, results[0]);
  const bestDrawdown = results.reduce((best, row) => row.maxDrawdown > best.maxDrawdown ? row : best, results[0]);
  console.log(path.resolve(args.output));
  console.log(`best_cagr period=${bestCagr.period} cagr=${(bestCagr.cagr * 100).toFixed(4)}% max_drawdown=${(bestCagr.maxDrawdown * 100).toFixed(4)}%`);
  console.log(`best_drawdown period=${bestDrawdown.period} cagr=${(bestDrawdown.cagr * 100).toFixed(4)}% max_drawdown=${(bestDrawdown.maxDrawdown * 100).toFixed(4)}%`);
}

main();
