// Port of api/src/indicators/calculator.py on top of the `ta` 0.11.0 implementations.
// Keep library quirks verbatim (see ADX). Column names match the Python DataFrame.
import {
  diff, ewmMean, isNa, npMean, npSum, rollingApply, rollingMax, rollingMean,
  rollingMin, rollingSum, shift,
} from "./pandas";

export type Bars = { o: number[]; h: number[]; l: number[]; c: number[]; v: number[] };
export type Columns = Record<string, (number | boolean)[]>;

const zip = (a: number[], b: number[], f: (x: number, y: number) => number) => a.map((x, i) => f(x, b[i]));
const sma = (x: number[], w: number) => rollingMean(x, w);
const ema = (x: number[], w: number) => ewmMean(x, { span: w }, w);

/** pandas Series.where(cond, 0.0): NaN fails the condition and becomes 0.0 */
const whereElseZero = (x: number[], cond: (v: number) => boolean) => x.map((v) => (cond(v) ? v : 0));

/** numpy max/min over a row: NaN if any element is NaN */
const npMax2 = (a: number, b: number) => (isNa(a) || isNa(b) ? NaN : Math.max(a, b));
const npMin2 = (a: number, b: number) => (isNa(a) || isNa(b) ? NaN : Math.min(a, b));

// ta.momentum.RSIIndicator
function rsi(close: number[], w: number) {
  const d = diff(close);
  const up = whereElseZero(d, (v) => v > 0);
  const dn = whereElseZero(d, (v) => v < 0).map((v) => -v);
  const emaup = ewmMean(up, { alpha: 1 / w }, w);
  const emadn = ewmMean(dn, { alpha: 1 / w }, w);
  return emaup.map((u, i) => (emadn[i] === 0 ? 100 : 100 - 100 / (1 + u / emadn[i])));
}

// np.average(x, weights=arange(1, n + 1)) inside rolling.apply
function wma(x: number[], n: number) {
  const w = Array.from({ length: n }, (_, i) => i + 1);
  const scl = npSum(w);
  return rollingApply(x, n, (win) => npSum(win.map((v, i) => v * w[i])) / scl);
}

// calculator.calculate_hull_ma
function hullMa(close: number[], period: number) {
  const half = Math.trunc(period / 2);
  const sq = Math.trunc(Math.sqrt(period));
  const hull = zip(wma(close, half), wma(close, period), (a, b) => 2 * a - b);
  return wma(hull, sq);
}

/** ta.trend.ADXIndicator._run / adx / adx_pos / adx_neg, verbatim (incl. loop bounds). */
function adxAll(high: number[], low: number[], close: number[], w: number) {
  const n = close.length;
  const cs = shift(close, 1);
  const ddm = zip(zip(high, cs, npMax2), zip(low, cs, npMin2), (a, b) => a - b);
  const len = n - (w - 1);
  // ta raises (np.zeros with negative size, or IndexError on adx_series[w]) when len <= w;
  // analyze_single_stock then returns an error row. Reproduce the failure, don't grow arrays.
  if (len <= w) throw new Error(`ADX needs more than ${2 * w - 1} bars, got ${n}`);
  const firstSum = (s: number[]) => npSum(s.filter((v) => !isNa(v)).slice(0, w));
  const smooth = (src: number[]) => {
    const out = new Array<number>(len).fill(0);
    out[0] = firstSum(src);
    for (let i = 1; i < len - 1; i++) out[i] = out[i - 1] - out[i - 1] / w + src[w + i];
    return out;
  };
  const trs = smooth(ddm);
  const diffUp = zip(high, shift(high, 1), (a, b) => a - b);
  const diffDown = zip(shift(low, 1), low, (a, b) => a - b);
  // bool * diff_up: NaN only when diff_up itself is NaN (a NaN comparison is just False)
  const pos = diffUp.map((u, i) => Math.abs((u > diffDown[i] && u > 0 ? 1 : 0) * u));
  const neg = diffDown.map((d, i) => Math.abs((d > diffUp[i] && d > 0 ? 1 : 0) * d));
  const dip = smooth(pos);
  const din = smooth(neg);

  const di = (num: number[]) => trs.map((t, i) => (t !== 0 ? 100 * (num[i] / t) : 0));
  const dipP = di(dip);
  const dinP = di(din);
  const dx = dipP.map((p, i) => (p + dinP[i] !== 0 ? 100 * Math.abs((p - dinP[i]) / (p + dinP[i])) : 0));
  const adxS = new Array<number>(len).fill(0);
  adxS[w] = npMean(dx.slice(0, w));
  for (let i = w + 1; i < len; i++) adxS[i] = (adxS[i - 1] * (w - 1) + dx[i - 1]) / w;
  const adx = [...new Array<number>(w - 1).fill(0), ...adxS];

  const shifted = (num: number[]) => {
    const out = new Array<number>(n).fill(0);
    for (let i = 1; i < len - 1; i++) out[i + w] = trs[i] !== 0 ? 100 * (num[i] / trs[i]) : 0;
    return out;
  };
  return { adx, pos: shifted(dip), neg: shifted(din) };
}

export function calculateAllIndicators(b: Bars): Columns {
  const { h: high, l: low, c: close, v: vol } = b;
  const col: Columns = { Open: b.o, High: high, Low: low, Close: close, Volume: vol };
  const half = (a: number[], c2: number[]) => zip(a, c2, (x, y) => 0.5 * (x + y));

  // Ichimoku (9, 26, 52), visual=False
  const conv = half(rollingMax(high, 9), rollingMin(low, 9));
  const base = half(rollingMax(high, 26), rollingMin(low, 26));
  col.Ichimoku_Base = base;
  col.Ichimoku_Conversion = conv;
  col.Ichimoku_A = half(conv, base);
  col.Ichimoku_B = half(rollingMax(high, 52, 0), rollingMin(low, 52, 0));

  for (const w of [5, 10, 20, 30, 50, 100, 200]) col[`SMA_${w}`] = sma(close, w);
  for (const w of [10, 13, 20, 30, 50, 100, 200]) col[`EMA_${w}`] = ema(close, w);

  // ta.volume.volume_weighted_average_price(window=20)
  const tp = high.map((x, i) => (x + low[i] + close[i]) / 3.0);
  const tpv = zip(tp, vol, (a, c2) => a * c2);
  col.VWMA_20 = zip(rollingSum(tpv, 20), rollingSum(vol, 20), (a, c2) => a / c2);

  col.Hull_MA_9 = hullMa(close, 9);
  col.RSI_14 = rsi(close, 14);

  // Stochastic (14, 3)
  const smin = rollingMin(low, 14);
  const smax = rollingMax(high, 14);
  const stochK = close.map((c2, i) => (100 * (c2 - smin[i])) / (smax[i] - smin[i]));
  col.Stoch_K = stochK;
  col.Stoch_D = rollingMean(stochK, 3);

  // CCI (20), constant 0.015
  const tpMean = rollingMean(tp, 20);
  const mad = rollingApply(tp, 20, (x) => {
    const m = npMean(x);
    return npMean(x.map((v) => Math.abs(v - m)));
  });
  col.CCI_20 = tp.map((t, i) => (t - tpMean[i]) / (0.015 * mad[i]));

  const adx = adxAll(high, low, close, 14);
  col.ADX_14 = adx.adx;

  const median = zip(high, low, (a, c2) => 0.5 * (a + c2));
  col.AO = zip(rollingMean(median, 5), rollingMean(median, 34), (a, c2) => a - c2);

  col.Momentum_10 = zip(close, shift(close, 10), (a, c2) => a - c2);

  const macd = zip(ema(close, 12), ema(close, 26), (a, c2) => a - c2);
  col.MACD = macd.map((x) => x * 1000);
  col.MACD_Signal = ewmMean(macd, { span: 9 }, 9).map((x) => x * 1000);

  // Stochastic RSI (14, 3, 3)
  const r = col.RSI_14 as number[];
  const lowR = rollingMin(r, 14);
  const highR = rollingMax(r, 14);
  const stochrsi = r.map((x, i) => (x - lowR[i]) / (highR[i] - lowR[i]));
  const srK = rollingMean(stochrsi, 3);
  col.StochRSI_K = srK.map((x) => x * 100);
  col.StochRSI_D = rollingMean(srK, 3).map((x) => x * 100);

  // Williams %R (14)
  col.Williams_R = close.map((c2, i) => (-100 * (smax[i] - c2)) / (smax[i] - smin[i]));

  // Ultimate Oscillator (7, 14, 28), weights 4, 2, 1
  const cs = shift(close, 1);
  const tr = high.map((x, i) => {
    // DataFrame.max(axis=1) skips NaN
    const parts = [x - low[i], Math.abs(x - cs[i]), Math.abs(low[i] - cs[i])].filter((v) => !isNa(v));
    return parts.length ? Math.max(...parts) : NaN;
  });
  // DataFrame.min(axis=1, skipna=False)
  const bp = close.map((c2, i) => c2 - npMin2(low[i], cs[i]));
  const avg = (w: number) => zip(rollingSum(bp, w), rollingSum(tr, w), (a, c2) => a / c2);
  const [a7, a14, a28] = [avg(7), avg(14), avg(28)];
  col.UO = a7.map((x, i) => (100.0 * (4 * x + 2 * a14[i] + 1 * a28[i])) / (4 + 2 + 1));

  col.DMI_Positive = adx.pos;
  col.DMI_Negative = adx.neg;

  const e13 = col.EMA_13 as number[];
  col.Bull_Power = zip(high, e13, (a, c2) => (a - c2) * 1000);
  col.Bear_Power = zip(low, e13, (a, c2) => (a - c2) * 1000);

  for (const w of [5, 10, 20, 50, 100, 200]) {
    const m = col[`SMA_${w}`] as number[];
    col[`Close_vs_MA${w}`] = close.map((c2, i) => ((c2 - m[i]) / m[i]) * 100);
  }
  const cv = (w: number) => col[`Close_vs_MA${w}`] as number[];
  col.STRENGTH_ST = close.map((_, i) => (cv(5)[i] + cv(10)[i] + cv(20)[i]) / 3);
  col.STRENGTH_LT = close.map((_, i) => (cv(5)[i] + cv(10)[i] + cv(20)[i] + cv(50)[i] + cv(200)[i]) / 5);

  const s50 = col.SMA_50 as number[];
  const s200 = col.SMA_200 as number[];
  col.MA50_GT_MA200 = s50.map((x, i) => x > s200[i]);

  const prev = (k: string) => shift(col[k] as number[], 1);
  col.RSI_Prev = prev("RSI_14");
  col.CCI_Prev = prev("CCI_20");
  col.ADX_Prev = prev("ADX_14");
  col.Momentum_Prev = prev("Momentum_10");
  col.Williams_R_Prev = prev("Williams_R");
  col.Bull_Power_Prev = prev("Bull_Power");
  col.Bear_Power_Prev = prev("Bear_Power");
  col.EMA_13_Prev = prev("EMA_13");
  col.AO_Prev = prev("AO");
  return col;
}
