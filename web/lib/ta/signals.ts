// Port of api/src/indicators/signals.py and api/src/utils/signal_counter.py.
import { isNa } from "./pandas";

export type Signal = "Buy" | "Sell" | "Neutral";
export type Ind = Record<string, number>;

const cmp = (price: number, v: number): Signal =>
  isNa(price) || isNa(v) ? "Neutral" : price > v ? "Buy" : price < v ? "Sell" : "Neutral";
const anyNa = (...v: number[]) => v.some(isNa);

export function evaluateAllSignals(i: Ind): Record<string, Signal> {
  const s: Record<string, Signal> = {};
  const p = i.Price;
  for (const w of [10, 20, 30, 50, 100, 200]) s[`MA_${w}_Signal`] = cmp(p, i[`SMA_${w}`]);
  for (const w of [10, 20, 30, 50, 100, 200]) s[`EMA_${w}_Signal`] = cmp(p, i[`EMA_${w}`]);
  s.VWMA_Signal = cmp(p, i.VWMA_20);
  s.Hull_MA_Signal = cmp(p, i.Hull_MA_9);
  const [a, b, base, conv] = [i.Ichimoku_A, i.Ichimoku_B, i.Ichimoku_Base, i.Ichimoku_Conversion];
  s.Ichimoku_Signal = anyNa(a, b, base, conv, p) ? "Neutral"
    : a > b && base > a && conv > base && p > conv ? "Buy"
    : a < b && base < a && conv < base && p < conv ? "Sell" : "Neutral";

  const prevRule = (v: number, pv: number, lo: number, hi: number): Signal =>
    anyNa(v, pv) ? "Neutral" : v < lo && v > pv ? "Buy" : v > hi && v < pv ? "Sell" : "Neutral";
  s.RSI_Signal = prevRule(i.RSI_14, i.RSI_Prev, 30, 70);
  const kd = (k: number, d: number): Signal =>
    anyNa(k, d) ? "Neutral" : k < 20 && d < 20 && k > d ? "Buy" : k > 80 && d > 80 && k < d ? "Sell" : "Neutral";
  s.Stochastic_Signal = kd(i.Stoch_K, i.Stoch_D);
  s.CCI_Signal = prevRule(i.CCI_20, i.CCI_Prev, -100, 100);
  s.ADX_Signal = anyNa(i.DMI_Positive, i.DMI_Negative, i.ADX_14, i.ADX_Prev) ? "Neutral"
    : i.DMI_Positive > i.DMI_Negative && i.ADX_14 > 20 && i.ADX_14 > i.ADX_Prev ? "Buy"
    : i.DMI_Positive < i.DMI_Negative && i.ADX_14 > 20 && i.ADX_14 > i.ADX_Prev ? "Sell" : "Neutral";
  s.AO_Signal = anyNa(i.AO, i.AO_Prev) ? "Neutral"
    : i.AO > 0 && i.AO > i.AO_Prev ? "Buy" : i.AO < 0 && i.AO < i.AO_Prev ? "Sell" : "Neutral";
  s.Momentum_Signal = anyNa(i.Momentum_10, i.Momentum_Prev) ? "Neutral"
    : i.Momentum_10 > i.Momentum_Prev ? "Buy" : i.Momentum_10 < i.Momentum_Prev ? "Sell" : "Neutral";
  s.MACD_Signal = cmp(i.MACD, i.MACD_Signal);
  s.StochRSI_Signal = kd(i.StochRSI_K, i.StochRSI_D);
  s.Williams_R_Signal = prevRule(i.Williams_R, i.Williams_R_Prev, -80, -20);
  s.BBP_Signal = anyNa(i.EMA_13, i.EMA_13_Prev, i.Bull_Power, i.Bear_Power, i.Bull_Power_Prev, i.Bear_Power_Prev) ? "Neutral"
    : i.EMA_13 > i.EMA_13_Prev && i.Bear_Power < 0 && i.Bear_Power > i.Bear_Power_Prev ? "Buy"
    : i.EMA_13 < i.EMA_13_Prev && i.Bull_Power > 0 && i.Bull_Power < i.Bull_Power_Prev ? "Sell" : "Neutral";
  s.UO_Signal = isNa(i.UO) ? "Neutral" : i.UO > 70 ? "Buy" : i.UO < 30 ? "Sell" : "Neutral";
  return s;
}

const OSC = ["RSI_Signal", "Stochastic_Signal", "CCI_Signal", "ADX_Signal", "AO_Signal", "Momentum_Signal",
  "MACD_Signal", "StochRSI_Signal", "Williams_R_Signal", "BBP_Signal", "UO_Signal"];
const MA = ["MA_10_Signal", "MA_20_Signal", "MA_30_Signal", "MA_50_Signal", "MA_100_Signal", "MA_200_Signal",
  "EMA_10_Signal", "EMA_20_Signal", "EMA_30_Signal", "EMA_50_Signal", "EMA_100_Signal", "EMA_200_Signal",
  "VWMA_Signal", "Hull_MA_Signal", "Ichimoku_Signal"];

export type Counts = { oscBuy: number; oscSell: number; maBuy: number; maSell: number };

export function countSignals(s: Record<string, Signal>): Counts {
  const n = (keys: string[], v: Signal) => keys.filter((k) => s[k] === v).length;
  return { oscBuy: n(OSC, "Buy"), oscSell: n(OSC, "Sell"), maBuy: n(MA, "Buy"), maSell: n(MA, "Sell") };
}

/** Rating 1 = Osc buy*2 - Osc sell + MA buy - MA sell; Rating 2 = Osc buy*2 + MA buy */
export function ratings(c: Counts) {
  return { r1: c.oscBuy * 2 - c.oscSell + c.maBuy * 1 - c.maSell, r2: c.oscBuy * 2 + c.maBuy * 1 };
}
