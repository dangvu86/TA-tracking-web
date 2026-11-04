// Faithful ports of the pandas 2.3.3 / numpy 2.2.6 numerics the server relies on
// (pandas/_libs/window/aggregations.pyx, pandas ewm, numpy pairwise_sum). Do not
// "simplify": tiny float differences flip Buy/Sell when price equals a moving
// average on flat stocks.

export const isNa = (v: number) => v !== v;
const signbit = (v: number) => v < 0 || Object.is(v, -0);

export function shift(x: number[], n: number): number[] {
  return x.map((_, i) => (i - n >= 0 && i - n < x.length ? x[i - n] : NaN));
}

export function diff(x: number[]): number[] {
  const prev = shift(x, 1);
  return x.map((v, i) => v - prev[i]);
}

/** numpy pairwise_sum for contiguous float64 arrays. */
export function npSum(a: number[]): number {
  const n = a.length;
  if (n < 8) {
    let res = -0;
    for (let i = 0; i < n; i++) res += a[i];
    return res;
  }
  if (n <= 128) {
    const r = a.slice(0, 8);
    let i = 8;
    for (; i < n - (n % 8); i += 8) for (let j = 0; j < 8; j++) r[j] += a[i + j];
    let res = ((r[0] + r[1]) + (r[2] + r[3])) + ((r[4] + r[5]) + (r[6] + r[7]));
    for (; i < n; i++) res += a[i];
    return res;
  }
  let n2 = Math.floor(n / 2);
  n2 -= n2 % 8;
  return npSum(a.slice(0, n2)) + npSum(a.slice(n2));
}

export const npMean = (a: number[]) => npSum(a) / a.length;

// pandas keeps separate Kahan terms for adding and removing (compensation_add / _remove)
type Acc = { nobs: number; sum: number; compAdd: number; compRemove: number; neg: number; same: number; prev: number };

/** pandas _prep_values: ±inf becomes NaN before any rolling aggregation. */
const prep = (x: number[]) => x.map((v) => (Number.isFinite(v) ? v : NaN));

function accAdd(s: Acc, v: number) {
  if (isNa(v)) return;
  s.nobs++;
  const y = v - s.compAdd;
  const t = s.sum + y;
  s.compAdd = t - s.sum - y;
  s.sum = t;
  if (signbit(v)) s.neg++;
  if (v === s.prev) s.same++;
  else s.same = 1;
  s.prev = v;
}

function accRemove(s: Acc, v: number) {
  if (isNa(v)) return;
  s.nobs--;
  const y = -v - s.compRemove;
  const t = s.sum + y;
  s.compRemove = t - s.sum - y;
  s.sum = t;
  if (signbit(v)) s.neg--;
}

/** Fixed-window online rolling (pandas roll_sum/roll_mean): remove leaving, then add entering.
 *  Both Kahan terms are set once at the start and never reset mid-series (w >= 2). */
function rollingOnline(xIn: number[], w: number, calc: (s: Acc) => number): number[] {
  const x = prep(xIn);
  const out = new Array<number>(x.length).fill(NaN);
  const s: Acc = { nobs: 0, sum: 0, compAdd: 0, compRemove: 0, neg: 0, same: 0, prev: NaN };
  for (let i = 0; i < x.length; i++) {
    const start = Math.max(0, i - w + 1);
    if (i === 0) accAdd(s, x[0]);
    else {
      for (let j = Math.max(0, i - w); j < start; j++) accRemove(s, x[j]);
      accAdd(s, x[i]);
    }
    out[i] = calc(s);
  }
  return out;
}

export function rollingMean(x: number[], w: number, minP = w): number[] {
  return rollingOnline(x, w, (s) => {
    if (!(s.nobs >= minP && s.nobs > 0)) return NaN;
    if (s.same >= s.nobs) return s.prev;
    const r = s.sum / s.nobs;
    if (s.neg === 0 && r < 0) return 0;
    if (s.neg === s.nobs && r > 0) return 0;
    return r;
  });
}

export function rollingSum(x: number[], w: number, minP = w): number[] {
  return rollingOnline(x, w, (s) => {
    if (s.nobs === 0 && minP === 0) return 0;
    if (s.nobs < minP || s.nobs === 0) return NaN;
    return s.same >= s.nobs ? s.prev * s.nobs : s.sum;
  });
}

function rollingWindow(xIn: number[], w: number, minP: number, fn: (win: number[]) => number): number[] {
  const x = prep(xIn);
  return x.map((_, i) => {
    const win = x.slice(Math.max(0, i - w + 1), i + 1);
    const valid = win.filter((v) => !isNa(v));
    return valid.length >= minP && valid.length > 0 ? fn(valid) : NaN;
  });
}

export const rollingMax = (x: number[], w: number, minP = w) => rollingWindow(x, w, minP, (v) => Math.max(...v));
export const rollingMin = (x: number[], w: number, minP = w) => rollingWindow(x, w, minP, (v) => Math.min(...v));

/** rolling(w).apply(fn, raw=True): fn sees the raw window once nobs >= w. */
export function rollingApply(xIn: number[], w: number, fn: (win: number[]) => number): number[] {
  const x = prep(xIn);
  return x.map((_, i) => {
    if (i < w - 1) return NaN;
    const win = x.slice(i - w + 1, i + 1);
    return win.some(isNa) ? NaN : fn(win);
  });
}

/** Series.ewm(span|alpha, min_periods, adjust=False, ignore_na=False).mean() */
export function ewmMean(x: number[], opt: { span?: number; alpha?: number }, minPeriods: number): number[] {
  const com = opt.span !== undefined ? (opt.span - 1) / 2 : (1 - opt.alpha!) / opt.alpha!;
  const alpha = 1 / (1 + com);
  const oldWtFactor = 1 - alpha;
  const newWt = alpha;
  const minP = Math.max(minPeriods, 1);
  const out = new Array<number>(x.length).fill(NaN);
  if (x.length === 0) return out;
  let weighted = x[0];
  let nobs = isNa(x[0]) ? 0 : 1;
  let oldWt = 1;
  out[0] = nobs >= minP ? weighted : NaN;
  for (let i = 1; i < x.length; i++) {
    const cur = x[i];
    const isObs = !isNa(cur);
    if (isObs) nobs++;
    if (!isNa(weighted)) {
      oldWt *= oldWtFactor;
      if (isObs) {
        if (weighted !== cur) {
          weighted = oldWt * weighted + newWt * cur;
          weighted /= oldWt + newWt;
        }
        oldWt = 1;
      }
    } else if (isObs) {
      weighted = cur;
    }
    out[i] = nobs >= minP ? weighted : NaN;
  }
  return out;
}
