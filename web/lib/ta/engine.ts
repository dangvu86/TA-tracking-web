// Port of analyze_single_stock (parallel_processor.py), totals (_run_analysis in app.py)
// and analyze_sectors_new / find_ma_crosses (sector_analysis.py). Output has the same
// shape as latest.json so the existing table and summary render it unchanged.
import { calculateAllIndicators, type Bars, type Columns } from "./indicators";
import { isNa, npSum } from "./pandas";
import { countSignals, evaluateAllSignals, ratings, type Ind } from "./signals";

type Num = number | null;
export type HistoryTicker = {
  Ticker: string; Sector: string; Exchange: string;
  d: string[]; o: Num[]; h: Num[]; l: Num[]; c: Num[]; v: Num[];
};
export type History = { as_of: string; window_days: number; tickers: HistoryTicker[] };
export type LiveBar = { o: number; h: number; l: number; c: number; v: number };
export type Row = Record<string, string | number | boolean | null>;

const SECTOR_MAPPING: Record<string, string> = {
  CK: "Chứng khoán", BDS: "Bất động sản", DTC: "Xây dựng & ĐTC, VLXD", XD: "Xây dựng & ĐTC, VLXD",
  VLXD: "Xây dựng & ĐTC, VLXD", DAU: "Dầu, Hàng không, Agri", HK: "Dầu, Hàng không, Agri",
  AGRI: "Dầu, Hàng không, Agri", XK: "Xuất khẩu", NH: "Ngân hàng", FAV: "FAV", Tech: "Tech", Rubber: "Cao su",
};
const SECTOR_COUNTS: [string, number, number][] = [
  ["Chứng khoán", 3, 3], ["Bất động sản", 3, 3], ["Xây dựng & ĐTC, VLXD", 3, 3], ["Dầu, Hàng không, Agri", 3, 3],
  ["Xuất khẩu", 2, 2], ["Ngân hàng", 3, 3], ["FAV", 3, 3], ["Tech", 2, 2], ["Cao su", 2, 2],
];
const TOTAL_KEYS = ["STRENGTH_ST", "STRENGTH_LT", "Rating_1_Current", "Rating_1_Prev1", "Rating_1_Prev2",
  "Rating_2_Current", "Rating_2_Prev1", "Rating_2_Prev2"];

const num = (v: Num | undefined) => (v === null || v === undefined ? NaN : v);
const clean = (v: number) => (isNa(v) || !Number.isFinite(v) ? null : v);
const minusDays = (iso: string, days: number) => {
  const t = new Date(`${iso}T00:00:00Z`);
  t.setUTCDate(t.getUTCDate() - days);
  return t.toISOString().slice(0, 10);
};

/** Bars in [asOf - windowDays, asOf], plus today's live bar when given. */
export function barsFor(t: HistoryTicker, asOf: string, windowDays: number, live?: LiveBar) {
  const start = minusDays(asOf, windowDays);
  const idx = t.d.flatMap((d, i) => (d >= start && d <= asOf ? [i] : []));
  const pick = (k: "o" | "h" | "l" | "c" | "v") => idx.map((i) => num(t[k][i]));
  const b: Bars & { d: string[] } = { d: idx.map((i) => t.d[i]), o: pick("o"), h: pick("h"), l: pick("l"), c: pick("c"), v: pick("v") };
  if (live && b.d[b.d.length - 1] !== asOf) {
    b.d.push(asOf); b.o.push(live.o); b.h.push(live.h); b.l.push(live.l); b.c.push(live.c); b.v.push(live.v);
  }
  return b;
}

/** get_latest_indicators for one row; booleans become 1/0 like numpy bools in comparisons. */
function rowAt(cols: Columns, i: number): Ind {
  const r: Ind = {};
  for (const [k, v] of Object.entries(cols)) r[k] = typeof v[i] === "boolean" ? (v[i] ? 1 : 0) : (v[i] as number);
  r.Price = cols.Close[i] as number;
  return r;
}

const emptyRow = (t: HistoryTicker, error: string | null): Row =>
  ({ Sector: t.Sector, Ticker: t.Ticker, Price: null, "% Change": null, "Osc Buy": 0, "Osc Sell": 0, "MA Buy": 0, "MA Sell": 0, error });

/** Mirrors analyze_single_stock's try/except: any calculation failure gives an error row. */
export function analyzeTicker(t: HistoryTicker, asOf: string, windowDays: number, live?: LiveBar): Row {
  try {
    return analyzeTickerUnsafe(t, asOf, windowDays, live);
  } catch (e) {
    return emptyRow(t, e instanceof Error ? e.message : String(e));
  }
}

function analyzeTickerUnsafe(t: HistoryTicker, asOf: string, windowDays: number, live?: LiveBar): Row {
  const b = barsFor(t, asOf, windowDays, live);
  if (b.c.length === 0) return emptyRow(t, null);
  const cols = calculateAllIndicators(b);
  const n = b.c.length;
  const cur = rowAt(cols, n - 1);
  const sig = evaluateAllSignals(cur);
  const cnt = countSignals(sig);
  const { r1, r2 } = ratings(cnt);
  const prevRating = (k: number) => (n < k ? { r1: "N/A", r2: "N/A" } : ratings(countSignals(evaluateAllSignals(rowAt(cols, n - k)))));
  const p1 = prevRating(2);
  const p2 = prevRating(3);
  const prevClose = n >= 2 ? b.c[n - 2] : cur.Price;
  const pct = isNa(prevClose) || prevClose === 0 ? 0 : ((cur.Price - prevClose) / prevClose) * 100;
  const prevVs = (ma: number) => (n >= 2 ? (cols[`Close_vs_MA${ma}`][n - 2] as number) : NaN);

  const row: Row = {
    Sector: t.Sector, Ticker: t.Ticker, Price: clean(cur.Price), "% Change": clean(pct),
    "Osc Buy": cnt.oscBuy, "Osc Sell": cnt.oscSell, "MA Buy": cnt.maBuy, "MA Sell": cnt.maSell,
    MA5: clean(cur.SMA_5),
    Rating_1_Current: r1, Rating_1_Prev1: p1.r1, Rating_1_Prev2: p2.r1,
    Rating_2_Current: r2, Rating_2_Prev1: p1.r2, Rating_2_Prev2: p2.r2,
    MA50_GT_MA200: cols.MA50_GT_MA200[n - 1] ? "Yes" : "No",
    error: null,
    Close_vs_MA20_Prev: clean(prevVs(20)), Close_vs_MA50_Prev: clean(prevVs(50)), Close_vs_MA200_Prev: clean(prevVs(200)),
    Date: `${b.d[n - 1]}T00:00:00`,
  };
  for (const k of ["Close_vs_MA5", "Close_vs_MA10", "Close_vs_MA20", "Close_vs_MA50", "Close_vs_MA100",
    "Close_vs_MA200", "STRENGTH_ST", "STRENGTH_LT", "High", "Low"]) row[k] = clean(cur[k]);
  Object.assign(row, sig);
  return row;
}

/** pandas nansum: NaN replaced by 0 in place, then numpy pairwise sum (never bottleneck). */
export function totals(rows: Row[]) {
  const out: Record<string, number | null> = {};
  for (const k of TOTAL_KEYS) {
    const vals = rows.map((r) => (typeof r[k] === "number" ? (r[k] as number) : 0));
    out[k] = clean(npSum(vals));
  }
  return out;
}

const color = (v: number) => (v > 0 ? "green" : v < 0 ? "red" : "black");
const mapped = (r: Row) => SECTOR_MAPPING[r.Sector as string] ?? (r.Sector as string);

export function sectors(rows: Row[]) {
  const valid = rows.filter((r) => r.Sector && r.Sector !== "TOTAL" && r.Sector !== "Index");
  const r1 = (r: Row) => (typeof r.Rating_1_Current === "number" ? r.Rating_1_Current : NaN);
  const p1 = (r: Row) => (typeof r.Rating_1_Prev1 === "number" ? r.Rating_1_Prev1 : NaN);
  const data: Record<string, { top_rating_data: object[]; bottom_rating_data: object[] }> = {};
  for (const [vn, top, bottom] of SECTOR_COUNTS) {
    const all = valid.filter((r) => mapped(r) === vn);
    if (all.length === 0) continue;
    const s = all.filter((r) => !isNa(r1(r)));
    // nlargest/nsmallest keep="first" and drop NaN: a stable sort keeps CSV order on ties
    const pick = (arr: Row[]) => arr.map((r) => ({ ticker: r.Ticker, rating: Math.trunc(r1(r)), color: color(Math.trunc(r1(r))) }));
    data[vn] = {
      top_rating_data: pick([...s].sort((a, b) => r1(b) - r1(a)).slice(0, top)),
      bottom_rating_data: pick([...s].sort((a, b) => r1(a) - r1(b)).slice(0, bottom)),
    };
  }
  const change = (r: Row) => r1(r) - p1(r);
  const fmt = (r: Row) => `${r.Ticker} (${Math.trunc(p1(r))} -> ${Math.trunc(r1(r))})`;
  const crosses: Record<string, { up: string; down: string }> = {};
  for (const ma of [20, 50, 200]) {
    const cur = (r: Row) => num(r[`Close_vs_MA${ma}`] as Num);
    const prev = (r: Row) => num(r[`Close_vs_MA${ma}_Prev`] as Num);
    crosses[`MA${ma}`] = {
      up: valid.filter((r) => prev(r) < 0 && cur(r) >= 0).map((r) => r.Ticker).join(", "),
      down: valid.filter((r) => prev(r) >= 0 && cur(r) < 0).map((r) => r.Ticker).join(", "),
    };
  }
  return {
    sectors: data,
    breakthrough_up: valid.filter((r) => change(r) >= 10).map(fmt).join(", "),
    breakthrough_down: valid.filter((r) => change(r) <= -10).map(fmt).join(", "),
    ma_crosses: crosses,
  };
}

/** Same shape as latest.json. `live` maps Ticker → today's bar; tickers without one use history only. */
export function runEngine(h: History, asOf: string, live: Record<string, LiveBar>) {
  const results = h.tickers.map((t) => analyzeTicker(t, asOf, h.window_days, live[t.Ticker]));
  return { results, totals: totals(results), sectors: sectors(results), errors: [] as string[], meta: { date: asOf, total_stocks: results.length } };
}
