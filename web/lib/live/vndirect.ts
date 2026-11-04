// Live intraday quotes from VNDirect public endpoints (CORS open, no auth, ~1 min delay).
// finfo prices are in thousand VND; dchart index values are in points.
import type { LiveBar } from "../ta/engine";

const FINFO = "https://api-finfo.vndirect.com.vn/v4/stock_prices";
const DCHART = "https://dchart-api.vndirect.com.vn/dchart/history";
// Matches FireAnt Volume exactly on HOSE; HNX/UPCOM differ slightly (odd lots), which only
// moves VWMA_20 of the provisional bar.
const VOLUME = (r: FinfoRow) => r.nmVolume;

export type FinfoRow = {
  code: string; date: string; open: number; high: number; low: number; close: number;
  nmVolume: number; ptVolume: number; adClose: number;
};

/**
 * Today's bar per ticker. Safety net only: base.json is rebuilt at 08:45, after FireAnt
 * applies corporate-action adjustments [chưa kiểm chứng, Task 6.4]. If VNDirect's adjusted
 * close for the previous session still differs from our base close by more than 0.2%
 * (providers round adjusted prices differently), the base was not adjusted in time:
 * live calc would show a false drop, so the ticker is reported as "adjusted".
 */
export function parseFinfo(rows: FinfoRow[], today: string, baseLastClose: Record<string, number>) {
  const bars: Record<string, LiveBar> = {};
  const adjusted: string[] = [];
  const byCode = new Map<string, FinfoRow[]>();
  for (const r of rows) byCode.set(r.code, [...(byCode.get(r.code) ?? []), r]);
  for (const [code, rs] of byCode) {
    const t = rs.find((r) => r.date === today);
    if (!t) continue;
    const prev = rs.filter((r) => r.date < today).sort((a, b) => (a.date < b.date ? 1 : -1))[0];
    const base = baseLastClose[code];
    if (prev && base !== undefined && Math.abs(prev.adClose * 1000 - base) > 0.002 * base) {
      adjusted.push(code);
      continue;
    }
    // 16.01 * 1000 = 16009.999999999998: round so prices are integers like FireAnt's
    const vnd = (x: number) => Math.round(x * 1000);
    bars[code] = { o: vnd(t.open), h: vnd(t.high), l: vnd(t.low), c: vnd(t.close), v: VOLUME(t) };
  }
  return { bars, adjusted };
}

async function getJson(url: string) {
  const res = await fetch(url, { cache: "no-store" }); // finfo sends max-age=300
  if (!res.ok) throw new Error(`VNDirect HTTP ${res.status}`);
  return res.json();
}

export async function fetchStockBars(codes: string[], today: string, prevSession: string, baseLastClose: Record<string, number>) {
  const q = `code:${codes.join(",")}~date:gte:${prevSession}`;
  const json = await getJson(`${FINFO}?q=${encodeURIComponent(q)}&size=${codes.length * 3}`);
  return parseFinfo(json.data ?? [], today, baseLastClose);
}

/** Daily bar for today from dchart (VNINDEX, VNMID). */
export async function fetchIndexBar(symbol: string, today: string): Promise<LiveBar | undefined> {
  const now = Math.floor(Date.now() / 1000);
  const j = await getJson(`${DCHART}?resolution=D&symbol=${symbol}&from=${now - 5 * 86400}&to=${now + 86400}`);
  const i = (j.t ?? []).length - 1;
  if (i < 0) return undefined;
  // Bars are stamped 00:00 UTC of the trading day
  const day = new Date(j.t[i] * 1000).toISOString().slice(0, 10);
  if (day !== today) return undefined;
  return { o: j.o[i], h: j.h[i], l: j.l[i], c: j.c[i], v: j.v[i] };
}
