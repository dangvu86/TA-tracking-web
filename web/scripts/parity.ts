// Daily guard: recompute the 15:00 final from history/final/<date>.json (the bars the
// server used) with the TS engine and compare with analysis/<date>.json. Also checks that
// this morning's base equals the final bars minus today (adjustments applied pre-open).
// Exit 1 on any mismatch. Usage: npm run parity [-- YYYY-MM-DD]
import { runEngine, type History } from "../lib/ta/engine";

const GCS = process.env.PARITY_GCS || "https://storage.googleapis.com/ta-tracking-data";
const EXACT = ["Rating_1_Current", "Rating_2_Current", "Rating_1_Prev1", "Rating_2_Prev1", "Rating_1_Prev2",
  "Rating_2_Prev2", "Osc Buy", "Osc Sell", "MA Buy", "MA Sell", "MA50_GT_MA200"];

async function get(path: string) {
  const res = await fetch(`${GCS}/${path}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`${path}: HTTP ${res.status}`);
  return res.json();
}

async function main() {
  const date: string = process.argv[2] ?? (await get("latest.json")).meta.date;
  const h: History = await get(`history/final/${date}.json`);
  const final = await get(`analysis/${date}.json`);
  const base: History = await get("history/base.json");
  const got = runEngine(h, date, {});
  const byT = Object.fromEntries(got.results.map((r) => [r.Ticker as string, r]));
  const bad: string[] = [];

  for (const e of final.results) {
    const g = byT[e.Ticker];
    for (const k of [...EXACT, ...Object.keys(e).filter((k) => k.endsWith("_Signal"))])
      if (e[k] !== g?.[k]) bad.push(`${e.Ticker} ${k}: server=${e[k]} ts=${g?.[k]}`);
  }
  if (JSON.stringify(got.sectors) !== JSON.stringify(final.sectors)) bad.push("sector summary differs");

  // Base (08:45) must be the final bars without today's: catches late corporate-action adjustments
  if (base.as_of === date) {
    const baseBy = Object.fromEntries(base.tickers.map((t) => [t.Ticker, t]));
    for (const t of h.tickers) {
      const b = baseBy[t.Ticker];
      const n = t.d[t.d.length - 1] === date ? t.d.length - 1 : t.d.length;
      const same = b && b.d.length === n &&
        b.d.every((d, i) => d === t.d[i] && b.c[i] === t.c[i] && b.h[i] === t.h[i] && b.l[i] === t.l[i] && b.v[i] === t.v[i]);
      if (!same) bad.push(`${t.Ticker}: base.json (08:45) differs from the bars used at 15:00`);
    }
  } else if (base.as_of < date) bad.push(`base.json is for ${base.as_of}, not ${date} (08:45 run missing?)`);
  else console.log(`base check skipped: base.json is already for ${base.as_of}`);

  console.log(`${date}: ${final.results.length} tickers, ${bad.length} mismatches`);
  bad.forEach((b) => console.log(b));
  process.exit(bad.length ? 1 : 0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
