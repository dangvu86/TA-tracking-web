import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { runEngine, totals, type History, type Row } from "./engine";

const DIR = join(__dirname, "__fixtures__", "pipeline");
const EXACT = ["Rating_1_Current", "Rating_1_Prev1", "Rating_1_Prev2", "Rating_2_Current", "Rating_2_Prev1",
  "Rating_2_Prev2", "Osc Buy", "Osc Sell", "MA Buy", "MA Sell", "MA50_GT_MA200"];
const FLOAT = ["Price", "% Change", "MA5", "Close_vs_MA5", "Close_vs_MA10", "Close_vs_MA20", "Close_vs_MA50",
  "Close_vs_MA100", "Close_vs_MA200", "STRENGTH_ST", "STRENGTH_LT", "Close_vs_MA20_Prev", "Close_vs_MA50_Prev",
  "Close_vs_MA200_Prev"];
const near = (a: number | null, b: number | null) =>
  a === null || b === null ? a === b : Math.abs(a - b) <= 1e-9 * Math.max(1, Math.abs(a));

const files = readdirSync(DIR);
const load = (f: string) => JSON.parse(readFileSync(join(DIR, f), "utf8")) as { history: History; expected: { results: Row[]; totals: Record<string, number>; sectors: unknown } };

describe.each(files)("pipeline %s", (file) => {
  const { history, expected } = load(file);
  const got = runEngine(history, history.as_of, {});
  const byT = Object.fromEntries(got.results.map((r) => [r.Ticker as string, r]));

  it("same tickers in the same order", () => {
    expect(got.results.map((r) => r.Ticker)).toEqual(expected.results.map((r) => r.Ticker));
  });
  it("signals, ratings and counts match exactly", () => {
    const bad: unknown[] = [];
    for (const e of expected.results) {
      const g = byT[e.Ticker as string];
      for (const k of [...EXACT, ...Object.keys(e).filter((k) => k.endsWith("_Signal"))])
        if (e[k] !== g[k]) bad.push([e.Ticker, k, e[k], g[k]]);
    }
    expect(bad).toEqual([]);
  });
  it("floats match within 1e-9", () => {
    const bad: unknown[] = [];
    for (const e of expected.results)
      for (const k of FLOAT)
        if (!near(e[k] as number | null, byT[e.Ticker as string][k] as number | null)) bad.push([e.Ticker, k, e[k], byT[e.Ticker as string][k]]);
    expect(bad).toEqual([]);
  });
  it("totals match", () => {
    for (const [k, v] of Object.entries(expected.totals)) expect(near(v, got.totals[k]), k).toBe(true);
  });
  it("sector summary matches exactly (incl. key order)", () => {
    expect(JSON.stringify(got.sectors)).toBe(JSON.stringify(expected.sectors));
  });
});

describe("live-mode row handling", () => {
  const { history, expected } = load(files[files.length - 1]);

  it("totals equal the sum of the rows actually shown after a substitution", () => {
    const rows = runEngine(history, history.as_of, {}).results.map((r) => ({ ...r }));
    rows[0] = { ...rows[0], Rating_1_Current: (rows[0].Rating_1_Current as number) + 5 };
    const sum = rows.reduce((s, r) => s + (typeof r.Rating_1_Current === "number" ? r.Rating_1_Current : 0), 0);
    expect(totals(rows).Rating_1_Current).toBe(sum);
  });

  it("a ticker without a live bar is recomputed with yesterday as Prev1", () => {
    const next = new Date(`${history.as_of}T00:00:00Z`);
    next.setUTCDate(next.getUTCDate() + 1);
    const tomorrow = next.toISOString().slice(0, 10);
    const got = runEngine(history, tomorrow, {});
    const hpgToday = expected.results.find((r) => r.Ticker === "HPG")!;
    const hpgTomorrow = got.results.find((r) => r.Ticker === "HPG")!;
    expect(hpgTomorrow.Rating_1_Prev1).not.toBe("N/A");
    // Same last bar, so the current rating stays computed from today's bar
    expect(hpgTomorrow.Price).toBe(hpgToday.Price);
  });
});
