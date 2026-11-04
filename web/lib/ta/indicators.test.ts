import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { calculateAllIndicators } from "./indicators";

const DIR = join(__dirname, "__fixtures__", "indicators");

// null = NaN; "Infinity"/"-Infinity" must match exactly (CCI with MAD = 0 flips signals)
function close(a: number | string | null, b: number) {
  if (a === null) return Number.isNaN(b);
  if (a === "Infinity") return b === Infinity;
  if (a === "-Infinity") return b === -Infinity;
  if (!Number.isFinite(b)) return false;
  return Math.abs((a as number) - b) <= 1e-9 * Math.max(1, Math.abs(a as number));
}
const toNum = (xs: (number | null)[]) => xs.map((x) => (x === null ? NaN : x));

describe.each(readdirSync(DIR))("indicators %s", (file) => {
  const { columns } = JSON.parse(readFileSync(join(DIR, file), "utf8"));
  const bars = { o: toNum(columns.Open), h: toNum(columns.High), l: toNum(columns.Low), c: toNum(columns.Close), v: toNum(columns.Volume) };
  const got = calculateAllIndicators(bars);
  for (const key of Object.keys(columns)) {
    if (["Open", "High", "Low", "Close", "Volume"].includes(key)) continue;
    it(key, () => {
      const exp = columns[key] as (number | string | boolean | null)[];
      const act = got[key];
      expect(act, `missing column ${key}`).toBeDefined();
      const bad = exp.flatMap((e, i) =>
        typeof e === "boolean" ? (e === act[i] ? [] : [i]) : close(e, act[i] as number) ? [] : [i],
      );
      expect(bad.slice(0, 5).map((i) => [i, exp[i], act[i]])).toEqual([]);
    });
  }
});
