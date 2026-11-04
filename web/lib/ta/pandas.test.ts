import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { ewmMean, npSum, rollingMax, rollingMean, rollingMin, rollingSum } from "./pandas";

type Cell = number | string | null;
const num = (v: Cell) => (v === null ? NaN : v === "Infinity" ? Infinity : v === "-Infinity" ? -Infinity : (v as number));
const { cases } = JSON.parse(readFileSync(join(__dirname, "__fixtures__", "numerics.json"), "utf8")) as {
  cases: Record<string, Cell[] | string | number[]>[];
};

// Bit-exact: every value must be identical to pandas/numpy (NaN == NaN)
const same = (a: number[], b: number[]) =>
  a.flatMap((v, i) => (Object.is(v, b[i]) || (v === b[i]) ? [] : [[i, v, b[i]]])).slice(0, 5);

describe.each(cases)("pandas primitives: $name", (c) => {
  const x = (c.x as Cell[]).map(num);
  const exp = (k: string) => (c[k] as Cell[]).map(num);
  it("rolling mean 3/20", () => {
    expect(same(rollingMean(x, 3), exp("mean3"))).toEqual([]);
    expect(same(rollingMean(x, 20), exp("mean20"))).toEqual([]);
  });
  it("rolling sum 7/20", () => {
    expect(same(rollingSum(x, 7), exp("sum7"))).toEqual([]);
    expect(same(rollingSum(x, 20), exp("sum20"))).toEqual([]);
  });
  it("rolling max 14 / min 52 (min_periods=0)", () => {
    expect(same(rollingMax(x, 14), exp("max14"))).toEqual([]);
    expect(same(rollingMin(x, 52, 0), exp("min52p0"))).toEqual([]);
  });
  it("ewm span / alpha", () => {
    expect(same(ewmMean(x, { span: 12 }, 12), exp("ema12"))).toEqual([]);
    expect(same(ewmMean(x, { alpha: 1 / 14 }, 14), exp("rsiw"))).toEqual([]);
  });
  it("numpy sum", () => {
    const finite = x.filter((v) => Number.isFinite(v));
    const got = (c.npsum_k as number[]).map((k) => npSum(finite.slice(0, k)));
    expect(got).toEqual(c.npsum);
  });
});

describe("pandas edge cases", () => {
  it("rolling mean returns the exact value for a constant window", () => {
    expect(rollingMean([0.1, 0.2, 0.3, 49300, 49300, 49300], 3)[5]).toBe(49300);
  });
  it("rolling treats ±inf as NaN", () => {
    expect(rollingMean([1, Infinity, 2, 3], 2)).toEqual([NaN, NaN, NaN, 2.5]);
  });
  it("ewm adjust=False starts at first observation", () => {
    expect(ewmMean([NaN, 2, 4], { span: 3 }, 1)).toEqual([NaN, 2, 3]);
  });
});
