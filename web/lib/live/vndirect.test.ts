import { expect, it } from "vitest";
import { parseFinfo, type FinfoRow } from "./vndirect";

const row = (o: Partial<FinfoRow>): FinfoRow => ({
  code: "HPG", date: "2026-11-04", open: 21.15, high: 21.6, low: 21.1, close: 21.5,
  nmVolume: 10_901_000, ptVolume: 0, adClose: 21.25, ...o,
});

it("builds today's bar in VND and flags tickers whose base is not adjusted", () => {
  const out = parseFinfo(
    [row({}), row({ date: "2026-11-03", close: 21.25, adClose: 21.27 }), // 0.09%: rounding, not adjusted
      row({ code: "SSI", close: 19.6 }), row({ code: "SSI", date: "2026-11-03", close: 19.9, adClose: 19.0 })],
    "2026-11-04", { HPG: 21250, SSI: 19900 },
  );
  expect(out.bars.HPG).toEqual({ o: 21150, h: 21600, l: 21100, c: 21500, v: 10_901_000 });
  expect(out.adjusted).toEqual(["SSI"]);
  expect(out.bars.SSI).toBeUndefined();
});

it("accepts an adjusted base (PC1 on 2026-11-03: FireAnt 15956.52, VNDirect adClose 15.957)", () => {
  const out = parseFinfo(
    [row({ code: "PC1", close: 18.4 }), row({ code: "PC1", date: "2026-11-03", close: 18.35, adClose: 15.957 })],
    "2026-11-04", { PC1: 15956.521739130438 },
  );
  expect(out.adjusted).toEqual([]);
  expect(out.bars.PC1.c).toBe(18400);
});

it("converts thousand-VND prices to exact integers", () => {
  expect(parseFinfo([row({ close: 16.01 })], "2026-11-04", {}).bars.HPG.c).toBe(16010);
});

it("returns nothing when VNDirect has no row for today (holiday)", () => {
  expect(parseFinfo([row({ date: "2026-11-03" })], "2026-11-04", { HPG: 21250 }).bars).toEqual({});
});
