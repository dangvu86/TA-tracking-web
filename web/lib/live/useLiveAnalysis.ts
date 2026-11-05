"use client";
// Live intraday mode: only on the latest view, on a trading day, once the 08:45 base
// file for today exists and until the 15:00 final for today has landed.
import { useEffect, useRef, useState } from "react";
import type { AnalyzeResponse } from "@/lib/api";
import { fetchLatestStatic } from "@/lib/api";
import { runEngine, sectors, totals, type History, type LiveBar } from "@/lib/ta/engine";
import { phase, vnMinutes, vnToday } from "./session";
import { fetchIndexBar, fetchStockBars } from "./vndirect";

export const LIVE_ENABLED = process.env.NEXT_PUBLIC_LIVE_ENABLED !== "false";
const POLL_MS = 30_000;
const VN_INDICES = ["VNINDEX", "VNMID"];
const STOP_WAITING_FINAL = 18 * 60; // stop polling latest.json at 18:00 VN

export type LiveState =
  | { kind: "final" }
  | { kind: "live"; updatedAt: Date; adjusted: string[] }
  | { kind: "error"; message: string };

export function useLiveAnalysis(finalData: AnalyzeResponse | null, isLatestView: boolean) {
  const [data, setData] = useState<AnalyzeResponse | null>(finalData);
  const [state, setState] = useState<LiveState>({ kind: "final" });
  const history = useRef<History | null>(null);

  useEffect(() => {
    setData(finalData);
    setState({ kind: "final" });
    if (!LIVE_ENABLED || !finalData || !isLatestView) return;
    let stopped = false;
    let inFlight = false; // one scheduler only: a visibility change never starts a second loop
    let timer: ReturnType<typeof setTimeout> | undefined;

    const schedule = (ms: number) => {
      clearTimeout(timer);
      if (!stopped) timer = setTimeout(tick, ms);
    };

    const refresh = async (today: string) => {
      if (!history.current) {
        const res = await fetch("/api/history", { cache: "no-store" });
        if (!res.ok) throw new Error("không tải được dữ liệu lịch sử");
        history.current = await res.json();
      }
      const h = history.current!;
      // base.json is built at 08:45 for today; an older file means the morning run failed
      if (h.as_of !== today) {
        history.current = null;
        throw new Error("chưa có dữ liệu lịch sử sáng nay");
      }
      const stocks = h.tickers.filter((t) => ["HOSE", "HNX", "UPCOM"].includes(t.Exchange));
      const lastClose: Record<string, number> = Object.fromEntries(
        stocks.map((t) => [t.Ticker, t.c[t.c.length - 1] ?? NaN]),
      );
      const prevSession = stocks.map((t) => t.d[t.d.length - 1]).filter(Boolean).sort().pop()!;
      const [{ bars, adjusted }, ...idx] = await Promise.all([
        fetchStockBars(stocks.map((t) => t.Ticker), today, prevSession, lastClose),
        ...VN_INDICES.map((s) => fetchIndexBar(s, today)),
      ]);
      const live: Record<string, LiveBar> = { ...bars };
      VN_INDICES.forEach((s, i) => {
        if (idx[i]) live[s] = idx[i]!;
      });
      if (Object.keys(live).length === 0) return; // holiday or no trade yet
      const out = runEngine(h, today, live);
      // Halted VN tickers and US indices keep the recomputed row: that is what 15:00 will
      // produce (US bars do not change during the VN session). Only tickers whose base is
      // not yet adjusted (safety net) fall back to yesterday's final row.
      const finalRow = Object.fromEntries(finalData.results.map((r) => [r.Ticker, r]));
      const fallback = new Set(adjusted);
      const results = out.results.map((r) =>
        fallback.has(r.Ticker as string) ? (finalRow[r.Ticker as string] ?? r) : r,
      ) as AnalyzeResponse["results"];
      // Totals and Summary from the rows actually shown
      const rows = results as unknown as Parameters<typeof totals>[0];
      if (!stopped) {
        setData({
          ...finalData,
          results,
          totals: totals(rows),
          sectors: sectors(rows) as unknown as AnalyzeResponse["sectors"],
          errors: [],
          meta: { ...finalData.meta, date: today },
        });
        setState({ kind: "live", updatedAt: new Date(), adjusted });
      }
    };

    const tick = async () => {
      if (stopped || inFlight) return;
      inFlight = true;
      const today = vnToday();
      const p = phase();
      const m = vnMinutes();
      let next: number | null = null;
      try {
        if (finalData.meta.date >= today || p === "closed") return; // nothing to do today
        if (p === "closing" || p === "after") {
          const latest = await fetchLatestStatic();
          if (latest.meta.date >= today) {
            setData(latest);
            setState({ kind: "final" });
            return;
          }
          // keep waiting for the 15:00 final (run + 5-min file cache), but not forever
          if (p === "after" && m >= STOP_WAITING_FINAL) return;
        }
        // lunch included: a page opened at 12:00 still shows the morning session
        if ((p === "open" || p === "lunch" || p === "closing") && document.visibilityState === "visible") await refresh(today);
        next = p === "pre" && m < 8 * 60 + 55 ? (9 * 60 - m) * 60_000 : POLL_MS; // sleep until the open
      } catch (e) {
        setState({ kind: "error", message: e instanceof Error ? e.message : String(e) });
        next = POLL_MS;
      } finally {
        inFlight = false;
        if (next !== null) schedule(next);
      }
    };

    const onVisible = () => {
      if (document.visibilityState === "visible") schedule(0);
    };
    document.addEventListener("visibilitychange", onVisible);
    tick();
    return () => {
      stopped = true;
      clearTimeout(timer);
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, [finalData, isLatestView]);

  return { data, state };
}
