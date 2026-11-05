"use client";

import { useState, useEffect, useCallback } from "react";
import { fetchLatestStatic, AnalyzeResponse } from "@/lib/api";
import SectorSummary from "@/components/SectorSummary";
import DataTable from "@/components/DataTable";
import LiveBadge from "@/components/LiveBadge";
import { useLiveAnalysis } from "@/lib/live/useLiveAnalysis";
import { vnToday } from "@/lib/live/session";

export default function Home() {
  const [date, setDate] = useState("");
  const [loading, setLoading] = useState(false);
  const [finalData, setData] = useState<AnalyzeResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  // "Latest" view follows today's session (intraday provisional numbers until the 15:00
  // final lands); picking a past date shows that day's archive and never goes live.
  const [followLatest, setFollowLatest] = useState(true);
  const { data, state: liveState } = useLiveAnalysis(finalData, followLatest);
  // The date actually on screen: today while live, else the final/archive date
  const shownDate = data?.meta.date ?? null;

  // While following the latest view, the date box tracks what is shown (05/11 once live data arrives)
  useEffect(() => {
    if (followLatest && shownDate) setDate(shownDate);
  }, [followLatest, shownDate]);

  // Load pre-computed latest data on mount
  useEffect(() => {
    fetchLatestStatic()
      .then((result) => {
        setData(result);
        setDate(result.meta.date);
      })
      .catch((e) => setError(e instanceof Error ? e.message : "Failed to load data"));
  }, []);

  const handleRefresh = useCallback(async () => {
    if (!date) return;
    setLoading(true);
    setError(null);
    try {
      const today = vnToday();
      if (date > today) throw new Error(`Chưa có dữ liệu cho ngày ${date}`);
      if (date === today) {
        // Today: latest final, which goes live during the session if today's final is not out yet
        setData(await fetchLatestStatic());
        setFollowLatest(true);
      } else {
        // Past date: the pre-computed archive as-is
        const result = await fetchLatestStatic(date);
        setFollowLatest(false);
        setData(result);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load data for this date");
    } finally {
      setLoading(false);
    }
  }, [date]);

  return (
    <main className="p-2 sm:p-4 max-w-full">
      {/* Header */}
      <div className="text-center p-1.5 sm:p-2 bg-green-100 rounded mb-2 sm:mb-3">
        <h1 className="text-base sm:text-xl font-bold text-gray-600">Technical Tracking Summary</h1>
      </div>

      {/* Controls */}
      <div className="flex flex-wrap items-center gap-2 sm:gap-4 mb-2 sm:mb-3">
        <div className="flex items-center gap-1 sm:gap-2">
          <label className="text-xs sm:text-sm font-medium">Date:</label>
          <input
            type="date"
            value={date}
            onChange={(e) => setDate(e.target.value)}
            className="border rounded px-2 py-1 text-sm"
          />
        </div>

        <button
          onClick={handleRefresh}
          disabled={loading || !date}
          className="bg-blue-600 text-white px-4 py-1.5 rounded text-sm font-medium hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed"
        >
          {loading ? "Loading..." : "Load Date"}
        </button>

        {/* Status */}
        {shownDate && !loading && (
          <span className="text-sm text-green-700">
            Data loaded for: {shownDate}
            {shownDate !== date && ` | Selected: ${date} - Click Load Date to update`}
          </span>
        )}
        {shownDate && !loading && <LiveBadge state={liveState} finalDate={shownDate} />}
        {!data && !loading && !error && (
          <span className="text-sm text-yellow-600">Loading latest data...</span>
        )}
      </div>

      {/* Loading */}
      {loading && (
        <div className="flex items-center gap-3 mb-3 p-3 bg-blue-50 rounded">
          <div className="animate-spin h-5 w-5 border-2 border-blue-600 border-t-transparent rounded-full" />
          <span className="text-sm">Loading data for {date}...</span>
        </div>
      )}

      {/* Error */}
      {error && (
        <div className="p-3 mb-3 bg-red-50 text-red-700 rounded text-sm">{error}</div>
      )}

      {/* Results */}
      {data && (
        <>
          {/* Meta */}
          {data.meta && (
            <div className="text-xs text-gray-500 mb-2">
              {data.meta.total_stocks} stocks{liveState.kind === "live" ? "" : ` analyzed in ${data.meta.elapsed_seconds}s`}
            </div>
          )}

          {/* Sector Summary */}
          {data.sectors?.sectors && (
            <SectorSummary
              sectors={data.sectors.sectors}
              breakthroughUp={data.sectors.breakthrough_up || ""}
              breakthroughDown={data.sectors.breakthrough_down || ""}
              maCrosses={data.sectors.ma_crosses || {}}
            />
          )}

          {/* Data Table */}
          <DataTable results={data.results} totals={data.totals} />

          {/* Errors */}
          {data.errors && data.errors.length > 0 && (
            <div className="mt-3 p-2 bg-yellow-50 rounded text-sm">
              <strong>Warnings:</strong>
              <ul className="list-disc ml-5">
                {data.errors.map((err, i) => (
                  <li key={i} className="text-yellow-700">{err}</li>
                ))}
              </ul>
            </div>
          )}
        </>
      )}
    </main>
  );
}
