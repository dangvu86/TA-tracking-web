"use client";

import { useState, useEffect, useCallback } from "react";
import { fetchLatestStatic, AnalyzeResponse } from "@/lib/api";
import SectorSummary from "@/components/SectorSummary";
import DataTable from "@/components/DataTable";

export default function Home() {
  const [date, setDate] = useState("");
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState<AnalyzeResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loadedDate, setLoadedDate] = useState<string | null>(null);

  // Load pre-computed latest data on mount
  useEffect(() => {
    fetchLatestStatic()
      .then((result) => {
        setData(result);
        setDate(result.meta.date);
        setLoadedDate(result.meta.date);
      })
      .catch((e) => setError(e instanceof Error ? e.message : "Failed to load data"));
  }, []);

  const handleRefresh = useCallback(async () => {
    if (!date) return;
    setLoading(true);
    setError(null);
    try {
      // Fetch pre-computed archive for the selected date (no realtime compute)
      const result = await fetchLatestStatic(date);
      setData(result);
      setLoadedDate(date);
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
        {loadedDate && !loading && (
          <span className="text-sm text-green-700">
            Data loaded for: {loadedDate}
            {loadedDate !== date && ` | Selected: ${date} - Click Refresh to update`}
          </span>
        )}
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
              {data.meta.total_stocks} stocks analyzed in {data.meta.elapsed_seconds}s
            </div>
          )}

          {/* Sector Summary */}
          {data.sectors?.sectors && (
            <SectorSummary
              sectors={data.sectors.sectors}
              breakthroughUp={data.sectors.breakthrough_up || ""}
              breakthroughDown={data.sectors.breakthrough_down || ""}
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
