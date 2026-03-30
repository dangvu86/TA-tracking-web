"use client";

import { useMemo } from "react";
import { StockResult } from "@/lib/api";

interface DataTableProps {
  results: StockResult[];
  totals: Record<string, number | null>;
}

// Column definitions - easy to customize
interface Column {
  key: string;
  label: string;
  width: number;
  pin?: boolean;
  format?: string;
  gradient?: string;
  separator?: boolean;
  align?: string;
}

const COLUMNS: Column[] = [
  { key: "Ticker", label: "Ticker", width: 70, pin: true },
  { key: "Rating_2_Current", label: "R2 T", width: 45, format: "int", gradient: "r2" },
  { key: "Rating_2_Prev1", label: "R2 T-1", width: 45, format: "int", gradient: "r2" },
  { key: "Rating_2_Prev2", label: "R2 T-2", width: 45, format: "int", gradient: "r2" },
  { key: "_sep1", label: "", width: 8, separator: true },
  { key: "Rating_1_Current", label: "R1 T", width: 45, format: "int", gradient: "r1" },
  { key: "Rating_1_Prev1", label: "R1 T-1", width: 45, format: "int", gradient: "r1" },
  { key: "Rating_1_Prev2", label: "R1 T-2", width: 45, format: "int", gradient: "r1" },
  { key: "MA50_GT_MA200", label: "MA50>200", width: 55 },
  { key: "_sep2", label: "", width: 8, separator: true },
  { key: "STRENGTH_ST", label: "ST Str", width: 55, format: "round", gradient: "st" },
  { key: "STRENGTH_LT", label: "LT Str", width: 55, format: "round", gradient: "lt" },
  { key: "_sep3", label: "", width: 8, separator: true },
  { key: "Price", label: "Price", width: 80, format: "price", align: "right" },
  { key: "% Change", label: "%Chg", width: 60, format: "changePct" },
  { key: "Close_vs_MA5", label: "vs MA5", width: 60, format: "changePct", gradient: "ma" },
  { key: "Close_vs_MA10", label: "vs MA10", width: 60, format: "changePct", gradient: "ma" },
  { key: "Close_vs_MA20", label: "vs MA20", width: 60, format: "changePct", gradient: "ma" },
  { key: "Close_vs_MA50", label: "vs MA50", width: 60, format: "changePct", gradient: "ma" },
  { key: "Close_vs_MA200", label: "vs MA200", width: 60, format: "changePct", gradient: "ma" },
  { key: "Sector", label: "Sector", width: 60 },
];

// Compute min/max for gradient scaling
function computeMinMax(results: StockResult[], keys: string[]): [number, number] {
  let min = 0, max = 0;
  for (const r of results) {
    for (const k of keys) {
      const v = Number(r[k]);
      if (!isNaN(v)) {
        if (v < min) min = v;
        if (v > max) max = v;
      }
    }
  }
  return [min, max];
}

function getGradientColor(value: number, min: number, max: number): string {
  const range = Math.max(Math.abs(min), Math.abs(max)) || 1;
  const alpha = Math.min(Math.abs(value) / range, 1) * 0.6;
  return value >= 0
    ? `rgba(34,197,94,${alpha})`
    : `rgba(239,68,68,${alpha})`;
}

function formatValue(value: unknown, format: string | undefined, sector?: string): string {
  if (value == null || value === "" || value === "N/A") return "";
  const n = Number(value);

  switch (format) {
    case "round":
      return isNaN(n) ? String(value) : Math.round(n).toString();
    case "int":
      return isNaN(n) ? String(value) : Math.round(n).toString();
    case "price":
      if (isNaN(n)) return String(value);
      if (sector === "Index") return n.toLocaleString("en-US", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
      return Math.round(n).toLocaleString("en-US");
    case "changePct":
      return isNaN(n) ? "" : `${n.toFixed(1)}%`;
    default:
      return String(value);
  }
}

export default function DataTable({ results, totals }: DataTableProps) {
  // Pre-compute gradient ranges
  const gradientRanges = useMemo(() => ({
    st: computeMinMax(results, ["STRENGTH_ST"]),
    lt: computeMinMax(results, ["STRENGTH_LT"]),
    r1: computeMinMax(results, ["Rating_1_Current", "Rating_1_Prev1", "Rating_1_Prev2"]),
    r2: computeMinMax(results, ["Rating_2_Current", "Rating_2_Prev1", "Rating_2_Prev2"]),
    ma: computeMinMax(results, ["Close_vs_MA5", "Close_vs_MA10", "Close_vs_MA20", "Close_vs_MA50", "Close_vs_MA200"]),
  }), [results]);

  // Build totals row
  const totalsRow = useMemo(() => {
    const row: Record<string, unknown> = { Ticker: "TOTAL", Sector: `(${results.length})` };
    const sumKeys = ["STRENGTH_ST", "STRENGTH_LT", "Rating_1_Current", "Rating_1_Prev1", "Rating_1_Prev2", "Rating_2_Current", "Rating_2_Prev1", "Rating_2_Prev2"];
    for (const k of sumKeys) {
      row[k] = totals[k] ?? "";
    }
    return row;
  }, [results, totals]);

  function getCellStyle(col: Column, value: unknown, isTotals: boolean): React.CSSProperties {
    if (isTotals) return { backgroundColor: "#dcfce7", fontWeight: "bold" };

    // Gradient
    if (col.gradient) {
      const n = Number(value);
      if (!isNaN(n) && value != null && value !== "N/A") {
        const [min, max] = gradientRanges[col.gradient as keyof typeof gradientRanges];
        return { backgroundColor: getGradientColor(n, min, max) };
      }
    }

    // MA50>MA200
    if (col.key === "MA50_GT_MA200") {
      return { color: value === "Yes" ? "#16a34a" : value === "No" ? "#dc2626" : "inherit" };
    }

    // % Change
    if (col.key === "% Change") {
      const n = Number(value);
      if (!isNaN(n)) return { color: n >= 0 ? "#16a34a" : "#dc2626" };
    }

    return {};
  }

  return (
    <div className="overflow-auto border rounded" style={{ maxHeight: 900, WebkitOverflowScrolling: "touch" }}>
      <table className="data-table border-collapse text-xs w-max">
        {/* Header */}
        <thead className="sticky top-0 z-10">
          <tr>
            {COLUMNS.map((col, i) => (
              <th
                key={i}
                className={`border border-gray-200 px-1 py-1.5 text-center font-semibold bg-gray-100 whitespace-nowrap ${
                  col.pin ? "sticky left-0 z-20 bg-gray-100" : ""
                }`}
                style={{ width: col.width, minWidth: col.width, fontSize: 12 }}
              >
                {col.label}
              </th>
            ))}
          </tr>
        </thead>

        <tbody>
          {/* Data rows */}
          {results.map((row, rowIdx) => (
            <tr key={rowIdx} className="hover:bg-green-50">
              {COLUMNS.map((col, colIdx) => {
                if (col.separator) return <td key={colIdx} className="border-none" style={{ width: col.width }} />;

                const value = row[col.key as keyof StockResult];
                const display = formatValue(value, col.format, row.Sector);
                const style = getCellStyle(col, value, false);

                return (
                  <td
                    key={colIdx}
                    className={`border border-gray-100 px-1 py-0.5 text-center ${
                      col.pin ? "sticky left-0 z-10 bg-white font-medium" : ""
                    } ${col.align === "right" ? "text-right" : ""}`}
                    style={{ ...style, width: col.width, fontSize: 12 }}
                  >
                    {display}
                  </td>
                );
              })}
            </tr>
          ))}

          {/* Totals row */}
          <tr>
            {COLUMNS.map((col, colIdx) => {
              if (col.separator) return <td key={colIdx} style={{ width: col.width }} />;

              const value = totalsRow[col.key];
              const display = formatValue(value, col.format);
              const style = getCellStyle(col, value, true);

              return (
                <td
                  key={colIdx}
                  className={`border border-gray-200 px-1 py-0.5 text-center ${
                    col.pin ? "sticky left-0 z-10" : ""
                  }`}
                  style={{ ...style, width: col.width, fontSize: 12, borderTop: "2px solid #6c757d" }}
                >
                  {display}
                </td>
              );
            })}
          </tr>
        </tbody>
      </table>
    </div>
  );
}
