const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

export interface StockResult {
  Sector: string;
  Ticker: string;
  Price: number | null;
  "% Change": number | null;
  STRENGTH_ST: number | null;
  STRENGTH_LT: number | null;
  Rating_1_Current: number | null;
  Rating_1_Prev1: number | string | null;
  Rating_1_Prev2: number | string | null;
  Rating_2_Current: number | null;
  Rating_2_Prev1: number | string | null;
  Rating_2_Prev2: number | string | null;
  MA50_GT_MA200: string;
  Close_vs_MA5: number | null;
  Close_vs_MA10: number | null;
  Close_vs_MA20: number | null;
  Close_vs_MA50: number | null;
  Close_vs_MA200: number | null;
  [key: string]: unknown;
}

export interface SectorItem {
  ticker: string;
  rating: number;
  color: string;
}

export interface SectorData {
  top_rating_data: SectorItem[];
  bottom_rating_data: SectorItem[];
}

export interface AnalyzeResponse {
  results: StockResult[];
  sectors: {
    sectors: Record<string, SectorData>;
    breakthrough_up: string;
    breakthrough_down: string;
  };
  totals: Record<string, number | null>;
  errors: string[];
  meta: {
    date: string;
    total_stocks: number;
    elapsed_seconds: number;
  };
}

export async function getLastTradingDate(): Promise<string> {
  const res = await fetch(`${API_URL}/api/last-trading-date`);
  if (!res.ok) throw new Error("Failed to fetch last trading date");
  const data = await res.json();
  return data.date;
}

export async function analyzeStocks(date: string): Promise<AnalyzeResponse> {
  const res = await fetch(`${API_URL}/api/analyze`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ date }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: "Server error" }));
    throw new Error(err.detail || "Analysis failed");
  }
  return res.json();
}
