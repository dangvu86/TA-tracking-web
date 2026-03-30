"use client";

import { SectorData } from "@/lib/api";

interface SectorSummaryProps {
  sectors: Record<string, SectorData>;
  breakthroughUp: string;
  breakthroughDown: string;
}

const SECTOR_ORDER = [
  "Chứng khoán",
  "Bất động sản",
  "Xây dựng & ĐTC, VLXD",
  "Dầu, Hàng không, Agri",
  "Xuất khẩu",
  "Ngân hàng",
  "FAV",
];

function RatingText({ ticker, rating, color }: { ticker: string; rating: number; color: string }) {
  const colorClass = color === "green" ? "text-green-700" : color === "red" ? "text-red-600" : "text-black";
  return <span className={colorClass}>{ticker} ({rating})</span>;
}

export default function SectorSummary({ sectors, breakthroughUp, breakthroughDown }: SectorSummaryProps) {
  if (!sectors || Object.keys(sectors).length === 0) return null;

  return (
    <div className="mb-4">
      <h3 className="text-lg font-semibold mb-2">Sector Summary</h3>
      <table className="w-full border-collapse bg-white">
        <thead>
          <tr>
            <th className="border border-gray-300 p-1.5 text-center text-sm font-bold bg-gray-100" style={{ width: "15%" }}>Rating</th>
            <th className="border border-gray-300 p-1.5 text-center text-sm font-bold bg-gray-100" style={{ width: "42.5%" }}>Top cao điểm</th>
            <th className="border border-gray-300 p-1.5 text-center text-sm font-bold bg-gray-100" style={{ width: "42.5%" }}>Top thấp điểm</th>
          </tr>
        </thead>
        <tbody>
          {SECTOR_ORDER.map((sectorName) => {
            const data = sectors[sectorName];
            if (!data) return null;
            return (
              <tr key={sectorName}>
                <td className="border border-gray-300 p-1 text-center text-sm">{sectorName}</td>
                <td className="border border-gray-300 p-1 text-center text-sm">
                  {data.top_rating_data.map((item, i) => (
                    <span key={item.ticker}>
                      {i > 0 && ", "}
                      <RatingText {...item} />
                    </span>
                  ))}
                </td>
                <td className="border border-gray-300 p-1 text-center text-sm">
                  {data.bottom_rating_data.map((item, i) => (
                    <span key={item.ticker}>
                      {i > 0 && ", "}
                      <RatingText {...item} />
                    </span>
                  ))}
                </td>
              </tr>
            );
          })}
          {/* Breakthrough rows - always show */}
          <tr>
            <td className="border border-gray-300 p-1 text-center text-sm italic text-gray-700">Nhóm đột phá</td>
            <td colSpan={2} className="border border-gray-300 p-1 text-left text-sm italic text-green-700">
              {breakthroughUp || "—"}
            </td>
          </tr>
          <tr>
            <td className="border border-gray-300 p-1 text-center text-sm italic text-gray-700">Nhóm giảm điểm</td>
            <td colSpan={2} className="border border-gray-300 p-1 text-left text-sm italic text-red-600">
              {breakthroughDown || "—"}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  );
}
