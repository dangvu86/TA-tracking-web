"use client";

import type { LiveState } from "@/lib/live/useLiveAnalysis";

export default function LiveBadge({ state, finalDate }: { state: LiveState; finalDate?: string | null }) {
  if (state.kind === "live") {
    const t = state.updatedAt.toLocaleTimeString("vi-VN", { timeZone: "Asia/Ho_Chi_Minh" });
    return (
      <span className="text-sm text-amber-700">
        ● Tạm tính trong phiên – cập nhật {t} (giá VNDirect, trễ khoảng 1 phút). Số chính thức chốt lúc 15:00.
        {state.adjusted.length > 0 &&
          ` Giữ số chốt hôm trước (giá lịch sử chưa được điều chỉnh): ${state.adjusted.join(", ")}.`}
      </span>
    );
  }
  if (state.kind === "error") {
    return (
      <span className="text-sm text-red-600">
        Không lấy được giá trong phiên ({state.message}) – đang hiện số chốt ngày {finalDate}.
      </span>
    );
  }
  return <span className="text-sm text-green-700">Số chốt ngày {finalDate}</span>;
}
