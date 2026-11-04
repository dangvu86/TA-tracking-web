// Trading-session clock in Asia/Ho_Chi_Minh (UTC+7, no DST), independent of the viewer's timezone.
export type Phase = "closed" | "pre" | "open" | "lunch" | "closing" | "after";

const vn = (now: Date) => new Date(now.getTime() + 7 * 3600_000);
export const vnToday = (now = new Date()) => vn(now).toISOString().slice(0, 10);
export const vnMinutes = (now = new Date()) => vn(now).getUTCHours() * 60 + vn(now).getUTCMinutes();

export function phase(now = new Date()): Phase {
  const dow = vn(now).getUTCDay();
  if (dow === 0 || dow === 6) return "closed";
  const m = vnMinutes(now);
  if (m < 9 * 60) return "pre";
  if (m < 11 * 60 + 30) return "open";
  if (m < 13 * 60) return "lunch";
  if (m < 14 * 60 + 50) return "open";
  if (m < 15 * 60 + 15) return "closing"; // ATC done, waiting for the 15:00 final run
  return "after";
}
