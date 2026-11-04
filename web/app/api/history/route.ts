export const dynamic = "force-dynamic";

const GCS_BASE =
  process.env.GCS_PUBLIC_BASE ||
  "https://storage.googleapis.com/ta-tracking-data";

// Pre-open price history (history/base.json, written at 08:45) for in-browser intraday calc
export async function GET() {
  const upstream = await fetch(`${GCS_BASE}/history/base.json`, { cache: "no-store" });
  if (!upstream.ok) {
    return Response.json(
      { error: `Upstream error ${upstream.status}` },
      { status: upstream.status === 404 ? 404 : 502 }
    );
  }
  return new Response(await upstream.text(), {
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "public, max-age=60",
    },
  });
}
