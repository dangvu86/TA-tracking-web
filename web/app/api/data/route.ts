import { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

const GCS_BASE =
  process.env.GCS_PUBLIC_BASE ||
  "https://storage.googleapis.com/ta-tracking-data";

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

export async function GET(req: NextRequest) {
  const date = req.nextUrl.searchParams.get("date");

  let path: string;
  if (date) {
    if (!DATE_RE.test(date)) {
      return Response.json(
        { error: "Invalid date format, expected YYYY-MM-DD" },
        { status: 400 }
      );
    }
    path = `analysis/${date}.json`;
  } else {
    path = "latest.json";
  }

  const url = `${GCS_BASE}/${path}`;
  const upstream = await fetch(url, { cache: "no-store" });

  if (!upstream.ok) {
    if (upstream.status === 404) {
      return Response.json(
        { error: `No data for ${date ?? "latest"}` },
        { status: 404 }
      );
    }
    return Response.json(
      { error: `Upstream error ${upstream.status}` },
      { status: 502 }
    );
  }

  return new Response(upstream.body, {
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "public, max-age=60",
    },
  });
}
