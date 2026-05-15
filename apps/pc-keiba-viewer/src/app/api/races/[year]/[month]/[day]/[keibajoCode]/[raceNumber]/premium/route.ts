import { NextResponse } from "next/server";

import type { RaceSource } from "../../../../../../../../../lib/codes";
import { fetchWithRetry } from "../../../../../../../../../lib/fetch-with-retry";

interface RouteContext {
  params: Promise<{
    day: string;
    keibajoCode: string;
    month: string;
    raceNumber: string;
    year: string;
  }>;
}

const isRaceSource = (value: string | null): value is RaceSource =>
  value === "jra" || value === "nar";

export async function GET(request: Request, context: RouteContext) {
  const sourceParam = new URL(request.url).searchParams.get("source");
  const source = isRaceSource(sourceParam) ? sourceParam : "jra";
  const { day, keibajoCode, month, raceNumber, year } = await context.params;
  const realtimeApiBaseUrl =
    process.env.NEXT_PUBLIC_REALTIME_DATA_API_BASE_URL ?? "https://sync-realtime-data.kkk4oru.com";
  const upstreamUrl = `${realtimeApiBaseUrl}/api/${source}/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/premium`;

  try {
    const response = await fetchWithRetry(upstreamUrl, { cache: "no-store" });
    const body = await response.text();
    return new Response(body, {
      headers: {
        "cache-control": "no-store",
        "content-type": response.headers.get("content-type") ?? "application/json; charset=utf-8",
      },
      status: response.status,
      statusText: response.statusText,
    });
  } catch {
    return NextResponse.json({ paddockBulletins: [] }, { status: 200 });
  }
}
