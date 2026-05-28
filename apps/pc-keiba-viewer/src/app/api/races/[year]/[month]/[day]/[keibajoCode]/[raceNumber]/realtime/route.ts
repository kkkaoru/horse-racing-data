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

// 15s edge cache lets repeated detail-page polls share a single upstream hit
// without delaying odds updates by more than one polling interval. The browser
// stays "no-store" via Cache-Control on the response so the React poll loop
// drives freshness, but Cloudflare's edge collapses concurrent hits, which
// kept the sync-realtime-data worker from melting under trend + detail load.
const UPSTREAM_EDGE_CACHE_TTL_SECONDS = 15;
// Drop default 3-attempt retry so a slow upstream returns 502 fast instead of
// piling 60s+ of retries on top of an already-saturated sync-realtime-data.
const UPSTREAM_FETCH_ATTEMPTS = 1;

export const dynamic = "force-dynamic";

export async function GET(request: Request, context: RouteContext) {
  const sourceParam = new URL(request.url).searchParams.get("source");
  if (!isRaceSource(sourceParam)) {
    return NextResponse.json({ error: "invalid source" }, { status: 400 });
  }

  const { day, keibajoCode, month, raceNumber, year } = await context.params;
  const realtimeApiBaseUrl =
    process.env.NEXT_PUBLIC_REALTIME_DATA_API_BASE_URL ?? "https://sync-realtime-data.kkk4oru.com";
  const upstreamUrl = `${realtimeApiBaseUrl}/api/${sourceParam}/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/realtime`;

  try {
    const response = await fetchWithRetry(
      upstreamUrl,
      { cf: { cacheTtl: UPSTREAM_EDGE_CACHE_TTL_SECONDS, cacheEverything: true } } as RequestInit,
      { attempts: UPSTREAM_FETCH_ATTEMPTS },
    );
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
    return NextResponse.json({ error: "failed to fetch realtime race payload" }, { status: 502 });
  }
}
