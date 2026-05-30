// Run with bun. Server-Sent Events proxy that streams 馬体重 updates from the
// sync-realtime-data Worker (specifically its HorseWeightDO) back to the
// browser. The upstream ReadableStream is forwarded as-is so Cloudflare keeps
// the connection open.

import { NextResponse } from "next/server";

import { safeGetCloudflareEnv } from "../../../../../../../../../lib/cloudflare-context.server";

interface RouteContext {
  params: Promise<{
    day: string;
    keibajoCode: string;
    month: string;
    raceNumber: string;
    year: string;
  }>;
}

const REALTIME_DATA_ORIGIN = "https://realtime";
const DEFAULT_SSE_CONTENT_TYPE = "text/event-stream";
const SSE_CACHE_CONTROL = "no-cache, no-transform";
const HTTP_BAD_REQUEST = 400;
const HTTP_SERVICE_UNAVAILABLE = 503;

const isJraOrNar = (value: string | null): value is "jra" | "nar" =>
  value === "jra" || value === "nar";

const padRaceNumber = (raceNumber: string): string => raceNumber.padStart(2, "0");

const buildUpstreamUrl = (
  source: "jra" | "nar",
  params: { day: string; keibajoCode: string; month: string; raceNumber: string; year: string },
): string =>
  `${REALTIME_DATA_ORIGIN}/api/${source}/races/${params.year}/${params.month}/${params.day}/${params.keibajoCode}/${padRaceNumber(params.raceNumber)}/horse-weights-stream`;

export const dynamic = "force-dynamic";

export const GET = async (request: Request, context: RouteContext): Promise<Response> => {
  const source = new URL(request.url).searchParams.get("source");
  if (!isJraOrNar(source)) {
    return NextResponse.json({ error: "invalid source" }, { status: HTTP_BAD_REQUEST });
  }
  const params = await context.params;
  const env = await safeGetCloudflareEnv();
  const realtimeData = env?.REALTIME_DATA;
  if (!realtimeData) {
    return NextResponse.json(
      { error: "binding unavailable" },
      { status: HTTP_SERVICE_UNAVAILABLE },
    );
  }
  const upstream = await realtimeData.fetch(buildUpstreamUrl(source, params));
  return new Response(upstream.body, {
    headers: {
      "Cache-Control": SSE_CACHE_CONTROL,
      "Content-Type": upstream.headers.get("Content-Type") ?? DEFAULT_SSE_CONTENT_TYPE,
    },
    status: upstream.status,
  });
};
