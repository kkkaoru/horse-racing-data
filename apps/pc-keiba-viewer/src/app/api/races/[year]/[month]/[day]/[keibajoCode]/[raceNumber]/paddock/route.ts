import { NextResponse } from "next/server";

import {
  getPaddockLiveUrl,
  getPaddockState,
  isPaddockAction,
  isPaddockRaceParams,
  isPaddockRealtimeAvailable,
  updatePaddockState,
} from "../../../../../../../../../lib/paddock-server";

export const dynamic = "force-dynamic";

interface PaddockRouteProps {
  params: Promise<{
    day: string;
    keibajoCode: string;
    month: string;
    raceNumber: string;
    year: string;
  }>;
}

const getCorsHeaders = (request: Request): Record<string, string> => {
  const origin = request.headers.get("origin");
  if (!origin) {
    return {};
  }
  const { hostname } = new URL(origin);
  if (hostname !== "localhost" && hostname !== "127.0.0.1" && hostname !== "0.0.0.0") {
    return {};
  }
  return {
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Origin": origin,
    Vary: "Origin",
  };
};

export async function OPTIONS(request: Request) {
  return new Response(null, { headers: getCorsHeaders(request), status: 204 });
}

export async function GET(request: Request, { params }: PaddockRouteProps) {
  const raceParams = await params;
  if (!isPaddockRaceParams(raceParams)) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }
  const liveUrl = getPaddockLiveUrl(raceParams);
  return NextResponse.json(await getPaddockState(raceParams), {
    headers: {
      ...getCorsHeaders(request),
      "Cache-Control": "private, max-age=0, no-store",
      ...(liveUrl ? { "X-Paddock-Live-Url": liveUrl } : {}),
      "X-Paddock-Realtime": isPaddockRealtimeAvailable() ? "1" : "0",
    },
  });
}

export async function POST(request: Request, { params }: PaddockRouteProps) {
  const raceParams = await params;
  if (!isPaddockRaceParams(raceParams)) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }

  const body: unknown = await request.json().catch(() => null);
  if (!isPaddockAction(body)) {
    return NextResponse.json({ error: "invalid_action" }, { status: 400 });
  }

  return NextResponse.json(await updatePaddockState(raceParams, body), {
    headers: {
      ...getCorsHeaders(request),
      "Cache-Control": "private, max-age=0, no-store",
    },
  });
}
