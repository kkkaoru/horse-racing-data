// Run with bun. `bun run --filter pc-keiba-viewer dev`
import { NextResponse } from "next/server";

import { type PaddockAction } from "../../../../../../../../../lib/paddock";
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

const USER_ID_PATTERN = /^[A-Za-z0-9_-]+$/u;
const USER_ID_MIN_LENGTH = 1;
const USER_ID_MAX_LENGTH = 128;

const isValidUserId = (value: unknown): value is string =>
  typeof value === "string" &&
  value.length >= USER_ID_MIN_LENGTH &&
  value.length <= USER_ID_MAX_LENGTH &&
  USER_ID_PATTERN.test(value);

const hasUserIdField = (value: object): value is { userId: unknown } => "userId" in value;

export const parsePaddockActionBody = (
  body: unknown,
): { action: PaddockAction } | { error: "invalid_user_id" } | null => {
  if (!isPaddockAction(body)) {
    return null;
  }
  if (body.type === "official-rank") {
    return { action: body };
  }
  if (!hasUserIdField(body) || body.userId === undefined) {
    return { action: body };
  }
  if (!isValidUserId(body.userId)) {
    return { error: "invalid_user_id" };
  }
  return { action: { ...body, userId: body.userId } };
};

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
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Expose-Headers": "X-Paddock-Live-Url,X-Paddock-Realtime",
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
  const [liveUrl, paddockState, realtimeAvailable] = await Promise.all([
    getPaddockLiveUrl(raceParams),
    getPaddockState(raceParams),
    isPaddockRealtimeAvailable(),
  ]);
  return NextResponse.json(paddockState, {
    headers: {
      ...getCorsHeaders(request),
      "Cache-Control": "private, max-age=0, no-store",
      ...(liveUrl ? { "X-Paddock-Live-Url": liveUrl } : {}),
      "X-Paddock-Realtime": realtimeAvailable ? "1" : "0",
    },
  });
}

export async function POST(request: Request, { params }: PaddockRouteProps) {
  const raceParams = await params;
  if (!isPaddockRaceParams(raceParams)) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }

  const body: unknown = await request.json().catch(() => null);
  const parsed = parsePaddockActionBody(body);
  if (parsed === null) {
    return NextResponse.json({ error: "invalid_action" }, { status: 400 });
  }
  if ("error" in parsed) {
    return NextResponse.json({ error: parsed.error }, { status: 400 });
  }

  return NextResponse.json(await updatePaddockState(raceParams, parsed.action), {
    headers: {
      ...getCorsHeaders(request),
      "Cache-Control": "private, max-age=0, no-store",
    },
  });
}
