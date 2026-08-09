// Run with bun (Next.js route). Re-score notification endpoint hit by
// finish-position-cron / sync-realtime-data after they rewrite the `pred:fp` /
// `pred:rs` KV entries for a race. Purges only the colo Cache API tier so the
// just-written KV value (the fresh, post-weight-rescore score) is served on
// the next request; the KV tier itself is left untouched.
import { NextResponse } from "next/server";

import { bustPredictionCacheApiForRace } from "../../../../lib/prediction-kv-cache.server";

export const dynamic = "force-dynamic";

const AUTH_HEADER = "x-pc-keiba-internal-token";

const isAuthorized = (request: Request): boolean => {
  const expected = process.env.PC_KEIBA_INTERNAL_TOKEN;
  if (!expected) return false;
  return request.headers.get(AUTH_HEADER) === expected;
};

const isSource = (value: unknown): value is "jra" | "nar" => value === "jra" || value === "nar";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isBustRequestBody = (
  value: unknown,
): value is {
  keibajoCode: string;
  mmdd: string;
  raceBango: string;
  source: "jra" | "nar";
  year: string;
} => {
  if (!isRecord(value)) return false;
  return (
    typeof value.keibajoCode === "string" &&
    typeof value.mmdd === "string" &&
    typeof value.raceBango === "string" &&
    isSource(value.source) &&
    typeof value.year === "string"
  );
};

const parseRequestBody = async (
  request: Request,
): Promise<{
  keibajoCode: string;
  mmdd: string;
  raceBango: string;
  source: "jra" | "nar";
  year: string;
} | null> => {
  const raw = await request.json().catch(() => null);
  return isBustRequestBody(raw) ? raw : null;
};

export async function POST(request: Request): Promise<Response> {
  if (!isAuthorized(request)) {
    return NextResponse.json({ error: "forbidden" }, { status: 403 });
  }
  const body = await parseRequestBody(request);
  if (!body) {
    return NextResponse.json({ error: "invalid body" }, { status: 400 });
  }
  const outcome = await bustPredictionCacheApiForRace(body);
  return NextResponse.json({ busted: outcome.busted, ok: true });
}
