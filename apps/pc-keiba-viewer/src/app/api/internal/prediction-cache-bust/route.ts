// Run with bun (Next.js route). Re-score notification endpoint hit by
// finish-position-cron / sync-realtime-data after they rewrite the `pred:fp` /
// `pred:rs` KV entries for a race. Purges:
//   1. colo Cache API copies of pred:fp / pred:rs (KV tier left intact so the
//      just-written score is served on the next request)
//   2. finish-prediction-inputs v4 (KV + Cache API). That layer is read
//      before pred:fp on the finish-prediction section and can otherwise keep
//      serving pre-rescore modelPredictionFeatures for up to 6h after post.
import { NextResponse } from "next/server";

import {
  buildFinishPredictionInputsCacheKeyFromRaceParts,
  deleteFinishPredictionInputsCache,
} from "../../../../lib/finish-prediction-inputs-cache.server";
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
  const inputsCacheKey = buildFinishPredictionInputsCacheKeyFromRaceParts(body);
  const [outcome] = await Promise.all([
    bustPredictionCacheApiForRace(body),
    deleteFinishPredictionInputsCache(inputsCacheKey),
  ]);
  return NextResponse.json({ busted: outcome.busted + 1, ok: true });
}
