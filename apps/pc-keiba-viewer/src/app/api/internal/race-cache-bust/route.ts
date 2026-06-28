// Run with bun (Next.js route). Per-race cache-bust endpoint hit by
// sync-realtime-data after `fetchAndStoreResults` lands. Deletes both main
// and stale-tier KV entries for every detail-section variant of the given
// race, and bumps a generation counter so the Cache API tier is defeated.
import { NextResponse } from "next/server";

import {
  parseRaceCacheBustRequest,
  type RaceCacheBustRequest,
} from "../../../../lib/race-cache-bust";
import { bustRaceCachesForRace } from "../../../../lib/race-cache-bust.server";

export const dynamic = "force-dynamic";

const AUTH_HEADER = "x-pc-keiba-internal-token";

const isAuthorized = (request: Request): boolean => {
  const expected = process.env.PC_KEIBA_INTERNAL_TOKEN;
  if (!expected) return false;
  return request.headers.get(AUTH_HEADER) === expected;
};

const parseRequestBody = async (request: Request): Promise<RaceCacheBustRequest | null> => {
  const raw = await request.json().catch(() => null);
  return parseRaceCacheBustRequest(raw);
};

export async function POST(request: Request): Promise<Response> {
  if (!isAuthorized(request)) {
    return NextResponse.json({ error: "forbidden" }, { status: 403 });
  }
  const body = await parseRequestBody(request);
  if (!body) {
    return NextResponse.json({ error: "invalid body" }, { status: 400 });
  }
  const outcome = await bustRaceCachesForRace(body);
  return NextResponse.json({
    busted: outcome.busted,
    generation: outcome.generation,
    ok: true,
  });
}
