// Run with bun. Viewer-side realtime API route. In production it calls the
// `sync-realtime-data-hot` Worker directly via a service binding so the
// legacy `sync-realtime-data` Worker (and its saturated D1) is no longer in
// the request path. In local dev the keiba.go.jp scrape path is preserved
// behind `PC_KEIBA_DEV_REALTIME_SCRAPER=1`. Horse weights are seeded from the
// new HorseWeightDO inside sync-realtime-data via the `REALTIME_DATA` service
// binding (horse-weights-latest endpoint). When the DO has not received a
// snapshot yet (just after deploy, hibernation, etc.) the route falls back to
// reading the latest snapshot directly from the legacy `REALTIME_DB` D1 so the
// first paint still renders weights.
import { NextResponse } from "next/server";

import { safeGetCloudflareEnv } from "../../../../../../../../../lib/cloudflare-context.server";
import type { RaceSource } from "../../../../../../../../../lib/codes";
import {
  buildDevRealtimePayload,
  isDevScraperEnabled,
} from "../../../../../../../../../lib/dev-realtime-scraper.server";
import {
  buildRealtimePayloadFromHot,
  type HotOddsPayload,
} from "../../../../../../../../../lib/hot-odds-payload.server";
import {
  buildRaceKey,
  buildRealtimePayloadForRequest,
  fetchOddsFromHot,
} from "../../../../../../../../../lib/realtime-payload.server";

interface RouteContext {
  params: Promise<{
    day: string;
    keibajoCode: string;
    month: string;
    raceNumber: string;
    year: string;
  }>;
}

const NO_STORE_HEADERS = { "cache-control": "no-store" };

const isRaceSource = (value: string | null): value is RaceSource =>
  value === "jra" || value === "nar";

export { buildRaceKey, buildRealtimePayloadFromHot, fetchOddsFromHot, type HotOddsPayload };

export const dynamic = "force-dynamic";

export async function GET(request: Request, context: RouteContext) {
  const sourceParam = new URL(request.url).searchParams.get("source");
  if (!isRaceSource(sourceParam)) {
    return NextResponse.json({ error: "invalid source" }, { status: 400 });
  }

  const { day, keibajoCode, month, raceNumber, year } = await context.params;

  if (isDevScraperEnabled()) {
    const payload = await buildDevRealtimePayload({
      day,
      keibajoCode,
      month,
      raceNumber,
      source: sourceParam,
      year,
    });
    return NextResponse.json(payload, { headers: NO_STORE_HEADERS });
  }

  const env = await safeGetCloudflareEnv();
  const merged = await buildRealtimePayloadForRequest({
    env,
    request: { day, keibajoCode, month, raceNumber, source: sourceParam, year },
  });
  return NextResponse.json(merged, { headers: NO_STORE_HEADERS });
}
