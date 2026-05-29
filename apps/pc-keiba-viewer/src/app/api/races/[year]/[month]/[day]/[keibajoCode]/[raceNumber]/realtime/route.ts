// Run with bun. Viewer-side realtime API route. In production it calls the
// `sync-realtime-data-hot` Worker directly via a service binding so the
// legacy `sync-realtime-data` Worker (and its saturated D1) is no longer in
// the request path. In local dev the keiba.go.jp scrape path is preserved
// behind `PC_KEIBA_DEV_REALTIME_SCRAPER=1`.
import { getCloudflareContext } from "@opennextjs/cloudflare";
import { NextResponse } from "next/server";

import type { RaceSource } from "../../../../../../../../../lib/codes";
import {
  buildDevRealtimePayload,
  isDevScraperEnabled,
} from "../../../../../../../../../lib/dev-realtime-scraper.server";
import {
  buildRealtimePayloadFromHot,
  HOT_WORKER_ORIGIN,
  type HotOddsPayload,
  isHotOddsPayload,
} from "../../../../../../../../../lib/hot-odds-payload.server";

interface RouteContext {
  params: Promise<{
    day: string;
    keibajoCode: string;
    month: string;
    raceNumber: string;
    year: string;
  }>;
}

interface BuildRaceKeyParams {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: RaceSource;
  year: string;
}

const NO_STORE_HEADERS = { "cache-control": "no-store" };

const isRaceSource = (value: string | null): value is RaceSource =>
  value === "jra" || value === "nar";

const padRaceNumber = (raceNumber: string): string => raceNumber.padStart(2, "0");

export const buildRaceKey = (params: BuildRaceKeyParams): string =>
  `${params.source}:${params.year}:${params.month}${params.day}:${params.keibajoCode}:${padRaceNumber(params.raceNumber)}`;

export const fetchOddsFromHot = async (
  hot: CloudflareEnv["REALTIME_HOT"],
  raceKey: string,
): Promise<HotOddsPayload | null> => {
  if (!hot) {
    return null;
  }
  try {
    const response = await hot.fetch(`${HOT_WORKER_ORIGIN}/api/odds/${raceKey}`);
    if (!response.ok) {
      return null;
    }
    const json: unknown = await response.json();
    return isHotOddsPayload(json) ? json : null;
  } catch {
    return null;
  }
};

export { buildRealtimePayloadFromHot, type HotOddsPayload };

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

  const raceKey = buildRaceKey({
    day,
    keibajoCode,
    month,
    raceNumber,
    source: sourceParam,
    year,
  });
  const { env } = await getCloudflareContext({ async: true });
  const odds = await fetchOddsFromHot(env.REALTIME_HOT, raceKey);
  const payload = buildRealtimePayloadFromHot(raceKey, odds);
  return NextResponse.json(payload, { headers: NO_STORE_HEADERS });
}
