// Run with bun. Fail-closed discovery barrier for day-base generation.
//
// A day-base built while realtime discovery is still partial can be internally
// valid but permanently incomplete. Compare the distinct races already present
// in D1 with the authoritative Catalog race keys before any Container starts.

import type { RaceEntry } from "./cron-decision";
import { getRunningStyleRaceReadiness } from "./running-style-readiness";
import type { Env, PredictCategory } from "./types";

interface DayBaseDiscoveryReadinessParams {
  category: PredictCategory;
  env: Env;
  runYmd: string;
}

interface CatalogRaceKeysPayload {
  rows: unknown[];
}

interface DiscoveryCountRow {
  race_count: number;
}

export interface DayBaseDiscoveryReadiness {
  ready: boolean;
  reason: string;
}

const CATALOG_ORIGIN = "https://pc-keiba-r2-catalog.internal";
const BAN_EI_VENUE_CODE = "83";
const DISCOVERY_CATEGORY_PREDICATES: Readonly<Record<PredictCategory, string>> = {
  "ban-ei": "source = 'nar' and printf('%02d', cast(keibajo_code as integer)) = '83'",
  jra: "source = 'jra'",
  nar: "source = 'nar' and printf('%02d', cast(keibajo_code as integer)) <> '83'",
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const parseCatalogRace = (row: unknown): RaceEntry => {
  if (!isRecord(row)) throw new Error("Catalog race-keys returned an invalid row");
  const source = row.source;
  const venueCode = row.keibajo_code;
  const raceNumber = row.race_bango;
  if (
    (source !== "jra" && source !== "nar") ||
    typeof venueCode !== "string" ||
    venueCode.length === 0 ||
    typeof raceNumber !== "string" ||
    raceNumber.length === 0
  ) {
    throw new Error("Catalog race-keys returned an invalid row");
  }
  const keibajoCode = venueCode.padStart(2, "0");
  const category = source === "jra" ? "jra" : keibajoCode === BAN_EI_VENUE_CODE ? "ban-ei" : "nar";
  return { category, keibajoCode, raceBango: raceNumber.padStart(2, "0") };
};

const fetchExpectedRaces = async (
  params: DayBaseDiscoveryReadinessParams,
): Promise<readonly RaceEntry[]> => {
  if (params.env.PC_KEIBA_R2_CATALOG === undefined)
    throw new Error("PC_KEIBA_R2_CATALOG binding is unavailable");
  const url = new URL("/v1/race-keys", CATALOG_ORIGIN);
  url.searchParams.set("date", params.runYmd);
  const response = await params.env.PC_KEIBA_R2_CATALOG.fetch(new Request(url));
  if (!response.ok)
    throw new Error(`Catalog discovery readiness failed with HTTP ${response.status}`);
  const payload: unknown = await response.json();
  if (!isRecord(payload) || !Array.isArray(payload.rows))
    throw new Error("Catalog discovery readiness returned invalid rows");
  const catalogPayload: CatalogRaceKeysPayload = { rows: payload.rows };
  const categoryRaces = catalogPayload.rows
    .map(parseCatalogRace)
    .filter((race) => race.category === params.category);
  return [
    ...new Map(
      categoryRaces.map((race) => [`${race.keibajoCode}:${race.raceBango}`, race] as const),
    ).values(),
  ];
};

const fetchDiscoveredRaceCount = async (
  params: DayBaseDiscoveryReadinessParams,
): Promise<number> => {
  const year = params.runYmd.slice(0, 4);
  const monthDay = params.runYmd.slice(4);
  const predicate = DISCOVERY_CATEGORY_PREDICATES[params.category];
  const row = await params.env.REALTIME_DB.prepare(
    `select count(*) as race_count
       from (
         select distinct source,
                printf('%02d', cast(keibajo_code as integer)) as keibajo_code,
                printf('%02d', cast(race_bango as integer)) as race_bango
           from realtime_race_sources
          where kaisai_nen = ?1 and kaisai_tsukihi = ?2 and ${predicate}
       )`,
  )
    .bind(year, monthDay)
    .first<DiscoveryCountRow>();
  const count = row?.race_count;
  if (!Number.isSafeInteger(count) || count === undefined || count < 0)
    throw new Error("D1 discovery readiness returned an invalid race count");
  return count;
};

export const getDayBaseDiscoveryReadiness = async (
  params: DayBaseDiscoveryReadinessParams,
): Promise<DayBaseDiscoveryReadiness> => {
  const expectedRaces = await fetchExpectedRaces(params);
  const expectedRaceCount = expectedRaces.length;
  if (expectedRaceCount === 0) return { ready: false, reason: "catalog-races-empty" };
  const discoveredRaceCount = await fetchDiscoveredRaceCount(params);
  if (discoveredRaceCount !== expectedRaceCount) {
    return {
      ready: false,
      reason: `discovery-race-count-${String(discoveredRaceCount)}-of-${String(expectedRaceCount)}`,
    };
  }
  const runningStyle = await getRunningStyleRaceReadiness({
    category: params.category,
    db: params.env.REALTIME_DB,
    races: expectedRaces,
    runYmd: params.runYmd,
  });
  const readyRunningStyleRaceCount = runningStyle.filter((race) => race.reason === null).length;
  if (readyRunningStyleRaceCount !== expectedRaceCount) {
    return {
      ready: false,
      reason: `running-style-race-count-${String(readyRunningStyleRaceCount)}-of-${String(expectedRaceCount)}`,
    };
  }
  return { ready: true, reason: "ready" };
};
