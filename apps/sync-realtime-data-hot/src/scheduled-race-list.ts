// Run with bun. Hyperdrive-direct read of today's races for the hot worker's
// self-discovery path. Lists `jvd_ra` (JRA) and `nvd_ra` (NAR) for the JST
// today date and upserts each row into `odds_fetch_state` so the per-minute
// polling cron has something to plan against, even when the legacy worker is
// down and `forwardRaceSourceToHot` never fires.
//
// This is purely SELECT against Hyperdrive; no INSERT/UPDATE/DELETE flows
// into Postgres. The hot D1 upsert uses `on conflict(race_key) do update`
// so the legacy worker's later forwarded payload still wins when it lands.
//
// NAR per-race deba URLs are resolved by fetching each venue's RaceList HTML
// once (one request per venue) and joining the per-race DebaTable links onto
// the Hyperdrive rows. Without this, the scraper would fall back to the venue
// race-list page, which exposes no odds links, and every fetch-odds job would
// throw "odds rows are empty". JRA URLs require a netkeiba-style checksum that
// is not available inside this worker yet — keep the placeholder so the legacy
// `forwardRaceSourceToHot` payload still wins via `on conflict do update`.

import type { Pool } from "pg";

import { invalidateRaceListInKv } from "./gates/race-list-kv-cache";
import { buildRaceListUrl, fetchRaceLinksFromRaceList } from "./keiba-go";
import { getHotPool } from "./postgres-pool";
import { upsertOddsFetchState } from "./storage";
import { formatRaceStartJst, getTodayJst } from "./time";
import type { Env, OddsSource } from "./types";

export interface TodayRaceRow {
  source: OddsSource;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  raceKey: string;
  raceStartAtJst: string;
  debaUrl: string;
  oddsLinksJson: string;
}

export interface ListTodayRacesContext {
  pool?: Pool;
  resolveNarDebaUrl?: NarDebaUrlResolver;
}

export interface PopulateTodayContext {
  pool?: Pool;
  resolveNarDebaUrl?: NarDebaUrlResolver;
}

export type NarDebaUrlResolver = (input: NarDebaUrlResolverInput) => Promise<string | null>;

export interface NarDebaUrlResolverInput {
  yyyymmdd: string;
  keibajoCode: string;
  raceBango: string;
}

interface SourcedRaceRow {
  [key: string]: unknown;
  source: OddsSource;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  hasso_jikoku: string | null;
  kaisai_kai: string | null;
  kaisai_nichime: string | null;
}

interface IntermediateRow {
  source: OddsSource;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  raceKey: string;
  raceStartAtJst: string;
}

const KEIBAJO_CODE_PAD_WIDTH = 2;
const RACE_BANGO_PAD_WIDTH = 2;
const HHMM_PATTERN = /^\d{4}$/u;
const JRA_PLACEHOLDER_URL = "https://www.jra.go.jp/";
const EMPTY_ODDS_LINKS_JSON = "{}";

const SELECT_TODAY_RACES_SQL = `
  select 'jra' as source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, hasso_jikoku, kaisai_kai, kaisai_nichime
  from jvd_ra
  where kaisai_nen = $1 and kaisai_tsukihi = $2
  union all
  select 'nar' as source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, hasso_jikoku, null as kaisai_kai, null as kaisai_nichime
  from nvd_ra
  where kaisai_nen = $1 and kaisai_tsukihi = $2
  order by source, keibajo_code, race_bango
`;

const isOddsSource = (value: unknown): value is OddsSource => value === "jra" || value === "nar";

const isNonEmptyString = (value: unknown): value is string =>
  typeof value === "string" && value.length > 0;

const isNullableString = (value: unknown): value is string | null =>
  value === null || typeof value === "string";

const isCompleteRow = (row: Record<string, unknown>): row is SourcedRaceRow =>
  isOddsSource(row.source) &&
  isNonEmptyString(row.kaisai_nen) &&
  isNonEmptyString(row.kaisai_tsukihi) &&
  isNonEmptyString(row.keibajo_code) &&
  isNonEmptyString(row.race_bango) &&
  isNullableString(row.hasso_jikoku) &&
  isNullableString(row.kaisai_kai) &&
  isNullableString(row.kaisai_nichime);

const normaliseCode = (value: string, width: number): string => value.padStart(width, "0");

const buildRaceKey = (row: SourcedRaceRow): string => {
  const keibajoCode = normaliseCode(row.keibajo_code, KEIBAJO_CODE_PAD_WIDTH);
  const raceBango = normaliseCode(row.race_bango, RACE_BANGO_PAD_WIDTH);
  return `${row.source}:${row.kaisai_nen}:${row.kaisai_tsukihi}:${keibajoCode}:${raceBango}`;
};

const buildRaceStartAtJst = (row: SourcedRaceRow): string | null => {
  const hhmm = row.hasso_jikoku;
  if (!hhmm || !HHMM_PATTERN.test(hhmm)) {
    return null;
  }
  return formatRaceStartJst(row.kaisai_nen, row.kaisai_tsukihi, hhmm);
};

const toIntermediateRow = (row: SourcedRaceRow): IntermediateRow | null => {
  const raceStartAtJst = buildRaceStartAtJst(row);
  if (!raceStartAtJst) {
    return null;
  }
  return {
    kaisaiNen: row.kaisai_nen,
    kaisaiTsukihi: row.kaisai_tsukihi,
    keibajoCode: normaliseCode(row.keibajo_code, KEIBAJO_CODE_PAD_WIDTH),
    raceBango: normaliseCode(row.race_bango, RACE_BANGO_PAD_WIDTH),
    raceKey: buildRaceKey(row),
    raceStartAtJst,
    source: row.source,
  };
};

const splitYyyymmdd = (yyyymmdd: string): { kaisaiNen: string; kaisaiTsukihi: string } => ({
  kaisaiNen: yyyymmdd.slice(0, 4),
  kaisaiTsukihi: yyyymmdd.slice(4, 8),
});

interface NarVenue {
  yyyymmdd: string;
  keibajoCode: string;
}

const buildVenueKey = (yyyymmdd: string, keibajoCode: string): string =>
  `${yyyymmdd}:${keibajoCode}`;

const collectNarVenues = (rows: IntermediateRow[]): NarVenue[] => {
  const seen = new Map<string, NarVenue>();
  rows.forEach((row) => {
    if (row.source !== "nar") {
      return;
    }
    const yyyymmdd = `${row.kaisaiNen}${row.kaisaiTsukihi}`;
    seen.set(buildVenueKey(yyyymmdd, row.keibajoCode), {
      keibajoCode: row.keibajoCode,
      yyyymmdd,
    });
  });
  return Array.from(seen.values());
};

const fetchNarVenueLinks = async (venue: NarVenue): Promise<Map<string, string>> => {
  const venueUrl = buildRaceListUrl(venue.yyyymmdd, venue.keibajoCode).url;
  try {
    const links = await fetchRaceLinksFromRaceList(venueUrl);
    return new Map(
      links.map((link) => [link.raceNumber.padStart(RACE_BANGO_PAD_WIDTH, "0"), link.url]),
    );
  } catch (error) {
    console.warn(
      `[scheduled-race-list] failed to fetch NAR venue race list: yyyymmdd=${venue.yyyymmdd} keibajo=${venue.keibajoCode}`,
      error,
    );
    return new Map();
  }
};

const buildDefaultNarResolver =
  (venueLinkMap: Map<string, Map<string, string>>): NarDebaUrlResolver =>
  async ({ yyyymmdd, keibajoCode, raceBango }) =>
    venueLinkMap.get(buildVenueKey(yyyymmdd, keibajoCode))?.get(raceBango) ?? null;

const prepareNarResolver = async (
  rows: IntermediateRow[],
  override: NarDebaUrlResolver | undefined,
): Promise<NarDebaUrlResolver> => {
  if (override) {
    return override;
  }
  const venues = collectNarVenues(rows);
  const entries = await Promise.all(
    venues.map(async (venue): Promise<[string, Map<string, string>]> => {
      const links = await fetchNarVenueLinks(venue);
      return [buildVenueKey(venue.yyyymmdd, venue.keibajoCode), links];
    }),
  );
  return buildDefaultNarResolver(new Map(entries));
};

const resolveDebaUrl = async (
  row: IntermediateRow,
  resolveNarDebaUrl: NarDebaUrlResolver,
): Promise<string | null> => {
  if (row.source === "jra") {
    return JRA_PLACEHOLDER_URL;
  }
  const yyyymmdd = `${row.kaisaiNen}${row.kaisaiTsukihi}`;
  const debaUrl = await resolveNarDebaUrl({
    keibajoCode: row.keibajoCode,
    raceBango: row.raceBango,
    yyyymmdd,
  });
  if (!debaUrl) {
    console.warn(
      `[scheduled-race-list] NAR per-race deba URL not found, skipping raceKey=${row.raceKey}`,
    );
    return null;
  }
  return debaUrl;
};

const attachDebaUrl = async (
  row: IntermediateRow,
  resolveNarDebaUrl: NarDebaUrlResolver,
): Promise<TodayRaceRow | null> => {
  const debaUrl = await resolveDebaUrl(row, resolveNarDebaUrl);
  if (!debaUrl) {
    return null;
  }
  return {
    debaUrl,
    kaisaiNen: row.kaisaiNen,
    kaisaiTsukihi: row.kaisaiTsukihi,
    keibajoCode: row.keibajoCode,
    oddsLinksJson: EMPTY_ODDS_LINKS_JSON,
    raceBango: row.raceBango,
    raceKey: row.raceKey,
    raceStartAtJst: row.raceStartAtJst,
    source: row.source,
  };
};

export const listTodayRacesFromHyperdrive = async (
  env: Env,
  yyyymmdd: string,
  context: ListTodayRacesContext = {},
): Promise<TodayRaceRow[]> => {
  const pool = context.pool ?? getHotPool(env);
  const { kaisaiNen, kaisaiTsukihi } = splitYyyymmdd(yyyymmdd);
  const result = await pool.query<Record<string, unknown>>(SELECT_TODAY_RACES_SQL, [
    kaisaiNen,
    kaisaiTsukihi,
  ]);
  const intermediates = result.rows
    .filter(isCompleteRow)
    .map(toIntermediateRow)
    .filter((entry): entry is IntermediateRow => entry !== null);
  const resolveNarDebaUrl = await prepareNarResolver(intermediates, context.resolveNarDebaUrl);
  const resolved = await Promise.all(
    intermediates.map((row) => attachDebaUrl(row, resolveNarDebaUrl)),
  );
  return resolved.filter((entry): entry is TodayRaceRow => entry !== null);
};

export interface PopulateTodayOddsFetchStateResult {
  inserted: number;
  total: number;
}

interface InvalidationTarget {
  source: OddsSource;
  yyyymmdd: string;
}

const collectInvalidationTargets = (rows: TodayRaceRow[]): InvalidationTarget[] => {
  const seen = new Map<string, InvalidationTarget>();
  rows.forEach((row) => {
    const yyyymmdd = `${row.kaisaiNen}${row.kaisaiTsukihi}`;
    seen.set(`${row.source}:${yyyymmdd}`, { source: row.source, yyyymmdd });
  });
  return Array.from(seen.values());
};

export const populateTodayOddsFetchState = async (
  env: Env,
  now: Date,
  context: PopulateTodayContext = {},
): Promise<PopulateTodayOddsFetchStateResult> => {
  const yyyymmdd = getTodayJst(now);
  const rows = await listTodayRacesFromHyperdrive(env, yyyymmdd, {
    pool: context.pool,
    resolveNarDebaUrl: context.resolveNarDebaUrl,
  });
  await Promise.all(
    rows.map((row) =>
      upsertOddsFetchState(env.REALTIME_HOT_DB, {
        debaUrl: row.debaUrl,
        kaisaiNen: row.kaisaiNen,
        kaisaiTsukihi: row.kaisaiTsukihi,
        keibajoCode: row.keibajoCode,
        oddsLinksJson: row.oddsLinksJson,
        raceBango: row.raceBango,
        raceKey: row.raceKey,
        raceStartAtJst: row.raceStartAtJst,
        source: row.source,
      }),
    ),
  );
  await Promise.all(
    collectInvalidationTargets(rows).map((target) =>
      invalidateRaceListInKv(env, target.source, target.yyyymmdd),
    ),
  );
  return { inserted: rows.length, total: rows.length };
};
