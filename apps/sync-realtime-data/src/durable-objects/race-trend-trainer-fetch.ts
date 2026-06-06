// run with: bun run test
// Hyperdrive-backed trainer (chokyoshimei_ryakusho) lookup for the DO's
// today-sibling rows. The D1 base table (race_entry_snapshots) does not
// carry trainer, so today's same-venue sibling races need a parallel
// Postgres query against jvd_se / nvd_se to surface the trainer cell in
// the viewer's race-trend section. Past-day rows (handled by
// daily-feature-build) already JOIN jvd_se / nvd_se directly so this path
// is a today-only complement.
//
// Failure mode is intentionally permissive: any error (Hyperdrive timeout,
// pool exhaustion, malformed row) collapses to an empty Map and the caller
// falls through to `chokyoshiName: null` (= viewer renders "-"). The base
// D1 starter rows must always reach the viewer even when trainer enrichment
// fails — never throw out of the fetch path.

import { getFinishPositionPool } from "../finish-position-lite-pool";
import type { Env } from "../types";
import type { RaceTrendDailyTrackSource } from "horse-racing-realtime/race-trend-daily-track-types";

// 5s timeout matches the existing Hyperdrive 5s budget used by the realtime
// hot worker. Longer would block the viewer's first /races render; shorter
// risks dropping trainer data for healthy queries during steady-state.
const HYPERDRIVE_TIMEOUT_MS = 5_000;
const JRA_SOURCE = "jra";
const JRA_TABLE = "jvd_se";
const NAR_TABLE = "nvd_se";

export interface TrainerFetchContext {
  source: RaceTrendDailyTrackSource;
  targetYmd: string;
  keibajoCode: string;
}

export interface TrainerPoolQueryResult<Row> {
  rows: ReadonlyArray<Row>;
}

export interface TrainerPoolLike {
  query: <Row>(sql: string, params: ReadonlyArray<unknown>) => Promise<TrainerPoolQueryResult<Row>>;
}

export interface TrainerFetchArgs {
  env: Env;
  parsed: TrainerFetchContext;
  raceBangoList: ReadonlyArray<string>;
  poolFactory?: (env: Env) => TrainerPoolLike;
}

export interface TrainerLookupEntry {
  raceBango: string;
  umaban: string;
  chokyoshimeiRyakusho: string | null;
}

interface TrainerLookupRawRow {
  race_bango: string;
  umaban: string;
  chokyoshimei_ryakusho: string | null;
}

interface QueryArgs {
  pool: TrainerPoolLike;
  parsed: TrainerFetchContext;
  raceBangoList: ReadonlyArray<string>;
}

const buildTrainerKey = (raceBango: string, umaban: string): string => `${raceBango}:${umaban}`;

const tableForSource = (source: RaceTrendDailyTrackSource): string =>
  source === JRA_SOURCE ? JRA_TABLE : NAR_TABLE;

// Placeholders are positional ($1..) on the pg side. The race_bango filter
// is expanded to an IN list with bind params (sql injection safe + cached
// plan friendly). race_bango is stored as a zero-padded 2-digit string on
// our DO side but the upstream jvd_se / nvd_se table also uses padded
// 2-digit strings, so no normalisation is needed.
const buildTrainerSelectSql = (source: RaceTrendDailyTrackSource, count: number): string => {
  const placeholders = Array.from({ length: count }, (_, index) => `$${index + 4}`).join(", ");
  return `
    select race_bango, umaban, chokyoshimei_ryakusho
    from ${tableForSource(source)}
    where kaisai_nen = $1
      and kaisai_tsukihi = $2
      and keibajo_code = $3
      and race_bango in (${placeholders})
      and chokyoshimei_ryakusho is not null
      and btrim(chokyoshimei_ryakusho) <> ''
  `;
};

const isStringOrNull = (value: unknown): value is string | null =>
  value === null || typeof value === "string";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isTrainerRawRow = (value: unknown): value is TrainerLookupRawRow => {
  if (!isRecord(value)) return false;
  return (
    typeof value.race_bango === "string" &&
    typeof value.umaban === "string" &&
    isStringOrNull(value.chokyoshimei_ryakusho)
  );
};

const normaliseRow = (row: TrainerLookupRawRow): TrainerLookupEntry => ({
  chokyoshimeiRyakusho: row.chokyoshimei_ryakusho,
  raceBango: row.race_bango,
  umaban: row.umaban,
});

const queryTrainerRows = async (args: QueryArgs): Promise<ReadonlyArray<TrainerLookupRawRow>> => {
  const kaisaiNen = args.parsed.targetYmd.slice(0, 4);
  const kaisaiTsukihi = args.parsed.targetYmd.slice(4, 8);
  const sql = buildTrainerSelectSql(args.parsed.source, args.raceBangoList.length);
  const result = await args.pool.query<TrainerLookupRawRow>(sql, [
    kaisaiNen,
    kaisaiTsukihi,
    args.parsed.keibajoCode,
    ...args.raceBangoList,
  ]);
  return result.rows.filter(isTrainerRawRow);
};

// Clear the timeout when either rail of Promise.race settles so the test
// runner does not see a dangling timer (the rejection branch is caught
// inline with .then(noop, noop) to suppress a second "Unhandled Error"
// because the work promise is observed twice: once by Promise.race and
// once by this cleanup hook).
const withTimeout = <T>(work: Promise<T>, timeoutMs: number): Promise<T> => {
  const timeoutPromise = new Promise<T>((_resolve, reject) => {
    const handle = setTimeout(() => {
      reject(new Error("trainer fetch hyperdrive timeout"));
    }, timeoutMs);
    work.then(
      () => clearTimeout(handle),
      () => clearTimeout(handle),
    );
  });
  return Promise.race([work, timeoutPromise]);
};

// Skip when no Hyperdrive binding is configured (e.g. local dev without
// pg) or when the caller has no sibling raceBango list (cold-start before
// any race_entry_snapshots rows). Returning an empty Map collapses every
// trainer lookup to null on the caller side.
const shouldSkipTrainerFetch = (args: TrainerFetchArgs): boolean => {
  if (args.raceBangoList.length === 0) return true;
  return !args.env.HYPERDRIVE?.connectionString;
};

const buildTrainerMap = (entries: ReadonlyArray<TrainerLookupEntry>): Map<string, string> => {
  const map = new Map<string, string>();
  entries.forEach((entry) => {
    if (entry.chokyoshimeiRyakusho === null) return;
    map.set(buildTrainerKey(entry.raceBango, entry.umaban), entry.chokyoshimeiRyakusho);
  });
  return map;
};

export const fetchTrainerMapForVenueDay = async (
  args: TrainerFetchArgs,
): Promise<Map<string, string>> => {
  if (shouldSkipTrainerFetch(args)) return new Map();
  const factory = args.poolFactory ?? getFinishPositionPool;
  try {
    const pool = factory(args.env);
    const rows = await withTimeout(
      queryTrainerRows({ parsed: args.parsed, pool, raceBangoList: args.raceBangoList }),
      HYPERDRIVE_TIMEOUT_MS,
    );
    return buildTrainerMap(rows.map(normaliseRow));
  } catch (error) {
    console.error("trainer fetch failed", error);
    return new Map();
  }
};

// Exported indirection so the DO and tests can share the key shape.
export const buildTrainerKeyForLookup = buildTrainerKey;

// Testable surface so the test file can exercise the SQL shape, the type
// guard, and the map builder without touching pg / Hyperdrive at all.
export const __testables = {
  HYPERDRIVE_TIMEOUT_MS,
  buildTrainerKey,
  buildTrainerMap,
  buildTrainerSelectSql,
  isTrainerRawRow,
  shouldSkipTrainerFetch,
  withTimeout,
};
