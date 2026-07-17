// Run with bun. Per-race coordinator for finish-position rescore scheduling.
//
// Mirrors the running-style per-race planner (sync-realtime-data
// running-style-cron.ts planRunningStylePredictionsForDate): a short-interval
// cron inspects realtime_race_sources.race_start_at_jst (JST ISO post-time) and
// enqueues a per-race "rescore" message for each race whose post time falls
// within the [now, now + T-X] window and that has not already been enqueued.
// This is the timing layer that lets each race be re-scored with the freshest
// bataiju/odds just before post (bataiju lands ~T-30..50min, odds drift until
// post). The per-race rescore message is consumed by the existing container
// held /predict mode=rescore path (queue-consumer.ts CONTAINER_PER_RACE_CATEGORIES),
// the same path the container's full pass uses via
// predict_upcoming._score_and_flush_races / score_races — so it inherits the
// current champion model, cell routing, and the production model version
// allowlist automatically. env.RESCORE_CATEGORIES scopes which categories are
// enqueued (see resolveRescoreCategories below); when the coordinator is
// disabled entirely (COORDINATOR_ENABLED !== "1") it is a shadow no-op — see
// worker.ts gating.

import { claimRescoreRace } from "./do-state";
import type { Env, PredictCategory, PredictMode, PredictQueueMessage } from "./types";

// Default lead time: enqueue a race for rescore when its post time is within
// T-X minutes from now. 25 min sits after bataiju is announced (~T-30..50) and
// before odds finalise at post, so the rescore picks up fresh weight + odds.
export const DEFAULT_RESCORE_LEAD_MINUTES = 25;
// Shadow gate: the coordinator only enqueues when COORDINATOR_ENABLED === "1".
// Until task B (rescore consumer) is wired and this flag is flipped, the cron
// fires but enqueues nothing — deploying it does not change production
// predictions.
const COORDINATOR_ENABLED_FLAG = "1";
const RESCORE_MODE: PredictMode = "rescore";
const RESCORE_DAYS_AHEAD = 0;
const WEIGHT_REBUILD_KEIBAJO = "WR";
const MS_PER_MINUTE = 60 * 1000;
const JST_OFFSET_HOURS = 9;
const JST_OFFSET_MS = JST_OFFSET_HOURS * 60 * 60 * 1000;
const ISO_DATE_LENGTH = 10;
const DATE_SEPARATOR = "-";
const KEIBAJO_PAD_WIDTH = 2;
const RACE_BANGO_PAD_WIDTH = 2;
const MINUTES_PER_HALF_HOUR = 30;
const HALF_HOUR_SLOT_PAD_WIDTH = 2;
// realtime_race_sources stores per-source rows; finish-position rescore targets
// the same jra/nar/ban-ei categories the predict pipeline produces. Ban-ei rows
// live under the nar source with keibajo_code 83 (帯広), so the coordinator
// must split the nar source by venue before enqueueing per-race container jobs.
const BAN_EI_KEIBAJO_CODES = ["83"] as const;
const RESCORE_CATEGORIES_SEPARATOR = ",";
// Default (and misconfiguration-fallback) scope for env.RESCORE_CATEGORIES:
// JRA per-race rescore routes to the container's existing mode=rescore path,
// which shares _score_and_flush_races / score_races with the full pass so it
// inherits the current champion, cell routing, and the production model
// version allowlist automatically. NAR/Ban-ei rescore is intentionally held
// back until the day-base full-pass speedup, so it does not contend with the
// morning bulk run for those categories' single per-category container slot.
const DEFAULT_RESCORE_CATEGORIES: ReadonlyArray<PredictCategory> = ["jra"];
// Kochi racecourse: the venue the is_final_race cell-routing dimension
// currently exists for (predict_lib.cell_router.py, no live routing rule
// yet -- see apps/pc-keiba-viewer/tmp/kochi-final/cell_design.md). Gates the
// extra D1 read in fetchCardMaxRaceBangoForKochi below so every other venue's
// dispatch pays zero added cost for a dimension it will never route on.
export const KOCHI_KEIBAJO_CODE = "54";
const KOCHI_SOURCE = "nar";
interface CategoryRaceFilter {
  keibajoCodes: ReadonlyArray<string>;
  keibajoMode: "all" | "exclude" | "include";
  sources: ReadonlyArray<string>;
}

const CATEGORY_RACE_FILTERS: Readonly<Record<PredictCategory, CategoryRaceFilter>> = {
  "ban-ei": {
    keibajoCodes: BAN_EI_KEIBAJO_CODES,
    keibajoMode: "include",
    sources: ["nar"],
  },
  jra: {
    keibajoCodes: [],
    keibajoMode: "all",
    sources: ["jra"],
  },
  nar: {
    keibajoCodes: BAN_EI_KEIBAJO_CODES,
    keibajoMode: "exclude",
    sources: ["nar"],
  },
};
const ALL_CATEGORIES: ReadonlyArray<PredictCategory> = ["jra", "nar", "ban-ei"];
const ALL_CATEGORIES_SET = new Set<string>(ALL_CATEGORIES);

interface RaceSourceRow {
  keibajo_code: string;
  race_bango: string;
  race_start_at_jst: string;
}

interface RunYmdParts {
  nen: string;
  tsukihi: string;
}

export interface CoordinatorRaceTarget {
  keibajoCode: string;
  raceBango: string;
  raceStartAtJst: string;
}

export interface RaceCoordinatorSummary {
  category: PredictCategory;
  date: string;
  enqueued: number;
  scanned: number;
  withinWindow: number;
  alreadyClaimed: number;
}

interface PlanRescoreParams {
  category: PredictCategory;
  // "YYYY-MM-DD" JST calendar date the coordinator is scanning.
  date: string;
  env: Env;
  leadMinutes: number;
  now: Date;
  // "YYYYMMDD" form of date — used for the realtime_race_sources lookup, the DO
  // dedup key, and the container /predict runDate param.
  runYmd: string;
}

interface BuildRescoreMessageParams {
  category: PredictCategory;
  date: string;
  runYmd: string;
  target: CoordinatorRaceTarget;
}

interface ClaimAndEnqueueParams {
  category: PredictCategory;
  date: string;
  env: Env;
  runYmd: string;
  target: CoordinatorRaceTarget;
}

interface RaceCoordinatorTickParams {
  env: Env;
  now: Date;
  leadMinutes: number;
}

interface WeightRebuildParams {
  category: PredictCategory;
  date: string;
  env: Env;
  now: Date;
  runYmd: string;
}

const pad = (value: string, width: number): string => value.padStart(width, "0");

// "YYYYMMDD" (JST) -> the (kaisai_nen, kaisai_tsukihi) pair realtime_race_sources
// stores its rows under. date is already a JST calendar date string.
const splitRunYmd = (runYmd: string): RunYmdParts => ({
  nen: runYmd.slice(0, 4),
  tsukihi: runYmd.slice(4, 8),
});

// "YYYY-MM-DD" (JST) for a UTC instant, used to scope the coordinator to today.
export const formatRunDateJst = (now: Date): string =>
  new Date(now.getTime() + JST_OFFSET_MS).toISOString().slice(0, ISO_DATE_LENGTH);

// "YYYYMMDD" (JST) form used for DO dedup keys and the queue runYmd field.
export const formatRunYmdJst = (now: Date): string =>
  formatRunDateJst(now).split(DATE_SEPARATOR).join("");

// Four-digit JST half-hour slot ("0900", "0930", ..), used as the synthetic
// weight-rebuild raceBango so the DO dedup key changes once per 30 minutes.
// Minutes are floored to the nearest half hour (e.g. 10:17 -> "1000", 10:45 ->
// "1030").
export const getJstHalfHourSlot = (now: Date): string => {
  const jst = new Date(now.getTime() + JST_OFFSET_MS);
  const hours = String(jst.getUTCHours()).padStart(HALF_HOUR_SLOT_PAD_WIDTH, "0");
  const slotMinutes = jst.getUTCMinutes() < MINUTES_PER_HALF_HOUR ? 0 : MINUTES_PER_HALF_HOUR;
  const minutes = String(slotMinutes).padStart(HALF_HOUR_SLOT_PAD_WIDTH, "0");
  return `${hours}${minutes}`;
};

const buildPlaceholders = (count: number): string =>
  Array.from({ length: count }, () => "?").join(", ");

const buildKeibajoFilter = (
  filter: CategoryRaceFilter,
): { binds: ReadonlyArray<string>; sql: string } => {
  if (filter.keibajoMode === "all") {
    return { binds: [], sql: "" };
  }
  const operator = filter.keibajoMode === "include" ? "in" : "not in";
  return {
    binds: filter.keibajoCodes,
    sql: `\n        and keibajo_code ${operator} (${buildPlaceholders(filter.keibajoCodes.length)})`,
  };
};

const listRacesForCategory = async (
  env: Env,
  category: PredictCategory,
  runYmd: string,
): Promise<RaceSourceRow[]> => {
  const { nen, tsukihi } = splitRunYmd(runYmd);
  const filter = CATEGORY_RACE_FILTERS[category];
  const keibajoFilter = buildKeibajoFilter(filter);
  const sql = `select keibajo_code, race_bango, race_start_at_jst
       from realtime_race_sources
      where source in (${buildPlaceholders(filter.sources.length)})
        and kaisai_nen = ?
        and kaisai_tsukihi = ?${keibajoFilter.sql}
      order by race_start_at_jst, keibajo_code, race_bango`;
  const result = await env.REALTIME_DB.prepare(sql)
    .bind(...filter.sources, nen, tsukihi, ...keibajoFilter.binds)
    .all<RaceSourceRow>();
  return result.results;
};

interface CardMaxRaceBangoRow {
  max_race_bango: number | null;
}

interface FetchCardMaxRaceBangoParams {
  env: Env;
  source: string;
  keibajoCode: string;
  runYmd: string;
}

// Registered-card-max derivation for the is_final_race cell-routing dimension
// (predict_lib.cell_router.resolve_dimension, no live routing rule yet -- see
// apps/pc-keiba-viewer/tmp/kochi-final/cell_design.md): the highest race_bango
// discovery has ever recorded for this (source, keibajoCode, day) card, over
// ALL realtime_race_sources rows for that day -- not just races that have
// already posted or settled, and a cancelled race still occupies its
// registered race_bango slot. This mirrors nvd_ra's registered-card-max
// semantics but reads from discovery instead: realtime_race_sources completes
// for the day well before any race's post time (the 20:05/21:00 crons land
// the schedule T-1), whereas a settlement-column read only lands after each
// race actually runs. Returns null when discovery has no rows yet for this
// card -- callers must treat null as "not yet known" (never a 0-race card)
// and omit the value entirely so the container's is_final_race dimension
// fails closed rather than guessing (predict_lib.cell_router.py's
// resolve_dimension does the same on a missing value).
export const fetchCardMaxRaceBango = async (
  params: FetchCardMaxRaceBangoParams,
): Promise<number | null> => {
  const { nen, tsukihi } = splitRunYmd(params.runYmd);
  const row = await params.env.REALTIME_DB.prepare(
    `select max(cast(race_bango as integer)) as max_race_bango
       from realtime_race_sources
      where source = ? and keibajo_code = ? and kaisai_nen = ? and kaisai_tsukihi = ?`,
  )
    .bind(params.source, params.keibajoCode, nen, tsukihi)
    .first<CardMaxRaceBangoRow>();
  return row?.max_race_bango ?? null;
};

interface ResolveCardMaxRaceBangoForKochiParams {
  env: Env;
  keibajoCode: string | undefined;
  runYmd: string;
}

// Gated, fail-closed convenience wrapper around fetchCardMaxRaceBango for the
// only venue any live cell-routing rule references today: every dispatch
// path (queue-consumer.ts, worker.ts's admin focused-full-race trigger) calls
// this rather than fetchCardMaxRaceBango directly, so a non-Kochi race (the
// overwhelming majority of NAR/JRA/Ban-ei dispatch) never pays the extra D1
// read, and a D1 failure never blocks a genuine prediction dispatch -- it
// just omits the value, which is indistinguishable downstream from "not
// computed" (both fail is_final_race closed to the category default).
export const resolveCardMaxRaceBangoForKochi = async (
  params: ResolveCardMaxRaceBangoForKochiParams,
): Promise<number | undefined> => {
  if (params.keibajoCode !== KOCHI_KEIBAJO_CODE) return undefined;
  try {
    const value = await fetchCardMaxRaceBango({
      env: params.env,
      keibajoCode: params.keibajoCode,
      runYmd: params.runYmd,
      source: KOCHI_SOURCE,
    });
    return value ?? undefined;
  } catch (err) {
    console.warn(
      `[race-coordinator] cardMaxRaceBango lookup failed keibajo=${params.keibajoCode} runYmd=${params.runYmd}:`,
      String(err),
    );
    return undefined;
  }
};

// A race is within window when its post time is in [now, now + leadMinutes].
// Past-post races are excluded (already gone), as are races further out than
// the lead window (they get picked up by a later cron tick closer to post).
export const isWithinRescoreWindow = (
  raceStartAtJst: string,
  now: Date,
  leadMinutes: number,
): boolean => {
  const postMs = Date.parse(raceStartAtJst);
  if (Number.isNaN(postMs)) {
    return false;
  }
  const nowMs = now.getTime();
  const windowEndMs = nowMs + leadMinutes * MS_PER_MINUTE;
  return postMs >= nowMs && postMs <= windowEndMs;
};

export const selectRacesWithinWindow = (
  rows: ReadonlyArray<RaceSourceRow>,
  now: Date,
  leadMinutes: number,
): CoordinatorRaceTarget[] =>
  rows
    .filter((row) => isWithinRescoreWindow(row.race_start_at_jst, now, leadMinutes))
    .map((row) => ({
      keibajoCode: pad(row.keibajo_code, KEIBAJO_PAD_WIDTH),
      raceBango: pad(row.race_bango, RACE_BANGO_PAD_WIDTH),
      raceStartAtJst: row.race_start_at_jst,
    }));

const buildRescoreMessage = (params: BuildRescoreMessageParams): PredictQueueMessage => ({
  category: params.category,
  daysAhead: RESCORE_DAYS_AHEAD,
  keibajoCode: params.target.keibajoCode,
  mode: RESCORE_MODE,
  raceBango: params.target.raceBango,
  runDate: params.date,
  runDateIso: params.date,
  runYmd: params.runYmd,
});

// Claim a single race in the DO (strong consistency) and, when this is the first
// claim, enqueue its per-race rescore message. Returns true when enqueued.
const claimAndEnqueueRace = async (params: ClaimAndEnqueueParams): Promise<boolean> => {
  const claim = await claimRescoreRace({
    category: params.category,
    env: params.env,
    keibajoCode: params.target.keibajoCode,
    raceBango: params.target.raceBango,
    runYmd: params.runYmd,
  });
  if (!claim.proceed) {
    return false;
  }
  await params.env.PREDICT_QUEUE.send(
    buildRescoreMessage({
      category: params.category,
      date: params.date,
      runYmd: params.runYmd,
      target: params.target,
    }),
  );
  return true;
};

export const planRescoreForCategory = async (
  params: PlanRescoreParams,
): Promise<RaceCoordinatorSummary> => {
  const rows = await listRacesForCategory(params.env, params.category, params.runYmd);
  const targets = selectRacesWithinWindow(rows, params.now, params.leadMinutes);
  const claimResults = await Promise.all(
    targets.map((target) =>
      claimAndEnqueueRace({
        category: params.category,
        date: params.date,
        env: params.env,
        runYmd: params.runYmd,
        target,
      }),
    ),
  );
  const enqueued = claimResults.filter((proceeded) => proceeded).length;
  return {
    alreadyClaimed: targets.length - enqueued,
    category: params.category,
    date: params.date,
    enqueued,
    scanned: rows.length,
    withinWindow: targets.length,
  };
};

const buildShadowSummary = (category: PredictCategory, date: string): RaceCoordinatorSummary => ({
  alreadyClaimed: 0,
  category,
  date,
  enqueued: 0,
  scanned: 0,
  withinWindow: 0,
});

export const isCoordinatorEnabled = (env: Env): boolean =>
  env.COORDINATOR_ENABLED === COORDINATOR_ENABLED_FLAG;

const isPredictCategory = (value: string): value is PredictCategory =>
  ALL_CATEGORIES_SET.has(value);

// Resolve the comma-separated env.RESCORE_CATEGORIES list into the
// PredictCategory values the coordinator should enqueue rescore for. Unknown
// tokens are dropped; an unset var, an empty string, or a list with no
// recognized category all fall back to DEFAULT_RESCORE_CATEGORIES so a
// misconfigured var fails toward the verified JRA-only scope rather than
// silently enqueueing nothing or reactivating NAR/Ban-ei contention.
export const resolveRescoreCategories = (env: Env): PredictCategory[] => {
  if (env.RESCORE_CATEGORIES === undefined) return [...DEFAULT_RESCORE_CATEGORIES];
  const resolved = env.RESCORE_CATEGORIES.split(RESCORE_CATEGORIES_SEPARATOR)
    .map((token) => token.trim())
    .filter(isPredictCategory);
  return resolved.length > 0 ? resolved : [...DEFAULT_RESCORE_CATEGORIES];
};

// Claim a synthetic WR race in the DO (strong consistency) and, when this is
// the first claim of the half-hour slot for the category, enumerate every
// race registered for the category+day (the same realtime_race_sources scan
// planRescoreForCategory uses) and enqueue one per-race rescore message per
// race -- so each lands on isPerRaceRescore's dispatch path in
// queue-consumer.ts exactly like the near-post per-race rescore does,
// letting each individually hit the per-race watermarked R2 cache
// (predict_upcoming._fetch_watermarked_per_race_cache) instead of falling
// back to a category-wide rebuild. Before this, a single scopeless message
// was sent instead; for a catalog source that always CacheMissErrors (no
// per-race object to watermark-check against a whole-category scope) and
// its fallback rebuilds EVERY race for the day in one call -- the largest
// remaining cost this coordinator could trigger. Deduped per-category
// per-half-hour (the JST half-hour slot is the synthetic raceBango) exactly
// as before; that one claim gates whether the whole fan-out fires this
// tick, not each individual race. Returns the number of per-race messages
// enqueued (0 when the claim is rejected or the day has no races for this
// category -- both are legitimate no-op outcomes, not errors).
export const triggerWeightRebuildIfNeeded = async (
  params: WeightRebuildParams,
): Promise<number> => {
  const claim = await claimRescoreRace({
    category: params.category,
    env: params.env,
    keibajoCode: WEIGHT_REBUILD_KEIBAJO,
    raceBango: getJstHalfHourSlot(params.now),
    runYmd: params.runYmd,
  });
  if (!claim.proceed) {
    return 0;
  }
  const rows = await listRacesForCategory(params.env, params.category, params.runYmd);
  await Promise.all(
    rows.map((row) =>
      params.env.PREDICT_QUEUE.send(
        buildRescoreMessage({
          category: params.category,
          date: params.date,
          runYmd: params.runYmd,
          target: {
            keibajoCode: pad(row.keibajo_code, KEIBAJO_PAD_WIDTH),
            raceBango: pad(row.race_bango, RACE_BANGO_PAD_WIDTH),
            raceStartAtJst: row.race_start_at_jst,
          },
        }),
      ),
    ),
  );
  return rows.length;
};

export const runRaceCoordinatorTick = async (
  params: RaceCoordinatorTickParams,
): Promise<RaceCoordinatorSummary[]> => {
  const date = formatRunDateJst(params.now);
  const categories = resolveRescoreCategories(params.env);
  // Shadow gate: when disabled, skip the D1 read + enqueue entirely so the cron
  // is a no-op for production. Reports empty per-category summaries.
  if (!isCoordinatorEnabled(params.env)) {
    return categories.map((category) => buildShadowSummary(category, date));
  }
  const runYmd = formatRunYmdJst(params.now);
  const summaries = await Promise.all(
    categories.map((category) =>
      planRescoreForCategory({
        category,
        date,
        env: params.env,
        leadMinutes: params.leadMinutes,
        now: params.now,
        runYmd,
      }),
    ),
  );
  // Trigger a per-race rescore fan-out (fresh weight data) for categories
  // that had at least one race newly enqueued. Deduped per-category
  // per-half-hour via claimRescoreRace with synthetic key WR:<JST half-hour
  // slot> — one fan-out per category per half-hour (see
  // triggerWeightRebuildIfNeeded for what "fan-out" means).
  await Promise.all(
    summaries
      .filter((summary) => summary.enqueued > 0)
      .map((summary) =>
        triggerWeightRebuildIfNeeded({
          category: summary.category,
          date: summary.date,
          env: params.env,
          now: params.now,
          runYmd,
        }),
      ),
  );
  return summaries;
};
