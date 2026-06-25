import { releaseEnqueueLock } from "./gates/enqueue-lock-kv";
import { extractOddsLinks, fetchOdds, fetchRacePage } from "./keiba-go";
import { fetchJraOddsWithPlaywright } from "./jra";
import { getCachedNarVenueLastRaceStartAtJst } from "./nar-venue-cache";
import {
  claimOddsFetch,
  completeOddsFetch,
  countOddsRows,
  failOddsFetch,
  filterChangedOdds,
  getLatestOddsFromD1,
  getOddsFetchState,
  insertOddsSnapshot,
  logFetch,
  updateOddsLinks,
} from "./storage";
import {
  getJraAdvanceOddsFetchSlotAt,
  getNarOddsFetchSlotAt,
  getNarOddsSaleStartAt,
  getOddsFetchSlotAt,
  parseRaceStartJst,
  toJstIsoString,
} from "./time";
import type { Env, OddsData, OddsFetchStateRow, OddsType } from "./types";

// D1-side lock TTL; short enough that a stuck fetch recovers within the 1-min cadence window
const ODDS_FETCH_LOCK_MINUTES = 3;
const MS_PER_MINUTE = 60_000;
// Non-retryable scrape errors leave the planner lock in place so we do not
// spin against a known-broken row. Anything else (transient browser timeout,
// JRA upstream stall) drops the lock so the next planner tick can retry.
const NON_RETRYABLE_ERROR_FRAGMENTS = [
  "JRA_BROWSER binding",
  "odds_fetch_state not found",
] satisfies readonly string[];
// fetch_logs job_type for a successful insert that missed some odds tabs.
// Status `warn` so dashboards can flag partial coverage without treating it
// as a hard error.
const JRA_PARTIAL_FETCH_JOB_TYPE = "jra-odds-partial-fetch";
const JRA_PARTIAL_FETCH_STATUS = "warn";

export interface FetchAndStoreOddsResult {
  fetchedAt: string;
  inserted: number;
  latest: Partial<Record<OddsType, OddsData[]>>;
}

interface ScrapeResult {
  latest: Partial<Record<OddsType, OddsData[]>>;
  missingTypes: OddsType[];
}

interface JraPartialFetchPayload {
  fetchedTypes: OddsType[];
  missingTypes: OddsType[];
}

const parseOddsLinks = (value: string): Partial<Record<OddsType, string>> => {
  try {
    const parsed = JSON.parse(value);
    return typeof parsed === "object" && parsed !== null
      ? (parsed as Partial<Record<OddsType, string>>)
      : {};
  } catch {
    return {};
  }
};

export const getRaceStartFromState = (state: OddsFetchStateRow): Date | null =>
  parseRaceStartJst(
    state.kaisaiNen,
    state.kaisaiTsukihi,
    state.raceStartAtJst.slice(11, 16).replace(":", ""),
  );

export const isSlotDue = (lastActivityAt: string | null, slotAt: string): boolean => {
  if (!lastActivityAt) {
    return true;
  }
  return new Date(lastActivityAt).getTime() < new Date(slotAt).getTime();
};

export const resolveOddsSlotAt = async (
  env: Env,
  state: OddsFetchStateRow,
  raceStart: Date,
  now: Date,
): Promise<string | null> => {
  if (state.source === "jra") {
    return getJraAdvanceOddsFetchSlotAt(raceStart, now) ?? getOddsFetchSlotAt(raceStart, now);
  }
  const venueLastRaceStartAtJst = await getCachedNarVenueLastRaceStartAtJst(env, {
    kaisaiNen: state.kaisaiNen,
    kaisaiTsukihi: state.kaisaiTsukihi,
    keibajoCode: state.keibajoCode,
  });
  const saleStart = getNarOddsSaleStartAt({
    keibajoCode: state.keibajoCode,
    raceStartAtJst: state.raceStartAtJst,
    venueLastRaceStartAtJst,
  });
  return getNarOddsFetchSlotAt(raceStart, now, saleStart);
};

const scrapeOddsForState = async (env: Env, state: OddsFetchStateRow): Promise<ScrapeResult> => {
  if (state.source === "jra") {
    if (!env.JRA_BROWSER) {
      throw new Error(`JRA_BROWSER binding required for ${state.raceKey}`);
    }
    const result = await fetchJraOddsWithPlaywright(env.JRA_BROWSER, state.debaUrl);
    return { latest: result.latest, missingTypes: result.missingTypes };
  }
  const entryHtml = await fetchRacePage(state.debaUrl);
  const cachedLinks = parseOddsLinks(state.oddsLinksJson);
  const oddsLinks =
    Object.keys(cachedLinks).length > 0 ? cachedLinks : extractOddsLinks(entryHtml, state.debaUrl);
  if (Object.keys(cachedLinks).length === 0) {
    await updateOddsLinks(env.REALTIME_HOT_DB, state.raceKey, oddsLinks);
  }
  const latest = await fetchOdds(state.debaUrl, oddsLinks);
  return { latest, missingTypes: [] };
};

export const isRetryableScrapeError = (error: unknown): boolean => {
  const message = error instanceof Error ? error.message : String(error);
  return !NON_RETRYABLE_ERROR_FRAGMENTS.some((fragment) => message.includes(fragment));
};

const logJraPartialFetch = async (
  env: Env,
  raceKey: string,
  payload: JraPartialFetchPayload,
): Promise<void> => {
  await logFetch(
    env.REALTIME_HOT_DB,
    JRA_PARTIAL_FETCH_JOB_TYPE,
    JRA_PARTIAL_FETCH_STATUS,
    raceKey,
    JSON.stringify(payload),
  );
};

export const fetchAndStoreOdds = async (
  env: Env,
  raceKey: string,
  now: Date,
): Promise<FetchAndStoreOddsResult | null> => {
  const nowIso = toJstIsoString(now);
  const lockUntil = toJstIsoString(
    new Date(now.getTime() + ODDS_FETCH_LOCK_MINUTES * MS_PER_MINUTE),
  );
  const claimed = await claimOddsFetch(env.REALTIME_HOT_DB, raceKey, lockUntil, nowIso);
  if (!claimed) {
    return null;
  }
  const state = await getOddsFetchState(env.REALTIME_HOT_DB, raceKey);
  if (!state) {
    await failOddsFetch(env.REALTIME_HOT_DB, raceKey);
    await logFetch(env.REALTIME_HOT_DB, "fetch-odds", "error", raceKey, "state not found");
    throw new Error(`odds_fetch_state not found: ${raceKey}`);
  }
  try {
    const raceStart = getRaceStartFromState(state);
    if (!raceStart) {
      // Bad race_start_at_jst is structural — leave the enqueue lock so the
      // planner does not spin on a known-broken row.
      await failOddsFetch(env.REALTIME_HOT_DB, raceKey);
      return null;
    }
    const oddsSlotAt = await resolveOddsSlotAt(env, state, raceStart, now);
    if (oddsSlotAt === null) {
      // Sale has not opened yet (NAR pre-sale window). Drop the enqueue
      // lock so the next planner tick can re-evaluate when sale opens; the
      // cadence-based lock would otherwise keep us off the race for the
      // full hourly tier even after sale starts.
      await failOddsFetch(env.REALTIME_HOT_DB, raceKey);
      await releaseEnqueueLock(env, raceKey);
      return null;
    }
    if (!isSlotDue(state.lastOddsFetchAt, oddsSlotAt)) {
      // Already fetched for this slot. Keep the lock — the next slot has
      // its own cadence-driven re-enqueue path.
      await failOddsFetch(env.REALTIME_HOT_DB, raceKey);
      return null;
    }
    const fetchedAt = toJstIsoString(now);
    const scrape = await scrapeOddsForState(env, state);
    // A scrape that produced no rows is a failure (upstream returned nothing),
    // so keep throwing as before. A scrape that produced rows but matches the
    // stored snapshot is a legitimate no-change and must not throw.
    if (countOddsRows(scrape.latest) === 0) {
      throw new Error(`odds rows are empty: ${raceKey}`);
    }
    const stored = await getLatestOddsFromD1(env.REALTIME_HOT_DB, raceKey);
    const changed = stored ? filterChangedOdds(scrape.latest, stored.latest) : scrape.latest;
    const inserted = await insertOddsSnapshot(env.REALTIME_HOT_DB, raceKey, fetchedAt, changed);
    await completeOddsFetch(env.REALTIME_HOT_DB, raceKey, fetchedAt);
    if (scrape.missingTypes.length > 0) {
      await logJraPartialFetch(env, raceKey, {
        fetchedTypes: Object.keys(scrape.latest) as OddsType[],
        missingTypes: scrape.missingTypes,
      });
    }
    return { fetchedAt, inserted, latest: scrape.latest };
  } catch (error) {
    await failOddsFetch(env.REALTIME_HOT_DB, raceKey);
    if (isRetryableScrapeError(error)) {
      await releaseEnqueueLock(env, raceKey);
    }
    throw error;
  }
};
