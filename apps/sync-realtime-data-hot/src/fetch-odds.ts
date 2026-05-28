import { extractOddsLinks, fetchOdds, fetchRacePage } from "./keiba-go";
import { fetchJraOddsWithPlaywright } from "./jra";
import {
  claimOddsFetch,
  completeOddsFetch,
  failOddsFetch,
  getNarVenueLastRaceStartAtJst,
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

const ODDS_FETCH_LOCK_MINUTES = 10;
const MS_PER_MINUTE = 60_000;

export interface FetchAndStoreOddsResult {
  fetchedAt: string;
  inserted: number;
  latest: Partial<Record<OddsType, OddsData[]>>;
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
  const venueLastRaceStartAtJst = await getNarVenueLastRaceStartAtJst(
    env.REALTIME_HOT_DB,
    state.kaisaiNen,
    state.kaisaiTsukihi,
    state.keibajoCode,
  );
  const saleStart = getNarOddsSaleStartAt({
    keibajoCode: state.keibajoCode,
    raceStartAtJst: state.raceStartAtJst,
    venueLastRaceStartAtJst,
  });
  return getNarOddsFetchSlotAt(raceStart, now, saleStart);
};

const scrapeOddsForState = async (
  env: Env,
  state: OddsFetchStateRow,
): Promise<Partial<Record<OddsType, OddsData[]>>> => {
  if (state.source === "jra") {
    if (!env.JRA_BROWSER) {
      throw new Error(`JRA_BROWSER binding required for ${state.raceKey}`);
    }
    const result = await fetchJraOddsWithPlaywright(env.JRA_BROWSER, state.debaUrl);
    return result.latest;
  }
  const entryHtml = await fetchRacePage(state.debaUrl);
  const cachedLinks = parseOddsLinks(state.oddsLinksJson);
  const oddsLinks =
    Object.keys(cachedLinks).length > 0 ? cachedLinks : extractOddsLinks(entryHtml, state.debaUrl);
  if (Object.keys(cachedLinks).length === 0) {
    await updateOddsLinks(env.REALTIME_HOT_DB, state.raceKey, oddsLinks);
  }
  return fetchOdds(state.debaUrl, oddsLinks);
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
      await failOddsFetch(env.REALTIME_HOT_DB, raceKey);
      return null;
    }
    const oddsSlotAt = await resolveOddsSlotAt(env, state, raceStart, now);
    if (!oddsSlotAt || !isSlotDue(state.lastOddsFetchAt, oddsSlotAt)) {
      await failOddsFetch(env.REALTIME_HOT_DB, raceKey);
      return null;
    }
    const fetchedAt = toJstIsoString(now);
    const latest = await scrapeOddsForState(env, state);
    const inserted = await insertOddsSnapshot(env.REALTIME_HOT_DB, raceKey, fetchedAt, latest);
    if (inserted === 0) {
      throw new Error(`odds rows are empty: ${raceKey}`);
    }
    await completeOddsFetch(env.REALTIME_HOT_DB, raceKey, fetchedAt);
    await logFetch(env.REALTIME_HOT_DB, "fetch-odds", "ok", raceKey, null);
    return { fetchedAt, inserted, latest };
  } catch (error) {
    await failOddsFetch(env.REALTIME_HOT_DB, raceKey);
    throw error;
  }
};
