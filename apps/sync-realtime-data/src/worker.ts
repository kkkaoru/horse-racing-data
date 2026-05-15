import {
  BABA_CODE_TO_LOCAL_KEIBAJO,
  buildRaceListUrl,
  buildRaceResultUrl,
  buildRaceKey,
  extractOddsLinks,
  fetchOdds,
  fetchRaceLinksFromRaceList,
  fetchRacePage,
  fetchTodayRaceListUrls,
  parseRaceMetadata,
  parseRaceEntries,
  parseHorseWeights,
  parseRaceEntryHorseNumbers,
  parseRaceResultExcludedHorseNumbers,
  parseRaceResults,
  parseRaceResultHorseWeights,
  type KeibaGoRaceLink,
} from "./keiba-go";
import { mergeJsonHeaders } from "./http";
import {
  buildJraEntryUrlFromRace,
  fetchJraOddsWithPlaywright,
  parseJraHorseWeights,
  parseJraRaceEntries,
} from "./jra";
import { fetchJraTrackConditionWithPlaywright } from "./jra-track-condition";
import { readCachedOdds, writeCachedOdds } from "./odds-cache";
import { fetchJraRacesByDate, fetchNarRacesByDate } from "./postgres";
import { raceKeyFromRealtimePath } from "./race-key";
import {
  buildRealtimePayload,
  claimOddsFetch,
  claimResultFetch,
  claimTrackConditionFetch,
  completeOddsFetch,
  completeResultFetch,
  completeTrackConditionFetch,
  countRaceSourcesByDate,
  failTrackConditionFetch,
  failOddsFetch,
  failResultFetch,
  getRaceSource,
  getLatestTrackConditionForRace,
  getLatestOddsFromD1,
  getSameDayVenueJockeyWins,
  insertRaceEntrySnapshot,
  insertRaceResultSnapshot,
  insertHorseWeightSnapshot,
  insertJraTrackConditionSnapshot,
  insertOddsSnapshot,
  listJraVenueTrackConditionSchedulesByDate,
  listRaceSourceKeibajoCodesByDate,
  listSchedulableRaceSourcesByDate,
  listTanshoHistory,
  logFetch,
  markOddsFetchQueued,
  markResultFetchQueued,
  markTrackConditionQueued,
  toHorseTrends,
  updateLastFetch,
  updateOddsLinks,
  upsertJraRaceSource,
  upsertNarRaceSource,
  type LocalRaceRow,
} from "./storage";
import { readCachedTrackCondition, writeCachedTrackCondition } from "./track-condition-cache";
import {
  getJraAdvanceOddsFetchSlotAt,
  getOddsFetchSlotAt,
  getTodayJst,
  isJstPollingWindow,
  parseRaceStartJst,
  toJstIsoString,
} from "./time";
import type { Env, Job, NarRaceSource } from "./types";

const QUEUE_SEND_BATCH_SIZE = 100;
const ODDS_FETCH_LOCK_MINUTES = 10;
const RESULT_FETCH_LOCK_MINUTES = 10;
const RESULT_FETCH_INTERVAL_MINUTES = 5;
const TRACK_CONDITION_FETCH_LOCK_MINUTES = 15;
const QUEUE_RETRY_DELAY_SECONDS = 60;

const getNow = (env: Env): Date => {
  if (!env.REALTIME_TEST_NOW) {
    return new Date();
  }
  const date = new Date(env.REALTIME_TEST_NOW);
  return Number.isNaN(date.getTime()) ? new Date() : date;
};

const json = (body: unknown, init?: ResponseInit): Response =>
  new Response(JSON.stringify(body), {
    headers: (() => {
      const headers = mergeJsonHeaders(init);
      headers.set("access-control-allow-origin", "*");
      if (!headers.has("cache-control")) {
        headers.set("cache-control", `public, max-age=${init?.status === 200 ? 10 : 0}`);
      }
      return headers;
    })(),
    status: init?.status ?? 200,
  });

const addDaysToYyyymmdd = (yyyymmdd: string, days: number): string => {
  const date = new Date(
    `${yyyymmdd.slice(0, 4)}-${yyyymmdd.slice(4, 6)}-${yyyymmdd.slice(6, 8)}T00:00:00+09:00`,
  );
  date.setUTCDate(date.getUTCDate() + days);
  return toJstIsoString(date).slice(0, 10).replace(/-/g, "");
};

const getCronJob = (cron: string): Job => {
  if (cron === "5 0 * * *") {
    return { date: getTodayJst(), type: "discover-urls" };
  }
  return { date: getTodayJst(), type: "plan-realtime-fetches" };
};

const buildFallbackRaceRow = (
  targetDate: string,
  link: KeibaGoRaceLink,
  html: string,
): LocalRaceRow | null => {
  const keibajoCode = BABA_CODE_TO_LOCAL_KEIBAJO[link.babaCode];
  if (!keibajoCode) {
    return null;
  }
  const metadata = parseRaceMetadata(html);
  if (!metadata.startTime) {
    return null;
  }
  return {
    hasso_jikoku: metadata.startTime,
    kaisai_nen: targetDate.slice(0, 4),
    kaisai_tsukihi: targetDate.slice(4, 8),
    keibajo_code: keibajoCode,
    kyosomei_hondai: metadata.raceName,
    race_bango: link.raceNumber,
  };
};

const upsertDiscoveredUrls = async (
  env: Env,
  targetDate: string,
): Promise<{
  fallbackRaceListCount: number;
  jraRaceCount: number;
  localRaceCount: number;
  topRaceListCount: number;
  upserted: number;
}> => {
  const [raceListUrls, localRaces, jraRaces] = await Promise.all([
    fetchTodayRaceListUrls(targetDate),
    fetchNarRacesByDate(env, targetDate),
    fetchJraRacesByDate(env, targetDate),
  ]);
  const fallbackRaceListUrls = Array.from(
    new Set(
      localRaces
        .map(
          (race) =>
            Object.entries(BABA_CODE_TO_LOCAL_KEIBAJO).find(
              ([, code]) => code === race.keibajo_code,
            )?.[0],
        )
        .filter((babaCode): babaCode is string => Boolean(babaCode)),
    ),
  ).map((babaCode) => buildRaceListUrl(targetDate, babaCode));
  const targetRaceListUrls = Array.from(
    new Map(
      [...raceListUrls, ...fallbackRaceListUrls].map((item) => [item.babaCode, item]),
    ).values(),
  );
  const localRaceMap = new Map(
    localRaces.map((race) => [
      buildRaceKey(race.kaisai_nen, race.kaisai_tsukihi, race.keibajo_code, race.race_bango),
      race,
    ]),
  );

  let upserted = 0;
  for (const race of jraRaces) {
    const entryUrl = buildJraEntryUrlFromRace(race);
    if (!entryUrl) {
      continue;
    }
    await upsertJraRaceSource(env.REALTIME_DB, race, entryUrl);
    upserted += 1;
  }
  for (const raceList of targetRaceListUrls) {
    const links = await fetchRaceLinksFromRaceList(raceList.url);
    for (const link of links) {
      const keibajoCode = BABA_CODE_TO_LOCAL_KEIBAJO[link.babaCode];
      if (!keibajoCode) {
        continue;
      }
      const raceKey = buildRaceKey(
        targetDate.slice(0, 4),
        targetDate.slice(4, 8),
        keibajoCode,
        link.raceNumber,
      );
      const racePageHtml = await fetchRacePage(link.url);
      const race =
        localRaceMap.get(raceKey) ?? buildFallbackRaceRow(targetDate, link, racePageHtml);
      if (!race) {
        continue;
      }
      await upsertNarRaceSource(
        env.REALTIME_DB,
        link,
        race,
        extractOddsLinks(racePageHtml, link.url),
      );
      upserted += 1;
    }
  }
  return {
    fallbackRaceListCount: fallbackRaceListUrls.length,
    jraRaceCount: jraRaces.length,
    localRaceCount: localRaces.length,
    topRaceListCount: raceListUrls.length,
    upserted,
  };
};

const ensureJraRaceSourcesAreCurrent = async (env: Env, targetDate: string): Promise<void> => {
  const [d1RaceCount, jraRaces] = await Promise.all([
    countRaceSourcesByDate(env.REALTIME_DB, targetDate),
    fetchJraRacesByDate(env, targetDate),
  ]);
  if (jraRaces.length === 0) {
    return;
  }
  if (d1RaceCount >= jraRaces.length) {
    const discoveredKeibajoCodes = new Set(
      await listRaceSourceKeibajoCodesByDate(env.REALTIME_DB, targetDate),
    );
    const expectedJraVenueCodes = Array.from(new Set(jraRaces.map((race) => race.keibajo_code)));
    if (expectedJraVenueCodes.every((keibajoCode) => discoveredKeibajoCodes.has(keibajoCode))) {
      return;
    }
  }
  for (const race of jraRaces) {
    const entryUrl = buildJraEntryUrlFromRace(race);
    if (entryUrl) {
      await upsertJraRaceSource(env.REALTIME_DB, race, entryUrl);
    }
  }
};

const getRaceStart = (race: NarRaceSource): Date | null =>
  parseRaceStartJst(
    race.kaisaiNen,
    race.kaisaiTsukihi,
    race.raceStartAtJst.slice(11, 16).replace(":", ""),
  );

const minutesUntilRace = (race: NarRaceSource, now = new Date()): number | null => {
  const raceStart = getRaceStart(race);
  if (!raceStart) {
    return null;
  }
  return (raceStart.getTime() - now.getTime()) / 60_000;
};

const getCurrentOddsSlotAt = (race: NarRaceSource, now: Date): string | null => {
  const raceStart = getRaceStart(race);
  if (!raceStart) {
    return null;
  }
  if (race.source === "jra") {
    return getJraAdvanceOddsFetchSlotAt(raceStart, now) ?? getOddsFetchSlotAt(raceStart, now);
  }
  return getOddsFetchSlotAt(raceStart, now);
};

const isDue = (
  lastFetchedAt: string | null,
  intervalMinutes: number,
  now = new Date(),
): boolean => {
  if (!lastFetchedAt) {
    return true;
  }
  const last = new Date(lastFetchedAt).getTime();
  return Number.isNaN(last) || now.getTime() - last >= intervalMinutes * 60_000;
};

const isSlotDue = (lastActivityAt: string | null, slotAt: string): boolean => {
  if (!lastActivityAt) {
    return true;
  }
  return new Date(lastActivityAt).getTime() < new Date(slotAt).getTime();
};

const latestTimestamp = (...timestamps: (string | null)[]): string | null => {
  const latest = timestamps
    .map((timestamp) => (timestamp ? new Date(timestamp).getTime() : Number.NaN))
    .filter((timestamp) => !Number.isNaN(timestamp))
    .sort((left, right) => right - left)[0];
  return latest === undefined ? null : new Date(latest).toISOString();
};

const isThreeMinuteTick = (date: Date): boolean => date.getUTCMinutes() % 3 === 0;

const getJstDayStart = (targetDate: string): Date =>
  new Date(
    `${targetDate.slice(0, 4)}-${targetDate.slice(4, 6)}-${targetDate.slice(6, 8)}T00:00:00+09:00`,
  );

const toJstSlotIso = (targetDate: string, hhmm: string): string =>
  `${targetDate.slice(0, 4)}-${targetDate.slice(4, 6)}-${targetDate.slice(6, 8)}T${hhmm.slice(0, 2)}:${hhmm.slice(2, 4)}:00+09:00`;

const floorToHalfHourJstSlot = (now: Date): string => {
  const current = toJstIsoString(now);
  const minute = Number(current.slice(14, 16));
  const flooredMinute = minute >= 30 ? "30" : "00";
  return `${current.slice(0, 14)}${flooredMinute}:00+09:00`;
};

const isTrackConditionDue = (
  schedule: {
    firstRaceStartAtJst: string;
    lastFetchAt: string | null;
    lastQueuedAt: string | null;
    lastRaceStartAtJst: string;
  },
  targetDate: string,
  now: Date,
): { due: boolean; slotAt: string | null } => {
  const today = getTodayJst(now);
  const dayBefore = addDaysToYyyymmdd(targetDate, -1);
  const nowMs = now.getTime();
  const lastActivity = latestTimestamp(schedule.lastFetchAt, schedule.lastQueuedAt);

  if (today === dayBefore) {
    const slotAt = toJstSlotIso(dayBefore, "1000");
    const dayBeforeSlot = new Date(getJstDayStart(targetDate).getTime() - 14 * 60 * 60_000);
    return {
      due: nowMs >= dayBeforeSlot.getTime() && isSlotDue(lastActivity, slotAt),
      slotAt,
    };
  }

  if (today !== targetDate) {
    return { due: false, slotAt: null };
  }

  if (nowMs < new Date(schedule.firstRaceStartAtJst).getTime()) {
    const slotAt = ["0600", "0900"]
      .map((hhmm) => toJstSlotIso(targetDate, hhmm))
      .filter((candidate) => nowMs >= new Date(candidate).getTime())
      .toSorted((left, right) => new Date(right).getTime() - new Date(left).getTime())[0];
    if (slotAt) {
      return { due: isSlotDue(lastActivity, slotAt), slotAt };
    }
  }

  const firstRaceMs = new Date(schedule.firstRaceStartAtJst).getTime();
  const lastRaceMs = new Date(schedule.lastRaceStartAtJst).getTime();
  if (nowMs >= firstRaceMs && nowMs <= lastRaceMs) {
    const slotAt = floorToHalfHourJstSlot(now);
    return { due: isSlotDue(lastActivity, slotAt), slotAt };
  }

  return { due: false, slotAt: null };
};

const isRaceFinished = (race: NarRaceSource, now: Date): boolean => {
  const minutes = minutesUntilRace(race, now);
  return minutes !== null && minutes <= 0;
};

const ensureDiscoveredUrlsAreCurrent = async (env: Env, targetDate: string): Promise<void> => {
  const [d1RaceCount, localRaces, jraRaces] = await Promise.all([
    countRaceSourcesByDate(env.REALTIME_DB, targetDate),
    fetchNarRacesByDate(env, targetDate),
    fetchJraRacesByDate(env, targetDate),
  ]);
  const raceListUrls = await fetchTodayRaceListUrls(targetDate);
  const expectedKeibajoCodes = raceListUrls
    .map((raceList) => BABA_CODE_TO_LOCAL_KEIBAJO[raceList.babaCode])
    .filter((keibajoCode): keibajoCode is string => Boolean(keibajoCode));
  const discoveredKeibajoCodes = new Set(
    await listRaceSourceKeibajoCodesByDate(env.REALTIME_DB, targetDate),
  );
  const hasAllExpectedKeibajoCodes = expectedKeibajoCodes.every((keibajoCode) =>
    discoveredKeibajoCodes.has(keibajoCode),
  );
  if (d1RaceCount >= localRaces.length + jraRaces.length && hasAllExpectedKeibajoCodes) {
    return;
  }
  await upsertDiscoveredUrls(env, targetDate);
};

const enqueueJobs = async (env: Env, jobs: Job[]): Promise<void> => {
  for (let index = 0; index < jobs.length; index += QUEUE_SEND_BATCH_SIZE) {
    const chunk = jobs.slice(index, index + QUEUE_SEND_BATCH_SIZE);
    if (chunk.length === 1) {
      await env.REALTIME_JOBS.send(chunk[0] as Job);
      continue;
    }
    await env.REALTIME_JOBS.sendBatch(chunk.map((body) => ({ body })));
  }
};

const planTrackConditionFetchesForDate = async (
  env: Env,
  targetDate: string,
  now: Date,
): Promise<Job[]> => {
  await ensureJraRaceSourcesAreCurrent(env, targetDate);
  const schedules = await listJraVenueTrackConditionSchedulesByDate(env.REALTIME_DB, targetDate);
  return schedules.flatMap((schedule) => {
    const due = isTrackConditionDue(schedule, targetDate, now);
    return due.due
      ? [{ date: targetDate, keibajoCode: schedule.keibajoCode, type: "fetch-jra-track-condition" }]
      : [];
  });
};

const planJraAdvanceOddsFetchesForDate = async (
  env: Env,
  targetDate: string,
  now: Date,
): Promise<Job[]> => {
  await ensureJraRaceSourcesAreCurrent(env, targetDate);
  const races = await listSchedulableRaceSourcesByDate(env.REALTIME_DB, targetDate);
  return races.flatMap((race) => {
    if (race.source !== "jra") {
      return [];
    }
    const oddsSlotAt = getCurrentOddsSlotAt(race, now);
    const oddsLockUntil = race.oddsFetchLockUntil
      ? new Date(race.oddsFetchLockUntil).getTime()
      : Number.NaN;
    const lastOddsActivity = latestTimestamp(race.lastOddsFetchAt, race.lastOddsQueuedAt);
    if (
      oddsSlotAt &&
      (Number.isNaN(oddsLockUntil) || oddsLockUntil <= now.getTime()) &&
      isSlotDue(lastOddsActivity, oddsSlotAt)
    ) {
      return [{ raceKey: race.raceKey, type: "fetch-odds" as const }];
    }
    return [];
  });
};

const planRealtimeFetches = async (env: Env, targetDate: string): Promise<number> => {
  const now = getNow(env);
  const jobs: Job[] = [];
  const shouldRunGeneralPolling = isJstPollingWindow(now);
  if (shouldRunGeneralPolling) {
    await ensureDiscoveredUrlsAreCurrent(env, targetDate);
    const races = await listSchedulableRaceSourcesByDate(env.REALTIME_DB, targetDate);
    for (const race of races) {
      const minutes = minutesUntilRace(race, now);
      if (minutes === null) {
        continue;
      }

      const oddsSlotAt = getCurrentOddsSlotAt(race, now);
      const oddsLockUntil = race.oddsFetchLockUntil
        ? new Date(race.oddsFetchLockUntil).getTime()
        : Number.NaN;
      const lastOddsActivity = latestTimestamp(race.lastOddsFetchAt, race.lastOddsQueuedAt);
      if (
        oddsSlotAt &&
        (Number.isNaN(oddsLockUntil) || oddsLockUntil <= now.getTime()) &&
        isSlotDue(lastOddsActivity, oddsSlotAt)
      ) {
        jobs.push({ raceKey: race.raceKey, type: "fetch-odds" });
      }

      if (isThreeMinuteTick(now) && minutes <= 20 && isDue(race.lastWeightFetchAt, 24 * 60, now)) {
        jobs.push({ raceKey: race.raceKey, type: "fetch-weights" });
      }

      const resultLockUntil = race.resultFetchLockUntil
        ? new Date(race.resultFetchLockUntil).getTime()
        : Number.NaN;
      if (
        minutes <= 0 &&
        race.source === "nar" &&
        !race.resultCompleteAt &&
        isDue(race.lastResultFetchAt, RESULT_FETCH_INTERVAL_MINUTES, now) &&
        (Number.isNaN(resultLockUntil) || resultLockUntil <= now.getTime()) &&
        !race.lastResultQueuedAt
      ) {
        jobs.push({ raceKey: race.raceKey, type: "fetch-results" });
      }
    }
  } else {
    jobs.push(...(await planJraAdvanceOddsFetchesForDate(env, targetDate, now)));
  }
  jobs.push(
    ...(await planJraAdvanceOddsFetchesForDate(env, addDaysToYyyymmdd(targetDate, 1), now)),
  );
  jobs.push(...(await planTrackConditionFetchesForDate(env, targetDate, now)));
  jobs.push(
    ...(await planTrackConditionFetchesForDate(env, addDaysToYyyymmdd(targetDate, 1), now)),
  );
  await enqueueJobs(env, jobs);
  const queuedAt = toJstIsoString(now);
  await markOddsFetchQueued(
    env.REALTIME_DB,
    jobs.flatMap((job) => (job.type === "fetch-odds" ? [job.raceKey] : [])),
    queuedAt,
  );
  await markResultFetchQueued(
    env.REALTIME_DB,
    jobs.flatMap((job) => (job.type === "fetch-results" ? [job.raceKey] : [])),
    queuedAt,
  );
  await markTrackConditionQueued(
    env.REALTIME_DB,
    jobs.flatMap((job) =>
      job.type === "fetch-jra-track-condition"
        ? [{ date: job.date, keibajoCode: job.keibajoCode }]
        : [],
    ),
    queuedAt,
  );
  return jobs.length;
};

const fetchAndStoreOdds = async (env: Env, raceKey: string): Promise<void> => {
  const now = getNow(env);
  const lockUntil = toJstIsoString(new Date(now.getTime() + ODDS_FETCH_LOCK_MINUTES * 60_000));
  const claimed = await claimOddsFetch(env.REALTIME_DB, raceKey, lockUntil, toJstIsoString(now));
  if (!claimed) {
    return;
  }
  const race = await getRaceSource(env.REALTIME_DB, raceKey);
  if (!race) {
    await failOddsFetch(env.REALTIME_DB, raceKey);
    throw new Error(`race source not found: ${raceKey}`);
  }
  try {
    const oddsSlotAt = getCurrentOddsSlotAt(race, now);
    if (!oddsSlotAt || !isSlotDue(race.lastOddsFetchAt, oddsSlotAt)) {
      await failOddsFetch(env.REALTIME_DB, raceKey);
      return;
    }
    const fetchedAt = toJstIsoString();
    let latest;
    if (race.source === "jra") {
      const result = await fetchJraOddsWithPlaywright(env.JRA_BROWSER, race.debaUrl);
      await insertRaceEntrySnapshot(
        env.REALTIME_DB,
        raceKey,
        fetchedAt,
        parseJraRaceEntries(result.entryHtml),
      );
      const weights = parseJraHorseWeights(result.entryHtml);
      if (weights.length > 0) {
        await insertHorseWeightSnapshot(env.REALTIME_DB, raceKey, fetchedAt, weights);
        await updateLastFetch(env.REALTIME_DB, raceKey, "last_weight_fetch_at", fetchedAt);
      }
      latest = result.latest;
    } else {
      const entryHtml = await fetchRacePage(race.debaUrl);
      await insertRaceEntrySnapshot(
        env.REALTIME_DB,
        raceKey,
        fetchedAt,
        parseRaceEntries(entryHtml),
      );
      const oddsLinks =
        Object.keys(race.oddsLinks).length > 0
          ? race.oddsLinks
          : extractOddsLinks(entryHtml, race.debaUrl);
      if (Object.keys(race.oddsLinks).length === 0) {
        await updateOddsLinks(env.REALTIME_DB, race.raceKey, oddsLinks);
      }
      latest = await fetchOdds(race.debaUrl, oddsLinks);
    }
    const inserted = await insertOddsSnapshot(env.REALTIME_DB, raceKey, fetchedAt, latest);
    if (inserted === 0) {
      throw new Error(`odds rows are empty: ${raceKey}`);
    }
    await completeOddsFetch(env.REALTIME_DB, raceKey, fetchedAt);
    const history = await listTanshoHistory(env.REALTIME_DB, raceKey);
    await writeCachedOdds(env, raceKey, { fetchedAt, history, latest });
  } catch (error) {
    await failOddsFetch(env.REALTIME_DB, raceKey);
    throw error;
  }
};

const fetchAndStoreWeights = async (env: Env, raceKey: string): Promise<void> => {
  const race = await getRaceSource(env.REALTIME_DB, raceKey);
  if (!race) {
    throw new Error(`race source not found: ${raceKey}`);
  }
  const fetchedAt = toJstIsoString();
  const html = await fetchRacePage(race.debaUrl);
  const entries = race.source === "jra" ? parseJraRaceEntries(html) : parseRaceEntries(html);
  await insertRaceEntrySnapshot(env.REALTIME_DB, raceKey, fetchedAt, entries);
  let weights = race.source === "jra" ? parseJraHorseWeights(html) : parseHorseWeights(html);
  if (race.source === "nar" && weights.length === 0) {
    const resultHtml = await fetchRacePage(buildRaceResultUrl(race.debaUrl));
    weights = parseRaceResultHorseWeights(resultHtml);
  }
  if (weights.length > 0 && weights.length < 2) {
    await insertHorseWeightSnapshot(env.REALTIME_DB, raceKey, fetchedAt, []);
    throw new Error(`horse weight rows are unexpectedly sparse: ${weights.length}`);
  }
  await insertHorseWeightSnapshot(env.REALTIME_DB, raceKey, fetchedAt, weights);
  if (weights.length > 0) {
    await updateLastFetch(env.REALTIME_DB, raceKey, "last_weight_fetch_at", fetchedAt);
  }
};

const fetchAndStoreResults = async (env: Env, raceKey: string): Promise<void> => {
  const now = getNow(env);
  const lockUntil = toJstIsoString(new Date(now.getTime() + RESULT_FETCH_LOCK_MINUTES * 60_000));
  const claimed = await claimResultFetch(env.REALTIME_DB, raceKey, lockUntil, toJstIsoString(now));
  if (!claimed) {
    return;
  }
  const race = await getRaceSource(env.REALTIME_DB, raceKey);
  if (!race) {
    await failResultFetch(env.REALTIME_DB, raceKey);
    throw new Error(`race source not found: ${raceKey}`);
  }
  if (race.source !== "nar") {
    await failResultFetch(env.REALTIME_DB, raceKey);
    return;
  }
  if (!isRaceFinished(race, now)) {
    await failResultFetch(env.REALTIME_DB, raceKey);
    return;
  }

  try {
    const fetchedAt = toJstIsoString();
    const [entryHtml, resultHtml] = await Promise.all([
      fetchRacePage(race.debaUrl),
      fetchRacePage(buildRaceResultUrl(race.debaUrl)),
    ]);
    const entries = parseRaceEntries(entryHtml);
    await insertRaceEntrySnapshot(env.REALTIME_DB, raceKey, fetchedAt, entries);
    const entryHorseNumbers = parseRaceEntryHorseNumbers(entryHtml);
    const excludedHorseNumbers = new Set(parseRaceResultExcludedHorseNumbers(resultHtml));
    const expectedHorseCount = entryHorseNumbers.filter(
      (horseNumber) => !excludedHorseNumbers.has(horseNumber),
    ).length;
    const results = parseRaceResults(resultHtml);
    const inserted = await insertRaceResultSnapshot(env.REALTIME_DB, raceKey, fetchedAt, results);
    await completeResultFetch(env.REALTIME_DB, raceKey, fetchedAt, {
      expectedHorseCount,
      isComplete: expectedHorseCount > 0 && inserted >= expectedHorseCount,
      savedHorseCount: inserted,
    });
  } catch (error) {
    await failResultFetch(env.REALTIME_DB, raceKey);
    throw error;
  }
};

const fetchAndStoreJraTrackCondition = async (
  env: Env,
  params: { date: string; keibajoCode: string },
): Promise<void> => {
  const now = getNow(env);
  const lockUntil = toJstIsoString(
    new Date(now.getTime() + TRACK_CONDITION_FETCH_LOCK_MINUTES * 60_000),
  );
  const claimed = await claimTrackConditionFetch(env.REALTIME_DB, {
    date: params.date,
    keibajoCode: params.keibajoCode,
    lockUntil,
    now: toJstIsoString(now),
  });
  if (!claimed) {
    return;
  }

  try {
    await ensureJraRaceSourcesAreCurrent(env, params.date);
    const fetchedAt = toJstIsoString();
    const condition = await fetchJraTrackConditionWithPlaywright(env.JRA_BROWSER, {
      kaisaiNen: params.date.slice(0, 4),
      keibajoCode: params.keibajoCode,
    });
    const payload = { ...condition, fetchedAt };
    const races = await insertJraTrackConditionSnapshot(env.REALTIME_DB, {
      condition: payload,
      date: params.date,
      fetchedAt,
      keibajoCode: params.keibajoCode,
    });
    await completeTrackConditionFetch(env.REALTIME_DB, {
      date: params.date,
      fetchedAt,
      keibajoCode: params.keibajoCode,
    });
    await Promise.all(
      races
        .filter((race) => new Date(fetchedAt).getTime() <= new Date(race.raceStartAtJst).getTime())
        .map((race) => writeCachedTrackCondition(env, race.raceKey, payload)),
    );
  } catch (error) {
    await failTrackConditionFetch(env.REALTIME_DB, params);
    throw error;
  }
};

export const handleJob = async (env: Env, job: Job): Promise<void> => {
  try {
    if (job.type === "discover-urls") {
      const result = await upsertDiscoveredUrls(env, job.date);
      await logFetch(env.REALTIME_DB, job.type, "ok", null, JSON.stringify(result));
      return;
    }
    if (job.type === "plan-realtime-fetches") {
      const count = await planRealtimeFetches(env, job.date);
      await logFetch(env.REALTIME_DB, job.type, "ok", null, `${count} jobs queued`);
      return;
    }
    if (job.type === "fetch-odds") {
      await fetchAndStoreOdds(env, job.raceKey);
      await logFetch(env.REALTIME_DB, job.type, "ok", job.raceKey, null);
      return;
    }
    if (job.type === "fetch-results") {
      await fetchAndStoreResults(env, job.raceKey);
      await logFetch(env.REALTIME_DB, job.type, "ok", job.raceKey, null);
      return;
    }
    if (job.type === "fetch-jra-track-condition") {
      await fetchAndStoreJraTrackCondition(env, job);
      await logFetch(
        env.REALTIME_DB,
        job.type,
        "ok",
        null,
        JSON.stringify({ date: job.date, keibajoCode: job.keibajoCode }),
      );
      return;
    }
    await fetchAndStoreWeights(env, job.raceKey);
    await logFetch(env.REALTIME_DB, job.type, "ok", job.raceKey, null);
  } catch (error) {
    await logFetch(
      env.REALTIME_DB,
      job.type,
      "error",
      "raceKey" in job ? job.raceKey : null,
      error instanceof Error ? error.message : String(error),
    );
    throw error;
  }
};

const raceKeyFromRequest = (url: URL): string | null => {
  return raceKeyFromRealtimePath(url.pathname);
};

const sameDayVenueJockeyWinsFromRequest = (
  url: URL,
): {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  year: string;
} | null => {
  const match = url.pathname.match(
    /^\/api\/nar\/races\/(\d{4})\/(\d{2})\/(\d{2})\/([0-9A-Z]{2})\/(\d{2})\/jockey-wins$/u,
  );
  if (!match?.[1] || !match[2] || !match[3] || !match[4] || !match[5]) {
    return null;
  }
  return {
    day: match[3],
    keibajoCode: match[4],
    month: match[2],
    raceNumber: match[5],
    year: match[1],
  };
};

export default {
  async fetch(request, env): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "access-control-allow-headers": "content-type",
          "access-control-allow-methods": "GET, OPTIONS, POST",
          "access-control-allow-origin": "*",
        },
      });
    }

    if (url.pathname === "/health") {
      return json({ ok: true });
    }

    if (url.pathname === "/api/jobs" && request.method === "POST") {
      const expectedToken = env.REALTIME_ADMIN_TOKEN;
      if (!expectedToken || request.headers.get("authorization") !== `Bearer ${expectedToken}`) {
        return json({ error: "forbidden" }, { status: 403 });
      }
      const job = (await request.json()) as Job;
      await env.REALTIME_JOBS.send(job);
      return json({ ok: true });
    }

    const raceKey = raceKeyFromRequest(url);
    if (raceKey && request.method === "GET") {
      const [source, cachedOdds, cachedTrackCondition] = await Promise.all([
        getRaceSource(env.REALTIME_DB, raceKey),
        readCachedOdds(env, raceKey),
        readCachedTrackCondition(env, raceKey),
      ]);
      const odds = cachedOdds ?? (await getLatestOddsFromD1(env.REALTIME_DB, raceKey));
      const trackCondition =
        cachedTrackCondition ?? (await getLatestTrackConditionForRace(env.REALTIME_DB, raceKey));
      const payload = await buildRealtimePayload(
        env.REALTIME_DB,
        raceKey,
        source,
        odds,
        trackCondition,
      );
      if (payload.odds && payload.odds.horseTrends.length === 0) {
        payload.odds.horseTrends = toHorseTrends(payload.odds.history);
      }
      return json(payload, {
        headers: {
          "cache-control": `public, max-age=${Number(env.REALTIME_API_CACHE_SECONDS ?? "20")}`,
        },
      });
    }

    const sameDayVenueJockeyWins = sameDayVenueJockeyWinsFromRequest(url);
    if (sameDayVenueJockeyWins && request.method === "GET") {
      return json(
        {
          jockeyWins: await getSameDayVenueJockeyWins(env.REALTIME_DB, {
            beforeRaceBango: sameDayVenueJockeyWins.raceNumber,
            kaisaiNen: sameDayVenueJockeyWins.year,
            kaisaiTsukihi: `${sameDayVenueJockeyWins.month}${sameDayVenueJockeyWins.day}`,
            keibajoCode: sameDayVenueJockeyWins.keibajoCode,
          }),
        },
        {
          headers: {
            "cache-control": `public, max-age=${Number(env.REALTIME_API_CACHE_SECONDS ?? "20")}`,
          },
        },
      );
    }

    return json({ error: "not found" }, { status: 404 });
  },

  async scheduled(controller, env, ctx): Promise<void> {
    ctx.waitUntil(env.REALTIME_JOBS.send(getCronJob(controller.cron)));
  },

  async queue(batch, env): Promise<void> {
    for (const message of batch.messages) {
      try {
        await handleJob(env, message.body);
        message.ack();
      } catch {
        if (message.body.type === "fetch-odds") {
          message.ack();
        } else {
          message.retry({ delaySeconds: QUEUE_RETRY_DELAY_SECONDS });
        }
      }
    }
  },
} satisfies ExportedHandler<Env, Job>;
