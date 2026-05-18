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
  buildJraResultUrlFromRaceSource,
  fetchJraResultHtmlWithPlaywright,
  fetchJraOddsWithPlaywright,
  isJraScratchStatus,
  parseJraRaceResultExcludedHorseNumbers,
  parseJraRaceResults,
  parseJraHorseWeights,
  parseJraRaceEntries,
  sanitizeJraRaceEntriesWithOdds,
} from "./jra";
import { fetchJraTrackConditionWithPlaywright } from "./jra-track-condition";
import { readCachedOdds, writeCachedOdds } from "./odds-cache";
import {
  buildPremiumUrl,
  discoverPremiumRaceLinks,
  fetchPremiumHtml,
  fetchPremiumHtmlAttempts,
  getPremiumRaceConfig,
  hasPremiumRaceFetchConfig,
  matchPremiumLinkToRace,
  parsePremiumPaddockBulletins,
  parsePremiumStableComments,
  parsePremiumTrainingReviews,
  summarizePremiumStableCommentHtml,
  type PremiumPaddockBulletin,
} from "./premium-race";
import {
  clearCachedPremiumPaddock,
  readCachedPremiumPaddock,
  writeCachedPremiumPaddock,
} from "./premium-paddock-cache";
import { fetchJraRacesByDate, fetchNarRacesByDate } from "./postgres";
import { buildRealtimeRaceKey, raceKeyFromRealtimePath, type RealtimeSource } from "./race-key";
import {
  buildRealtimePayload,
  claimPremiumPaddockNotificationSend,
  claimOddsFetch,
  claimResultFetch,
  claimTrackConditionFetch,
  completeOddsFetch,
  completeResultFetch,
  completeTrackConditionFetch,
  countJraRaceSourcesMissingRaceDateFieldsByDate,
  countRaceSourcesByDate,
  failTrackConditionFetch,
  failOddsFetch,
  failResultFetch,
  getPremiumRaceLink,
  getPremiumRacePayload,
  getPremiumPaddockFetchState,
  getPremiumPaddockNotificationState,
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
  listPremiumRaceDataFetchCandidatesByDate,
  listRaceSourceKeibajoCodesByDate,
  listSchedulableRaceSourcesByDate,
  listOddsHistoryByType,
  listTanshoHistory,
  logFetch,
  markOddsFetchQueued,
  markPremiumPaddockQueued,
  markPremiumRaceDataQueued,
  markResultFetchQueued,
  markTrackConditionQueued,
  recordPremiumPaddockNotificationEvent,
  replacePremiumRaceData,
  toHorseTrends,
  toOddsTrendsByType,
  updateLastFetch,
  updateOddsLinks,
  updatePremiumRaceDataFetchState,
  updatePremiumPaddockFetchState,
  updatePremiumPaddockNotificationState,
  upsertJraRaceSource,
  upsertNarRaceSource,
  upsertPremiumRaceLink,
  type LocalRaceRow,
} from "./storage";
import { runFinishPositionLiteCronTick } from "./finish-position-lite-cron";
import { handleFinishPositionLiteJob } from "./finish-position-lite-queue";
import { RUNNING_STYLE_INFERENCE_CRON, runRunningStyleCronTick } from "./running-style-cron";
import { readCachedTrackCondition, writeCachedTrackCondition } from "./track-condition-cache";
import {
  getJraAdvanceOddsFetchSlotAt,
  getNextOddsFetchSlotAt,
  getOddsFetchSlotAt,
  getTodayJst,
  isJstPollingWindow,
  parseRaceStartJst,
  toJstIsoString,
} from "./time";
import type { Env, HorseWeight, Job, NarRaceSource, RaceEntry } from "./types";

const QUEUE_SEND_BATCH_SIZE = 100;
const ODDS_FETCH_LOCK_MINUTES = 10;
const RESULT_FETCH_LOCK_MINUTES = 10;
const RESULT_FETCH_INTERVAL_MINUTES = 5;
const TRACK_CONDITION_FETCH_LOCK_MINUTES = 15;
const QUEUE_RETRY_DELAY_SECONDS = 60;
const PREMIUM_RACE_DATA_RETRY_DELAY_SECONDS = 20 * 60;
const PREMIUM_PADDOCK_RETRY_DELAY_SECONDS = 120;
const PREMIUM_PADDOCK_RECHECK_MINUTES = 3;
const PREMIUM_PADDOCK_WINDOW_BEFORE_MINUTES = 35;
const PREMIUM_PADDOCK_WINDOW_AFTER_MINUTES = 2;
const REALTIME_PLAN_SELF_SCHEDULE_DELAY_SECONDS = 60;
const REALTIME_PLAN_SELF_SCHEDULE_STALE_SECONDS = 90;
const DEFAULT_PREMIUM_RACE_QUEUE_DELAY_SECONDS = 15;
const DEFAULT_PREMIUM_PADDOCK_DISCORD_BOT_NAME = "外部パドック速報";
const DEFAULT_DETAIL_ORIGIN = "https://pc-keiba-viewer.kkk4oru.com";
const PREMIUM_PADDOCK_NOTIFICATION_FORMAT_VERSION = "2026-05-16-v2";
const PREMIUM_PADDOCK_NOTIFICATION_LOCK_SECONDS = 90;
const JRA_KEIBAJO_NAMES: Record<string, string> = {
  "01": "札幌",
  "02": "函館",
  "03": "福島",
  "04": "新潟",
  "05": "東京",
  "06": "中山",
  "07": "中京",
  "08": "京都",
  "09": "阪神",
  "10": "小倉",
};

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

const JRA_PREMIUM_LINK_CRONS = new Set(["0 4 * * 5", "0 4 * * 6"]);
const JRA_PREMIUM_DATA_CRONS = new Set(["0 5 * * 5", "0 5 * * 6"]);

const getCronJob = (cron: string, now = new Date()): Job => {
  const today = getTodayJst(now);
  if (JRA_PREMIUM_LINK_CRONS.has(cron)) {
    return { date: addDaysToYyyymmdd(today, 1), type: "discover-premium-race-links" };
  }
  if (JRA_PREMIUM_DATA_CRONS.has(cron)) {
    return { date: addDaysToYyyymmdd(today, 1), type: "plan-premium-race-data-fetches" };
  }
  if (cron === "5 0 * * *") {
    return { date: today, type: "discover-urls" };
  }
  return { date: today, type: "plan-realtime-fetches" };
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
  const [d1RaceCount, missingRaceDateFieldCount, jraRaces] = await Promise.all([
    countRaceSourcesByDate(env.REALTIME_DB, targetDate),
    countJraRaceSourcesMissingRaceDateFieldsByDate(env.REALTIME_DB, targetDate),
    fetchJraRacesByDate(env, targetDate),
  ]);
  if (jraRaces.length === 0) {
    return;
  }
  if (d1RaceCount >= jraRaces.length && missingRaceDateFieldCount === 0) {
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

const discoverPremiumRacesForDate = async (
  env: Env,
  targetDate: string,
): Promise<{ configured: boolean; discovered: number; linked: number }> => {
  const config = getPremiumRaceConfig(env);
  if (!hasPremiumRaceFetchConfig(config) || !config.topPathTemplate) {
    return { configured: false, discovered: 0, linked: 0 };
  }
  await ensureJraRaceSourcesAreCurrent(env, targetDate);
  const topUrl = buildPremiumUrl(config, config.topPathTemplate, { date: targetDate });
  if (!topUrl) {
    return { configured: false, discovered: 0, linked: 0 };
  }
  const [html, races] = await Promise.all([
    fetchPremiumHtml(config, topUrl),
    listSchedulableRaceSourcesByDate(env.REALTIME_DB, targetDate),
  ]);
  const links = discoverPremiumRaceLinks(html, config);
  let linked = 0;
  for (const race of races.filter((item) => item.source === "jra")) {
    const link = matchPremiumLinkToRace(links, race);
    if (!link) {
      continue;
    }
    await upsertPremiumRaceLink(env.REALTIME_DB, race.raceKey, link);
    linked += 1;
  }
  return { configured: true, discovered: links.length, linked };
};

const ensurePremiumRaceLink = async (
  env: Env,
  race: NarRaceSource,
): Promise<Awaited<ReturnType<typeof getPremiumRaceLink>>> => {
  const existing = await getPremiumRaceLink(env.REALTIME_DB, race.raceKey);
  if (existing || race.source !== "jra") {
    return existing;
  }
  const targetDate = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
  await discoverPremiumRacesForDate(env, targetDate);
  return getPremiumRaceLink(env.REALTIME_DB, race.raceKey);
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

const isPremiumRaceDiscoveryTick = (date: Date): boolean => {
  const jst = toJstIsoString(date);
  return jst.slice(11, 16) === "20:00";
};

const getLatestSuccessfulRealtimePlanAt = async (env: Env): Promise<string | null> => {
  const row = await env.REALTIME_DB.prepare(
    `
      select created_at
      from fetch_logs
      where job_type in ('plan-realtime-fetches', 'plan-realtime-fetches-self')
        and status = 'ok'
      order by created_at desc
      limit 1
    `,
  ).first<{ created_at: string }>();
  return row?.created_at ?? null;
};

const getLatestQueuedSelfRealtimePlanAt = async (env: Env): Promise<string | null> => {
  const row = await env.REALTIME_DB.prepare(
    `
      select created_at
      from fetch_logs
      where job_type = 'plan-realtime-fetches-self'
        and status = 'queued'
      order by created_at desc
      limit 1
    `,
  ).first<{ created_at: string }>();
  return row?.created_at ?? null;
};

const enqueueSelfRealtimePlanIfStale = async (env: Env, date: string, now = getNow(env)) => {
  if (!isJstPollingWindow(now)) {
    return;
  }
  const latest = await getLatestSuccessfulRealtimePlanAt(env);
  if (
    latest &&
    new Date(latest).getTime() > now.getTime() - REALTIME_PLAN_SELF_SCHEDULE_STALE_SECONDS * 1000
  ) {
    return;
  }
  const latestQueued = await getLatestQueuedSelfRealtimePlanAt(env);
  if (
    latestQueued &&
    new Date(latestQueued).getTime() >
      now.getTime() - REALTIME_PLAN_SELF_SCHEDULE_DELAY_SECONDS * 1000
  ) {
    return;
  }
  await env.REALTIME_JOBS.send(
    { date, selfSchedule: true, type: "plan-realtime-fetches" },
    { delaySeconds: REALTIME_PLAN_SELF_SCHEDULE_DELAY_SECONDS },
  );
  await logFetch(env.REALTIME_DB, "plan-realtime-fetches-self", "queued", null, date);
};

const enqueueNextSelfRealtimePlan = async (env: Env, date: string, now = getNow(env)) => {
  if (!isJstPollingWindow(now)) {
    return;
  }
  await env.REALTIME_JOBS.send(
    { date, selfSchedule: true, type: "plan-realtime-fetches" },
    { delaySeconds: REALTIME_PLAN_SELF_SCHEDULE_DELAY_SECONDS },
  );
};

const runRealtimePlannerWatchdogIfStale = async (env: Env, date: string, now = getNow(env)) => {
  if (!isJstPollingWindow(now)) {
    return;
  }
  const latest = await getLatestSuccessfulRealtimePlanAt(env);
  if (
    latest &&
    new Date(latest).getTime() > now.getTime() - REALTIME_PLAN_SELF_SCHEDULE_STALE_SECONDS * 1000
  ) {
    return;
  }
  await handleJob(env, { date, selfSchedule: true, type: "plan-realtime-fetches" });
};

const seedRealtimePlannerWatchdog = (env: Env, ctx: ExecutionContext): void => {
  const now = getNow(env);
  if (!isJstPollingWindow(now)) {
    return;
  }
  ctx.waitUntil(runRealtimePlannerWatchdogIfStale(env, getTodayJst(now), now));
};

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
    const slotAt = ["0600", "0700", "0900"]
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
  const premiumDelaySeconds = Math.max(
    1,
    Number(env.PREMIUM_RACE_QUEUE_DELAY_SECONDS ?? DEFAULT_PREMIUM_RACE_QUEUE_DELAY_SECONDS),
  );
  const orderedJobs = jobs.toSorted((left, right) => {
    if (left.type === "fetch-premium-paddock" && right.type !== "fetch-premium-paddock") {
      return -1;
    }
    if (left.type !== "fetch-premium-paddock" && right.type === "fetch-premium-paddock") {
      return 1;
    }
    return 0;
  });
  let premiumJobIndex = 0;
  for (let index = 0; index < orderedJobs.length; index += QUEUE_SEND_BATCH_SIZE) {
    const chunk = orderedJobs.slice(index, index + QUEUE_SEND_BATCH_SIZE);
    if (chunk.some(isPremiumRaceJob)) {
      for (const job of chunk) {
        if (isPremiumRaceJob(job)) {
          await (env.PREMIUM_RACE_JOBS ?? env.REALTIME_JOBS).send(job, {
            delaySeconds:
              job.type === "fetch-premium-paddock"
                ? premiumJobIndex
                : premiumJobIndex * premiumDelaySeconds,
          });
          premiumJobIndex += 1;
        } else {
          await env.REALTIME_JOBS.send(job);
        }
      }
      continue;
    }
    if (chunk.length === 1) {
      await env.REALTIME_JOBS.send(chunk[0] as Job);
      continue;
    }
    await env.REALTIME_JOBS.sendBatch(chunk.map((body) => ({ body })));
  }
};

const isPremiumRaceJob = (job: Job): boolean =>
  job.type === "discover-premium-race-links" ||
  job.type === "discover-premium-races" ||
  job.type === "plan-premium-race-data-fetches" ||
  job.type === "fetch-premium-race-data" ||
  job.type === "fetch-premium-paddock";

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

const planPremiumPaddockFetchesForDate = async (
  env: Env,
  targetDate: string,
  now: Date,
): Promise<Job[]> => {
  if (!hasPremiumRaceFetchConfig(getPremiumRaceConfig(env))) {
    return [];
  }
  await ensureJraRaceSourcesAreCurrent(env, targetDate);
  const races = await listSchedulableRaceSourcesByDate(env.REALTIME_DB, targetDate);
  const jobs: Job[] = [];
  for (const race of races) {
    if (race.source !== "jra") {
      continue;
    }
    const minutes = minutesUntilRace(race, now);
    if (minutes === null || minutes > PREMIUM_PADDOCK_WINDOW_BEFORE_MINUTES) {
      continue;
    }
    const state = await getPremiumPaddockFetchState(env.REALTIME_DB, race.raceKey);
    if (minutes < -PREMIUM_PADDOCK_WINDOW_AFTER_MINUTES) {
      continue;
    }
    if (state?.retryAfter && new Date(state.retryAfter).getTime() > now.getTime()) {
      continue;
    }
    if (
      state?.lastQueuedAt &&
      new Date(state.lastQueuedAt).getTime() >
        now.getTime() - PREMIUM_PADDOCK_RECHECK_MINUTES * 60_000
    ) {
      continue;
    }
    if (
      state?.lastFetchAt &&
      new Date(state.lastFetchAt).getTime() >
        now.getTime() - PREMIUM_PADDOCK_RECHECK_MINUTES * 60_000
    ) {
      continue;
    }
    jobs.push({ raceKey: race.raceKey, type: "fetch-premium-paddock" });
  }
  return jobs;
};

const planPremiumRaceDataFetchesForDate = async (
  env: Env,
  targetDate: string,
  now: Date,
): Promise<Job[]> => {
  if (!hasPremiumRaceFetchConfig(getPremiumRaceConfig(env))) {
    return [];
  }
  const candidates = await listPremiumRaceDataFetchCandidatesByDate(
    env.REALTIME_DB,
    targetDate,
    toJstIsoString(now),
  );
  return candidates.map((candidate) => ({
    raceKey: candidate.raceKey,
    type: "fetch-premium-race-data",
  }));
};

const tryEnsureDiscoveredUrlsAreCurrent = async (env: Env, targetDate: string): Promise<void> => {
  try {
    await ensureDiscoveredUrlsAreCurrent(env, targetDate);
  } catch (error) {
    await logFetch(
      env.REALTIME_DB,
      "discover-urls",
      "error",
      null,
      error instanceof Error ? error.message : String(error),
    );
  }
};

const truncate = (value: string, maxLength: number): string =>
  value.length <= maxLength ? value : `${value.slice(0, Math.max(0, maxLength - 1))}…`;

const buildPremiumPaddockSignature = async (
  bulletins: readonly PremiumPaddockBulletin[],
): Promise<string> => {
  const signaturePayload = {
    formatVersion: PREMIUM_PADDOCK_NOTIFICATION_FORMAT_VERSION,
    rows: bulletins
      .map((row) => ({
        commentText: row.commentText ?? "",
        evaluationText: row.evaluationText ?? "",
        frameNumber: row.frameNumber ?? "",
        groupKey: row.groupKey,
        horseName: row.horseName ?? "",
        horseNumber: row.horseNumber,
      }))
      .toSorted((left, right) =>
        `${left.groupKey}:${left.horseNumber}`.localeCompare(
          `${right.groupKey}:${right.horseNumber}`,
        ),
      ),
  };
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(JSON.stringify(signaturePayload)),
  );
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
};

const formatPremiumPaddockBulletinLine = (row: PremiumPaddockBulletin): string =>
  [
    `**${row.horseNumber} 番 ${truncate(row.horseName ?? "-", 32)}**　${row.groupKey === "value" ? "穴馬" : "人気馬"} / ${row.evaluationText ?? "-"}`,
    row.commentText ? `> ${truncate(row.commentText, 140)}` : "> コメントなし",
  ].join("\n");

const buildDetailUrl = (race: NarRaceSource): string => {
  const origin = DEFAULT_DETAIL_ORIGIN;
  return `${origin}/races/${race.kaisaiNen}/${race.kaisaiTsukihi.slice(0, 2)}/${race.kaisaiTsukihi.slice(2, 4)}/${race.keibajoCode}/${race.raceBango}`;
};

const formatRaceStartForDiscord = (raceStartAtJst: string): string =>
  new Intl.DateTimeFormat("ja-JP", {
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    month: "long",
    timeZone: "Asia/Tokyo",
    year: "numeric",
  }).format(new Date(raceStartAtJst));

const formatMinutesUntilRace = (raceStartAtJst: string, now: Date): string => {
  const diffMinutes = Math.ceil((new Date(raceStartAtJst).getTime() - now.getTime()) / 60_000);
  if (diffMinutes > 0) {
    return `発走まで残り${diffMinutes}分`;
  }
  if (diffMinutes === 0) {
    return "まもなく発走";
  }
  return `発走から${Math.abs(diffMinutes)}分経過`;
};

const notifyPremiumPaddockIfNeeded = async (
  env: Env,
  race: NarRaceSource,
  bulletins: readonly PremiumPaddockBulletin[],
  fetchedAt: string,
): Promise<void> => {
  const payloadSignature = await buildPremiumPaddockSignature(bulletins);
  if (new Date(race.raceStartAtJst).getTime() <= getNow(env).getTime()) {
    await recordPremiumPaddockNotificationEvent(env.REALTIME_DB, {
      fetchedAt,
      message: "race already started",
      payloadSignature,
      raceKey: race.raceKey,
      skipReason: "race_started",
      status: "skipped_started",
    });
    await updatePremiumPaddockNotificationState(env.REALTIME_DB, {
      message: "race already started",
      payloadFetchedAt: fetchedAt,
      payloadSignature,
      raceKey: race.raceKey,
      skipReason: "race_started",
      status: "skipped_started",
    });
    return;
  }
  if (bulletins.length === 0) {
    await recordPremiumPaddockNotificationEvent(env.REALTIME_DB, {
      fetchedAt,
      message: "premium paddock rows are empty",
      payloadSignature,
      raceKey: race.raceKey,
      skipReason: "empty",
      status: "skipped_empty",
    });
    await updatePremiumPaddockNotificationState(env.REALTIME_DB, {
      message: "premium paddock rows are empty",
      payloadFetchedAt: fetchedAt,
      payloadSignature,
      raceKey: race.raceKey,
      skipReason: "empty",
      status: "skipped_empty",
    });
    return;
  }
  if (!env.PREMIUM_PADDOCK_DISCORD_WEBHOOK_URL) {
    await recordPremiumPaddockNotificationEvent(env.REALTIME_DB, {
      fetchedAt,
      message: "discord webhook is not configured",
      payloadSignature,
      raceKey: race.raceKey,
      skipReason: "webhook_not_configured",
      status: "skipped_unconfigured",
    });
    await updatePremiumPaddockNotificationState(env.REALTIME_DB, {
      message: "discord webhook is not configured",
      payloadFetchedAt: fetchedAt,
      payloadSignature,
      raceKey: race.raceKey,
      skipReason: "webhook_not_configured",
      status: "skipped_unconfigured",
    });
    return;
  }
  const currentNotification = await getPremiumPaddockNotificationState(
    env.REALTIME_DB,
    race.raceKey,
  );
  if (currentNotification?.lastNotifiedAt) {
    if (currentNotification.lastNotifiedAt !== fetchedAt) {
      await recordPremiumPaddockNotificationEvent(env.REALTIME_DB, {
        fetchedAt,
        payloadSignature,
        raceKey: race.raceKey,
        skipReason: "already_notified",
        status: "skipped_duplicate",
      });
    }
    await updatePremiumPaddockNotificationState(env.REALTIME_DB, {
      message: "premium paddock notification was already sent for this race",
      payloadFetchedAt: fetchedAt,
      payloadSignature,
      raceKey: race.raceKey,
      skipReason: "already_notified",
      status: "skipped_duplicate",
    });
    return;
  }

  const sendAttemptAt = toJstIsoString(getNow(env));
  const lockBefore = toJstIsoString(
    new Date(getNow(env).getTime() - PREMIUM_PADDOCK_NOTIFICATION_LOCK_SECONDS * 1000),
  );
  const claimed = await claimPremiumPaddockNotificationSend(env.REALTIME_DB, {
    lockBefore,
    payloadFetchedAt: fetchedAt,
    payloadSignature,
    raceKey: race.raceKey,
    sendAttemptAt,
  });
  if (!claimed) {
    return;
  }

  const raceNumberLabel = `${Number(race.raceBango)}R`;
  const raceOrderLabel = `${Number(race.raceBango)}番目`;
  const racePlace = JRA_KEIBAJO_NAMES[race.keibajoCode] ?? `競馬場 ${race.keibajoCode}`;
  const raceName = race.raceName ?? "レース名未取得";
  const startLabel = `${formatRaceStartForDiscord(race.raceStartAtJst)}発走（JST）`;
  const remainingLabel = formatMinutesUntilRace(race.raceStartAtJst, getNow(env));
  const response = await fetch(env.PREMIUM_PADDOCK_DISCORD_WEBHOOK_URL, {
    body: JSON.stringify({
      embeds: [
        {
          author: { name: "External Paddock Feed" },
          color: 0xf97316,
          description: [
            `🏟️ **${racePlace} ${raceNumberLabel}（${raceOrderLabel}のレース）**`,
            `🏷️ **${truncate(raceName, 120)}**`,
            `🕒 ${startLabel}`,
            `⏳ ${remainingLabel}`,
            `[レース詳細を開く](${buildDetailUrl(race)})`,
            "",
            truncate(
              bulletins.map(formatPremiumPaddockBulletinLine).join("\n────────────\n"),
              1400,
            ),
          ].join("\n"),
          footer: {
            text: `外部速報 ${bulletins.length}件 / 取得 ${fetchedAt}`,
          },
          timestamp: new Date().toISOString(),
          title: "🚨 外部パドック速報",
        },
      ],
      username: env.PREMIUM_PADDOCK_DISCORD_BOT_NAME ?? DEFAULT_PREMIUM_PADDOCK_DISCORD_BOT_NAME,
    }),
    headers: { "Content-Type": "application/json" },
    method: "POST",
  });

  if (!response.ok) {
    await recordPremiumPaddockNotificationEvent(env.REALTIME_DB, {
      fetchedAt,
      message: `discord webhook failed: ${response.status}`,
      payloadSignature,
      raceKey: race.raceKey,
      sentAt: sendAttemptAt,
      status: "failed",
    });
    await updatePremiumPaddockNotificationState(env.REALTIME_DB, {
      message: `discord webhook failed: ${response.status}`,
      payloadFetchedAt: fetchedAt,
      payloadSignature,
      raceKey: race.raceKey,
      sendAttemptAt,
      skipReason: null,
      status: "failed",
    });
    throw new Error(`premium paddock notification failed: ${response.status}`);
  }

  await recordPremiumPaddockNotificationEvent(env.REALTIME_DB, {
    fetchedAt,
    payloadSignature,
    raceKey: race.raceKey,
    sentAt: sendAttemptAt,
    status: "ok",
  });
  await updatePremiumPaddockNotificationState(env.REALTIME_DB, {
    message: null,
    notifiedAt: fetchedAt,
    payloadFetchedAt: fetchedAt,
    payloadSignature,
    raceKey: race.raceKey,
    sendAttemptAt,
    skipReason: null,
    status: "ok",
  });
};

const getPremiumPaddockRetryDelaySeconds = (race: NarRaceSource, now = new Date()): number => {
  const minutes = minutesUntilRace(race, now);
  if (minutes !== null && minutes <= 15 && minutes >= -PREMIUM_PADDOCK_WINDOW_AFTER_MINUTES) {
    return 30;
  }
  return PREMIUM_PADDOCK_RETRY_DELAY_SECONDS;
};

const getPremiumPaddockRetryAfter = (env: Env, race: NarRaceSource): string =>
  toJstIsoString(
    new Date(getNow(env).getTime() + getPremiumPaddockRetryDelaySeconds(race, getNow(env)) * 1000),
  );

const retryPremiumPaddockWhileInWindow = async (env: Env, race: NarRaceSource): Promise<void> => {
  const minutes = minutesUntilRace(race, getNow(env));
  if (minutes === null || minutes < -PREMIUM_PADDOCK_WINDOW_AFTER_MINUTES) {
    return;
  }
  await env.REALTIME_JOBS.send(
    { raceKey: race.raceKey, type: "fetch-premium-paddock" },
    { delaySeconds: getPremiumPaddockRetryDelaySeconds(race, getNow(env)) },
  );
};

const assertJraHorseWeightsComplete = (
  raceKey: string,
  entries: Omit<RaceEntry, "fetchedAt">[],
  weights: HorseWeight[],
): void => {
  if (weights.length === 0) {
    return;
  }
  const expectedHorseNumbers = new Set(
    entries
      .filter((entry) => !entry.status || !isJraScratchStatus(entry.status))
      .map((entry) => entry.horseNumber),
  );
  const actualHorseNumbers = new Set(weights.map((weight) => weight.horseNumber));
  const missingHorseNumbers = Array.from(expectedHorseNumbers).filter(
    (horseNumber) => !actualHorseNumbers.has(horseNumber),
  );
  if (missingHorseNumbers.length > 0) {
    throw new Error(
      `JRA horse weight rows are sparse: ${raceKey} missing=${missingHorseNumbers.join(",")}`,
    );
  }
};

const planRealtimeFetches = async (env: Env, targetDate: string): Promise<number> => {
  const now = getNow(env);
  const jobs: Job[] = [];
  jobs.push(...(await planTrackConditionFetchesForDate(env, targetDate, now)));
  jobs.push(
    ...(await planTrackConditionFetchesForDate(env, addDaysToYyyymmdd(targetDate, 1), now)),
  );
  const shouldRunGeneralPolling = isJstPollingWindow(now);
  if (shouldRunGeneralPolling) {
    await tryEnsureDiscoveredUrlsAreCurrent(env, targetDate);
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
        (race.source === "nar" || race.source === "jra") &&
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
  if (isPremiumRaceDiscoveryTick(now)) {
    jobs.push({ date: addDaysToYyyymmdd(targetDate, 1), type: "discover-premium-races" });
  }
  jobs.push(...(await planPremiumRaceDataFetchesForDate(env, targetDate, now)));
  jobs.push(
    ...(await planPremiumRaceDataFetchesForDate(env, addDaysToYyyymmdd(targetDate, 1), now)),
  );
  jobs.push(...(await planPremiumPaddockFetchesForDate(env, targetDate, now)));
  jobs.push(
    ...(await planPremiumPaddockFetchesForDate(env, addDaysToYyyymmdd(targetDate, 1), now)),
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
  await markPremiumPaddockQueued(
    env.REALTIME_DB,
    jobs.flatMap((job) => (job.type === "fetch-premium-paddock" ? [job.raceKey] : [])),
    queuedAt,
  );
  await markPremiumRaceDataQueued(
    env.REALTIME_DB,
    jobs.flatMap((job) => (job.type === "fetch-premium-race-data" ? [job.raceKey] : [])),
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
    const raceStart = getRaceStart(race);
    const oddsSlotAt = getCurrentOddsSlotAt(race, now);
    if (!oddsSlotAt || !isSlotDue(race.lastOddsFetchAt, oddsSlotAt)) {
      await failOddsFetch(env.REALTIME_DB, raceKey);
      return;
    }
    const fetchedAt = toJstIsoString();
    let latest;
    if (race.source === "jra") {
      const result = await fetchJraOddsWithPlaywright(env.JRA_BROWSER, race.debaUrl);
      latest = result.latest;
      await insertRaceEntrySnapshot(
        env.REALTIME_DB,
        raceKey,
        fetchedAt,
        sanitizeJraRaceEntriesWithOdds(parseJraRaceEntries(result.entryHtml), latest),
      );
      const weights = parseJraHorseWeights(result.entryHtml);
      if (weights.length > 0) {
        assertJraHorseWeightsComplete(raceKey, parseJraRaceEntries(result.entryHtml), weights);
        await insertHorseWeightSnapshot(env.REALTIME_DB, raceKey, fetchedAt, weights);
        await updateLastFetch(env.REALTIME_DB, raceKey, "last_weight_fetch_at", fetchedAt);
      }
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
    const nextSlotAt = raceStart ? getNextOddsFetchSlotAt(raceStart, now, race.source) : null;
    if (nextSlotAt) {
      const delaySeconds = Math.max(
        1,
        Math.ceil((new Date(nextSlotAt).getTime() - now.getTime()) / 1000),
      );
      await env.REALTIME_JOBS.send({ raceKey, type: "fetch-odds" }, { delaySeconds });
      await markOddsFetchQueued(env.REALTIME_DB, [raceKey], nextSlotAt);
    }
    const [history, historyByType] = await Promise.all([
      listTanshoHistory(env.REALTIME_DB, raceKey),
      listOddsHistoryByType(env.REALTIME_DB, raceKey),
    ]);
    await writeCachedOdds(env, raceKey, { fetchedAt, history, historyByType, latest });
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
  const latestOdds =
    race.source === "jra" ? await getLatestOddsFromD1(env.REALTIME_DB, raceKey) : null;
  const entries =
    race.source === "jra"
      ? sanitizeJraRaceEntriesWithOdds(parseJraRaceEntries(html), latestOdds?.latest)
      : parseRaceEntries(html);
  await insertRaceEntrySnapshot(env.REALTIME_DB, raceKey, fetchedAt, entries);
  let weights = race.source === "jra" ? parseJraHorseWeights(html) : parseHorseWeights(html);
  if (race.source === "nar" && weights.length === 0) {
    const resultHtml = await fetchRacePage(buildRaceResultUrl(race.debaUrl));
    weights = parseRaceResultHorseWeights(resultHtml);
  }
  if (race.source === "jra") {
    assertJraHorseWeightsComplete(raceKey, entries, weights);
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
  if (!isRaceFinished(race, now)) {
    await failResultFetch(env.REALTIME_DB, raceKey);
    return;
  }

  try {
    const fetchedAt = toJstIsoString();
    const resultUrl =
      race.source === "jra"
        ? buildJraResultUrlFromRaceSource(race)
        : buildRaceResultUrl(race.debaUrl);
    if (!resultUrl) {
      throw new Error(`race result url is unavailable: ${raceKey}`);
    }
    const [entryHtml, resultHtml] = await Promise.all([
      race.source === "jra"
        ? fetchJraResultHtmlWithPlaywright(env.JRA_BROWSER, race.debaUrl)
        : fetchRacePage(race.debaUrl),
      race.source === "jra"
        ? fetchJraResultHtmlWithPlaywright(env.JRA_BROWSER, resultUrl)
        : fetchRacePage(resultUrl),
    ]);
    const entries =
      race.source === "jra"
        ? sanitizeJraRaceEntriesWithOdds(parseJraRaceEntries(entryHtml), null)
        : parseRaceEntries(entryHtml);
    await insertRaceEntrySnapshot(env.REALTIME_DB, raceKey, fetchedAt, entries);
    const entryHorseNumbers =
      race.source === "jra"
        ? entries.map((entry) => entry.horseNumber)
        : parseRaceEntryHorseNumbers(entryHtml);
    const excludedHorseNumbers = new Set(
      race.source === "jra"
        ? [
            ...entries
              .filter((entry) => entry.status && isJraScratchStatus(entry.status))
              .map((entry) => entry.horseNumber),
            ...parseJraRaceResultExcludedHorseNumbers(resultHtml),
          ]
        : parseRaceResultExcludedHorseNumbers(resultHtml),
    );
    const expectedHorseCount = entryHorseNumbers.filter(
      (horseNumber) => !excludedHorseNumbers.has(horseNumber),
    ).length;
    const results =
      race.source === "jra" ? parseJraRaceResults(resultHtml) : parseRaceResults(resultHtml);
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

const fetchAndStorePremiumRaceData = async (env: Env, raceKey: string): Promise<void> => {
  const race = await getRaceSource(env.REALTIME_DB, raceKey);
  if (!race || race.source !== "jra") {
    return;
  }
  const config = getPremiumRaceConfig(env);
  if (!hasPremiumRaceFetchConfig(config)) {
    return;
  }
  const link = await ensurePremiumRaceLink(env, race);
  if (!link) {
    throw new Error(`premium race link not found: ${raceKey}`);
  }
  const [workUrl, commentUrl] = [
    buildPremiumUrl(config, config.workPathTemplate, { sourceRaceId: link.sourceRaceId }),
    buildPremiumUrl(config, config.commentPathTemplate, { sourceRaceId: link.sourceRaceId }),
  ];
  const fetchedAt = toJstIsoString();
  const [workResult, commentResult] = await Promise.allSettled([
    workUrl ? fetchPremiumHtml(config, workUrl) : Promise.resolve(""),
    commentUrl ? fetchPremiumHtml(config, commentUrl) : Promise.resolve(""),
  ]);
  const workHtml = workResult.status === "fulfilled" ? workResult.value : "";
  const commentHtml = commentResult.status === "fulfilled" ? commentResult.value : "";
  if (!workHtml && !commentHtml) {
    const retryAfter = toJstIsoString(
      new Date(getNow(env).getTime() + PREMIUM_RACE_DATA_RETRY_DELAY_SECONDS * 1000),
    );
    await updatePremiumRaceDataFetchState(env.REALTIME_DB, {
      message: [workResult, commentResult]
        .flatMap((result) =>
          result.status === "rejected"
            ? [result.reason instanceof Error ? result.reason.message : String(result.reason)]
            : [],
        )
        .join("; "),
      raceKey,
      retryAfter,
      status: "failed",
    });
    throw new Error(`premium race data fetch failed: ${raceKey}`);
  }
  const trainingReviews = workHtml ? parsePremiumTrainingReviews(workHtml, env) : undefined;
  const stableComments = commentHtml ? parsePremiumStableComments(commentHtml, env) : undefined;
  await replacePremiumRaceData(env.REALTIME_DB, {
    fetchedAt,
    link,
    raceKey,
    stableComments,
    trainingReviews,
  });
  await updatePremiumRaceDataFetchState(env.REALTIME_DB, {
    fetchedAt,
    message: JSON.stringify({
      commentError:
        commentResult.status === "rejected"
          ? commentResult.reason instanceof Error
            ? commentResult.reason.message
            : String(commentResult.reason)
          : null,
      commentHtmlLength: commentHtml.length,
      stableCommentCount: stableComments?.length ?? null,
      stableCommentSample:
        commentHtml && (stableComments?.length ?? 0) === 0
          ? summarizePremiumStableCommentHtml(commentHtml)
          : null,
      trainingReviewCount: trainingReviews?.length ?? null,
      workError:
        workResult.status === "rejected"
          ? workResult.reason instanceof Error
            ? workResult.reason.message
            : String(workResult.reason)
          : null,
      workHtmlLength: workHtml.length,
    }),
    raceKey,
    status:
      (trainingReviews?.length ?? 0) > 0 || (stableComments?.length ?? 0) > 0 ? "ok" : "empty",
  });
};

const fetchAndStorePremiumPaddock = async (env: Env, raceKey: string): Promise<void> => {
  const race = await getRaceSource(env.REALTIME_DB, raceKey);
  if (!race || race.source !== "jra") {
    return;
  }
  const currentState = await getPremiumPaddockFetchState(env.REALTIME_DB, raceKey);
  if (
    currentState?.retryAfter &&
    new Date(currentState.retryAfter).getTime() > getNow(env).getTime()
  ) {
    return;
  }
  const config = getPremiumRaceConfig(env);
  if (!hasPremiumRaceFetchConfig(config)) {
    return;
  }
  const link = await ensurePremiumRaceLink(env, race);
  if (!link) {
    throw new Error(`premium race link not found: ${raceKey}`);
  }
  const paddockUrl = buildPremiumUrl(config, config.paddockPathTemplate, {
    sourceRaceId: link.sourceRaceId,
  });
  if (!paddockUrl) {
    return;
  }
  let attempts: Awaited<ReturnType<typeof fetchPremiumHtmlAttempts>>;
  try {
    attempts = await fetchPremiumHtmlAttempts(config, paddockUrl);
  } catch (error: unknown) {
    const existingPayload = await getPremiumRacePayload(env.REALTIME_DB, raceKey).catch(() => null);
    if (existingPayload && existingPayload.paddockBulletins.length > 0) {
      const latestFetchedAt = existingPayload.paddockBulletins.reduce<string | null>(
        (latest, row) => (latest && latest > row.fetchedAt ? latest : row.fetchedAt),
        null,
      );
      await updatePremiumPaddockFetchState(env.REALTIME_DB, {
        fetchedAt: latestFetchedAt,
        message: null,
        raceKey,
        status: "ok",
      });
      return;
    }
    const retryAfter = getPremiumPaddockRetryAfter(env, race);
    await updatePremiumPaddockFetchState(env.REALTIME_DB, {
      message: error instanceof Error ? error.message : String(error),
      raceKey,
      retryAfter,
      status: "failed",
    });
    throw error;
  }
  const parsedAttempts = attempts.map((attempt) => ({
    mode: attempt.mode,
    parsed: parsePremiumPaddockBulletins(attempt.html, env),
  }));
  const selectedAttempt =
    parsedAttempts.find((attempt) => attempt.parsed.bulletins.length > 0) ??
    parsedAttempts.find((attempt) => attempt.mode === "proxy" && attempt.parsed.authRequired) ??
    parsedAttempts.find((attempt) => attempt.parsed.pending) ??
    parsedAttempts[0];
  if (!selectedAttempt) {
    throw new Error(`premium paddock fetch returned no attempts: ${raceKey}`);
  }
  const parsed = selectedAttempt.parsed;
  const fetchedAt = toJstIsoString();
  if (parsed.authRequired) {
    await clearCachedPremiumPaddock(env, raceKey);
    const retryAfter = getPremiumPaddockRetryAfter(env, race);
    const payloadSignature = await buildPremiumPaddockSignature([]);
    await updatePremiumPaddockFetchState(env.REALTIME_DB, {
      fetchedAt,
      message: `auth_required:${selectedAttempt.mode}`,
      raceKey,
      retryAfter,
      status: "auth_required",
    });
    await recordPremiumPaddockNotificationEvent(env.REALTIME_DB, {
      fetchedAt,
      message: `premium paddock auth required: ${selectedAttempt.mode}`,
      payloadSignature,
      raceKey,
      skipReason: "auth_required",
      status: "skipped_auth_required",
    });
    await updatePremiumPaddockNotificationState(env.REALTIME_DB, {
      message: `premium paddock auth required: ${selectedAttempt.mode}`,
      payloadFetchedAt: fetchedAt,
      payloadSignature,
      raceKey,
      skipReason: "auth_required",
      status: "skipped_auth_required",
    });
    await retryPremiumPaddockWhileInWindow(env, race);
    return;
  }
  if (parsed.unavailable) {
    await clearCachedPremiumPaddock(env, raceKey);
    const retryAfter = getPremiumPaddockRetryAfter(env, race);
    const payloadSignature = await buildPremiumPaddockSignature([]);
    await updatePremiumPaddockFetchState(env.REALTIME_DB, {
      fetchedAt,
      message: `unavailable:${selectedAttempt.mode}`,
      raceKey,
      retryAfter,
      status: "unavailable",
    });
    await recordPremiumPaddockNotificationEvent(env.REALTIME_DB, {
      fetchedAt,
      message: `premium paddock is unavailable: ${selectedAttempt.mode}`,
      payloadSignature,
      raceKey,
      skipReason: "unavailable",
      status: "skipped_unavailable",
    });
    await updatePremiumPaddockNotificationState(env.REALTIME_DB, {
      message: `premium paddock is unavailable: ${selectedAttempt.mode}`,
      payloadFetchedAt: fetchedAt,
      payloadSignature,
      raceKey,
      skipReason: "unavailable",
      status: "skipped_unavailable",
    });
    await retryPremiumPaddockWhileInWindow(env, race);
    return;
  }
  if (parsed.pending) {
    await clearCachedPremiumPaddock(env, raceKey);
    const retryAfter = getPremiumPaddockRetryAfter(env, race);
    const payloadSignature = await buildPremiumPaddockSignature([]);
    await updatePremiumPaddockFetchState(env.REALTIME_DB, {
      fetchedAt,
      message: `pending:${selectedAttempt.mode}`,
      raceKey,
      retryAfter,
      status: "pending",
    });
    await recordPremiumPaddockNotificationEvent(env.REALTIME_DB, {
      fetchedAt,
      message: `premium paddock is pending: ${selectedAttempt.mode}`,
      payloadSignature,
      raceKey,
      skipReason: "pending",
      status: "skipped_pending",
    });
    await updatePremiumPaddockNotificationState(env.REALTIME_DB, {
      message: `premium paddock is pending: ${selectedAttempt.mode}`,
      payloadFetchedAt: fetchedAt,
      payloadSignature,
      raceKey,
      skipReason: "pending",
      status: "skipped_pending",
    });
    await retryPremiumPaddockWhileInWindow(env, race);
    return;
  }
  if (parsed.bulletins.length === 0) {
    await clearCachedPremiumPaddock(env, raceKey);
    const retryAfter = getPremiumPaddockRetryAfter(env, race);
    const payloadSignature = await buildPremiumPaddockSignature([]);
    await updatePremiumPaddockFetchState(env.REALTIME_DB, {
      fetchedAt,
      message: `empty:${selectedAttempt.mode}`,
      raceKey,
      retryAfter,
      status: "empty",
    });
    await recordPremiumPaddockNotificationEvent(env.REALTIME_DB, {
      fetchedAt,
      message: `premium paddock rows are empty: ${selectedAttempt.mode}`,
      payloadSignature,
      raceKey,
      skipReason: "empty",
      status: "skipped_empty",
    });
    await updatePremiumPaddockNotificationState(env.REALTIME_DB, {
      message: `premium paddock rows are empty: ${selectedAttempt.mode}`,
      payloadFetchedAt: fetchedAt,
      payloadSignature,
      raceKey,
      skipReason: "empty",
      status: "skipped_empty",
    });
    await retryPremiumPaddockWhileInWindow(env, race);
    return;
  }
  await replacePremiumRaceData(env.REALTIME_DB, {
    fetchedAt,
    link,
    paddockBulletins: parsed.bulletins,
    raceKey,
  });
  const payload = await getPremiumRacePayload(env.REALTIME_DB, raceKey);
  await writeCachedPremiumPaddock(env, raceKey, {
    fetchedAt,
    paddockBulletins: payload.paddockBulletins,
  });
  await updatePremiumPaddockFetchState(env.REALTIME_DB, {
    fetchedAt,
    message: null,
    raceKey,
    status: parsed.bulletins.length > 0 ? "ok" : "empty",
  });
  await notifyPremiumPaddockIfNeeded(env, race, parsed.bulletins, fetchedAt);
};

export const handleJob = async (env: Env, job: Job): Promise<void> => {
  try {
    if (job.type === "discover-urls") {
      const [result, premiumResult] = await Promise.all([
        upsertDiscoveredUrls(env, job.date),
        discoverPremiumRacesForDate(env, job.date),
      ]);
      const races = await listSchedulableRaceSourcesByDate(env.REALTIME_DB, job.date);
      await enqueueJobs(
        env,
        races
          .filter((race) => race.source === "jra")
          .map((race) => ({ raceKey: race.raceKey, type: "fetch-premium-race-data" })),
      );
      await logFetch(env.REALTIME_DB, job.type, "ok", null, JSON.stringify(result));
      await logFetch(
        env.REALTIME_DB,
        "discover-premium-races",
        "ok",
        null,
        JSON.stringify(premiumResult),
      );
      return;
    }
    if (job.type === "plan-realtime-fetches") {
      const count = await planRealtimeFetches(env, job.date);
      await logFetch(env.REALTIME_DB, job.type, "ok", null, `${count} jobs queued`);
      if (job.selfSchedule) {
        await logFetch(
          env.REALTIME_DB,
          "plan-realtime-fetches-self",
          "ok",
          null,
          `${count} jobs queued`,
        );
        await enqueueNextSelfRealtimePlan(env, job.date);
      }
      return;
    }
    if (job.type === "discover-premium-races") {
      const result = await discoverPremiumRacesForDate(env, job.date);
      const races = await listSchedulableRaceSourcesByDate(env.REALTIME_DB, job.date);
      await enqueueJobs(
        env,
        races
          .filter((race) => race.source === "jra")
          .map((race) => ({ raceKey: race.raceKey, type: "fetch-premium-race-data" })),
      );
      await logFetch(env.REALTIME_DB, job.type, "ok", null, JSON.stringify(result));
      return;
    }
    if (job.type === "discover-premium-race-links") {
      const result = await discoverPremiumRacesForDate(env, job.date);
      await logFetch(env.REALTIME_DB, job.type, "ok", null, JSON.stringify(result));
      return;
    }
    if (job.type === "plan-premium-race-data-fetches") {
      const premiumResult = await discoverPremiumRacesForDate(env, job.date);
      const jobs = await planPremiumRaceDataFetchesForDate(env, job.date, getNow(env));
      await enqueueJobs(env, jobs);
      await markPremiumRaceDataQueued(
        env.REALTIME_DB,
        jobs.flatMap((queuedJob) =>
          queuedJob.type === "fetch-premium-race-data" ? [queuedJob.raceKey] : [],
        ),
        toJstIsoString(getNow(env)),
      );
      await logFetch(
        env.REALTIME_DB,
        job.type,
        "ok",
        null,
        JSON.stringify({ premiumResult, queued: jobs.length }),
      );
      return;
    }
    if (job.type === "fetch-premium-race-data") {
      await fetchAndStorePremiumRaceData(env, job.raceKey);
      await logFetch(env.REALTIME_DB, job.type, "ok", job.raceKey, null);
      return;
    }
    if (job.type === "fetch-premium-paddock") {
      await fetchAndStorePremiumPaddock(env, job.raceKey);
      await logFetch(env.REALTIME_DB, job.type, "ok", job.raceKey, null);
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

const premiumRaceKeyFromRequest = (url: URL): string | null => {
  const match = url.pathname.match(
    /^\/api\/(jra|nar)\/races\/(\d{4})\/(\d{2})\/(\d{2})\/([0-9A-Z]{2})\/(\d{2})\/premium$/u,
  );
  if (!match?.[1] || !match[2] || !match[3] || !match[4] || !match[5] || !match[6]) {
    return null;
  }
  return buildRealtimeRaceKey(
    match[1] as RealtimeSource,
    match[2],
    `${match[3]}${match[4]}`,
    match[5],
    match[6],
  );
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
  async fetch(request, env, ctx): Promise<Response> {
    const url = new URL(request.url);
    if (request.method !== "OPTIONS") {
      seedRealtimePlannerWatchdog(env, ctx);
    }
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
      await enqueueJobs(env, [job]);
      return json({ ok: true });
    }

    const premiumRaceKey = premiumRaceKeyFromRequest(url);
    if (premiumRaceKey && request.method === "GET") {
      const [payload, cachedPaddock] = await Promise.all([
        getPremiumRacePayload(env.REALTIME_DB, premiumRaceKey),
        readCachedPremiumPaddock(env, premiumRaceKey),
      ]);
      return json(
        payload.paddockBulletins.length > 0 && cachedPaddock && typeof cachedPaddock === "object"
          ? { ...payload, ...cachedPaddock }
          : payload,
        {
          headers: {
            "cache-control": `public, max-age=${Number(env.REALTIME_API_CACHE_SECONDS ?? "20")}`,
          },
        },
      );
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
      if (payload.odds?.historyByType && !payload.odds.trendsByType) {
        payload.odds.trendsByType = toOddsTrendsByType(payload.odds.historyByType);
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
    const scheduledAt =
      typeof controller.scheduledTime === "number"
        ? new Date(controller.scheduledTime)
        : new Date();
    if (controller.cron === RUNNING_STYLE_INFERENCE_CRON) {
      ctx.waitUntil(runRunningStyleCronTick(env, scheduledAt).then(() => undefined));
      return;
    }
    if (controller.cron === "*/15 * * * *") {
      ctx.waitUntil(runFinishPositionLiteCronTick(env, scheduledAt).then(() => undefined));
      return;
    }
    const job = getCronJob(controller.cron, scheduledAt);
    ctx.waitUntil(handleJob(env, job));
    if (job.type === "plan-realtime-fetches") {
      ctx.waitUntil(enqueueSelfRealtimePlanIfStale(env, job.date));
    }
  },

  async queue(batch, env): Promise<void> {
    for (const message of batch.messages) {
      const body = message.body as { type?: string };
      if (body.type === "finish-position-lite-infer") {
        try {
          await handleFinishPositionLiteJob(env, message.body as never);
          message.ack();
        } catch {
          message.retry({ delaySeconds: QUEUE_RETRY_DELAY_SECONDS });
        }
        continue;
      }
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
