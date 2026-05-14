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
  parseHorseWeights,
  parseRaceResults,
  parseRaceResultHorseWeights,
} from "./keiba-go";
import { mergeJsonHeaders } from "./http";
import { readCachedOdds, writeCachedOdds } from "./odds-cache";
import { fetchNarRacesByDate } from "./postgres";
import {
  buildRealtimePayload,
  claimOddsFetch,
  claimResultFetch,
  completeOddsFetch,
  completeResultFetch,
  countRaceSourcesByDate,
  failOddsFetch,
  failResultFetch,
  getRaceSource,
  getLatestOddsFromD1,
  insertRaceResultSnapshot,
  insertHorseWeightSnapshot,
  insertOddsSnapshot,
  listSchedulableRaceSourcesByDate,
  listTanshoHistory,
  logFetch,
  markOddsFetchQueued,
  markResultFetchQueued,
  toHorseTrends,
  updateLastFetch,
  updateOddsLinks,
  upsertNarRaceSource,
} from "./storage";
import {
  getOddsFetchSlotAt,
  getTodayJst,
  isJstPollingWindow,
  parseRaceStartJst,
  toJstIsoString,
} from "./time";
import type { Env, Job, NarRaceSource, OddsType } from "./types";

const QUEUE_SEND_BATCH_SIZE = 100;
const ODDS_FETCH_LOCK_MINUTES = 10;
const RESULT_FETCH_LOCK_MINUTES = 10;
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

const getCronJob = (cron: string): Job => {
  if (cron === "5 0 * * *") {
    return { date: getTodayJst(), type: "discover-urls" };
  }
  return { date: getTodayJst(), type: "plan-realtime-fetches" };
};

const upsertDiscoveredUrls = async (
  env: Env,
  targetDate: string,
): Promise<{
  fallbackRaceListCount: number;
  localRaceCount: number;
  topRaceListCount: number;
  upserted: number;
}> => {
  const [raceListUrls, localRaces] = await Promise.all([
    fetchTodayRaceListUrls(targetDate),
    fetchNarRacesByDate(env, targetDate),
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
      const race = localRaceMap.get(raceKey);
      if (!race) {
        continue;
      }
      const racePageHtml = await fetchRacePage(link.url);
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
    localRaceCount: localRaces.length,
    topRaceListCount: raceListUrls.length,
    upserted,
  };
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

const isRaceFinished = (race: NarRaceSource, now: Date): boolean => {
  const minutes = minutesUntilRace(race, now);
  return minutes !== null && minutes <= 0;
};

const ensureDiscoveredUrlsAreCurrent = async (
  env: Env,
  targetDate: string,
): Promise<void> => {
  const [d1RaceCount, localRaces] = await Promise.all([
    countRaceSourcesByDate(env.REALTIME_DB, targetDate),
    fetchNarRacesByDate(env, targetDate),
  ]);
  if (d1RaceCount >= localRaces.length) {
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

const planRealtimeFetches = async (env: Env, targetDate: string): Promise<number> => {
  const now = getNow(env);
  if (!isJstPollingWindow(now)) {
    return 0;
  }
  await ensureDiscoveredUrlsAreCurrent(env, targetDate);
  const races = await listSchedulableRaceSourcesByDate(env.REALTIME_DB, targetDate);
  if (races.length === 0) {
    return 0;
  }

  const jobs: Job[] = [];
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
      !race.lastResultFetchAt &&
      (Number.isNaN(resultLockUntil) || resultLockUntil <= now.getTime()) &&
      !race.lastResultQueuedAt
    ) {
      jobs.push({ raceKey: race.raceKey, type: "fetch-results" });
    }
  }
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
  return jobs.length;
};

const ensureOddsLinks = async (
  env: Env,
  race: NarRaceSource,
): Promise<Partial<Record<OddsType, string>>> => {
  if (Object.keys(race.oddsLinks).length > 0) {
    return race.oddsLinks;
  }
  const html = await fetchRacePage(race.debaUrl);
  const oddsLinks = extractOddsLinks(html, race.debaUrl);
  await updateOddsLinks(env.REALTIME_DB, race.raceKey, oddsLinks);
  return oddsLinks;
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
    const oddsLinks = await ensureOddsLinks(env, race);
    const latest = await fetchOdds(race.debaUrl, oddsLinks);
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
  let weights = parseHorseWeights(html);
  if (weights.length === 0) {
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
  if (!isRaceFinished(race, now)) {
    await failResultFetch(env.REALTIME_DB, raceKey);
    return;
  }

  try {
    const fetchedAt = toJstIsoString();
    const html = await fetchRacePage(buildRaceResultUrl(race.debaUrl));
    const results = parseRaceResults(html);
    if (results.length > 0 && results.length < 2) {
      await failResultFetch(env.REALTIME_DB, raceKey);
      throw new Error(`race result rows are unexpectedly sparse: ${results.length}`);
    }
    const inserted = await insertRaceResultSnapshot(env.REALTIME_DB, raceKey, fetchedAt, results);
    if (inserted === 0) {
      await failResultFetch(env.REALTIME_DB, raceKey);
      return;
    }
    await completeResultFetch(env.REALTIME_DB, raceKey, fetchedAt);
  } catch (error) {
    await failResultFetch(env.REALTIME_DB, raceKey);
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
  const match = url.pathname.match(
    /^\/api\/nar\/races\/(\d{4})\/(\d{2})\/(\d{2})\/([0-9A-Z]{2})\/(\d{2})\/realtime$/u,
  );
  if (!match?.[1] || !match[2] || !match[3] || !match[4] || !match[5]) {
    return null;
  }
  return buildRaceKey(match[1], `${match[2]}${match[3]}`, match[4], match[5]);
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
      const [source, cachedOdds] = await Promise.all([
        getRaceSource(env.REALTIME_DB, raceKey),
        readCachedOdds(env, raceKey),
      ]);
      const odds = cachedOdds ?? (await getLatestOddsFromD1(env.REALTIME_DB, raceKey));
      const payload = await buildRealtimePayload(env.REALTIME_DB, raceKey, source, odds);
      if (payload.odds && payload.odds.horseTrends.length === 0) {
        payload.odds.horseTrends = toHorseTrends(payload.odds.history);
      }
      return json(payload, {
        headers: {
          "cache-control": `public, max-age=${Number(env.REALTIME_API_CACHE_SECONDS ?? "20")}`,
        },
      });
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
