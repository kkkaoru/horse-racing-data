import {
  BABA_CODE_TO_LOCAL_KEIBAJO,
  buildRaceListUrl,
  buildRaceKey,
  extractOddsLinks,
  fetchOdds,
  fetchRaceLinksFromRaceList,
  fetchRacePage,
  fetchTodayRaceListUrls,
  parseHorseWeights,
} from "./keiba-go";
import { mergeJsonHeaders } from "./http";
import { readCachedOdds, writeCachedOdds } from "./odds-cache";
import { fetchNarRacesByDate } from "./postgres";
import {
  buildRealtimePayload,
  getRaceSource,
  getLatestOddsFromD1,
  insertHorseWeightSnapshot,
  insertOddsSnapshot,
  listFutureRaceSources,
  listTanshoHistory,
  logFetch,
  toHorseTrends,
  updateLastFetch,
  updateOddsLinks,
  upsertNarRaceSource,
} from "./storage";
import { getTodayJst, isJstPollingWindow, parseRaceStartJst, toJstIsoString } from "./time";
import type { Env, Job, NarRaceSource, OddsType } from "./types";

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

const minutesUntilRace = (race: NarRaceSource, now = new Date()): number | null => {
  const raceStart = parseRaceStartJst(
    race.kaisaiNen,
    race.kaisaiTsukihi,
    race.raceStartAtJst.slice(11, 16).replace(":", ""),
  );
  if (!raceStart) {
    return null;
  }
  return (raceStart.getTime() - now.getTime()) / 60_000;
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

const planRealtimeFetches = async (env: Env, targetDate: string): Promise<number> => {
  if (!isJstPollingWindow()) {
    return 0;
  }
  const races = await listFutureRaceSources(env.REALTIME_DB, targetDate, toJstIsoString());
  if (races.length === 0) {
    return 0;
  }

  let queued = 0;
  for (const race of races) {
    const minutes = minutesUntilRace(race);
    if (minutes === null || minutes <= 0) {
      continue;
    }

    const oddsInterval = minutes <= 30 ? 3 : 10;
    if (isDue(race.lastOddsFetchAt, oddsInterval)) {
      await env.REALTIME_JOBS.send({ raceKey: race.raceKey, type: "fetch-odds" });
      queued += 1;
    }

    if (minutes <= 20 && isDue(race.lastWeightFetchAt, 24 * 60)) {
      await env.REALTIME_JOBS.send({ raceKey: race.raceKey, type: "fetch-weights" });
      queued += 1;
    }
  }
  return queued;
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
  const race = await getRaceSource(env.REALTIME_DB, raceKey);
  if (!race) {
    throw new Error(`race source not found: ${raceKey}`);
  }
  const fetchedAt = toJstIsoString();
  const oddsLinks = await ensureOddsLinks(env, race);
  const latest = await fetchOdds(race.debaUrl, oddsLinks);
  await insertOddsSnapshot(env.REALTIME_DB, raceKey, fetchedAt, latest);
  await updateLastFetch(env.REALTIME_DB, raceKey, "last_odds_fetch_at", fetchedAt);
  const history = await listTanshoHistory(env.REALTIME_DB, raceKey);
  await writeCachedOdds(env, raceKey, { fetchedAt, history, latest });
};

const fetchAndStoreWeights = async (env: Env, raceKey: string): Promise<void> => {
  const race = await getRaceSource(env.REALTIME_DB, raceKey);
  if (!race) {
    throw new Error(`race source not found: ${raceKey}`);
  }
  const fetchedAt = toJstIsoString();
  const html = await fetchRacePage(race.debaUrl);
  const weights = parseHorseWeights(html);
  if (weights.length > 0 && weights.length < 2) {
    await insertHorseWeightSnapshot(env.REALTIME_DB, raceKey, fetchedAt, []);
    throw new Error(`horse weight rows are unexpectedly sparse: ${weights.length}`);
  }
  await insertHorseWeightSnapshot(env.REALTIME_DB, raceKey, fetchedAt, weights);
  await updateLastFetch(env.REALTIME_DB, raceKey, "last_weight_fetch_at", fetchedAt);
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
      await handleJob(env, message.body);
      message.ack();
    }
  },
} satisfies ExportedHandler<Env, Job>;
