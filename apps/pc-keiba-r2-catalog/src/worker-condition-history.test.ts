import { expect, it } from "vitest";

import { cacheRequestFor, conditionHistoryStatsDescriptor, kvKeyFor } from "./cache";
import type { CacheStore, Env, Fetcher, KvStore, WorkerDependencies } from "./types";
import { handleRequest } from "./worker";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const conditionUrl =
  "https://catalog.test/v1/condition-history-stats?year=2026&month=08&day=22&keibajoCode=07&raceNumber=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=1";

const queryKind = (query: string): string => {
  if (query.includes("career_history")) return "correlation-career";
  if (query.includes("AS top3_count")) return "correlation-target";
  if (query.includes("AS horse_name") && query.includes("AS odds")) return "correlation-entries";
  if (query.includes("AS body_weight")) return "weight";
  if (query.includes("AS carried_weight")) return "carried";
  if (query.includes("IN (1, 2, 3, 4, 5)")) return "finish";
  if (query.includes("AS target_race_date")) return "target-races";
  if (query.includes("AS race_count")) return "race-time";
  if (query.includes("AS frame_number")) return "frame";
  return "other";
};

const createConditionHarness = () => {
  const cacheEntries = new Map<string, Response>();
  const kvEntries = new Map<string, string>();
  const fetchCalls: string[] = [];
  const inflight = { current: 0, max: 0 };
  const cache: CacheStore = {
    async delete(request) {
      return cacheEntries.delete(request.url);
    },
    async match(request) {
      return cacheEntries.get(request.url)?.clone();
    },
    async put(request, response) {
      cacheEntries.set(request.url, response.clone());
    },
  };
  const kv: KvStore = {
    async delete(key) {
      kvEntries.delete(key);
    },
    async get(key) {
      return kvEntries.get(key) ?? null;
    },
    async put(key, value) {
      kvEntries.set(key, value);
    },
  };
  const fetchImpl: Fetcher = async (_input, init) => {
    inflight.current += 1;
    inflight.max = inflight.current > inflight.max ? inflight.current : inflight.max;
    await Promise.resolve();
    const rawBody = init?.body;
    const text = typeof rawBody === "string" ? rawBody : "";
    const parsed: unknown = JSON.parse(text);
    const query = isRecord(parsed) && typeof parsed.query === "string" ? parsed.query : "";
    const kind = queryKind(query);
    fetchCalls.push(kind);
    inflight.current -= 1;
    if (kind === "frame") {
      return Response.json({
        result: {
          rows: [
            {
              average_finish: 2,
              average_popularity: 3,
              count: 10,
              frame_number: "1",
              median_finish: 2,
              median_popularity: 3,
              quinella_count: 4,
              runner_count: 16,
              show_count: 5,
              win_count: 2,
            },
          ],
        },
        success: true,
      });
    }
    if (kind === "weight") {
      return Response.json({
        result: {
          rows: [
            {
              class_key: "480-499",
              quinella_count: 1,
              show_count: 2,
              starts: 4,
              win_count: 1,
            },
          ],
        },
        success: true,
      });
    }
    if (kind === "carried") {
      return Response.json({
        result: {
          rows: [
            {
              class_key: "55.5-57",
              quinella_count: 1,
              show_count: 1,
              starts: 2,
              win_count: 0,
            },
          ],
        },
        success: true,
      });
    }
    if (kind === "finish") {
      return Response.json({
        result: {
          rows: [
            {
              bamei: "イクイノックス",
              finish_position: 1,
              jockey_name: "ルメール",
              kakutei_chakujun: "01",
              keibajo_code: "05",
              odds: 3.2,
              popularity: "02",
              popularity_number: 2,
              race_bango: "08",
              race_name: "天皇賞",
              soha_time: "1450",
              target_race_date: "20241027",
              umaban: "05",
              wakuban: "03",
              win_odds: "32",
            },
          ],
        },
        success: true,
      });
    }
    if (kind === "correlation-target") {
      return Response.json({
        result: {
          rows: [{ average_odds: 6.5, average_popularity: 3.2, top3_count: 30, win_count: 10 }],
        },
        success: true,
      });
    }
    if (kind === "correlation-entries") {
      return Response.json({
        result: {
          rows: [
            {
              horse_name: "イクイノックス",
              jockey_name: "ルメール",
              ketto_toroku_bango: "2019105219",
              odds: 1.3,
              owner_name: "シルク",
              popularity: 1,
              trainer_name: "木村",
              umaban: "05",
            },
          ],
        },
        success: true,
      });
    }
    if (kind === "correlation-career") {
      return Response.json({
        result: {
          rows: [
            { entity: "2019105219", kind: "horse", show_count: 10, starts: 10, win_count: 8 },
            { entity: "ルメール", kind: "jockey", show_count: 600, starts: 1000, win_count: 300 },
            { entity: "木村", kind: "trainer", show_count: 250, starts: 500, win_count: 100 },
            { entity: "シルク", kind: "owner", show_count: 800, starts: 2000, win_count: 300 },
          ],
        },
        success: true,
      });
    }
    if (kind === "target-races") {
      return Response.json({
        result: {
          rows: [
            {
              bamei: "イクイノックス",
              jockey_name: "ルメール",
              keibajo_code: "05",
              kohan_3f: 351,
              owner_name: "シルク",
              popularity: "01",
              race_bango: "08",
              race_name: "天皇賞",
              race_time: 1450,
              target_race_date: "20241027",
              trainer_name: "堀",
              umaban: "05",
            },
          ],
        },
        success: true,
      });
    }
    return Response.json({
      result: {
        rows: [
          {
            average_kohan_3f: 35,
            average_race_time: 1400,
            fastest_kohan_3f: 34,
            fastest_race_time: 1330,
            median_kohan_3f: 35,
            median_race_time: 1390,
            race_count: 8,
          },
        ],
      },
      success: true,
    });
  };
  const env: Env = {
    CACHE_TTL_SECONDS: "15",
    CATALOG_KV: kv,
    KV_TTL_SECONDS: "120",
    R2_SQL_ACCOUNT_ID: "account",
    R2_SQL_BUCKET_NAME: "pc-keiba-r2-catalog",
    R2_SQL_NAMESPACE: "pc_keiba",
    R2_SQL_TOKEN: "r2-secret",
  };
  const dependencies: WorkerDependencies = { cache, fetchImpl };
  return { cacheEntries, dependencies, env, fetchCalls, inflight, kvEntries };
};

it("queries split condition-history aggregates and caches them for 36 hours", async () => {
  const harness = createConditionHarness();
  const response = await handleRequest(
    new Request(conditionUrl),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("X-Catalog-Cache")).toBe("r2-sql");
  await expect(response.json()).resolves.toStrictEqual({
    carriedWeightClassStats: [
      {
        key: "55.5-57",
        quinellaCount: 1,
        quinellaRate: 50,
        showCount: 1,
        showRate: 50,
        starts: 2,
        winCount: 0,
        winRate: 0,
      },
    ],
    finishPositionStats: [
      {
        averageOdds: 3.2,
        averagePopularity: 2,
        count: 1,
        details: [
          {
            date: "20241027",
            frameNumber: "03",
            horseName: "イクイノックス",
            horseNumber: "05",
            jockeyName: "ルメール",
            keibajoCode: "05",
            popularity: "02",
            raceName: "天皇賞",
            raceNumber: "08",
            raceTime: "1450",
            rank: "01",
            winOdds: "32",
          },
        ],
        finishPosition: 1,
        medianOdds: 3.2,
        medianPopularity: 2,
      },
    ],
    frameStats: [
      {
        averageFinish: 2,
        averagePopularity: 3,
        count: 10,
        details: [],
        frameNumber: "1",
        medianFinish: 2,
        medianPopularity: 3,
        quinellaCount: 4,
        quinellaRate: 40,
        runnerCount: 16,
        score: 1,
        showCount: 5,
        showRate: 50,
        winCount: 2,
        winRate: 20,
      },
    ],
    raceTimeStats: {
      averageKohan3f: 35,
      averageRaceTime: 1400,
      correlationRows: [
        {
          details: [
            {
              key: "horseShow",
              label: "出走馬の複勝率",
              reason: "対象レース1〜3着馬の対象レース前の複勝率平均との差",
              score: 1,
              target: 100,
              value: 100,
              weight: 0.2,
            },
            {
              key: "horseWin",
              label: "出走馬の勝率",
              reason: "対象レース1〜3着馬の対象レース前の勝率平均との差",
              score: 0.53,
              target: 33.3,
              value: 80,
              weight: 0.1,
            },
            {
              key: "jockeyShow",
              label: "騎手の複勝率",
              reason: "今回騎乗予定騎手の今回レース前の複勝率を評価",
              score: 0.6,
              target: null,
              value: 60,
              weight: 0.15,
            },
            {
              key: "trainerShow",
              label: "調教師の複勝率",
              reason: "今回出走馬の調教師の今回レース前の複勝率を評価",
              score: 0.5,
              target: null,
              value: 50,
              weight: 0.15,
            },
            {
              key: "ownerShow",
              label: "馬主の複勝率",
              reason: "今回出走馬の馬主の今回レース前の複勝率を評価",
              score: 0.4,
              target: null,
              value: 40,
              weight: 0.15,
            },
            {
              key: "popularity",
              label: "人気順",
              reason: "対象レース1〜3着馬の人気平均との差",
              score: 0.56,
              target: 3.2,
              value: 1,
              weight: 0.125,
            },
            {
              key: "odds",
              label: "単勝オッズ",
              reason: "対象レース1〜3着馬の単勝オッズ平均との差",
              score: 0.48,
              target: 6.5,
              value: 1.3,
              weight: 0.125,
            },
          ],
          horseName: "イクイノックス",
          horseNumber: "5",
          score: 0.61,
        },
      ],
      fastestDetail: null,
      fastestKohan3f: 34,
      fastestRaceTime: 1330,
      medianKohan3f: 35,
      medianRaceTime: 1390,
      raceCount: 8,
      targetRaces: [
        {
          date: "20241027",
          horseName: "イクイノックス",
          horseNumber: "05",
          jockeyName: "ルメール",
          keibajoCode: "05",
          kohan3f: "351",
          kyori: "",
          ownerName: "シルク",
          popularity: "01",
          raceName: "天皇賞",
          raceNumber: "08",
          raceTime: "1450",
          trainerName: "堀",
        },
      ],
    },
    weightClassStats: [
      {
        key: "480-499",
        quinellaCount: 1,
        quinellaRate: 25,
        showCount: 2,
        showRate: 50,
        starts: 4,
        winCount: 1,
        winRate: 25,
      },
    ],
  });
  expect(harness.fetchCalls.slice(0, 2).toSorted()).toStrictEqual(["frame", "weight"]);
  expect(harness.fetchCalls.slice(2, 4).toSorted()).toStrictEqual(["carried", "finish"]);
  expect(harness.fetchCalls.slice(4, 6).toSorted()).toStrictEqual(["race-time", "target-races"]);
  expect(harness.fetchCalls.slice(6, 8).toSorted()).toStrictEqual([
    "correlation-entries",
    "correlation-target",
  ]);
  expect(harness.fetchCalls.slice(8)).toStrictEqual(["correlation-career"]);
  expect(harness.inflight.max).toBe(2);
  const descriptor = conditionHistoryStatsDescriptor({
    date: "20260822",
    includeDistance: true,
    includeSurface: true,
    includeTurn: true,
    includeVenue: true,
    keibajoCode: "07",
    raceBango: "08",
    source: "jra",
    years: 10,
  });
  const cached = harness.cacheEntries.get(cacheRequestFor(descriptor).url);
  expect(cached?.headers.get("Cache-Control")).toBe("public, max-age=129600");
});

it("omits the carried-weight query for Ban'ei venues", async () => {
  const harness = createConditionHarness();
  const response = await handleRequest(
    new Request(
      "https://catalog.test/v1/condition-history-stats?year=2026&month=08&day=22&keibajoCode=83&raceNumber=08&source=nar&years=10&includeVenue=0&includeDistance=0&includeSurface=0&includeTurn=0",
    ),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  expect(harness.fetchCalls.slice(0, 2).toSorted()).toStrictEqual(["frame", "weight"]);
  expect(harness.fetchCalls.slice(2, 3)).toStrictEqual(["finish"]);
  expect(harness.fetchCalls.slice(3, 5).toSorted()).toStrictEqual(["race-time", "target-races"]);
  expect(harness.fetchCalls.slice(5, 7).toSorted()).toStrictEqual([
    "correlation-entries",
    "correlation-target",
  ]);
  expect(harness.fetchCalls.slice(7)).toStrictEqual(["correlation-career"]);
  expect(harness.inflight.max).toBe(2);
  const payload: unknown = await response.json();
  if (!isRecord(payload)) throw new Error("expected object");
  expect(payload.carriedWeightClassStats).toStrictEqual([]);
});

it("coalesces concurrent condition-history misses into one R2 SQL set", async () => {
  const harness = createConditionHarness();
  const [first, second] = await Promise.all([
    handleRequest(new Request(conditionUrl), harness.env, harness.dependencies),
    handleRequest(new Request(conditionUrl), harness.env, harness.dependencies),
  ]);
  expect(first.status).toBe(200);
  expect(second.status).toBe(200);
  expect(harness.fetchCalls.slice(0, 2).toSorted()).toStrictEqual(["frame", "weight"]);
  expect(harness.fetchCalls.slice(2, 4).toSorted()).toStrictEqual(["carried", "finish"]);
  expect(harness.fetchCalls.slice(4, 6).toSorted()).toStrictEqual(["race-time", "target-races"]);
  expect(harness.fetchCalls.slice(6, 8).toSorted()).toStrictEqual([
    "correlation-entries",
    "correlation-target",
  ]);
  expect(harness.fetchCalls.slice(8)).toStrictEqual(["correlation-career"]);
  expect(harness.inflight.max).toBe(2);
});

it("reads condition-history stats from Cache API then KV before R2 SQL", async () => {
  const harness = createConditionHarness();
  const descriptor = conditionHistoryStatsDescriptor({
    date: "20260822",
    includeDistance: true,
    includeSurface: true,
    includeTurn: true,
    includeVenue: true,
    keibajoCode: "07",
    raceBango: "08",
    source: "jra",
    years: 10,
  });
  const cachedBody = JSON.stringify({
    carriedWeightClassStats: [],
    finishPositionStats: [],
    frameStats: [],
    raceTimeStats: { raceCount: 0 },
    weightClassStats: [],
  });
  await harness.dependencies.cache.put(
    cacheRequestFor(descriptor),
    new Response(cachedBody, {
      headers: { "Content-Type": "application/json; charset=utf-8" },
    }),
  );
  const cacheHit = await handleRequest(
    new Request(conditionUrl),
    harness.env,
    harness.dependencies,
  );
  expect(cacheHit.headers.get("X-Catalog-Cache")).toBe("cache-api");
  expect(harness.fetchCalls).toStrictEqual([]);

  const kvHarness = createConditionHarness();
  await kvHarness.env.CATALOG_KV.put(kvKeyFor(descriptor), cachedBody, { expirationTtl: 120 });
  const kvHit = await handleRequest(
    new Request(conditionUrl),
    kvHarness.env,
    kvHarness.dependencies,
  );
  expect(kvHit.headers.get("X-Catalog-Cache")).toBe("kv");
  expect(kvHarness.fetchCalls).toStrictEqual([]);
});

it("rejects condition-history requests with invalid filters", async () => {
  const harness = createConditionHarness();
  const missingSource = await handleRequest(
    new Request(
      "https://catalog.test/v1/condition-history-stats?year=2026&month=08&day=22&keibajoCode=07&raceNumber=08",
    ),
    harness.env,
    harness.dependencies,
  );
  const badFlag = await handleRequest(
    new Request(
      "https://catalog.test/v1/condition-history-stats?year=2026&month=08&day=22&keibajoCode=07&raceNumber=08&source=jra&includeVenue=2",
    ),
    harness.env,
    harness.dependencies,
  );
  expect(missingSource.status).toBe(400);
  expect(badFlag.status).toBe(400);
});
