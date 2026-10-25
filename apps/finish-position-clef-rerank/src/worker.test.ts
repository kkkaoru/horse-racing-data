// Run with bun (bunx vitest).
import { afterEach, expect, it, vi } from "vitest";

import worker, {
  bundleKey,
  indexKey,
  jstYmd,
  rerankRace,
  resultKey,
  warmDay,
  type Env,
} from "./worker";

const race = { ymd: "20261018", keibajoCode: "08", raceBango: "11" };

const bundleJson = (): string =>
  JSON.stringify({
    modelVersion: "clef-rerank-v1",
    raceKey: "jra:2026:1018:08:11",
    label: "clef-flash 補正",
    baseModels: ["base-v1"],
    weights: { base: 1, feature: 0, clef: [{ source: "top3", weight: 2 }] },
    runners: [
      { horseNumber: 1, featureScore: 0 },
      { horseNumber: 2, featureScore: 0 },
    ],
    request: {
      state: {
        runners: [
          { no: 1, br: null },
          { no: 2, br: null },
        ],
      },
      questions: {
        winner: { type: "choice", instructions: "w", criteria: { "1": "a", "2": "b" } },
        second: { type: "choice", instructions: "s", criteria: { "1": "a", "2": "b" } },
        top3_1: { type: "noul", instructions: "1" },
        top3_2: { type: "noul", instructions: "2" },
        top5_1: { type: "noul", instructions: "5-1" },
        top5_2: { type: "noul", instructions: "5-2" },
      },
    },
  });

const baseJson = (at: string): string =>
  JSON.stringify([
    {
      horseNumber: "1",
      modelVersion: "base-v1",
      predictedFinishNorm: 0,
      predictionGeneratedAt: at,
    },
    {
      horseNumber: "2",
      modelVersion: "base-v1",
      predictedFinishNorm: 1,
      predictionGeneratedAt: at,
    },
  ]);

const clefAnswer = {
  answers: {
    winner: { probabilities: { "1": 0.1, "2": 0.9 } },
    second: { probabilities: { "1": 0.6, "2": 0.4 } },
    top3_1: { noul: 0.2 },
    top3_2: { noul: 0.9 },
    top5_1: { noul: 0.5 },
    top5_2: { noul: 0.95 },
  },
};

const makeEnv = (store: Record<string, string>): Env => ({
  AI: { run: vi.fn(async () => clefAnswer) },
  DETAIL_SECTION_CACHE_KV: {
    get: vi.fn(async (key: string) => store[key] ?? null),
    put: vi.fn(async () => undefined),
  },
  VIEWER: { fetch: vi.fn(async () => new Response("missing", { status: 404 })) },
});

afterEach(() => vi.restoreAllMocks());

it("builds stable KV keys", () => {
  expect(bundleKey(race)).toBe("clefrr:v1:bundle:20261018:08:11");
  expect(resultKey(race)).toBe("clefrr:v1:result:20261018:08:11");
  expect(indexKey("20261018")).toBe("clefrr:v1:index:20261018");
});

it("scores a race with clef-flash and stores the fused order", async () => {
  const env = makeEnv({
    "clefrr:v1:bundle:20261018:08:11": bundleJson(),
    "pred:fp:v1:20261018:08:11": baseJson("2026-10-18T00:00:00Z"),
  });
  const outcome = await rerankRace(env, race);
  expect(outcome.status).toBe(200);
  expect(
    (outcome.body as { predictions: { horseNumber: number }[] }).predictions.map(
      (row) => row.horseNumber,
    ),
  ).toStrictEqual([2, 1]);
  expect(env.AI.run).toHaveBeenCalledWith("@cf/cloudflare/clef-flash", {
    model: "clef-flash",
    state: {
      runners: [
        { no: 1, br: 1 },
        { no: 2, br: 2 },
      ],
    },
    questions: {
      winner: { type: "choice", instructions: "w", criteria: { "1": "a", "2": "b" } },
      second: { type: "choice", instructions: "s", criteria: { "1": "a", "2": "b" } },
      top3_1: { type: "noul", instructions: "1" },
      top3_2: { type: "noul", instructions: "2" },
      top5_1: { type: "noul", instructions: "5-1" },
      top5_2: { type: "noul", instructions: "5-2" },
    },
  });
  expect(env.DETAIL_SECTION_CACHE_KV.put).toHaveBeenCalledTimes(1);
});

it("keeps the served order for a base model the rerank was not approved for", async () => {
  const env = makeEnv({
    "clefrr:v1:bundle:20261018:08:11": bundleJson(),
    "pred:fp:v1:20261018:08:11": baseJson("2026-10-18T00:00:00Z").replaceAll("base-v1", "base-v2"),
  });
  expect(await rerankRace(env, race)).toStrictEqual({
    status: 404,
    body: { error: "clef rerank is not available" },
  });
  expect(env.AI.run).toHaveBeenCalledTimes(0);
});

it("reuses a cached result while the base generation is unchanged", async () => {
  const cached = { modelVersion: "clef-rerank-v1", baseGeneratedAt: "2026-10-18T00:00:00Z" };
  const env = makeEnv({
    "clefrr:v1:bundle:20261018:08:11": bundleJson(),
    "pred:fp:v1:20261018:08:11": baseJson("2026-10-18T00:00:00Z"),
    "clefrr:v1:result:20261018:08:11": JSON.stringify(cached),
  });
  expect(await rerankRace(env, race)).toStrictEqual({ status: 200, body: cached });
  expect(env.AI.run).not.toHaveBeenCalled();
});

it("rescoring happens after the base prediction is regenerated", async () => {
  const env = makeEnv({
    "clefrr:v1:bundle:20261018:08:11": bundleJson(),
    "pred:fp:v1:20261018:08:11": baseJson("2026-10-18T05:00:00Z"),
    "clefrr:v1:result:20261018:08:11": JSON.stringify({
      modelVersion: "clef-rerank-v1",
      baseGeneratedAt: "2026-10-18T00:00:00Z",
    }),
  });
  expect((await rerankRace(env, race)).status).toBe(200);
  expect(env.AI.run).toHaveBeenCalledTimes(1);
});

it("returns 404 without a bundle or a base prediction", async () => {
  expect((await rerankRace(makeEnv({}), race)).status).toBe(404);
  expect(
    (await rerankRace(makeEnv({ "clefrr:v1:bundle:20261018:08:11": "{broken" }), race)).status,
  ).toBe(404);
});

it("returns 502 when clef-flash answers are malformed", async () => {
  const env: Env = {
    ...makeEnv({
      "clefrr:v1:bundle:20261018:08:11": bundleJson(),
      "pred:fp:v1:20261018:08:11": baseJson("2026-10-18T00:00:00Z"),
    }),
    AI: { run: vi.fn(async () => ({ answers: {} })) },
  };
  expect((await rerankRace(env, race)).status).toBe(502);
});

it("serves race and index routes and fails closed on errors", async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  const env = makeEnv({
    "clefrr:v1:index:20261018": JSON.stringify(["08:11", "bad", 3]),
    "clefrr:v1:bundle:20261018:08:11": bundleJson(),
    "pred:fp:v1:20261018:08:11": baseJson("2026-10-18T00:00:00Z"),
  });
  const index = await worker.fetch(new Request("https://x/api/clef-rerank/20261018"), env);
  expect(await index.json()).toStrictEqual([
    { ymd: "20261018", keibajoCode: "08", raceBango: "11" },
  ]);
  const ok = await worker.fetch(new Request("https://x/api/clef-rerank/20261018/08/11"), env);
  expect(ok.status).toBe(200);
  const down: Env = {
    ...env,
    AI: { run: vi.fn(async () => Promise.reject(new Error("AI down"))) },
  };
  const failed = await worker.fetch(new Request("https://x/api/clef-rerank/20261018/08/11"), down);
  expect(failed.status).toBe(503);
  const missing = await worker.fetch(new Request("https://x/api/clef-rerank/other"), env);
  expect(missing.status).toBe(404);
});

it("returns an empty index when none was uploaded", async () => {
  const response = await worker.fetch(
    new Request("https://x/api/clef-rerank/20261019"),
    makeEnv({ "clefrr:v1:index:20261019": "{}" }),
  );
  expect(await response.json()).toStrictEqual([]);
});

it("warms every indexed race in bounded groups from the cron", async () => {
  vi.spyOn(console, "log").mockImplementation(() => undefined);
  const env = makeEnv({
    "clefrr:v1:index:20261018": JSON.stringify(["08:11", "08:10"]),
    "clefrr:v1:bundle:20261018:08:11": bundleJson(),
    "pred:fp:v1:20261018:08:11": baseJson("2026-10-18T00:00:00Z"),
  });
  expect(await warmDay(env, "20261018")).toStrictEqual([200, 404]);
  await worker.scheduled({ scheduledTime: Date.parse("2026-10-18T00:10:00Z") }, env);
  expect(console.log).toHaveBeenCalledWith("clef rerank warm", "[200,404]");
  expect(console.log).toHaveBeenCalledTimes(1);
});

it("warms races in post-time order from the viewer race list", async () => {
  const env = makeEnv({
    "clefrr:v1:index:20261018": JSON.stringify(["08:11", "08:10", "05:01"]),
    "clefrr:v1:bundle:20261018:08:11": bundleJson(),
    "pred:fp:v1:20261018:08:11": baseJson("2026-10-18T00:00:00Z"),
  });
  vi.mocked(env.VIEWER.fetch).mockResolvedValue(
    Response.json({
      finished: [
        {
          kaisaiNen: "2026",
          kaisaiTsukihi: "1017",
          keibajoCode: "08",
          raceBango: "10",
          hassoJikoku: "0900",
        },
      ],
      upcoming: [
        {
          kaisaiNen: "2026",
          kaisaiTsukihi: "1018",
          keibajoCode: "08",
          raceBango: "11",
          hassoJikoku: "1540",
        },
        {
          kaisaiNen: "2026",
          kaisaiTsukihi: "1018",
          keibajoCode: "08",
          raceBango: "10",
          hassoJikoku: "1500",
        },
        "noise",
      ],
    }),
  );
  expect(await warmDay(env, "20261018")).toStrictEqual([404, 200, 404]);
});

it("keeps index order when the race list is not an object or the viewer is down", async () => {
  const env = makeEnv({
    "clefrr:v1:index:20261018": JSON.stringify(["08:11", "08:10"]),
    "clefrr:v1:bundle:20261018:08:11": bundleJson(),
    "pred:fp:v1:20261018:08:11": baseJson("2026-10-18T00:00:00Z"),
  });
  vi.mocked(env.VIEWER.fetch).mockResolvedValueOnce(Response.json([1]));
  expect(await warmDay(env, "20261018")).toStrictEqual([200, 404]);
  vi.mocked(env.VIEWER.fetch).mockRejectedValueOnce(new Error("offline"));
  expect(await warmDay(env, "20261018")).toStrictEqual([200, 404]);
});

it("converts the cron time to the JST race date", () => {
  expect(jstYmd(Date.parse("2026-10-17T23:00:00Z"))).toBe("20261018");
});

it("forwards probe requests to hosted clef-flash and rejects non-object bodies", async () => {
  const env = makeEnv({});
  const ok = await worker.fetch(
    new Request("https://x/api/clef-rerank/probe", {
      method: "POST",
      body: '{"model":"clef-flash"}',
    }),
    env,
  );
  expect(await ok.json()).toStrictEqual({
    answers: {
      winner: { probabilities: { "1": 0.1, "2": 0.9 } },
      second: { probabilities: { "1": 0.6, "2": 0.4 } },
      top3_1: { noul: 0.2 },
      top3_2: { noul: 0.9 },
      top5_1: { noul: 0.5 },
      top5_2: { noul: 0.95 },
    },
  });
  const bad = await worker.fetch(
    new Request("https://x/api/clef-rerank/probe", { method: "POST", body: "[1]" }),
    env,
  );
  expect(bad.status).toBe(400);
  const get = await worker.fetch(new Request("https://x/api/clef-rerank/probe"), env);
  expect(get.status).toBe(404);
});

it("serves bet suggestions with the requested budget", async () => {
  const env = makeEnv({});
  vi.mocked(env.VIEWER.fetch).mockImplementation(async (request: Request) =>
    request.url.includes("win-rate-heatmap")
      ? Response.json({
          runners: [
            { umaban: "01", wakuban: "1", bamei: "A" },
            { umaban: "02", wakuban: "2", bamei: "B" },
          ],
        })
      : Response.json({
          odds: {
            fetchedAt: "t",
            latest: {
              tansho: [
                { combination: "1", odds: 2 },
                { combination: "2", odds: 3 },
              ],
              fukusho: [{ combination: "1", minOdds: 1.1, maxOdds: 1.4 }],
            },
          },
        }),
  );
  vi.mocked(env.AI.run).mockResolvedValue({
    answers: {
      winner: { probabilities: { "1": 0.7, "2": 0.3 } },
      bet_type: { probabilities: { tansho: 1 } },
      bet_style: { probabilities: { box: 1 } },
      axis: { probabilities: { "1": 1 } },
      top3_1: { noul: 0.9 },
      top3_2: { noul: 0.8 },
    },
  });
  const response = await worker.fetch(
    new Request("https://x/api/clef-rerank/bets/20261018/08/11?budget=300&target=0"),
    env,
  );
  expect(response.status).toBe(200);
  const body: unknown = await response.json();
  expect(
    typeof body === "object" && body !== null && "suggestion" in body ? body.suggestion : null,
  ).toStrictEqual({
    betType: "tansho",
    style: "single",
    axis: 1,
    legs: [[1]],
    budget: 300,
    totalStake: 300,
    expectedReturn: 361,
    tickets: [
      {
        horses: [1],
        odds: 2,
        maxOdds: null,
        probability: 0.6021800615402064,
        stake: 300,
        payout: 600,
        payoutMax: null,
      },
    ],
  });
});

it("rejects a budget that is not a multiple of 100", async () => {
  const response = await worker.fetch(
    new Request("https://x/api/clef-rerank/bets/20261018/08/11?budget=250"),
    makeEnv({}),
  );
  expect(response.status).toBe(400);
  expect(await response.json()).toStrictEqual({
    error: "budget must be a multiple of 100 from 100 to 1000000",
  });
});

it("rejects a target profit that is not whole yen", async () => {
  const response = await worker.fetch(
    new Request("https://x/api/clef-rerank/bets/20261018/08/11?budget=300&target=1.5"),
    makeEnv({}),
  );
  expect(response.status).toBe(400);
  expect(await response.json()).toStrictEqual({
    error: "target must be whole yen from 0 to 10000000",
  });
});

it("passes the required-all switch to the bet suggestion", async () => {
  const env = makeEnv({});
  vi.mocked(env.VIEWER.fetch).mockImplementation(async (request: Request) =>
    request.url.includes("win-rate-heatmap")
      ? Response.json({
          runners: [
            { umaban: "01", wakuban: "1", bamei: "A" },
            { umaban: "02", wakuban: "2", bamei: "B" },
          ],
        })
      : Response.json({
          odds: {
            fetchedAt: "t",
            latest: {
              tansho: [
                { combination: "1", odds: 2 },
                { combination: "2", odds: 3 },
              ],
              fukusho: [{ combination: "1", minOdds: 1.1, maxOdds: 1.4 }],
            },
          },
        }),
  );
  vi.mocked(env.AI.run).mockResolvedValue({
    answers: {
      winner: { probabilities: { "1": 0.7, "2": 0.3 } },
      bet_type: { probabilities: { tansho: 1 } },
      bet_style: { probabilities: { box: 1 } },
      axis: { probabilities: { "1": 1 } },
      top3_1: { noul: 0.9 },
      top3_2: { noul: 0.8 },
    },
  });
  const response = await worker.fetch(
    new Request("https://x/api/clef-rerank/bets/20261018/08/11?budget=300&target=100&requireAll=1"),
    env,
  );
  expect(response.status).toBe(404);
  expect(await response.json()).toStrictEqual({
    error: "the selected bet types cannot all fit the budget and target",
    reason: "required-types-unfit",
  });
});

it("rejects an unknown bet type selection", async () => {
  const response = await worker.fetch(
    new Request("https://x/api/clef-rerank/bets/20261018/08/11?budget=300&types=tansho,foo"),
    makeEnv({}),
  );
  expect(response.status).toBe(400);
  expect(await response.json()).toStrictEqual({
    error: "types must be a comma-separated list of bet types",
  });
});

it("suggests only the selected bet types", async () => {
  const env = makeEnv({});
  vi.mocked(env.VIEWER.fetch).mockImplementation(async (request: Request) =>
    request.url.includes("win-rate-heatmap")
      ? Response.json({
          runners: [
            { umaban: "01", wakuban: "1", bamei: "A" },
            { umaban: "02", wakuban: "2", bamei: "B" },
          ],
        })
      : Response.json({
          odds: {
            fetchedAt: "t",
            latest: {
              tansho: [
                { combination: "1", odds: 2 },
                { combination: "2", odds: 3 },
              ],
              fukusho: [{ combination: "1", minOdds: 1.1, maxOdds: 1.4 }],
            },
          },
        }),
  );
  vi.mocked(env.AI.run).mockResolvedValue({
    answers: {
      winner: { probabilities: { "1": 0.7, "2": 0.3 } },
      bet_type: { probabilities: { tansho: 1 } },
      bet_style: { probabilities: { box: 1 } },
      axis: { probabilities: { "1": 1 } },
      top3_1: { noul: 0.9 },
      top3_2: { noul: 0.8 },
    },
  });
  const response = await worker.fetch(
    new Request("https://x/api/clef-rerank/bets/20261018/08/11?budget=300&target=0&types=fukusho"),
    env,
  );
  expect(response.status).toBe(200);
  const body: unknown = await response.json();
  expect(
    typeof body === "object" &&
      body !== null &&
      "plans" in body &&
      typeof body.plans === "object" &&
      body.plans !== null &&
      "plans" in body.plans &&
      Array.isArray(body.plans.plans)
      ? body.plans.plans.map((plan: { betType: string }) => plan.betType)
      : null,
  ).toStrictEqual(["fukusho"]);
});

it("fails closed when the viewer cannot be reached", async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  const env = makeEnv({});
  vi.mocked(env.VIEWER.fetch).mockRejectedValue(new Error("offline"));
  const response = await worker.fetch(
    new Request("https://x/api/clef-rerank/bets/20261018/08/11"),
    env,
  );
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({ error: "clef bet suggestion failed" });
});
