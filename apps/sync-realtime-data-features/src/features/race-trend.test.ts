// Run with: bun run --filter sync-realtime-data-features test
import { beforeEach, expect, it, vi } from "vitest";

vi.mock("./parquet", () => ({
  decodeRaceFeaturesParquet: vi.fn(async () => []),
}));

import { decodeRaceFeaturesParquet } from "./parquet";
import {
  buildRaceParquetKey,
  buildRaceTrendCacheKey,
  buildRaceTrendPayload,
  buildRaceTrendPrefix,
  expandDateRange,
  handleRaceTrend,
} from "./race-trend";
import type { DailyRaceEntryRow, Env } from "../types";

interface MockCaches {
  default: {
    match: ReturnType<typeof vi.fn>;
    put: ReturnType<typeof vi.fn>;
  };
}

const baseRow: DailyRaceEntryRow = {
  babajotai_code_dirt: null,
  babajotai_code_shiba: null,
  bamei: null,
  banushimei: null,
  barei: null,
  bataiju: null,
  chokyoshimei_ryakusho: null,
  corner1_norm: null,
  corner2_norm: null,
  corner3_norm: null,
  corner4_norm: null,
  corner_1: null,
  corner_2: null,
  corner_3: null,
  corner_4: null,
  finish_norm: null,
  finish_position: null,
  futan_juryo: null,
  grade_code: null,
  hasso_jikoku: null,
  juryo_shubetsu_code: null,
  kaisai_nen: "2026",
  kaisai_tsukihi: "0529",
  keibajo_code: "30",
  ketto_toroku_bango: "kt",
  kishumei_ryakusho: null,
  kohan_3f: null,
  kyori: null,
  kyoso_joken_code: null,
  kyoso_shubetsu_code: null,
  race_bango: "08",
  race_date: "20260529",
  race_name: null,
  seibetsu_code: null,
  shusso_tosu: null,
  soha_time: null,
  source: "nar",
  tansho_ninkijun: null,
  tansho_odds: null,
  time_sa: null,
  track_code: null,
  umaban: null,
  wakuban: null,
  zogen_fugo: null,
  zogen_sa: null,
};

const buildCaches = (): MockCaches => ({
  default: {
    match: vi.fn().mockResolvedValue(undefined),
    put: vi.fn().mockResolvedValue(undefined),
  },
});

interface BuildEnvOverrides {
  withKv?: boolean;
  kvStore?: Map<string, string>;
  archiveList?: ReturnType<typeof vi.fn>;
  archiveGet?: ReturnType<typeof vi.fn>;
}

const buildEnv = (overrides: BuildEnvOverrides = {}): Env => {
  const kvStore = overrides.kvStore ?? new Map<string, string>();
  const kv = {
    get: vi.fn(async (key: string) => kvStore.get(key) ?? null),
    put: vi.fn(async (key: string, value: string) => {
      kvStore.set(key, value);
    }),
    delete: vi.fn(),
  };
  const archive = {
    list: overrides.archiveList ?? vi.fn().mockResolvedValue({ objects: [] }),
    get: overrides.archiveGet ?? vi.fn().mockResolvedValue(null),
    put: vi.fn(),
  };
  const env: Record<string, unknown> = {
    REALTIME_FEATURES_DB: {},
    FEATURES_ARCHIVE: archive as unknown as R2Bucket,
  };
  if (overrides.withKv !== false) {
    env.FEATURES_KV = kv as unknown as KVNamespace;
  }
  return env as unknown as Env;
};

beforeEach(() => {
  vi.unstubAllGlobals();
  vi.mocked(decodeRaceFeaturesParquet).mockReset();
  vi.mocked(decodeRaceFeaturesParquet).mockResolvedValue([]);
});

it("buildRaceTrendCacheKey returns colon-delimited stable key", () => {
  expect(
    buildRaceTrendCacheKey({
      source: "nar",
      keibajoCode: "30",
      raceBango: "08",
      from: "20260516",
      to: "20260529",
    }),
  ).toBe("race-trend:nar:30:08:20260516:20260529");
});

it("buildRaceTrendPrefix splits ymd into year/month/day", () => {
  expect(buildRaceTrendPrefix({ source: "jra", ymd: "20260529", keibajoCode: "5" })).toBe(
    "features/by-race/2026/05/29/jra/05/",
  );
});

it("buildRaceParquetKey zero-pads raceBango", () => {
  expect(
    buildRaceParquetKey({ source: "nar", ymd: "20260529", keibajoCode: "30", raceBango: "8" }),
  ).toBe("features/by-race/2026/05/29/nar/30/08.parquet");
});

it("expandDateRange returns single date when from equals to", () => {
  expect(expandDateRange("20260529", "20260529")).toStrictEqual(["20260529"]);
});

it("expandDateRange returns inclusive sequence across month boundary", () => {
  expect(expandDateRange("20260430", "20260502")).toStrictEqual([
    "20260430",
    "20260501",
    "20260502",
  ]);
});

it("expandDateRange returns empty when from is after to", () => {
  expect(expandDateRange("20260601", "20260530")).toStrictEqual([]);
});

it("handleRaceTrend returns empty payload when source missing", async () => {
  const env = buildEnv();
  vi.stubGlobal("caches", buildCaches());
  const response = await handleRaceTrend(
    env,
    new Request(
      "https://x/api/features/race-trend?keibajoCode=30&raceBango=08&from=20260529&to=20260529",
    ),
  );
  expect(response.status).toBe(200);
  await expect(response.json()).resolves.toStrictEqual({
    byJockey: {},
    byWaku: {},
    raceCount: 0,
    starterCount: 0,
    starterRows: [],
  });
});

it("handleRaceTrend returns empty payload when source is invalid", async () => {
  const env = buildEnv();
  vi.stubGlobal("caches", buildCaches());
  const response = await handleRaceTrend(
    env,
    new Request(
      "https://x/api/features/race-trend?source=zzz&keibajoCode=30&raceBango=08&from=20260529&to=20260529",
    ),
  );
  await expect(response.json()).resolves.toStrictEqual({
    byJockey: {},
    byWaku: {},
    raceCount: 0,
    starterCount: 0,
    starterRows: [],
  });
});

it("handleRaceTrend returns empty payload when keibajoCode missing", async () => {
  const env = buildEnv();
  vi.stubGlobal("caches", buildCaches());
  const response = await handleRaceTrend(
    env,
    new Request(
      "https://x/api/features/race-trend?source=nar&raceBango=08&from=20260529&to=20260529",
    ),
  );
  await expect(response.json()).resolves.toStrictEqual({
    byJockey: {},
    byWaku: {},
    raceCount: 0,
    starterCount: 0,
    starterRows: [],
  });
});

it("handleRaceTrend returns empty payload when from has wrong shape", async () => {
  const env = buildEnv();
  vi.stubGlobal("caches", buildCaches());
  const response = await handleRaceTrend(
    env,
    new Request(
      "https://x/api/features/race-trend?source=nar&keibajoCode=30&raceBango=08&from=2026-05-29&to=20260529",
    ),
  );
  await expect(response.json()).resolves.toStrictEqual({
    byJockey: {},
    byWaku: {},
    raceCount: 0,
    starterCount: 0,
    starterRows: [],
  });
});

it("handleRaceTrend returns empty payload when to has wrong shape", async () => {
  const env = buildEnv();
  vi.stubGlobal("caches", buildCaches());
  const response = await handleRaceTrend(
    env,
    new Request(
      "https://x/api/features/race-trend?source=nar&keibajoCode=30&raceBango=08&from=20260529",
    ),
  );
  await expect(response.json()).resolves.toStrictEqual({
    byJockey: {},
    byWaku: {},
    raceCount: 0,
    starterCount: 0,
    starterRows: [],
  });
});

it("handleRaceTrend serves edge cache hit without touching R2", async () => {
  const cached = new Response(
    '{"raceCount":42,"starterCount":0,"starterRows":[],"byJockey":{},"byWaku":{}}',
  );
  const cachesStub = buildCaches();
  cachesStub.default.match.mockResolvedValueOnce(cached);
  vi.stubGlobal("caches", cachesStub);
  const env = buildEnv();
  const response = await handleRaceTrend(
    env,
    new Request(
      "https://x/api/features/race-trend?source=nar&keibajoCode=30&raceBango=08&from=20260529&to=20260529",
    ),
  );
  expect(response).toBe(cached);
  expect(env.FEATURES_ARCHIVE.list).not.toHaveBeenCalled();
});

it("buildRaceTrendPayload returns aggregate from R2 list hit + Parquet fetch", async () => {
  const archiveGet = vi.fn().mockResolvedValue({
    arrayBuffer: async () => new Uint8Array([1, 2]).buffer,
  });
  const archiveList = vi
    .fn()
    .mockResolvedValue({ objects: [{ key: "features/by-race/2026/05/29/nar/30/08.parquet" }] });
  const env = buildEnv({ archiveGet, archiveList });
  vi.stubGlobal("caches", buildCaches());
  vi.mocked(decodeRaceFeaturesParquet).mockResolvedValueOnce([
    { ...baseRow, finish_position: 1, kishumei_ryakusho: "山田", wakuban: "1" },
  ]);
  const result = await buildRaceTrendPayload(env, {
    source: "nar",
    keibajoCode: "30",
    raceBango: "08",
    from: "20260529",
    to: "20260529",
  });
  expect(result.raceCount).toBe(1);
  expect(result.starterCount).toBe(1);
  expect(result.byJockey).toStrictEqual({
    山田: { quinellas: 1, runs: 1, shows: 1, wins: 1 },
  });
  expect(result.byWaku).toStrictEqual({
    1: { quinellas: 1, runs: 1, shows: 1, wins: 1 },
  });
});

it("buildRaceTrendPayload reuses KV list cache on second call", async () => {
  const archiveList = vi
    .fn()
    .mockResolvedValue({ objects: [{ key: "features/by-race/2026/05/29/nar/30/08.parquet" }] });
  const archiveGet = vi.fn().mockResolvedValue({
    arrayBuffer: async () => new Uint8Array([1, 2]).buffer,
  });
  const env = buildEnv({ archiveGet, archiveList });
  vi.stubGlobal("caches", buildCaches());
  vi.mocked(decodeRaceFeaturesParquet).mockResolvedValue([]);
  await buildRaceTrendPayload(env, {
    source: "nar",
    keibajoCode: "30",
    raceBango: "08",
    from: "20260529",
    to: "20260529",
  });
  await buildRaceTrendPayload(env, {
    source: "nar",
    keibajoCode: "30",
    raceBango: "08",
    from: "20260529",
    to: "20260529",
  });
  expect(archiveList).toHaveBeenCalledTimes(1);
});

it("buildRaceTrendPayload reuses Cache API parquet bytes on second call", async () => {
  const archiveList = vi
    .fn()
    .mockResolvedValue({ objects: [{ key: "features/by-race/2026/05/29/nar/30/08.parquet" }] });
  const archiveGet = vi.fn().mockResolvedValue({
    arrayBuffer: async () => new Uint8Array([7, 8, 9]).buffer,
  });
  const env = buildEnv({ archiveGet, archiveList });
  const cachesStub = buildCaches();
  const cachedBytes = new Response(new Uint8Array([1, 2, 3]));
  cachesStub.default.match.mockImplementation(async (request: Request) =>
    request.url.includes("parquet-bytes") ? cachedBytes.clone() : undefined,
  );
  vi.stubGlobal("caches", cachesStub);
  vi.mocked(decodeRaceFeaturesParquet).mockResolvedValue([]);
  await buildRaceTrendPayload(env, {
    source: "nar",
    keibajoCode: "30",
    raceBango: "08",
    from: "20260529",
    to: "20260529",
  });
  expect(archiveGet).not.toHaveBeenCalled();
});

it("buildRaceTrendPayload falls back to live list when FEATURES_KV unbound", async () => {
  const archiveList = vi
    .fn()
    .mockResolvedValue({ objects: [{ key: "features/by-race/2026/05/29/nar/30/08.parquet" }] });
  const archiveGet = vi.fn().mockResolvedValue({
    arrayBuffer: async () => new Uint8Array([1, 2]).buffer,
  });
  const env = buildEnv({ archiveGet, archiveList, withKv: false });
  vi.stubGlobal("caches", buildCaches());
  vi.mocked(decodeRaceFeaturesParquet).mockResolvedValue([]);
  await buildRaceTrendPayload(env, {
    source: "nar",
    keibajoCode: "30",
    raceBango: "08",
    from: "20260529",
    to: "20260529",
  });
  await buildRaceTrendPayload(env, {
    source: "nar",
    keibajoCode: "30",
    raceBango: "08",
    from: "20260529",
    to: "20260529",
  });
  expect(archiveList).toHaveBeenCalledTimes(2);
});

it("buildRaceTrendPayload returns zero rows when target parquet missing from list", async () => {
  const archiveList = vi
    .fn()
    .mockResolvedValue({ objects: [{ key: "features/by-race/2026/05/29/nar/30/05.parquet" }] });
  const archiveGet = vi.fn().mockResolvedValue(null);
  const env = buildEnv({ archiveGet, archiveList });
  vi.stubGlobal("caches", buildCaches());
  const result = await buildRaceTrendPayload(env, {
    source: "nar",
    keibajoCode: "30",
    raceBango: "08",
    from: "20260529",
    to: "20260529",
  });
  expect(result.starterCount).toBe(0);
  expect(archiveGet).not.toHaveBeenCalled();
});

it("buildRaceTrendPayload returns zero rows when R2 get yields null", async () => {
  const archiveList = vi
    .fn()
    .mockResolvedValue({ objects: [{ key: "features/by-race/2026/05/29/nar/30/08.parquet" }] });
  const archiveGet = vi.fn().mockResolvedValue(null);
  const env = buildEnv({ archiveGet, archiveList });
  vi.stubGlobal("caches", buildCaches());
  const result = await buildRaceTrendPayload(env, {
    source: "nar",
    keibajoCode: "30",
    raceBango: "08",
    from: "20260529",
    to: "20260529",
  });
  expect(result.starterCount).toBe(0);
});

it("buildRaceTrendPayload assigns race_date from kaisai_nen + kaisai_tsukihi", async () => {
  const archiveList = vi
    .fn()
    .mockResolvedValue({ objects: [{ key: "features/by-race/2026/05/29/nar/30/08.parquet" }] });
  const archiveGet = vi.fn().mockResolvedValue({
    arrayBuffer: async () => new Uint8Array([1]).buffer,
  });
  const env = buildEnv({ archiveGet, archiveList });
  vi.stubGlobal("caches", buildCaches());
  vi.mocked(decodeRaceFeaturesParquet).mockResolvedValueOnce([
    {
      ...baseRow,
      finish_position: 1,
      kishumei_ryakusho: "山田",
      race_date: "ignored",
      wakuban: "1",
    },
  ]);
  const result = await buildRaceTrendPayload(env, {
    source: "nar",
    keibajoCode: "30",
    raceBango: "08",
    from: "20260529",
    to: "20260529",
  });
  expect(result.starterRows[0]!.kaisaiNen).toBe("2026");
  expect(result.starterRows[0]!.kaisaiTsukihi).toBe("0529");
});

it("handleRaceTrend writes edge cache on miss", async () => {
  const archiveList = vi.fn().mockResolvedValue({ objects: [] });
  const env = buildEnv({ archiveList });
  const cachesStub = buildCaches();
  vi.stubGlobal("caches", cachesStub);
  await handleRaceTrend(
    env,
    new Request(
      "https://x/api/features/race-trend?source=nar&keibajoCode=30&raceBango=08&from=20260529&to=20260529",
    ),
  );
  expect(cachesStub.default.put).toHaveBeenCalledTimes(1);
});
