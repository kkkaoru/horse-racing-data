// Run with: bun run --filter sync-realtime-data-features test
import { expect, it, vi } from "vitest";

vi.mock("./features/postgres-pool", () => ({
  getFeaturesPool: vi.fn(),
}));

import { getFeaturesPool } from "./features/postgres-pool";
import {
  listTodayRaceKeysFromHyperdrive,
  listTodayRaceKeysWithKvCache,
  listTomorrowRaceKeysFromHyperdrive,
  listTomorrowRaceKeysWithKvCache,
  toRaceJobKeyFromTodayRaceKey,
} from "./scheduled-race-list";
import type { Env } from "./types";

const buildEnv = (): Env =>
  ({
    REALTIME_FEATURES_DB: {} as unknown as D1Database,
    FEATURES_KV: {} as unknown as KVNamespace,
    FEATURES_ARCHIVE: {} as unknown as R2Bucket,
    HYPERDRIVE: { connectionString: "postgres://test" },
  }) as unknown as Env;

interface MockKv {
  get: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
}

interface BuildKvEnvArgs {
  jraCached: string | null;
  narCached: string | null;
}

const buildKvEnv = (args: BuildKvEnvArgs): { env: Env; kv: MockKv } => {
  const kv: MockKv = {
    get: vi.fn(),
    put: vi.fn().mockResolvedValue(undefined),
  };
  kv.get.mockImplementation((key: string) => {
    if (key === "race-keys:v1:jra:20260529") {
      return Promise.resolve(args.jraCached);
    }
    if (key === "race-keys:v1:nar:20260529") {
      return Promise.resolve(args.narCached);
    }
    if (key === "race-keys:v1:jra:20260530") {
      return Promise.resolve(args.jraCached);
    }
    if (key === "race-keys:v1:nar:20260530") {
      return Promise.resolve(args.narCached);
    }
    return Promise.resolve(null);
  });
  return {
    env: {
      FEATURES_KV: kv as unknown as KVNamespace,
    } as unknown as Env,
    kv,
  };
};

it("listTodayRaceKeysFromHyperdrive returns mapped JRA + NAR rows", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        source: "jra",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0529",
        keibajo_code: "8",
        race_bango: "1",
      },
      {
        source: "nar",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0529",
        keibajo_code: "30",
        race_bango: "08",
      },
      {
        source: "nar",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0529",
        keibajo_code: "83",
        race_bango: "11",
      },
    ],
  });
  const env = buildEnv();
  const rows = await listTodayRaceKeysFromHyperdrive(env, "20260529", {
    pool: { query } as never,
  });
  expect(rows).toStrictEqual([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "08",
      raceBango: "01",
      raceKey: "jra:2026:0529:08:01",
      source: "jra",
    },
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      raceBango: "08",
      raceKey: "nar:2026:0529:30:08",
      source: "nar",
    },
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "83",
      raceBango: "11",
      raceKey: "nar:2026:0529:83:11",
      source: "nar",
    },
  ]);
});

it("listTodayRaceKeysFromHyperdrive binds kaisaiNen and kaisaiTsukihi extracted from yyyymmdd", async () => {
  const query = vi.fn().mockResolvedValue({ rows: [] });
  const env = buildEnv();
  await listTodayRaceKeysFromHyperdrive(env, "20260529", { pool: { query } as never });
  expect(query).toHaveBeenCalledWith(expect.any(String), ["2026", "0529"]);
});

it("listTodayRaceKeysFromHyperdrive returns empty array when no rows match", async () => {
  const query = vi.fn().mockResolvedValue({ rows: [] });
  const env = buildEnv();
  const rows = await listTodayRaceKeysFromHyperdrive(env, "20260529", {
    pool: { query } as never,
  });
  expect(rows).toStrictEqual([]);
});

it("listTodayRaceKeysFromHyperdrive skips rows with unknown source", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        source: "unknown",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0529",
        keibajo_code: "30",
        race_bango: "08",
      },
      {
        source: "jra",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0529",
        keibajo_code: "06",
        race_bango: "12",
      },
    ],
  });
  const env = buildEnv();
  const rows = await listTodayRaceKeysFromHyperdrive(env, "20260529", {
    pool: { query } as never,
  });
  expect(rows).toStrictEqual([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "06",
      raceBango: "12",
      raceKey: "jra:2026:0529:06:12",
      source: "jra",
    },
  ]);
});

it("listTodayRaceKeysFromHyperdrive skips rows with missing string columns", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        source: "jra",
        kaisai_nen: null,
        kaisai_tsukihi: "0529",
        keibajo_code: "08",
        race_bango: "01",
      },
      {
        source: "nar",
        kaisai_nen: "2026",
        kaisai_tsukihi: 529,
        keibajo_code: "30",
        race_bango: "08",
      },
      {
        source: "jra",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0529",
        keibajo_code: null,
        race_bango: "01",
      },
      {
        source: "nar",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0529",
        keibajo_code: "30",
        race_bango: undefined,
      },
    ],
  });
  const env = buildEnv();
  const rows = await listTodayRaceKeysFromHyperdrive(env, "20260529", {
    pool: { query } as never,
  });
  expect(rows).toStrictEqual([]);
});

it("listTodayRaceKeysFromHyperdrive falls back to getFeaturesPool when context.pool absent", async () => {
  const query = vi.fn().mockResolvedValue({ rows: [] });
  vi.mocked(getFeaturesPool).mockReturnValueOnce({ query } as never);
  const env = buildEnv();
  await listTodayRaceKeysFromHyperdrive(env, "20260529");
  expect(getFeaturesPool).toHaveBeenCalledWith(env);
  expect(query).toHaveBeenCalledWith(expect.any(String), ["2026", "0529"]);
});

it("listTomorrowRaceKeysFromHyperdrive binds tomorrow date params", async () => {
  const query = vi.fn().mockResolvedValue({ rows: [] });
  const env = buildEnv();
  await listTomorrowRaceKeysFromHyperdrive(env, new Date("2026-05-29T03:00:00Z"), {
    pool: { query } as never,
  });
  expect(query).toHaveBeenCalledWith(expect.any(String), ["2026", "0530"]);
});

it("listTomorrowRaceKeysFromHyperdrive returns mapped rows for tomorrow", async () => {
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        source: "jra",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0530",
        keibajo_code: "5",
        race_bango: "11",
      },
    ],
  });
  const env = buildEnv();
  const rows = await listTomorrowRaceKeysFromHyperdrive(env, new Date("2026-05-29T03:00:00Z"), {
    pool: { query } as never,
  });
  expect(rows).toStrictEqual([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0530",
      keibajoCode: "05",
      raceBango: "11",
      raceKey: "jra:2026:0530:05:11",
      source: "jra",
    },
  ]);
});

it("listTomorrowRaceKeysFromHyperdrive falls back to getFeaturesPool when context.pool absent", async () => {
  const query = vi.fn().mockResolvedValue({ rows: [] });
  vi.mocked(getFeaturesPool).mockReturnValueOnce({ query } as never);
  const env = buildEnv();
  await listTomorrowRaceKeysFromHyperdrive(env, new Date("2026-05-29T03:00:00Z"));
  expect(getFeaturesPool).toHaveBeenCalledWith(env);
  expect(query).toHaveBeenCalledWith(expect.any(String), ["2026", "0530"]);
});

it("listTodayRaceKeysWithKvCache returns combined cached entries without hitting Hyperdrive", async () => {
  const { env, kv } = buildKvEnv({
    jraCached: JSON.stringify([
      {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0529",
        keibajoCode: "08",
        raceBango: "01",
        raceKey: "jra:2026:0529:08:01",
        source: "jra",
      },
    ]),
    narCached: JSON.stringify([
      {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0529",
        keibajoCode: "30",
        raceBango: "08",
        raceKey: "nar:2026:0529:30:08",
        source: "nar",
      },
    ]),
  });
  const query = vi.fn();
  const rows = await listTodayRaceKeysWithKvCache({
    context: { pool: { query } as never },
    env,
    yyyymmdd: "20260529",
  });
  expect(query).not.toHaveBeenCalled();
  expect(kv.put).not.toHaveBeenCalled();
  expect(rows).toStrictEqual([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "08",
      raceBango: "01",
      raceKey: "jra:2026:0529:08:01",
      source: "jra",
    },
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      raceBango: "08",
      raceKey: "nar:2026:0529:30:08",
      source: "nar",
    },
  ]);
});

it("listTodayRaceKeysWithKvCache falls through to Hyperdrive on full miss and populates both per-source caches", async () => {
  const { env, kv } = buildKvEnv({ jraCached: null, narCached: null });
  const query = vi.fn().mockResolvedValue({
    rows: [
      {
        source: "jra",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0529",
        keibajo_code: "08",
        race_bango: "01",
      },
      {
        source: "nar",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0529",
        keibajo_code: "30",
        race_bango: "08",
      },
    ],
  });
  const rows = await listTodayRaceKeysWithKvCache({
    context: { pool: { query } as never },
    env,
    yyyymmdd: "20260529",
  });
  expect(query).toHaveBeenCalledTimes(1);
  expect(kv.put).toHaveBeenCalledWith(
    "race-keys:v1:jra:20260529",
    '[{"kaisaiNen":"2026","kaisaiTsukihi":"0529","keibajoCode":"08","raceBango":"01","raceKey":"jra:2026:0529:08:01","source":"jra"}]',
    { expirationTtl: 1800 },
  );
  expect(kv.put).toHaveBeenCalledWith(
    "race-keys:v1:nar:20260529",
    '[{"kaisaiNen":"2026","kaisaiTsukihi":"0529","keibajoCode":"30","raceBango":"08","raceKey":"nar:2026:0529:30:08","source":"nar"}]',
    { expirationTtl: 1800 },
  );
  expect(rows).toStrictEqual([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "08",
      raceBango: "01",
      raceKey: "jra:2026:0529:08:01",
      source: "jra",
    },
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      raceBango: "08",
      raceKey: "nar:2026:0529:30:08",
      source: "nar",
    },
  ]);
});

it("listTodayRaceKeysWithKvCache refreshes Hyperdrive when only one source entry is cached", async () => {
  const { env, kv } = buildKvEnv({
    jraCached: JSON.stringify([]),
    narCached: null,
  });
  const query = vi.fn().mockResolvedValue({ rows: [] });
  const rows = await listTodayRaceKeysWithKvCache({
    context: { pool: { query } as never },
    env,
    yyyymmdd: "20260529",
  });
  expect(query).toHaveBeenCalledTimes(1);
  expect(kv.put).toHaveBeenCalledTimes(2);
  expect(rows).toStrictEqual([]);
});

it("listTodayRaceKeysWithKvCache returns empty list and does not cache when Hyperdrive throws", async () => {
  const { env, kv } = buildKvEnv({ jraCached: null, narCached: null });
  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const query = vi.fn().mockRejectedValue(new Error("hyperdrive boom"));
  const rows = await listTodayRaceKeysWithKvCache({
    context: { pool: { query } as never },
    env,
    yyyymmdd: "20260529",
  });
  expect(rows).toStrictEqual([]);
  expect(kv.put).not.toHaveBeenCalled();
  expect(consoleSpy).toHaveBeenCalledTimes(1);
  consoleSpy.mockRestore();
});

it("listTomorrowRaceKeysWithKvCache resolves tomorrow yyyymmdd before consulting KV", async () => {
  const { env, kv } = buildKvEnv({
    jraCached: JSON.stringify([]),
    narCached: JSON.stringify([
      {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0530",
        keibajoCode: "30",
        raceBango: "08",
        raceKey: "nar:2026:0530:30:08",
        source: "nar",
      },
    ]),
  });
  const query = vi.fn();
  const rows = await listTomorrowRaceKeysWithKvCache({
    context: { pool: { query } as never },
    env,
    now: new Date("2026-05-29T03:00:00Z"),
  });
  expect(query).not.toHaveBeenCalled();
  expect(kv.get).toHaveBeenCalledWith("race-keys:v1:jra:20260530");
  expect(kv.get).toHaveBeenCalledWith("race-keys:v1:nar:20260530");
  expect(rows).toStrictEqual([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0530",
      keibajoCode: "30",
      raceBango: "08",
      raceKey: "nar:2026:0530:30:08",
      source: "nar",
    },
  ]);
});

it("toRaceJobKeyFromTodayRaceKey maps fields 1:1", async () => {
  const result = toRaceJobKeyFromTodayRaceKey({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "30",
    raceBango: "08",
    raceKey: "nar:2026:0529:30:08",
    source: "nar",
  });
  expect(result).toStrictEqual({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "30",
    raceBango: "08",
    raceKey: "nar:2026:0529:30:08",
    source: "nar",
  });
});
