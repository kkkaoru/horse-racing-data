// Run with: bun run --filter sync-realtime-data-features test
import { expect, it, vi } from "vitest";

vi.mock("./features/postgres-pool", () => ({
  getFeaturesPool: vi.fn(),
}));

import { getFeaturesPool } from "./features/postgres-pool";
import {
  listTodayRaceKeysFromHyperdrive,
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
