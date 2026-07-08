import { expect, it, vi } from "vitest";
import {
  buildCatalogStagingR2Key,
  buildLiveOddsR2Key,
  buildSnapshotOddsR2Key,
  readOddsPayloadFromR2,
  writeOddsPayloadToR2,
} from "./r2-odds-store";
import type { Env } from "./types";

const buildKv = (): KVNamespace =>
  ({
    delete: vi.fn(async () => undefined),
    get: vi.fn(async () => null),
    put: vi.fn(async () => undefined),
  }) as unknown as KVNamespace;

const buildR2 = (stored: unknown = null): R2Bucket =>
  ({
    delete: vi.fn(async () => undefined),
    get: vi.fn(async () => (stored ? { json: vi.fn(async () => stored) } : null)),
    put: vi.fn(async () => ({})),
  }) as unknown as R2Bucket;

const buildEnv = (overrides: Partial<Env> = {}): Env =>
  ({
    ODDS_ARCHIVE: buildR2(),
    ODDS_HOT_KV: buildKv(),
    ...overrides,
  }) as Env;

it("builds deterministic R2 keys for live payload, snapshots, and catalog staging", () => {
  expect(buildLiveOddsR2Key("nar:2026:0708:45:12")).toBe(
    "odds-live/v1/nar/20260708/nar:2026:0708:45:12/payload.json",
  );
  expect(buildSnapshotOddsR2Key("nar:2026:0708:45:12", "2026-07-08T18:01:28+09:00")).toBe(
    "odds-snapshots/v1/nar/20260708/nar:2026:0708:45:12/2026-07-08T18:01:28_09:00.json",
  );
  expect(buildCatalogStagingR2Key("nar:2026:0708:45:12", "2026-07-08T18:01:28+09:00")).toBe(
    "odds-catalog-staging/v1/kaisai_yyyymmdd=20260708/nar:2026:0708:45:12/2026-07-08T18:01:28_09:00.ndjson",
  );
});

it("writeOddsPayloadToR2 writes canonical payload, raw snapshot, catalog staging, and KV pointer", async () => {
  const catalogSend = vi.fn(async () => undefined);
  const env = buildEnv({ ODDS_CATALOG_STREAM: { send: catalogSend } });
  const payload = await writeOddsPayloadToR2(
    env,
    "nar:2026:0708:45:12",
    "2026-07-08T18:01:28+09:00",
    {
      tansho: [{ combination: "01", odds: 2.5, rank: 1 }],
    },
  );
  const putMock = env.ODDS_ARCHIVE.put as unknown as ReturnType<typeof vi.fn>;
  expect(payload.history).toStrictEqual([
    {
      horseNumber: "01",
      points: [
        {
          fetchedAt: "2026-07-08T18:01:28+09:00",
          horseNumber: "01",
          odds: 2.5,
          popularity: 1,
        },
      ],
    },
  ]);
  expect(putMock).toHaveBeenCalledTimes(3);
  expect(catalogSend).toHaveBeenCalledWith([
    {
      average_odds: null,
      combination: "01",
      fetched_at: "2026-07-08T18:01:28+09:00",
      kaisai_yyyymmdd: "20260708",
      max_odds: null,
      min_odds: null,
      odds: 2.5,
      odds_type: "tansho",
      race_key: "nar:2026:0708:45:12",
      rank: 1,
      source: "nar",
    },
  ]);
  expect(env.ODDS_HOT_KV.put).toHaveBeenCalledWith(
    "odds:r2:payload:nar:2026:0708:45:12",
    "odds-live/v1/nar/20260708/nar:2026:0708:45:12/payload.json",
    { expirationTtl: 172800 },
  );
});

it("readOddsPayloadFromR2 uses the KV pointer and rejects mismatched race payloads", async () => {
  const kv = buildKv();
  const getMock = kv.get as unknown as ReturnType<typeof vi.fn>;
  getMock.mockResolvedValueOnce("custom/payload.json");
  const env = buildEnv({
    ODDS_ARCHIVE: buildR2({
      fetchedAt: "2026-07-08T18:01:28+09:00",
      historyByType: {},
      latest: {},
      raceKey: "nar:2026:0708:45:11",
      schemaVersion: 1,
    }),
    ODDS_HOT_KV: kv,
  });
  expect(await readOddsPayloadFromR2(env, "nar:2026:0708:45:12")).toBeNull();
});

it("readOddsPayloadFromR2 returns null when R2 get fails", async () => {
  const env = buildEnv({
    ODDS_ARCHIVE: {
      ...buildR2(),
      get: vi.fn(async () => {
        throw new Error("r2 down");
      }),
    } as unknown as R2Bucket,
  });
  expect(await readOddsPayloadFromR2(env, "nar:2026:0708:45:12")).toBeNull();
});

it("writeOddsPayloadToR2 merges existing history and honors pointer TTL override", async () => {
  const env = buildEnv({
    ODDS_ARCHIVE: buildR2({
      fetchedAt: "2026-07-08T18:00:00+09:00",
      historyByType: {
        tansho: [
          {
            combination: "01",
            fetchedAt: "2026-07-08T18:00:00+09:00",
            odds: 2.8,
            rank: 2,
          },
        ],
      },
      latest: { tansho: [{ combination: "01", odds: 2.8, rank: 2 }] },
      raceKey: "nar:2026:0708:45:12",
      schemaVersion: 1,
    }),
    ODDS_R2_POINTER_KV_TTL_SECONDS: "3600",
  });
  const payload = await writeOddsPayloadToR2(
    env,
    "nar:2026:0708:45:12",
    "2026-07-08T18:01:28+09:00",
    {
      tansho: [{ combination: "01", odds: 2.5, rank: 1 }],
    },
  );
  expect(payload.history[0]?.points).toStrictEqual([
    {
      fetchedAt: "2026-07-08T18:00:00+09:00",
      horseNumber: "01",
      odds: 2.8,
      popularity: 2,
    },
    {
      fetchedAt: "2026-07-08T18:01:28+09:00",
      horseNumber: "01",
      odds: 2.5,
      popularity: 1,
    },
  ]);
  expect(env.ODDS_HOT_KV.put).toHaveBeenCalledWith(
    "odds:r2:payload:nar:2026:0708:45:12",
    "odds-live/v1/nar/20260708/nar:2026:0708:45:12/payload.json",
    { expirationTtl: 3600 },
  );
});
