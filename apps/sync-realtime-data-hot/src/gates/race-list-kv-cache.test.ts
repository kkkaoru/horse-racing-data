// Run with bun.
import { expect, it, vi } from "vitest";

import {
  getRaceListFromKv,
  invalidateRaceListInKv,
  patchLastFetchInKv,
  putRaceListToKv,
} from "./race-list-kv-cache";
import type { Env, RaceListEntry } from "../types";

interface KvMockHandle {
  get: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
  delete: ReturnType<typeof vi.fn>;
}

const buildEnv = (overrides: Partial<Env> = {}): Env => {
  const kv = {
    delete: vi.fn(async () => undefined),
    get: vi.fn(async () => null),
    put: vi.fn(async () => undefined),
  } as unknown as KVNamespace;
  return {
    ODDS_HOT_KV: kv,
    ...overrides,
  } as Env;
};

const sampleList = (): RaceListEntry[] => [
  {
    lastOddsFetchAt: null,
    raceKey: "nar:20260528:42:01",
    raceStartAtJst: "2026-05-28T10:00:00+09:00",
    source: "nar",
  },
];

it("getRaceListFromKv returns parsed list on hit", async () => {
  const env = buildEnv();
  const getMock = env.ODDS_HOT_KV.get as unknown as KvMockHandle["get"];
  getMock.mockResolvedValueOnce(JSON.stringify(sampleList()));
  const result = await getRaceListFromKv(env, "nar", "20260528");
  expect(getMock).toHaveBeenCalledWith("odds:race-list:v1:nar:20260528");
  expect(result).toStrictEqual([
    {
      lastOddsFetchAt: null,
      raceKey: "nar:20260528:42:01",
      raceStartAtJst: "2026-05-28T10:00:00+09:00",
      source: "nar",
    },
  ]);
});

it("getRaceListFromKv returns null on miss", async () => {
  const env = buildEnv();
  const result = await getRaceListFromKv(env, "jra", "20260528");
  expect(result).toBeNull();
});

it("putRaceListToKv uses default TTL when env unset", async () => {
  const env = buildEnv();
  await putRaceListToKv(env, "nar", "20260528", sampleList());
  expect(env.ODDS_HOT_KV.put).toHaveBeenCalledWith(
    "odds:race-list:v1:nar:20260528",
    JSON.stringify(sampleList()),
    { expirationTtl: 21600 },
  );
});

it("putRaceListToKv honors env override TTL", async () => {
  const env = buildEnv({ ODDS_RACE_LIST_KV_TTL_SECONDS: "3600" });
  await putRaceListToKv(env, "jra", "20260528", []);
  expect(env.ODDS_HOT_KV.put).toHaveBeenCalledWith("odds:race-list:v1:jra:20260528", "[]", {
    expirationTtl: 3600,
  });
});

it("putRaceListToKv falls back to default when env value is NaN", async () => {
  const env = buildEnv({ ODDS_RACE_LIST_KV_TTL_SECONDS: "not-a-number" });
  await putRaceListToKv(env, "nar", "20260528", []);
  expect(env.ODDS_HOT_KV.put).toHaveBeenCalledWith("odds:race-list:v1:nar:20260528", "[]", {
    expirationTtl: 21600,
  });
});

it("putRaceListToKv falls back to default when env value is zero", async () => {
  const env = buildEnv({ ODDS_RACE_LIST_KV_TTL_SECONDS: "0" });
  await putRaceListToKv(env, "nar", "20260528", []);
  expect(env.ODDS_HOT_KV.put).toHaveBeenCalledWith("odds:race-list:v1:nar:20260528", "[]", {
    expirationTtl: 21600,
  });
});

it("invalidateRaceListInKv calls delete with correct key", async () => {
  const env = buildEnv();
  await invalidateRaceListInKv(env, "nar", "20260528");
  expect(env.ODDS_HOT_KV.delete).toHaveBeenCalledWith("odds:race-list:v1:nar:20260528");
});

it("patchLastFetchInKv updates field when race exists", async () => {
  const env = buildEnv();
  const getMock = env.ODDS_HOT_KV.get as unknown as KvMockHandle["get"];
  getMock.mockResolvedValueOnce(JSON.stringify(sampleList()));
  const result = await patchLastFetchInKv(
    env,
    "nar",
    "20260528",
    "nar:20260528:42:01",
    "2026-05-28T10:05:00+09:00",
  );
  expect(result).toBe(true);
  expect(env.ODDS_HOT_KV.put).toHaveBeenCalledWith(
    "odds:race-list:v1:nar:20260528",
    JSON.stringify([
      {
        lastOddsFetchAt: "2026-05-28T10:05:00+09:00",
        raceKey: "nar:20260528:42:01",
        raceStartAtJst: "2026-05-28T10:00:00+09:00",
        source: "nar",
      },
    ]),
    { expirationTtl: 21600 },
  );
});

it("patchLastFetchInKv returns false when list is missing", async () => {
  const env = buildEnv();
  const result = await patchLastFetchInKv(
    env,
    "nar",
    "20260528",
    "nar:20260528:42:01",
    "2026-05-28T10:05:00+09:00",
  );
  expect(result).toBe(false);
});

it("patchLastFetchInKv returns false when race not in list", async () => {
  const env = buildEnv();
  const getMock = env.ODDS_HOT_KV.get as unknown as KvMockHandle["get"];
  getMock.mockResolvedValueOnce(JSON.stringify(sampleList()));
  const result = await patchLastFetchInKv(
    env,
    "nar",
    "20260528",
    "nar:20260528:99:99",
    "2026-05-28T10:05:00+09:00",
  );
  expect(result).toBe(false);
  expect(env.ODDS_HOT_KV.put).not.toHaveBeenCalled();
});
