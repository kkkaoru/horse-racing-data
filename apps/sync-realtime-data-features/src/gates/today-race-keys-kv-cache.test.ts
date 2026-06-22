// Run with: bun run --filter sync-realtime-data-features test
import { expect, it, vi } from "vitest";

import { getTodayRaceKeysFromKv, putTodayRaceKeysToKv } from "./today-race-keys-kv-cache";
import type { TodayRaceKey } from "../scheduled-race-list";
import type { Env } from "../types";

interface MockKv {
  get: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
}

interface BuildEnvArgs {
  ttl?: string;
}

const buildEnv = (args: BuildEnvArgs): { env: Env; kv: MockKv } => {
  const kv: MockKv = { get: vi.fn(), put: vi.fn() };
  return {
    env: {
      FEATURES_KV: kv as unknown as KVNamespace,
      FEATURES_TODAY_RACE_KEYS_KV_TTL_SECONDS: args.ttl,
    } as unknown as Env,
    kv,
  };
};

it("getTodayRaceKeysFromKv returns parsed array when KV has entry", async () => {
  const { env, kv } = buildEnv({});
  const stored: TodayRaceKey[] = [
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "08",
      raceBango: "01",
      raceKey: "jra:2026:0529:08:01",
      source: "jra",
    },
  ];
  kv.get.mockResolvedValueOnce(JSON.stringify(stored));
  const list = await getTodayRaceKeysFromKv(env, "jra", "20260529");
  expect(list).toStrictEqual([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "08",
      raceBango: "01",
      raceKey: "jra:2026:0529:08:01",
      source: "jra",
    },
  ]);
});

it("getTodayRaceKeysFromKv returns null when KV miss", async () => {
  const { env, kv } = buildEnv({});
  kv.get.mockResolvedValueOnce(null);
  await expect(getTodayRaceKeysFromKv(env, "nar", "20260529")).resolves.toBeNull();
});

it("getTodayRaceKeysFromKv requests the per-source key", async () => {
  const { env, kv } = buildEnv({});
  kv.get.mockResolvedValueOnce(null);
  await getTodayRaceKeysFromKv(env, "nar", "20260529");
  expect(kv.get).toHaveBeenCalledWith("race-keys:v1:nar:20260529");
});

it("putTodayRaceKeysToKv writes with default 1800s TTL when env var unset", async () => {
  const { env, kv } = buildEnv({});
  await putTodayRaceKeysToKv(env, "jra", "20260529", []);
  expect(kv.put).toHaveBeenCalledWith("race-keys:v1:jra:20260529", "[]", {
    expirationTtl: 1800,
  });
});

it("putTodayRaceKeysToKv honors env-supplied TTL when numeric", async () => {
  const { env, kv } = buildEnv({ ttl: "600" });
  await putTodayRaceKeysToKv(env, "nar", "20260529", []);
  expect(kv.put).toHaveBeenCalledWith("race-keys:v1:nar:20260529", "[]", {
    expirationTtl: 600,
  });
});

it("putTodayRaceKeysToKv falls back to default TTL when env value is non-numeric", async () => {
  const { env, kv } = buildEnv({ ttl: "not-a-number" });
  await putTodayRaceKeysToKv(env, "jra", "20260529", []);
  expect(kv.put).toHaveBeenCalledWith("race-keys:v1:jra:20260529", "[]", {
    expirationTtl: 1800,
  });
});

it("putTodayRaceKeysToKv falls back to default TTL when env value is zero", async () => {
  const { env, kv } = buildEnv({ ttl: "0" });
  await putTodayRaceKeysToKv(env, "nar", "20260529", []);
  expect(kv.put).toHaveBeenCalledWith("race-keys:v1:nar:20260529", "[]", {
    expirationTtl: 1800,
  });
});

it("putTodayRaceKeysToKv serialises the race key array", async () => {
  const { env, kv } = buildEnv({});
  const list: TodayRaceKey[] = [
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      raceBango: "08",
      raceKey: "nar:2026:0529:30:08",
      source: "nar",
    },
  ];
  await putTodayRaceKeysToKv(env, "nar", "20260529", list);
  expect(kv.put).toHaveBeenCalledWith(
    "race-keys:v1:nar:20260529",
    '[{"kaisaiNen":"2026","kaisaiTsukihi":"0529","keibajoCode":"30","raceBango":"08","raceKey":"nar:2026:0529:30:08","source":"nar"}]',
    { expirationTtl: 1800 },
  );
});
