// Run with bun.
import type { D1Database, KVNamespace } from "@cloudflare/workers-types";
import { expect, it, vi } from "vitest";

import type { Env } from "../types";

import { shouldRunOddsCron } from "./polling-window-gate";

interface BuildEnvOptions {
  kvGet: ReturnType<typeof vi.fn>;
  kvPut?: ReturnType<typeof vi.fn>;
  dbFirst?: ReturnType<typeof vi.fn>;
}

const KV_KEY = "odds-polling-window:active";
const TTL_SECONDS = 60;
const NOW_ISO = "2026-06-07T05:00:00.000Z";
// `now - 30min` and `now + 3h` for assertions against the bound ISO strings.
const FROM_ISO = "2026-06-07T04:30:00.000Z";
const TO_ISO = "2026-06-07T08:00:00.000Z";

const buildEnv = (options: BuildEnvOptions): Env => {
  const dbFirst = options.dbFirst ?? vi.fn(async () => null);
  const dbBind = vi.fn(() => ({ first: dbFirst }));
  const dbPrepare = vi.fn(() => ({ bind: dbBind }));
  const kvDelete = vi.fn(async () => undefined);
  const kvPut = options.kvPut ?? vi.fn(async () => undefined);
  const kv = {
    delete: kvDelete,
    get: options.kvGet,
    put: kvPut,
  } as unknown as KVNamespace;
  const db = { prepare: dbPrepare } as unknown as D1Database;
  return {
    ODDS_HOT_KV: kv,
    REALTIME_HOT_DB: db,
  } as unknown as Env;
};

it("returns true and skips D1 when KV cache hits true", async () => {
  const kvGet = vi.fn(async () => "true");
  const dbFirst = vi.fn(async () => ({ "1": 1 }));
  const env = buildEnv({ dbFirst, kvGet });
  const result = await shouldRunOddsCron(env, new Date(NOW_ISO));
  expect(result).toBe(true);
  expect(kvGet).toHaveBeenCalledWith(KV_KEY);
  expect(dbFirst).not.toHaveBeenCalled();
});

it("returns false and skips D1 when KV cache hits false", async () => {
  const kvGet = vi.fn(async () => "false");
  const dbFirst = vi.fn(async () => ({ "1": 1 }));
  const env = buildEnv({ dbFirst, kvGet });
  const result = await shouldRunOddsCron(env, new Date(NOW_ISO));
  expect(result).toBe(false);
  expect(dbFirst).not.toHaveBeenCalled();
});

it("queries D1 on cache miss, returns true and writes 'true' when a race row exists", async () => {
  const kvGet = vi.fn(async () => null);
  const kvPut = vi.fn(async () => undefined);
  const dbFirst = vi.fn(async () => ({ "1": 1 }));
  const env = buildEnv({ dbFirst, kvGet, kvPut });
  const result = await shouldRunOddsCron(env, new Date(NOW_ISO));
  expect(result).toBe(true);
  expect(kvPut).toHaveBeenCalledWith(KV_KEY, "true", { expirationTtl: TTL_SECONDS });
});

it("queries D1 on cache miss, returns false and writes 'false' when no race row exists", async () => {
  const kvGet = vi.fn(async () => null);
  const kvPut = vi.fn(async () => undefined);
  const dbFirst = vi.fn(async () => null);
  const env = buildEnv({ dbFirst, kvGet, kvPut });
  const result = await shouldRunOddsCron(env, new Date(NOW_ISO));
  expect(result).toBe(false);
  expect(kvPut).toHaveBeenCalledWith(KV_KEY, "false", { expirationTtl: TTL_SECONDS });
});

it("binds [now - 30min, now + 3h] as ISO 8601 strings for the D1 query", async () => {
  const kvGet = vi.fn(async () => null);
  const dbFirst = vi.fn(async () => null);
  const dbBind = vi.fn(() => ({ first: dbFirst }));
  const dbPrepare = vi.fn(() => ({ bind: dbBind }));
  const kv = {
    delete: vi.fn(async () => undefined),
    get: kvGet,
    put: vi.fn(async () => undefined),
  } as unknown as KVNamespace;
  const db = { prepare: dbPrepare } as unknown as D1Database;
  const env = { ODDS_HOT_KV: kv, REALTIME_HOT_DB: db } as unknown as Env;
  await shouldRunOddsCron(env, new Date(NOW_ISO));
  expect(dbBind).toHaveBeenCalledWith(FROM_ISO, TO_ISO);
});

it("treats unrecognized cached strings as a miss and falls through to D1", async () => {
  const kvGet = vi.fn(async () => "unexpected");
  const kvPut = vi.fn(async () => undefined);
  const dbFirst = vi.fn(async () => null);
  const env = buildEnv({ dbFirst, kvGet, kvPut });
  const result = await shouldRunOddsCron(env, new Date(NOW_ISO));
  expect(result).toBe(false);
  expect(dbFirst).toHaveBeenCalledTimes(1);
});
