// Run with bun. Exercises manifest SQL against SQLite including owner takeover races.

import { DatabaseSync } from "node:sqlite";
import { afterEach, expect, test, vi } from "vitest";
import {
  getDayBaseManifest,
  getDayBaseSource,
  publishDayBaseManifest,
  stageDayBaseManifest,
} from "./day-base-manifest";
import type { Env } from "./types";

const stores: DatabaseSync[] = [];

afterEach(() => {
  stores.splice(0).forEach((store) => store.close());
});

const fixture = () => {
  const sqlite = new DatabaseSync(":memory:");
  stores.push(sqlite);
  sqlite.exec(`create table finish_position_day_base_repair_requests (
    category text not null, run_ymd text not null, requested_at text not null,
    repair_generation_id text, primary key(category, run_ymd));
    create table finish_position_day_base_manifests (
      category text not null, run_ymd text not null, repair_generation_id text not null,
      status text not null check(status in ('pending','published')),
      object_key text, object_etag text, watermark_json text, updated_at text not null,
      primary key(category, run_ymd),
      check(status = 'pending' or (object_key is not null and object_etag is not null and watermark_json is not null)));`);
  const db = {
    prepare: (sql: string) => ({
      bind: (...values: (string | null)[]) => ({
        first: async <T>(): Promise<T | null> =>
          (sqlite.prepare(sql).get(...values) ?? null) as T | null,
        run: async () => ({
          meta: { changes: Number(sqlite.prepare(sql).run(...values).changes) },
        }),
      }),
    }),
  } as unknown as D1Database;
  const head = vi.fn(
    async (
      _key: string,
    ): Promise<{ etag: string; customMetadata: Record<string, string> } | null> => null,
  );
  const env = {
    DAY_BASE_MANIFEST_REQUIRED: "0",
    FEATURES_CACHE: { head },
    FINISH_POSITION_CRON_DB: db,
  } as unknown as Env;
  const owner = (generationId: string) =>
    sqlite
      .prepare(`insert into finish_position_day_base_repair_requests
        (category,run_ymd,requested_at,repair_generation_id) values ('nar','20260923','now',?1)
        on conflict(category,run_ymd) do update set repair_generation_id=excluded.repair_generation_id`)
      .run(generationId);
  const scope = { category: "nar" as const, env, runYmd: "20260923" };
  const watermark = {
    maxDataSakuseiNengappi: "2026-09-23T00:00:00Z",
    rowCount: 123,
    rsContentHash: "rs-hash",
    rsPredictedAtMax: "2026-09-23T00:00:00Z",
    rsRowCount: 123,
  };
  return { env, head, owner, scope, sqlite, watermark };
};

test("a late old owner cannot publish or become visible after a new owner takes over", async () => {
  const { owner, scope, watermark } = fixture();
  owner("old-owner");
  await expect(stageDayBaseManifest({ ...scope, generationId: "old-owner" })).resolves.toBe(true);
  await expect(
    publishDayBaseManifest({ ...scope, etag: "old-etag", generationId: "old-owner", watermark }),
  ).resolves.toBe(true);
  await expect(getDayBaseManifest(scope)).resolves.toStrictEqual({
    etag: "old-etag",
    generationId: "old-owner",
    key: "feat-daybase/catalog-v1/nar/20260923/generations/old-owner/features.parquet",
    state: "published",
    watermark,
  });

  owner("new-owner");
  await expect(getDayBaseManifest(scope)).resolves.toStrictEqual({ state: "pending" });
  await expect(stageDayBaseManifest({ ...scope, generationId: "new-owner" })).resolves.toBe(true);
  await expect(
    publishDayBaseManifest({ ...scope, etag: "late-old", generationId: "old-owner", watermark }),
  ).resolves.toBe(false);
  await expect(getDayBaseManifest(scope)).resolves.toStrictEqual({ state: "pending" });
  await expect(
    publishDayBaseManifest({ ...scope, etag: "new-etag", generationId: "new-owner", watermark }),
  ).resolves.toBe(true);
  await expect(getDayBaseManifest(scope)).resolves.toStrictEqual({
    etag: "new-etag",
    generationId: "new-owner",
    key: "feat-daybase/catalog-v1/nar/20260923/generations/new-owner/features.parquet",
    state: "published",
    watermark,
  });
});

test("a delayed old R2 upload cannot replace a newer published day-base", async () => {
  const { head, owner, scope, watermark } = fixture();
  const oldKey = "feat-daybase/catalog-v1/nar/20260923/generations/old-owner/features.parquet";
  const newKey = "feat-daybase/catalog-v1/nar/20260923/generations/new-owner/features.parquet";
  const objects = new Map<string, { etag: string; customMetadata: Record<string, string> }>();
  head.mockImplementation(async (key: string) => objects.get(key) ?? null);
  const metadata = {
    "max-data-sakusei-nengappi": "2026-09-23T00:00:00Z",
    "row-count": "123",
    "rs-content-hash": "rs-hash",
    "rs-predicted-at-max": "2026-09-23T00:00:00Z",
    "rs-row-count": "123",
  };
  owner("old-owner");
  await stageDayBaseManifest({ ...scope, generationId: "old-owner" });
  owner("new-owner");
  await stageDayBaseManifest({ ...scope, generationId: "new-owner" });
  objects.set(newKey, { etag: "new-etag", customMetadata: metadata });
  await expect(
    publishDayBaseManifest({ ...scope, etag: "new-etag", generationId: "new-owner", watermark }),
  ).resolves.toBe(true);
  // Simulate the old PUT completing after the current owner has committed.
  objects.set(oldKey, { etag: "old-etag", customMetadata: metadata });
  await expect(
    publishDayBaseManifest({ ...scope, etag: "old-etag", generationId: "old-owner", watermark }),
  ).resolves.toBe(false);
  await expect(getDayBaseSource(scope)).resolves.toStrictEqual({
    key: newKey,
    object: { etag: "new-etag", customMetadata: metadata },
  });
  expect(head).toHaveBeenCalledWith(newKey);
  expect(head).not.toHaveBeenCalledWith(oldKey);
  expect(head).not.toHaveBeenCalledWith("feat-daybase/catalog-v1/nar/20260923/features.parquet");
});

test("same owner replay preserves published pointer but a released owner cannot republish", async () => {
  const { owner, scope, sqlite, watermark } = fixture();
  owner("owner-1");
  await expect(stageDayBaseManifest({ ...scope, generationId: "owner-1" })).resolves.toBe(true);
  await expect(stageDayBaseManifest({ ...scope, generationId: "owner-1" })).resolves.toBe(true);
  await expect(
    publishDayBaseManifest({ ...scope, etag: "etag-1", generationId: "owner-1", watermark }),
  ).resolves.toBe(true);
  await expect(stageDayBaseManifest({ ...scope, generationId: "owner-1" })).resolves.toBe(true);
  await expect(getDayBaseManifest(scope)).resolves.toStrictEqual({
    etag: "etag-1",
    generationId: "owner-1",
    key: "feat-daybase/catalog-v1/nar/20260923/generations/owner-1/features.parquet",
    state: "published",
    watermark,
  });
  sqlite.exec("delete from finish_position_day_base_repair_requests");
  await expect(stageDayBaseManifest({ ...scope, generationId: "owner-1" })).resolves.toBe(false);
  await expect(
    publishDayBaseManifest({ ...scope, etag: "new-etag", generationId: "owner-1", watermark }),
  ).resolves.toBe(false);
});

test("corrupted manifest watermarks fail closed instead of sending readers to a stale object", async () => {
  const { owner, scope, sqlite, watermark } = fixture();
  owner("owner-1");
  await stageDayBaseManifest({ ...scope, generationId: "owner-1" });
  await publishDayBaseManifest({ ...scope, etag: "etag-1", generationId: "owner-1", watermark });
  sqlite.exec("update finish_position_day_base_manifests set watermark_json = 'not-json'");
  await expect(getDayBaseManifest(scope)).resolves.toStrictEqual({ state: "pending" });
  sqlite.exec("update finish_position_day_base_manifests set watermark_json = '[]'");
  await expect(getDayBaseManifest(scope)).resolves.toStrictEqual({ state: "pending" });
  sqlite.exec("update finish_position_day_base_manifests set watermark_json = '{\"rowCount\": 1}'");
  await expect(getDayBaseManifest(scope)).resolves.toStrictEqual({ state: "pending" });
  sqlite.exec(
    "update finish_position_day_base_manifests set object_key = 'wrong', watermark_json = '{}'",
  );
  await expect(getDayBaseManifest(scope)).resolves.toStrictEqual({ state: "pending" });
  sqlite.exec(`update finish_position_day_base_manifests
    set object_key = 'feat-daybase/catalog-v1/nar/20260923/generations/owner-1/features.parquet',
        object_etag = '',
        watermark_json = '{"maxDataSakuseiNengappi":"now","rowCount":1,"rsPredictedAtMax":"now","rsRowCount":1}'`);
  await expect(getDayBaseManifest(scope)).resolves.toStrictEqual({ state: "pending" });
});

test("readers honor manifest pointer, R2 ETag and watermark; only absent pre-cutover days use legacy", async () => {
  const { env, head, owner, scope, watermark } = fixture();
  head.mockResolvedValueOnce({ etag: "legacy", customMetadata: {} });
  await expect(getDayBaseSource(scope)).resolves.toStrictEqual({
    key: "feat-daybase/catalog-v1/nar/20260923/features.parquet",
    object: { etag: "legacy", customMetadata: {} },
  });
  await expect(getDayBaseSource(scope)).resolves.toBeNull();
  env.DAY_BASE_MANIFEST_REQUIRED = "1";
  await expect(getDayBaseSource(scope)).resolves.toBeNull();
  expect(head).toHaveBeenCalledTimes(2);

  owner("owner-1");
  await expect(getDayBaseSource(scope)).resolves.toBeNull();
  await stageDayBaseManifest({ ...scope, generationId: "owner-1" });
  await publishDayBaseManifest({
    ...scope,
    etag: "owner-etag",
    generationId: "owner-1",
    watermark,
  });
  head.mockResolvedValueOnce({
    etag: "owner-etag",
    customMetadata: {
      "max-data-sakusei-nengappi": "2026-09-23T00:00:00Z",
      "row-count": "123",
      "rs-content-hash": "rs-hash",
      "rs-predicted-at-max": "2026-09-23T00:00:00Z",
      "rs-row-count": "123",
    },
  });
  await expect(getDayBaseSource(scope)).resolves.toStrictEqual({
    key: "feat-daybase/catalog-v1/nar/20260923/generations/owner-1/features.parquet",
    object: {
      etag: "owner-etag",
      customMetadata: {
        "max-data-sakusei-nengappi": "2026-09-23T00:00:00Z",
        "row-count": "123",
        "rs-content-hash": "rs-hash",
        "rs-predicted-at-max": "2026-09-23T00:00:00Z",
        "rs-row-count": "123",
      },
    },
  });
  head.mockResolvedValueOnce({ etag: "older-etag", customMetadata: {} });
  await expect(getDayBaseSource(scope)).resolves.toBeNull();
  head.mockResolvedValueOnce({ etag: "owner-etag", customMetadata: {} });
  await expect(getDayBaseSource(scope)).resolves.toBeNull();
});

test("a reservation without a manifest fails closed and an absent day has no pointer", async () => {
  const { owner, scope } = fixture();
  await expect(getDayBaseManifest(scope)).resolves.toStrictEqual({ state: "missing" });
  owner("new-owner");
  await expect(getDayBaseManifest(scope)).resolves.toStrictEqual({ state: "pending" });
  await expect(stageDayBaseManifest({ ...scope, generationId: "stale-owner" })).resolves.toBe(
    false,
  );
  await expect(getDayBaseManifest(scope)).resolves.toStrictEqual({ state: "pending" });
});
