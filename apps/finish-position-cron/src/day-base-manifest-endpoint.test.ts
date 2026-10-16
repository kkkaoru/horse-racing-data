import { expect, test, vi } from "vitest";
import { DAY_BASE_MANIFEST_PATH, handleDayBaseManifest } from "./day-base-manifest-endpoint";
import type { Env } from "./types";

const first = vi.fn(async () => ({
  state: "published",
  generation_id: "owner-1",
  object_key: "feat-daybase/catalog-v1/jra/20260923/generations/owner-1/features.parquet",
  object_etag: "source-etag",
  watermark_json: JSON.stringify({
    maxDataSakuseiNengappi: "20260923",
    rowCount: 12,
    rsPredictedAtMax: "2026-09-23T00:00:00Z",
    rsRowCount: 12,
  }),
}));
const env = {
  TRIGGER_TOKEN: "secret",
  FINISH_POSITION_CRON_DB: { prepare: vi.fn(() => ({ bind: () => ({ first }) })) },
} as unknown as Env;
const url = `https://finish-position-cron.internal${DAY_BASE_MANIFEST_PATH}?category=jra&runYmd=20260923`;

test("manifest endpoint requires the internal secret before querying D1", async () => {
  first.mockClear();
  const response = await handleDayBaseManifest(new Request(url), env);
  expect(response.status).toBe(401);
  expect(first).not.toHaveBeenCalled();
});

test("manifest endpoint rejects invalid scope and exposes only the committed pointer", async () => {
  first.mockClear();
  const headers = { Authorization: "Bearer secret" };
  const invalid = await handleDayBaseManifest(
    new Request(url.replace("20260923", "invalid"), { headers }),
    env,
  );
  expect(invalid.status).toBe(400);
  expect(first).not.toHaveBeenCalled();
  const response = await handleDayBaseManifest(new Request(url, { headers }), env);
  expect(response.status).toBe(200);
  expect(response.headers.get("Cache-Control")).toBe("no-store");
  expect(await response.json()).toMatchObject({
    key: "feat-daybase/catalog-v1/jra/20260923/generations/owner-1/features.parquet",
    state: "published",
  });
});
