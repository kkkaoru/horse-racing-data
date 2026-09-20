// Run with bun via the package Vitest scripts.
import { beforeEach, expect, test, vi } from "vitest";

import type { CatalogRaceDetailBinding } from "../lib/race-detail-catalog";

const mocks = vi.hoisted(() => ({
  getDb: vi.fn<() => unknown>(),
  fetch: vi.fn<(request: Request) => Promise<Response>>(),
  env: vi.fn<() => Promise<{ R2_CATALOG?: CatalogRaceDetailBinding } | null>>(),
}));
vi.mock("server-only", () => ({}));
vi.mock("./client", () => ({ getDatabaseTarget: () => "local", getDb: mocks.getDb }));
vi.mock("../lib/cloudflare-context.server", () => ({ safeGetCloudflareEnv: mocks.env }));

import { getRaceCourseInfo } from "./queries";

beforeEach(() => {
  vi.clearAllMocks();
  mocks.env.mockResolvedValue({ R2_CATALOG: { fetch: mocks.fetch } });
  mocks.fetch.mockResolvedValue(
    Response.json({ course: null }, { headers: { "Cache-Control": "no-store" } }),
  );
});

test("reads course data through the Catalog binding without PostgreSQL", async () => {
  mocks.fetch.mockResolvedValueOnce(
    Response.json(
      { course: { courseKaishuNengappi: "20240106", courseSetsumei: "コース説明" } },
      { headers: { "Cache-Control": "no-store" } },
    ),
  );
  await expect(getRaceCourseInfo("05", "1600", "11")).resolves.toStrictEqual({
    courseKaishuNengappi: "20240106",
    courseSetsumei: "コース説明",
  });
  expect(mocks.fetch).toHaveBeenCalledTimes(1);
  expect(mocks.getDb).not.toHaveBeenCalled();
});

test("returns null for an absent course", async () => {
  await expect(getRaceCourseInfo("05", "1600", "11")).resolves.toBeNull();
  expect(mocks.getDb).not.toHaveBeenCalled();
});

test.each([
  { distance: null, track: "11" },
  { distance: "1600", track: undefined },
])("does not fetch for missing course identity: %j", async ({ distance, track }) => {
  await expect(getRaceCourseInfo("05", distance, track)).resolves.toBeNull();
  expect(mocks.fetch).not.toHaveBeenCalled();
  expect(mocks.getDb).not.toHaveBeenCalled();
});

test.each([null, {}])("fails closed when the Catalog binding is absent: %j", async (env) => {
  mocks.env.mockResolvedValueOnce(env);
  await expect(getRaceCourseInfo("05", "1600", "11")).rejects.toThrow("Catalog course unavailable");
  expect(mocks.getDb).not.toHaveBeenCalled();
});

test("never falls back to PostgreSQL on an upstream error", async () => {
  mocks.fetch.mockRejectedValueOnce(new Error("upstream failure"));
  await expect(getRaceCourseInfo("05", "1600", "11")).rejects.toThrow("Catalog course unavailable");
  expect(mocks.getDb).not.toHaveBeenCalled();
});
