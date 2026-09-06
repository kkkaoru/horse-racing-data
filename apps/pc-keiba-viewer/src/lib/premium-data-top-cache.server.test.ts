import { afterEach, expect, it, vi } from "vitest";
vi.mock("server-only", () => ({}));
vi.mock("./cloudflare-context.server", () => ({
  safeGetCloudflareRuntime: async () => ({ env: null }),
}));
import { getPremiumDataTopHorsesWithCache } from "./premium-data-top-cache.server";

const race = {
  source: "jra",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0906",
  keibajoCode: "01",
  raceBango: "01",
} satisfies Parameters<typeof getPremiumDataTopHorsesWithCache>[0];
afterEach(() => vi.unstubAllGlobals());

it("propagates upstream HTTP errors rather than displaying a successful empty section", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(null, { status: 503 })));
  await expect(getPremiumDataTopHorsesWithCache(race)).rejects.toThrow(
    "Premium data-top source failed with HTTP 503",
  );
});
it("propagates network failure for the section retry path", async () => {
  vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("network unavailable")));
  await expect(getPremiumDataTopHorsesWithCache(race)).rejects.toThrow("network unavailable");
});
it.each([
  null,
  {},
  { dataTopHorses: "bad" },
  { dataTopHorses: [null] },
  { dataTopHorses: [{ horseNumber: "1" }] },
])("rejects malformed source payload %j", async (payload) => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
  await expect(getPremiumDataTopHorsesWithCache(race)).rejects.toThrow(
    "Invalid premium data-top source payload",
  );
});
it("preserves a genuinely unpublished empty list", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json({ dataTopHorses: [] })));
  await expect(getPremiumDataTopHorsesWithCache(race)).resolves.toStrictEqual([]);
});
it("returns the fetched picks and reasons", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue(
      Response.json({
        dataTopHorses: [
          {
            horseNumber: "2",
            rank: 1,
            fetchedAt: "2026-09-06T01:00:00Z",
            reasons: ["コース実績"],
          },
        ],
      }),
    ),
  );
  await expect(getPremiumDataTopHorsesWithCache(race)).resolves.toStrictEqual([
    { horseNumber: "2", rank: 1, fetchedAt: "2026-09-06T01:00:00Z", reasons: ["コース実績"] },
  ]);
});
