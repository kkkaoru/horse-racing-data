// Run with bun (bunx vitest).
import { beforeEach, expect, it, vi } from "vitest";

import type { WinRateHeatmapCatalogQuery } from "./win-rate-heatmap-catalog.server";

vi.mock("server-only", () => ({}));
const { envMock } = vi.hoisted(() => ({ envMock: vi.fn<() => Promise<CloudflareEnv | null>>() }));
vi.mock("./cloudflare-context.server", () => ({ safeGetCloudflareEnv: envMock }));
import { fetchHeatmapPartnershipRows } from "./win-rate-heatmap-partnership.server";

const query: WinRateHeatmapCatalogQuery = {
  day: "13",
  month: "09",
  year: "2026",
  keibajoCode: "06",
  raceNumber: "01",
  source: "jra",
  years: 10,
  includeDistance: true,
  includeSurface: true,
  includeTurn: true,
  includeVenue: true,
};
const row = {
  kind: "horseJockey",
  name: "Horse × Jockey",
  umaban: 1,
  starts: 6,
  wins: 1,
  places: 2,
  shows: 3,
};
const serve = (payload: unknown): void => {
  envMock.mockResolvedValue({
    R2_CATALOG: { fetch: vi.fn<typeof fetch>(async () => Response.json(payload)) },
  });
};

beforeEach(() => envMock.mockReset());

it("warms independent catalog stats and maps all rates without using similar-condition counts", async () => {
  const fetch = vi.fn<typeof globalThis.fetch>(async () =>
    Response.json({
      partnershipRows: [
        row,
        { ...row, kind: "jockeyVenue", starts: 0, wins: 0, places: 0, shows: 0 },
        { ...row, kind: "jockeyTrainerVenue" },
        { ...row, kind: "ownerVenue" },
        { ...row, kind: "jockeyTrainerOwner" },
      ],
    }),
  );
  envMock.mockResolvedValue({ R2_CATALOG: { fetch } });
  const rows = await fetchHeatmapPartnershipRows(query);
  expect(
    rows?.map((item) => [
      item.category,
      item.currentHorseNumbers,
      item.winRate,
      item.quinellaRate,
      item.showRate,
    ]),
  ).toStrictEqual([
    ["horseJockey", "1", 16.7, 33.3, 50],
    ["jockeyVenue", "1", 0, 0, 0],
    ["jockeyTrainerVenue", "1", 16.7, 33.3, 50],
    ["ownerVenue", "1", 16.7, 33.3, 50],
    ["jockeyTrainerOwner", "1", 16.7, 33.3, 50],
  ]);
  expect(fetch).toHaveBeenCalledWith(
    "https://pc-keiba-r2-catalog.internal/v1/heatmap-partnership-stats?year=2026&month=09&day=13&keibajoCode=06&raceNumber=01&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=1&warm=1",
  );
});

it("reports missing bindings and failed catalog warm requests", async () => {
  envMock.mockResolvedValue(null);
  expect(await fetchHeatmapPartnershipRows(query)).toBeNull();
  envMock.mockResolvedValue({});
  expect(await fetchHeatmapPartnershipRows(query)).toBeNull();
  envMock.mockResolvedValue({
    R2_CATALOG: { fetch: vi.fn<typeof fetch>(async () => new Response(null, { status: 503 })) },
  });
  await expect(fetchHeatmapPartnershipRows(query)).rejects.toThrow("request failed: 503");
});

it.each([null, {}, { partnershipRows: null }])(
  "rejects malformed envelopes: %j",
  async (payload) => {
    serve(payload);
    await expect(fetchHeatmapPartnershipRows(query)).rejects.toThrow("payload is malformed");
  },
);

it.each([null, { ...row, kind: "trainer" }, { ...row, name: null }])(
  "rejects malformed rows: %j",
  async (item) => {
    serve({ partnershipRows: [item] });
    await expect(fetchHeatmapPartnershipRows(query)).rejects.toThrow("row is malformed");
  },
);

it.each([null, "1", -1, 1.5])("rejects malformed counts: %j", async (starts) => {
  serve({ partnershipRows: [{ ...row, starts }] });
  await expect(fetchHeatmapPartnershipRows(query)).rejects.toThrow("count is malformed");
});

it.each([{ umaban: 0 }, { umaban: 100 }, { wins: 3 }, { places: 4 }, { shows: 7 }])(
  "rejects inconsistent counts: %j",
  async (override) => {
    serve({ partnershipRows: [{ ...row, ...override }] });
    await expect(fetchHeatmapPartnershipRows(query)).rejects.toThrow("counts are inconsistent");
  },
);
