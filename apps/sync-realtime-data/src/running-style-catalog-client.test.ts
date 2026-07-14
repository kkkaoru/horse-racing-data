// run with: bun run test
import { expect, it, vi } from "vitest";
import type { RunningStyleRaceParams } from "./running-style-features";

const RACE: RunningStyleRaceParams = {
  kaisaiNen: "2026",
  kaisaiTsukihi: "0715",
  keibajoCode: "5",
  raceBango: "1",
  source: "jra",
};

const validFeatureRow = (): Record<string, unknown> => ({
  bamei: "Test Horse",
  category: "jra",
  gradeCode: null,
  kaisaiNen: "2026",
  kaisaiTsukihi: "0715",
  keibajoCode: "05",
  kettoTorokuBango: "2023100001",
  kyori: 1600,
  kyosoJokenCode: "005",
  narSubClass: null,
  peerInputs: {
    careerWinRate: 0.2,
    kohan3fAvg5: null,
    pastCorner1NormAvg5: 0.3,
    pastFirst3fAvg5: 35.1,
    pastNigeRate: 0.1,
    pastOikomiRate: 0.2,
    pastSashiRate: 0.3,
    pastSenkouRate: 0.4,
    speedIndexAvg5: 88,
    speedIndexBest5: 91,
  },
  perHorseFeatures: { career_win_rate: 0.2, kohan3f_avg_5: null },
  raceBango: "01",
  raceKey: "jra:20260715:05:01",
  shussoTosu: 12,
  source: "jra",
  trackCode: "10",
  umaban: 1,
});

const catalogReturning = (payload: unknown, status = 200) => ({
  fetch: vi.fn<(request: Request) => Promise<Response>>(
    async () =>
      new Response(JSON.stringify(payload), {
        headers: { "Content-Type": "application/json" },
        status,
      }),
  ),
});

it("fetchRunningStyleFeaturesFromCatalog validates and maps the fixed generation", async () => {
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const catalog = catalogReturning({
    featureNames: ["career_win_rate", "kohan3f_avg_5"],
    generation: "raw-iceberg-v1",
    rows: [validFeatureRow()],
  });
  const rows = await fetchRunningStyleFeaturesFromCatalog(catalog, RACE, ["career_win_rate"]);
  expect(rows[0]?.raceKey).toBe("jra:20260715:05:01");
  expect(rows[0]?.perHorseFeatures).toStrictEqual({
    career_win_rate: 0.2,
    kohan3f_avg_5: null,
  });
  const request = catalog.fetch.mock.calls[0]?.[0];
  expect(request?.url).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/running-style-features?date=20260715&source=jra&keibajoCode=05&raceBango=01",
  );
});

it("rejects Catalog HTTP errors", async () => {
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  await expect(
    fetchRunningStyleFeaturesFromCatalog(catalogReturning({}, 502), RACE, ["f1"]),
  ).rejects.toThrow("failed with HTTP 502");
});

it("requests the dedicated ban-ei Catalog scope while preserving NAR row source", async () => {
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const row = validFeatureRow();
  row.category = "ban-ei";
  row.keibajoCode = "83";
  row.raceKey = "nar:20260715:83:01";
  row.source = "nar";
  const catalog = catalogReturning({
    featureNames: ["career_win_rate"],
    generation: "raw-iceberg-v1",
    rows: [row],
  });
  await fetchRunningStyleFeaturesFromCatalog(
    catalog,
    { ...RACE, keibajoCode: "83", source: "nar" },
    ["career_win_rate"],
  );
  const request = catalog.fetch.mock.calls[0]?.[0];
  expect(request?.url).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/running-style-features?date=20260715&source=ban-ei&keibajoCode=83&raceBango=01",
  );
});

it("rejects stale or malformed generations", async () => {
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  await expect(
    fetchRunningStyleFeaturesFromCatalog(
      catalogReturning({ featureNames: ["f1"], generation: "legacy-v1", rows: [] }),
      RACE,
      ["f1"],
    ),
  ).rejects.toThrow("invalid generation");
});

it("rejects invalid or incomplete feature-name contracts", async () => {
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  await expect(
    fetchRunningStyleFeaturesFromCatalog(
      catalogReturning({ featureNames: "f1", generation: "raw-iceberg-v1", rows: [] }),
      RACE,
      ["f1"],
    ),
  ).rejects.toThrow("invalid featureNames");
  await expect(
    fetchRunningStyleFeaturesFromCatalog(
      catalogReturning({ featureNames: ["f2"], generation: "raw-iceberg-v1", rows: [] }),
      RACE,
      ["f1"],
    ),
  ).rejects.toThrow("missing requested model features");
});

it("rejects invalid rows and cross-race contamination", async () => {
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  await expect(
    fetchRunningStyleFeaturesFromCatalog(
      catalogReturning({ featureNames: ["f1"], generation: "raw-iceberg-v1", rows: {} }),
      RACE,
      ["f1"],
    ),
  ).rejects.toThrow("invalid rows");
  const wrongRace = validFeatureRow();
  wrongRace.raceKey = "jra:20260715:05:02";
  await expect(
    fetchRunningStyleFeaturesFromCatalog(
      catalogReturning({ featureNames: ["f1"], generation: "raw-iceberg-v1", rows: [wrongRace] }),
      RACE,
      ["f1"],
    ),
  ).rejects.toThrow("contains another race");
});

it("rejects malformed row metadata, feature cells, and peer inputs", async () => {
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const invalidMetadata = validFeatureRow();
  invalidMetadata.umaban = "1";
  await expect(
    fetchRunningStyleFeaturesFromCatalog(
      catalogReturning({
        featureNames: ["f1"],
        generation: "raw-iceberg-v1",
        rows: [invalidMetadata],
      }),
      RACE,
      ["f1"],
    ),
  ).rejects.toThrow("invalid umaban");
  const invalidFeatures = validFeatureRow();
  invalidFeatures.perHorseFeatures = { f1: "bad" };
  await expect(
    fetchRunningStyleFeaturesFromCatalog(
      catalogReturning({
        featureNames: ["f1"],
        generation: "raw-iceberg-v1",
        rows: [invalidFeatures],
      }),
      RACE,
      ["f1"],
    ),
  ).rejects.toThrow("invalid perHorseFeatures.f1");
  const invalidPeers = validFeatureRow();
  invalidPeers.peerInputs = null;
  await expect(
    fetchRunningStyleFeaturesFromCatalog(
      catalogReturning({
        featureNames: ["f1"],
        generation: "raw-iceberg-v1",
        rows: [invalidPeers],
      }),
      RACE,
      ["f1"],
    ),
  ).rejects.toThrow("invalid peerInputs");
  const invalidSource = validFeatureRow();
  invalidSource.source = "ban-ei";
  await expect(
    fetchRunningStyleFeaturesFromCatalog(
      catalogReturning({
        featureNames: ["f1"],
        generation: "raw-iceberg-v1",
        rows: [invalidSource],
      }),
      RACE,
      ["f1"],
    ),
  ).rejects.toThrow("invalid source");
});

it("fetchRunningStyleRaceKeysFromCatalog accepts JRA and NAR only", async () => {
  const { fetchRunningStyleRaceKeysFromCatalog } = await import("./running-style-catalog-client");
  await expect(
    fetchRunningStyleRaceKeysFromCatalog(
      catalogReturning({
        rows: [
          {
            kaisai_nen: "2026",
            kaisai_tsukihi: "0715",
            keibajo_code: "05",
            race_bango: "01",
            source: "nar",
          },
        ],
      }),
      "20260715",
    ),
  ).resolves.toStrictEqual([
    {
      kaisai_nen: "2026",
      kaisai_tsukihi: "0715",
      keibajo_code: "05",
      race_bango: "01",
      source: "nar",
    },
  ]);
  await expect(
    fetchRunningStyleRaceKeysFromCatalog(
      catalogReturning({ rows: [{ source: "other" }] }),
      "20260715",
    ),
  ).rejects.toThrow("invalid source");
});

it("fetchRunningStyleRaceKeysFromCatalog rejects a non-array response", async () => {
  const { fetchRunningStyleRaceKeysFromCatalog } = await import("./running-style-catalog-client");
  await expect(
    fetchRunningStyleRaceKeysFromCatalog(catalogReturning([]), "20260715"),
  ).rejects.toThrow("invalid rows");
});

it("fetchRunningStyleFeatureCountsFromCatalog counts raw rows by race", async () => {
  const { fetchRunningStyleFeatureCountsFromCatalog } =
    await import("./running-style-catalog-client");
  const catalog = catalogReturning({
    rows: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0715",
        keibajo_code: "05",
        race_bango: "01",
        source: "jra",
      },
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0715",
        keibajo_code: "05",
        race_bango: "01",
        source: "jra",
      },
    ],
  });
  const counts = await fetchRunningStyleFeatureCountsFromCatalog(catalog, "20260715");
  expect(counts.get("jra:20260715:05:01")).toBe(2);
  const request = catalog.fetch.mock.calls[0]?.[0];
  expect(request?.url).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/race-features?date=20260715&source=all",
  );
});

it("fetchRunningStyleFeatureCountsFromCatalog rejects malformed responses", async () => {
  const { fetchRunningStyleFeatureCountsFromCatalog } =
    await import("./running-style-catalog-client");
  await expect(
    fetchRunningStyleFeatureCountsFromCatalog(catalogReturning([]), "20260715"),
  ).rejects.toThrow("invalid rows");
  await expect(
    fetchRunningStyleFeatureCountsFromCatalog(catalogReturning({ rows: [null] }), "20260715"),
  ).rejects.toThrow("invalid row");
});
