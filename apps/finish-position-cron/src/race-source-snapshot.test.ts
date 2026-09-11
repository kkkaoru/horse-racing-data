// Run with bun. Tests for bounded race-scoped Catalog source fingerprints.

import { describe, expect, test, vi } from "vitest";

import { fetchCatalogRaceSourceSnapshots } from "./race-source-snapshot";

const baseRow = {
  babajotai_code_dirt: "1",
  babajotai_code_shiba: null,
  bamei: "Horse A",
  banushimei: "Owner A",
  barei: 4,
  bataiju: 470,
  chokyoshimei_ryakusho: "Trainer A",
  corner1_norm: null,
  corner2_norm: null,
  corner3_norm: null,
  corner4_norm: null,
  corner_1: null,
  corner_2: null,
  corner_3: null,
  corner_4: null,
  finish_norm: null,
  finish_position: null,
  futan_juryo: 56,
  grade_code: "A",
  hasso_jikoku: "1605",
  juryo_shubetsu_code: "1",
  kaisai_nen: "2026",
  kaisai_tsukihi: "0910",
  keibajo_code: "50",
  ketto_toroku_bango: "2020100001",
  kishumei_ryakusho: "Jockey A",
  kohan_3f: null,
  kyori: 1400,
  kyoso_joken_code: "010",
  kyoso_shubetsu_code: "1",
  race_bango: "11",
  race_date: "20260910",
  race_name: "Race A",
  seibetsu_code: "1",
  shusso_tosu: 2,
  soha_time: null,
  source: "nar",
  tansho_ninkijun: 1,
  tansho_odds: 2.3,
  time_sa: null,
  track_code: "24",
  umaban: 1,
  wakuban: "1",
  zogen_fugo: "+",
  zogen_sa: 4,
};

describe("race source snapshot", () => {
  test("groups a category payload into deterministic race snapshots", async () => {
    const fetch = vi.fn(
      async (_request: Request) =>
        new Response(
          JSON.stringify({
            rows: [
              baseRow,
              {
                ...baseRow,
                bamei: "Horse B",
                ketto_toroku_bango: "2020100002",
                umaban: 2,
              },
              {
                ...baseRow,
                bamei: "Horse C",
                ketto_toroku_bango: "2020100003",
                race_bango: "12",
              },
            ],
          }),
        ),
    );
    const catalog = { fetch };

    const snapshots = await fetchCatalogRaceSourceSnapshots({
      catalog,
      category: "nar",
      runYmd: "20260910",
    });

    expect([...snapshots.keys()]).toStrictEqual(["nar:2026:0910:50:11", "nar:2026:0910:50:12"]);
    expect(snapshots.get("nar:2026:0910:50:11")?.rowCount).toBe(2);
    expect(fetch.mock.calls[0]?.[0].url).toBe(
      "https://pc-keiba-r2-catalog.internal/v1/race-features?date=20260910&source=nar",
    );
  });

  test("ignores live odds, weight, and settled result changes", async () => {
    const first = await fetchCatalogRaceSourceSnapshots({
      catalog: {
        fetch: vi.fn(async () => new Response(JSON.stringify({ rows: [baseRow] }))),
      },
      category: "nar",
      raceBango: "11",
      runYmd: "20260910",
      venueCode: "50",
    });
    const second = await fetchCatalogRaceSourceSnapshots({
      catalog: {
        fetch: vi.fn(
          async () =>
            new Response(
              JSON.stringify({
                rows: [
                  {
                    ...baseRow,
                    bataiju: 492,
                    corner_1: 5,
                    finish_position: 1,
                    kohan_3f: 35.1,
                    soha_time: 831,
                    tansho_ninkijun: 3,
                    tansho_odds: 7.8,
                    time_sa: 0,
                    zogen_fugo: "-",
                    zogen_sa: 8,
                  },
                ],
              }),
            ),
        ),
      },
      category: "nar",
      raceBango: "11",
      runYmd: "20260910",
      venueCode: "50",
    });

    expect(first.get("nar:2026:0910:50:11")?.stableSourceHash).toBe(
      second.get("nar:2026:0910:50:11")?.stableSourceHash,
    );
  });

  test("changes the fingerprint when a day-stable input changes", async () => {
    const first = await fetchCatalogRaceSourceSnapshots({
      catalog: {
        fetch: vi.fn(async () => new Response(JSON.stringify({ rows: [baseRow] }))),
      },
      category: "nar",
      runYmd: "20260910",
    });
    const second = await fetchCatalogRaceSourceSnapshots({
      catalog: {
        fetch: vi.fn(
          async () => new Response(JSON.stringify({ rows: [{ ...baseRow, futan_juryo: 57 }] })),
        ),
      },
      category: "nar",
      runYmd: "20260910",
    });

    expect(first.get("nar:2026:0910:50:11")?.stableSourceHash).not.toBe(
      second.get("nar:2026:0910:50:11")?.stableSourceHash,
    );
  });

  test("normalizes Ban-ei to the NAR source identity", async () => {
    const fetch = vi.fn(
      async (_request: Request) =>
        new Response(
          JSON.stringify({ rows: [{ ...baseRow, keibajo_code: "83", race_bango: "1" }] }),
        ),
    );

    const snapshots = await fetchCatalogRaceSourceSnapshots({
      catalog: { fetch },
      category: "ban-ei",
      raceBango: "1",
      runYmd: "20260910",
      venueCode: "83",
    });

    expect([...snapshots.keys()]).toStrictEqual(["nar:2026:0910:83:01"]);
    expect(fetch.mock.calls[0]?.[0].url).toBe(
      "https://pc-keiba-r2-catalog.internal/v1/race-features?date=20260910&source=ban-ei&keibajoCode=83&raceBango=1",
    );
  });

  test("rejects an incomplete scope", async () => {
    await expect(
      fetchCatalogRaceSourceSnapshots({
        catalog: { fetch: vi.fn() },
        category: "nar",
        runYmd: "20260910",
        venueCode: "50",
      }),
    ).rejects.toThrow("incomplete-race-scope");
  });

  test("rejects invalid and empty catalog payloads", async () => {
    await expect(
      fetchCatalogRaceSourceSnapshots({
        catalog: { fetch: vi.fn(async () => new Response(JSON.stringify({ rows: [] }))) },
        category: "nar",
        runYmd: "20260910",
      }),
    ).rejects.toThrow("catalog-row-limit");
    await expect(
      fetchCatalogRaceSourceSnapshots({
        catalog: { fetch: vi.fn(async () => new Response(JSON.stringify({ invalid: [] }))) },
        category: "nar",
        runYmd: "20260910",
      }),
    ).rejects.toThrow("Catalog race source snapshot returned invalid rows");
  });

  test("rejects malformed rows and wrong category venues", async () => {
    await expect(
      fetchCatalogRaceSourceSnapshots({
        catalog: {
          fetch: vi.fn(
            async () =>
              new Response(JSON.stringify({ rows: [{ ...baseRow, ketto_toroku_bango: "" }] })),
          ),
        },
        category: "nar",
        runYmd: "20260910",
      }),
    ).rejects.toThrow("catalog-row-invalid");
    await expect(
      fetchCatalogRaceSourceSnapshots({
        catalog: {
          fetch: vi.fn(
            async () =>
              new Response(JSON.stringify({ rows: [{ ...baseRow, keibajo_code: "83" }] })),
          ),
        },
        category: "nar",
        runYmd: "20260910",
      }),
    ).rejects.toThrow("catalog-row-invalid");
  });

  test("rejects duplicate race entries", async () => {
    await expect(
      fetchCatalogRaceSourceSnapshots({
        catalog: {
          fetch: vi.fn(async () => new Response(JSON.stringify({ rows: [baseRow, baseRow] }))),
        },
        category: "nar",
        runYmd: "20260910",
      }),
    ).rejects.toThrow("catalog-race-invalid");
  });

  test("rejects upstream failures and a multi-race scoped response", async () => {
    await expect(
      fetchCatalogRaceSourceSnapshots({
        catalog: { fetch: vi.fn(async () => new Response(null, { status: 503 })) },
        category: "nar",
        runYmd: "20260910",
      }),
    ).rejects.toThrow("Catalog race source snapshot failed with HTTP 503");
    await expect(
      fetchCatalogRaceSourceSnapshots({
        catalog: {
          fetch: vi.fn(
            async () =>
              new Response(JSON.stringify({ rows: [baseRow, { ...baseRow, race_bango: "12" }] })),
          ),
        },
        category: "nar",
        raceBango: "11",
        runYmd: "20260910",
        venueCode: "50",
      }),
    ).rejects.toThrow("catalog-race-scope-mismatch");
  });
});
