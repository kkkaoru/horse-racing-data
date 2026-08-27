import { describe, expect, it } from "vitest";
import {
  readEntityGenerationManifest,
  readEntityObjectHistory,
  readEntityObjectTarget,
} from "./race-entity-object-store";
import type { ObjectStore, RaceEntityRecentResultsFilters } from "./types";

const filters: RaceEntityRecentResultsFilters = {
  cursor: null,
  date: "20260827",
  entityType: "owner",
  horseNumber: "04",
  keibajoCode: "50",
  limit: 2,
  raceBango: "05",
  source: "nar",
};

const rawRow = (overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
  babajotai_code_dirt: "1",
  babajotai_code_shiba: "",
  bamei: "テストホース",
  banushi_code: "018803",
  banushimei: "テスト馬主",
  bataiju: "480",
  chokyoshi_code: "05700",
  chokyoshimei_ryakusho: "テスト調教師",
  corner_1: "01",
  corner_2: "02",
  corner_3: "03",
  corner_4: "04",
  entity_bucket: "f",
  entity_id: "018803",
  entity_name: "テスト馬主",
  entity_type: "owner",
  futan_juryo: "560",
  grade_code: "E",
  hasso_jikoku: "1230",
  ijo_kubun_code: "0",
  kaisai_nen: "2026",
  kaisai_tsukihi: "0820",
  kakutei_chakujun: "01",
  keibajo_code: "50",
  ketto_toroku_bango: "2022100001",
  kishu_code: "21379",
  kishumei_ryakusho: "テスト騎手",
  kohan_3f: "380",
  kyori: "1400",
  kyoso_joken_meisho: "Ｃ１",
  kyosomei_hondai: "テスト競走",
  race_bango: "03",
  result_id: "nar:20260820:50:03:04:2022100001",
  shusso_tosu: "10",
  soha_time: "1320",
  source: "nar",
  tansho_ninkijun: "01",
  tansho_odds: "025",
  tenko_code: "1",
  time_sa: "-000",
  track_code: "24",
  umaban: "04",
  wakuban: "4",
  zogen_fugo: "+",
  zogen_sa: "004",
  ...overrides,
});

const gzip = async (value: unknown): Promise<Uint8Array> => {
  const stream = new Blob([JSON.stringify(value)])
    .stream()
    .pipeThrough(new CompressionStream("gzip"));
  return new Uint8Array(await new Response(stream).arrayBuffer());
};

const objectStore = async (entries: Record<string, unknown>): Promise<ObjectStore> => {
  const values = new Map<string, Uint8Array>();
  for (const [key, value] of Object.entries(entries)) {
    const bytes = key.endsWith(".gz")
      ? await gzip(value)
      : new TextEncoder().encode(JSON.stringify(value));
    values.set(key, bytes);
  }
  return {
    async get(key) {
      const value = values.get(key);
      return value === undefined
        ? null
        : { body: new Blob([value]).stream(), size: value.byteLength };
    },
  };
};

const manifestValue = { version: 1, years: { "2025": "old", "2026": "current" } };
const prefix = "entity-serving-v1";

describe("race entity object serving", () => {
  it("reads the bounded generation manifest", async () => {
    const store = await objectStore({ [`${prefix}/generations.json`]: manifestValue });
    await expect(readEntityGenerationManifest(store)).resolves.toEqual(manifestValue);
  });

  it("resolves a completed target without R2 SQL", async () => {
    const key = `${prefix}/data/2026/current/target/nar/0827.json.gz`;
    const store = await objectStore({
      [`${prefix}/generations.json`]: manifestValue,
      [key]: { version: 1, rows: [rawRow({ kaisai_tsukihi: "0827", race_bango: "05" })] },
    });
    const manifest = await readEntityGenerationManifest(store);
    await expect(readEntityObjectTarget(store, manifest, filters)).resolves.toMatchObject({
      entityBucket: "f",
      entityId: "018803",
      horseId: "2022100001",
      runnerFound: true,
    });
  });

  it("returns sorted point-in-time history and applies a cursor", async () => {
    const key = `${prefix}/data/2026/current/history/owner/nar/f-3.json.gz`;
    const rows = [
      rawRow(),
      rawRow({
        hasso_jikoku: "1300",
        kaisai_tsukihi: "0827",
        race_bango: "06",
        result_id: "future",
      }),
      rawRow({
        hasso_jikoku: "1100",
        kaisai_tsukihi: "0827",
        race_bango: "02",
        result_id: "same-day-before",
      }),
      rawRow({
        kaisai_tsukihi: "0819",
        result_id: "older",
      }),
      rawRow({ entity_id: "other", result_id: "other" }),
      rawRow({ kaisai_tsukihi: "0828", result_id: "next-day" }),
      rawRow({
        hasso_jikoku: null,
        kaisai_tsukihi: "0827",
        source: "jra",
        result_id: "wrong-source",
      }),
      rawRow({
        hasso_jikoku: null,
        kaisai_tsukihi: "0827",
        keibajo_code: "49",
        result_id: "wrong-venue",
      }),
      rawRow({
        hasso_jikoku: null,
        kaisai_tsukihi: "0827",
        race_bango: "06",
        result_id: "later-race",
      }),
    ];
    const store = await objectStore({ [key]: { version: 1, rows } });
    const target = {
      entityBucket: "f",
      entityId: "018803",
      entityName: "テスト馬主",
      horseId: "2022100001",
      horseName: "テストホース",
      raceName: "対象競走",
      raceStartTime: "1240",
      runnerFound: true,
    };
    const first = await readEntityObjectHistory(store, manifestValue, filters, target, null);
    expect(first).toHaveLength(3);
    expect(first.map((row) => row.resultId)).toEqual([
      "same-day-before",
      "nar:20260820:50:03:04:2022100001",
      "older",
    ]);
    const cursor = first[0];
    expect(cursor).toBeDefined();
    if (cursor === undefined) throw new Error("expected cursor row");
    const second = await readEntityObjectHistory(store, manifestValue, filters, target, cursor);
    expect(second.map((row) => row.resultId)).toEqual([
      "nar:20260820:50:03:04:2022100001",
      "older",
    ]);
  });

  it("loads prior year partitions when the current year is insufficient", async () => {
    const currentKey = `${prefix}/data/2026/current/history/owner/nar/f-3.json.gz`;
    const oldKey = `${prefix}/data/2025/old/history/owner/nar/f-3.json.gz`;
    const store = await objectStore({
      [currentKey]: { version: 1, rows: [rawRow()] },
      [oldKey]: {
        version: 1,
        rows: [
          rawRow({ kaisai_nen: "2025", result_id: "old-1" }),
          rawRow({ kaisai_nen: "2025", kaisai_tsukihi: "0710", result_id: "old-2" }),
        ],
      },
    });
    const result = await readEntityObjectHistory(
      store,
      manifestValue,
      filters,
      {
        entityBucket: "f",
        entityId: "018803",
        entityName: "テスト馬主",
        horseId: "2022100001",
        horseName: "テストホース",
        raceName: "対象競走",
        raceStartTime: "1240",
        runnerFound: true,
      },
      null,
    );
    expect(result.map((row) => row.resultId)).toEqual([
      "nar:20260820:50:03:04:2022100001",
      "old-1",
      "old-2",
    ]);
  });

  it("rejects unavailable and malformed manifests", async () => {
    await expect(readEntityGenerationManifest(await objectStore({}))).rejects.toThrow(
      "unavailable",
    );
    const oversizedManifest: ObjectStore = {
      async get() {
        return { body: new Blob([]).stream(), size: 2 * 1024 * 1024 };
      },
    };
    await expect(readEntityGenerationManifest(oversizedManifest)).rejects.toThrow("unavailable");
    for (const value of [[], { version: 2 }, { version: 1, years: [] }]) {
      const malformed = await objectStore({ [`${prefix}/generations.json`]: value });
      await expect(readEntityGenerationManifest(malformed)).rejects.toThrow("malformed");
    }
    const empty = await objectStore({
      [`${prefix}/generations.json`]: {
        version: 1,
        years: { "2026": "", invalid: "generation", "2025": 42 },
      },
    });
    await expect(readEntityGenerationManifest(empty)).rejects.toThrow("contains no years");
  });

  it("handles missing targets and rejects malformed envelopes", async () => {
    await expect(
      readEntityObjectTarget(await objectStore({}), { version: 1, years: {} }, filters),
    ).resolves.toBeNull();
    await expect(
      readEntityObjectTarget(await objectStore({}), manifestValue, filters),
    ).resolves.toBeNull();
    const key = `${prefix}/data/2026/current/target/nar/0827.json.gz`;
    const noMatch = await objectStore({
      [key]: {
        version: 1,
        rows: [
          rawRow({ entity_type: "trainer" }),
          rawRow({ source: "jra" }),
          rawRow({ keibajo_code: "49" }),
          rawRow({ race_bango: "06" }),
          rawRow({ umaban: "05" }),
        ],
      },
    });
    await expect(readEntityObjectTarget(noMatch, manifestValue, filters)).resolves.toBeNull();
    for (const value of [[], { version: 2, rows: [] }, { version: 1, rows: {} }]) {
      await expect(
        readEntityObjectTarget(await objectStore({ [key]: value }), manifestValue, filters),
      ).rejects.toThrow("envelope is malformed");
    }
    await expect(
      readEntityObjectTarget(
        await objectStore({ [key]: { version: 1, rows: [null] } }),
        manifestValue,
        filters,
      ),
    ).rejects.toThrow("rows are malformed");
    const oversized: ObjectStore = {
      async get() {
        return { body: new Blob([]).stream(), size: 17 * 1024 * 1024 };
      },
    };
    await expect(readEntityObjectTarget(oversized, manifestValue, filters)).rejects.toThrow(
      "compressed limit",
    );
  });

  it("covers horse cross-source rows, missing partitions, and numeric nulls", async () => {
    const horseFilters = { ...filters, entityType: "horse" as const, limit: 4 };
    const target = {
      entityBucket: "a",
      entityId: "2022100001",
      entityName: "Horse",
      horseId: "2022100001",
      horseName: "Horse",
      raceName: "Target",
      raceStartTime: null,
      runnerFound: true,
    };
    const jraKey = `${prefix}/data/2026/current/history/horse/jra/a-1.json.gz`;
    const narKey = `${prefix}/data/2026/current/history/horse/nar/a-1.json.gz`;
    const store = await objectStore({
      [jraKey]: {
        version: 1,
        rows: [
          rawRow({
            entity_bucket: "a",
            entity_id: "2022100001",
            entity_type: "horse",
            futan_juryo: "bad",
            hasso_jikoku: null,
            kohan_3f: "000",
            result_id: "jra-row",
            soha_time: "0000",
            source: "jra",
            tansho_odds: "0000",
            zogen_fugo: "-",
            zogen_sa: "004",
          }),
        ],
      },
      [narKey]: {
        version: 1,
        rows: [
          rawRow({
            entity_bucket: "a",
            entity_id: "2022100001",
            entity_type: "horse",
            result_id: "nar-row",
            zogen_sa: "000",
          }),
        ],
      },
    });
    const rows = await readEntityObjectHistory(store, manifestValue, horseFilters, target, null);
    expect(rows.map((row) => row.resultId)).toEqual(["nar-row", "jra-row"]);
    expect(rows.find((row) => row.resultId === "jra-row")).toMatchObject({
      carriedWeight: null,
      final3FSeconds: null,
      horseWeightDiff: -4,
      raceTimeSeconds: null,
      source: "jra",
      winOdds: null,
    });
    expect(rows.find((row) => row.resultId === "nar-row")?.horseWeightDiff).toBeNull();
  });

  it("rejects malformed object partitions and supports horse-year fallback", async () => {
    const target = {
      entityBucket: "z",
      entityId: "abcd1",
      entityName: "Horse",
      horseId: "abcd1",
      horseName: "Horse",
      raceName: "Target",
      raceStartTime: "1240",
      runnerFound: true,
    };
    await expect(
      readEntityObjectHistory(
        await objectStore({}),
        manifestValue,
        { ...filters, entityType: "horse" },
        target,
        null,
      ),
    ).rejects.toThrow("bucket");
    await expect(
      readEntityObjectHistory(
        await objectStore({}),
        manifestValue,
        { ...filters, entityType: "horse" },
        { ...target, entityBucket: "a", entityId: "abcdx" },
        null,
      ),
    ).rejects.toThrow("shard");
    await expect(
      readEntityObjectHistory(
        await objectStore({}),
        { version: 1, years: {} },
        { ...filters, entityType: "horse" },
        { ...target, entityBucket: "a" },
        null,
      ),
    ).resolves.toEqual([]);
  });
});
