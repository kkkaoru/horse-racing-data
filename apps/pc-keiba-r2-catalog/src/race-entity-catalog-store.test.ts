// Run with bun (bunx vitest). Exercises direct reads of Catalog-managed Parquet.
import { expect, it, vi } from "vitest";

import {
  readEntityCatalogHistory,
  readEntityCatalogManifest,
  readEntityCatalogTarget,
} from "./race-entity-catalog-store";
import type { ObjectStore, RaceEntityRecentResultsFilters } from "./types";

vi.mock("hyparquet", () => ({
  parquetReadObjects: vi.fn(async ({ file }: { file: ArrayBuffer }) => {
    const value: unknown = JSON.parse(new TextDecoder().decode(file));
    return Array.isArray(value) ? value : [];
  }),
}));

const encoder = new TextEncoder();

const objectStore = (entries: Record<string, unknown>): ObjectStore => {
  const objects = new Map(
    Object.entries(entries).map(([key, value]) => [key, encoder.encode(JSON.stringify(value))]),
  );
  return {
    async get(key) {
      const bytes = objects.get(key);
      return bytes === undefined
        ? null
        : { body: new Blob([bytes]).stream(), size: bytes.byteLength };
    },
  };
};

const filters: RaceEntityRecentResultsFilters = {
  cursor: null,
  date: "20260827",
  entityType: "jockey",
  horseNumber: "07",
  keibajoCode: "50",
  limit: 2,
  raceBango: "05",
  source: "nar",
};

const runner = {
  bamei: "Horse",
  banushi_code: "768006",
  banushimei: "Owner",
  chokyoshi_code: "20692",
  chokyoshimei_ryakusho: "Trainer",
  kaisai_nen: "2026",
  kaisai_tsukihi: "0827",
  keibajo_code: "50",
  ketto_toroku_bango: "2022103916",
  kishu_code: "21379",
  kishumei_ryakusho: "Jockey",
  race_bango: "05",
  umaban: "07",
};

const race = {
  hasso_jikoku: "1240",
  kaisai_nen: "2026",
  kaisai_tsukihi: "0827",
  keibajo_code: "50",
  kyosomei_hondai: "Target",
  race_bango: "05",
};

const table = (key: string, rows: unknown[]) => ({
  dataPrefix: "",
  partitions: { "2026": [[key, encoder.encode(JSON.stringify(rows)).byteLength]] },
  snapshotId: `${key}-snapshot`,
});

const targetStore = (runnerRows: unknown[], raceRows: unknown[]): ObjectStore =>
  objectStore({
    "entity-catalog-serving-v1/manifest.json": {
      history: table("unused.parquet", []),
      raw: {
        nvd_ra: table("race.parquet", raceRows),
        nvd_se: table("runner.parquet", runnerRows),
      },
      version: 1,
    },
    "race.parquet": raceRows,
    "runner.parquet": runnerRows,
    "unused.parquet": [],
  });

it("validates the atomic Catalog manifest", async () => {
  await expect(readEntityCatalogManifest(objectStore({}))).rejects.toThrow(
    "Catalog serving manifest is unavailable",
  );
  await expect(
    readEntityCatalogManifest(
      objectStore({ "entity-catalog-serving-v1/manifest.json": { version: 2 } }),
    ),
  ).rejects.toThrow("Catalog serving manifest is malformed");
  await expect(
    readEntityCatalogManifest(
      objectStore({
        "entity-catalog-serving-v1/manifest.json": {
          history: {},
          raw: {},
          version: 1,
        },
      }),
    ),
  ).rejects.toThrow("Catalog table manifest is malformed");
  await expect(
    readEntityCatalogManifest(
      objectStore({
        "entity-catalog-serving-v1/manifest.json": {
          history: { dataPrefix: "", partitions: {}, snapshotId: 1 },
          raw: {},
          version: 1,
        },
      }),
    ),
  ).rejects.toThrow("Catalog table snapshot is malformed");
  await expect(
    readEntityCatalogManifest(
      objectStore({
        "entity-catalog-serving-v1/manifest.json": {
          history: {
            dataPrefix: "",
            partitions: { bad: [["file", 0]] },
            snapshotId: "1",
          },
          raw: {},
          version: 1,
        },
      }),
    ),
  ).rejects.toThrow("Catalog manifest file is malformed");
});

it("rejects non-array manifest file lists", async () => {
  await expect(
    readEntityCatalogManifest(
      objectStore({
        "entity-catalog-serving-v1/manifest.json": {
          history: {
            dataPrefix: "",
            partitions: { bad: {} },
            snapshotId: "1",
          },
          raw: {},
          version: 1,
        },
      }),
    ),
  ).rejects.toThrow("Catalog manifest file list is malformed");
});

it("resolves every entity type from raw Catalog target tables", async () => {
  const store = targetStore([runner], [race]);
  const manifest = await readEntityCatalogManifest(store);
  await expect(readEntityCatalogTarget(store, manifest, filters)).resolves.toStrictEqual({
    entityBucket: "a",
    entityId: "21379",
    entityName: "Jockey",
    horseId: "2022103916",
    horseName: "Horse",
    raceName: "Target",
    raceStartTime: "1240",
    runnerFound: true,
  });
  await expect(
    readEntityCatalogTarget(store, manifest, { ...filters, entityType: "horse" }),
  ).resolves.toMatchObject({ entityBucket: "e", entityId: "2022103916" });
  await expect(
    readEntityCatalogTarget(store, manifest, { ...filters, entityType: "trainer" }),
  ).resolves.toMatchObject({ entityBucket: "6", entityId: "20692" });
  await expect(
    readEntityCatalogTarget(store, manifest, { ...filters, entityType: "owner" }),
  ).resolves.toMatchObject({ entityBucket: "a", entityId: "768006" });
});

it("resolves a JRA target from the JRA raw snapshots", async () => {
  const runnerRows = [runner];
  const raceRows = [race];
  const store = objectStore({
    "entity-catalog-serving-v1/manifest.json": {
      history: table("unused.parquet", []),
      raw: {
        jvd_ra: table("race.parquet", raceRows),
        jvd_se: table("runner.parquet", runnerRows),
      },
      version: 1,
    },
    "race.parquet": raceRows,
    "runner.parquet": runnerRows,
    "unused.parquet": [],
  });
  const manifest = await readEntityCatalogManifest(store);
  await expect(
    readEntityCatalogTarget(store, manifest, { ...filters, source: "jra" }),
  ).resolves.toMatchObject({ entityId: "21379", runnerFound: true });
});

it("distinguishes a missing race, runner, and entity ID", async () => {
  const noRaceStore = targetStore([runner], []);
  const noRaceManifest = await readEntityCatalogManifest(noRaceStore);
  await expect(readEntityCatalogTarget(noRaceStore, noRaceManifest, filters)).resolves.toBeNull();

  const noRunnerStore = targetStore([], [race]);
  const noRunnerManifest = await readEntityCatalogManifest(noRunnerStore);
  await expect(
    readEntityCatalogTarget(noRunnerStore, noRunnerManifest, filters),
  ).resolves.toStrictEqual({
    entityBucket: null,
    entityId: null,
    entityName: null,
    horseId: null,
    horseName: null,
    raceName: "Target",
    raceStartTime: "1240",
    runnerFound: false,
  });

  const noIdStore = targetStore([{ ...runner, kishu_code: "　" }], [race]);
  const noIdManifest = await readEntityCatalogManifest(noIdStore);
  await expect(readEntityCatalogTarget(noIdStore, noIdManifest, filters)).resolves.toMatchObject({
    entityBucket: null,
    entityId: null,
    runnerFound: true,
  });
});

it("normalises rich history and excludes future leakage", async () => {
  const rich = {
    babajotai_code_dirt: "2",
    babajotai_code_shiba: "1",
    bamei: "Horse",
    banushi_code: "768006",
    banushimei: "Owner",
    bataiju: "480",
    chokyoshi_code: "20692",
    chokyoshimei_ryakusho: "Trainer",
    corner_1: "03",
    corner_2: "02",
    corner_3: "01",
    corner_4: "01",
    entity_id: "21379",
    futan_juryo: "550",
    grade_code: "A",
    hasso_jikoku: "1210",
    ijo_kubun_code: "0",
    kaisai_nen: "2026",
    kaisai_tsukihi: "0827",
    kakutei_chakujun: "01",
    keibajo_code: "50",
    ketto_toroku_bango: "2022103916",
    kishu_code: "21379",
    kishumei_ryakusho: "Jockey",
    kohan_3f: "350",
    kyori: "1400",
    kyoso_joken_meisho: "Class",
    kyosomei_hondai: "Earlier",
    race_bango: "04",
    result_id: "nar:20260827:50:04:07:2022103916",
    shusso_tosu: "12",
    soha_time: "830",
    source: "nar",
    tansho_ninkijun: "02",
    tansho_odds: "0120",
    tenko_code: "1",
    time_sa: "+001",
    track_code: "23",
    umaban: "07",
    wakuban: "4",
    zogen_fugo: "-",
    zogen_sa: "006",
  };
  const future = { ...rich, hasso_jikoku: "1300", race_bango: "06", result_id: "future" };
  const historyRows = [future, rich];
  const historySize = encoder.encode(JSON.stringify(historyRows)).byteLength;
  const store = objectStore({
    "entity-catalog-serving-v1/manifest.json": {
      history: {
        dataPrefix: "",
        partitions: { "jockey/nar/a/2026": [["history.parquet", historySize]] },
        snapshotId: "history",
      },
      raw: {},
      version: 1,
    },
    "history.parquet": historyRows,
  });
  const manifest = await readEntityCatalogManifest(store);
  const rows = await readEntityCatalogHistory(
    store,
    manifest,
    filters,
    {
      entityBucket: "a",
      entityId: "21379",
      entityName: "Jockey",
      horseId: "2022103916",
      horseName: "Horse",
      raceName: "Target",
      raceStartTime: "1240",
      runnerFound: true,
    },
    null,
  );
  expect(rows).toHaveLength(1);
  expect(rows[0]).toMatchObject({
    carriedWeight: 55,
    distance: 1400,
    finishPosition: 1,
    horseWeight: 480,
    horseWeightDiff: -6,
    raceStartSortKey: "202608271210",
    raceTimeSeconds: 83,
    winOdds: 12,
  });
});

it("applies cursor ordering and conservative missing-time fallback", async () => {
  const earlierVenue = {
    entity_id: "21379",
    hasso_jikoku: "",
    kaisai_nen: "2026",
    kaisai_tsukihi: "0827",
    keibajo_code: "50",
    race_bango: "04",
    result_id: "nar:20260827:50:04:01:horse",
    source: "nar",
  };
  const otherVenue = {
    ...earlierVenue,
    keibajo_code: "51",
    race_bango: "01",
    result_id: "nar:20260827:51:01:01:horse",
  };
  const historyRows = [earlierVenue, otherVenue];
  const historySize = encoder.encode(JSON.stringify(historyRows)).byteLength;
  const store = objectStore({
    "entity-catalog-serving-v1/manifest.json": {
      history: {
        dataPrefix: "",
        partitions: { "jockey/nar/a/2026": [["history.parquet", historySize]] },
        snapshotId: "history",
      },
      raw: {},
      version: 1,
    },
    "history.parquet": historyRows,
  });
  const manifest = await readEntityCatalogManifest(store);
  const rows = await readEntityCatalogHistory(
    store,
    manifest,
    filters,
    {
      entityBucket: "a",
      entityId: "21379",
      entityName: "Jockey",
      horseId: "horse",
      horseName: "Horse",
      raceName: "Target",
      raceStartTime: null,
      runnerFound: true,
    },
    { raceStartSortKey: "202608270000", resultId: "nar:20260827:50:05:01:horse" },
  );
  expect(rows).toHaveLength(1);
  expect(rows[0]?.resultId).toBe("nar:20260827:50:04:01:horse");
});

it("reads cross-source horse history and handles invalid registration years", async () => {
  const jraRow = {
    entity_id: "2022103916",
    hasso_jikoku: "1200",
    kaisai_nen: "2026",
    kaisai_tsukihi: "0820",
    keibajo_code: "07",
    kyori: "invalid",
    race_bango: "03",
    result_id: "jra:20260820:07:03:01:2022103916",
    source: "jra",
    zogen_fugo: "+",
    zogen_sa: "006",
  };
  const narRow = {
    ...jraRow,
    keibajo_code: "50",
    result_id: "nar:20260820:50:03:01:2022103916",
    source: "nar",
  };
  const nextDay = {
    ...narRow,
    kaisai_tsukihi: "0828",
    result_id: "nar:20260828:50:03:01:2022103916",
  };
  const jraRows = [jraRow];
  const narRows = [narRow, nextDay];
  const jraSize = encoder.encode(JSON.stringify(jraRows)).byteLength;
  const narSize = encoder.encode(JSON.stringify(narRows)).byteLength;
  const store = objectStore({
    "entity-catalog-serving-v1/manifest.json": {
      history: {
        dataPrefix: "",
        partitions: {
          "horse/jra/e/2026": [["jra.parquet", jraSize]],
          "horse/nar/e/2026": [["nar.parquet", narSize]],
        },
        snapshotId: "history",
      },
      raw: {},
      version: 1,
    },
    "jra.parquet": jraRows,
    "nar.parquet": narRows,
  });
  const manifest = await readEntityCatalogManifest(store);
  const rows = await readEntityCatalogHistory(
    store,
    manifest,
    { ...filters, entityType: "horse" },
    {
      entityBucket: "e",
      entityId: "2022103916",
      entityName: "Horse",
      horseId: "2022103916",
      horseName: "Horse",
      raceName: "Target",
      raceStartTime: "1240",
      runnerFound: true,
    },
    null,
  );
  expect(rows).toHaveLength(2);
  expect(rows[0]).toMatchObject({ horseWeightDiff: 6, source: "nar" });
  expect(rows[1]).toMatchObject({ distance: null, source: "jra" });
  await expect(
    readEntityCatalogHistory(
      store,
      manifest,
      { ...filters, entityType: "horse" },
      {
        entityBucket: "e",
        entityId: "foreign",
        entityName: "Horse",
        horseId: "foreign",
        horseName: "Horse",
        raceName: "Target",
        raceStartTime: "1240",
        runnerFound: true,
      },
      null,
    ),
  ).resolves.toStrictEqual([]);
});

it("rejects malformed buckets and unavailable Catalog files", async () => {
  const emptyStore = objectStore({
    "entity-catalog-serving-v1/manifest.json": {
      history: {
        dataPrefix: "",
        partitions: { "jockey/nar/a/2026": [["missing.parquet", 10]] },
        snapshotId: "history",
      },
      raw: {},
      version: 1,
    },
  });
  const manifest = await readEntityCatalogManifest(emptyStore);
  await expect(
    readEntityCatalogHistory(
      emptyStore,
      manifest,
      filters,
      {
        entityBucket: "z",
        entityId: "21379",
        entityName: "Jockey",
        horseId: "horse",
        horseName: "Horse",
        raceName: "Target",
        raceStartTime: "1240",
        runnerFound: true,
      },
      null,
    ),
  ).rejects.toThrow("entity bucket is malformed");
  await expect(
    readEntityCatalogHistory(
      emptyStore,
      manifest,
      filters,
      {
        entityBucket: "a",
        entityId: "21379",
        entityName: "Jockey",
        horseId: "horse",
        horseName: "Horse",
        raceName: "Target",
        raceStartTime: "1240",
        runnerFound: true,
      },
      null,
    ),
  ).rejects.toThrow("Catalog Parquet file is unavailable or changed");
});
