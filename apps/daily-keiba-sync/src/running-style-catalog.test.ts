// Runs with bun via the package Vitest scripts.
import { Miniflare } from "miniflare";
import { afterAll, beforeAll, beforeEach, expect, test, vi } from "vitest";
import {
  readObjectBatch,
  runningStyleDayPrefixes,
  syncRunningStyleCatalog,
  toRunningStyleCatalogRecord,
} from "./running-style-catalog";

interface FakeTransaction {
  append(options: { records: readonly Record<string, unknown>[] }): Promise<void>;
}

interface TestEnvironment {
  CATALOG_BUCKET: R2Bucket;
  FEATURES_ARCHIVE: R2Bucket;
  R2_BUCKET_NAME: string;
  R2_CATALOG_NAMESPACE: string;
  R2_CATALOG_TOKEN: string;
  R2_CATALOG_URI: string;
  R2_CATALOG_WAREHOUSE: string;
  SYNC_CACHE: KVNamespace;
}

const mocks = vi.hoisted(() => ({
  append: vi.fn(),
  connect: vi.fn(),
  createResolver: vi.fn(),
  readObjects: vi.fn(),
  snapshot: { "current-snapshot-id": 41n } satisfies { "current-snapshot-id"?: bigint },
  transaction: vi.fn(),
}));

vi.mock("icebird", () => ({ icebergTransaction: mocks.transaction }));
vi.mock("hyparquet", () => ({ parquetReadObjects: mocks.readObjects }));
vi.mock("./catalog-sync", () => ({ connectCatalog: mocks.connect }));
vi.mock("./r2-resolver", () => ({ createR2Resolver: mocks.createResolver }));

let miniflare: Miniflare;
let env: TestEnvironment;

beforeAll(async () => {
  miniflare = new Miniflare({
    compatibilityDate: "2026-06-18",
    kvNamespaces: ["SYNC_CACHE"],
    modules: true,
    r2Buckets: { CATALOG_BUCKET: "catalog-test", FEATURES_ARCHIVE: "features-test" },
    script: "export default {}",
  });
  const bindings = await miniflare.getBindings<{
    CATALOG_BUCKET: R2Bucket;
    FEATURES_ARCHIVE: R2Bucket;
    SYNC_CACHE: KVNamespace;
  }>();
  env = {
    ...bindings,
    R2_BUCKET_NAME: "pc-keiba-r2-catalog",
    R2_CATALOG_NAMESPACE: "pc_keiba",
    R2_CATALOG_TOKEN: "token",
    R2_CATALOG_URI: "https://catalog.example",
    R2_CATALOG_WAREHOUSE: "warehouse",
  };
});

afterAll(async () => {
  await miniflare.dispose();
});

beforeEach(async () => {
  const listed = await env.FEATURES_ARCHIVE.list();
  await Promise.all(listed.objects.map((object) => env.FEATURES_ARCHIVE.delete(object.key)));
  const keys = await env.SYNC_CACHE.list();
  await Promise.all(keys.keys.map((key) => env.SYNC_CACHE.delete(key.name)));
  mocks.append.mockReset().mockResolvedValue(undefined);
  mocks.connect.mockReset().mockResolvedValue({ type: "rest" });
  mocks.createResolver.mockReset().mockReturnValue({ reader: vi.fn() });
  mocks.readObjects.mockReset();
  mocks.transaction
    .mockReset()
    .mockImplementation(
      async (_options: unknown, callback: (transaction: FakeTransaction) => Promise<void>) => {
        await callback({ append: mocks.append });
        return mocks.snapshot;
      },
    );
});

test("toRunningStyleCatalogRecord maps a Parquet row onto the Iceberg schema", () => {
  expect(
    toRunningStyleCatalogRecord({
      source: "nar",
      kaisai_nen: "2026",
      kaisai_tsukihi: "1020",
      keibajo_code: "42",
      race_bango: "11",
      ketto_toroku_bango: "2023100157",
      umaban: 3,
      p_nige: 0.1,
      p_senkou: 0.2,
      p_sashi: 0.3,
      p_oikomi: 0.4,
      predicted_label: "oikomi",
      predicted_class: 3,
      cell_model_key: null,
      cell_variant_id: "variant-a",
      model_version: "nar-running-style-lgbm-prod-v3",
      predicted_at: "2026-10-20T08:15:00.000Z",
    }),
  ).toStrictEqual({
    model_version: "nar-running-style-lgbm-prod-v3",
    source: "nar",
    kaisai_nen: "2026",
    kaisai_tsukihi: "1020",
    keibajo_code: "42",
    race_bango: "11",
    ketto_toroku_bango: "2023100157",
    predicted_label: "oikomi",
    p_nige: 0.1,
    p_senkou: 0.2,
    p_sashi: 0.3,
    p_oikomi: 0.4,
    umaban: 3,
    predicted_class: 3,
    cell_model_key: null,
    cell_variant_id: "variant-a",
    predicted_corner_front_score: null,
    predicted_corner_rank: null,
    prediction_generated_at: new Date("2026-10-20T08:15:00.000Z"),
  });
});

test("toRunningStyleCatalogRecord rejects rows that do not match the export contract", () => {
  const valid = {
    source: "jra",
    kaisai_nen: "2026",
    kaisai_tsukihi: "1018",
    keibajo_code: "05",
    race_bango: "11",
    ketto_toroku_bango: "2022105001",
    umaban: 1,
    p_nige: 0.25,
    p_senkou: 0.25,
    p_sashi: 0.25,
    p_oikomi: 0.25,
    predicted_label: "nige",
    predicted_class: 0,
    model_version: "jra-running-style-lgbm-prod-v3",
    predicted_at: "2026-10-18T05:00:00.000Z",
  };
  expect(() => toRunningStyleCatalogRecord(["row"])).toThrow(
    "Running-style Parquet row is not an object",
  );
  expect(() => toRunningStyleCatalogRecord({ ...valid, source: "" })).toThrow(
    "Running-style Parquet row has an invalid source",
  );
  expect(() => toRunningStyleCatalogRecord({ ...valid, p_sashi: Number.NaN })).toThrow(
    "Running-style Parquet row has an invalid p_sashi",
  );
  expect(() => toRunningStyleCatalogRecord({ ...valid, umaban: 1.5 })).toThrow(
    "Running-style Parquet row has an invalid umaban",
  );
  expect(() => toRunningStyleCatalogRecord({ ...valid, cell_model_key: 7 })).toThrow(
    "Running-style Parquet row has an invalid cell_model_key",
  );
  expect(() => toRunningStyleCatalogRecord({ ...valid, predicted_at: "not-a-time" })).toThrow(
    "Running-style Parquet row has an invalid predicted_at",
  );
});

test("runningStyleDayPrefixes lists the run date and the lookback days across a month edge", () => {
  expect(runningStyleDayPrefixes("20261002", 3)).toStrictEqual([
    "running-style/predictions/by-day/raw-iceberg-v1/2026/09/29/",
    "running-style/predictions/by-day/raw-iceberg-v1/2026/09/30/",
    "running-style/predictions/by-day/raw-iceberg-v1/2026/10/01/",
    "running-style/predictions/by-day/raw-iceberg-v1/2026/10/02/",
  ]);
});

test("syncRunningStyleCatalog appends only rows newer than each object's watermark", async () => {
  await Promise.all([
    env.FEATURES_ARCHIVE.put(
      "running-style/predictions/by-day/raw-iceberg-v1/2026/10/20/nar/nar-v3.parquet",
      "nar",
    ),
    env.FEATURES_ARCHIVE.put(
      "running-style/predictions/by-day/raw-iceberg-v1/2026/10/21/jra/jra-v3.parquet",
      "jra",
    ),
    env.FEATURES_ARCHIVE.put(
      "running-style/predictions/by-day/raw-iceberg-v1/2026/10/21/jra/readme.txt",
      "ignored",
    ),
    env.FEATURES_ARCHIVE.put(
      "running-style/predictions/by-day/raw-iceberg-v1/2026/10/14/jra/old.parquet",
      "outside the window",
    ),
    env.SYNC_CACHE.put(
      "running-style-catalog:v1:running-style/predictions/by-day/raw-iceberg-v1/2026/10/20/nar/nar-v3.parquet",
      "2026-10-20T09:00:00.000Z",
    ),
  ]);
  const row = {
    source: "nar",
    kaisai_nen: "2026",
    kaisai_tsukihi: "1020",
    keibajo_code: "42",
    race_bango: "11",
    umaban: 3,
    p_nige: 0.1,
    p_senkou: 0.2,
    p_sashi: 0.3,
    p_oikomi: 0.4,
    predicted_label: "oikomi",
    predicted_class: 3,
    cell_model_key: null,
    cell_variant_id: null,
    model_version: "nar-v3",
  };
  mocks.readObjects.mockImplementation(async ({ file }: { file: ArrayBuffer }) =>
    new TextDecoder().decode(file) === "nar"
      ? [
          { ...row, ketto_toroku_bango: "H1", predicted_at: "2026-10-20T08:00:00.000Z" },
          { ...row, ketto_toroku_bango: "H2", predicted_at: "2026-10-20T10:30:00.000Z" },
        ]
      : [
          {
            ...row,
            source: "jra",
            ketto_toroku_bango: "J1",
            predicted_at: "2026-10-21T01:00:00.000Z",
          },
        ],
  );

  const result = await syncRunningStyleCatalog(env, "20261021");

  expect(result).toStrictEqual({ appendedRows: 2, objects: 2, snapshotId: "41" });
  expect(mocks.transaction).toHaveBeenCalledWith(
    {
      catalog: { type: "rest" },
      namespace: "pc_keiba",
      resolver: { reader: expect.any(Function) },
      table: "race_running_style_model_predictions",
    },
    expect.any(Function),
  );
  expect(mocks.createResolver).toHaveBeenCalledWith(env.CATALOG_BUCKET, "pc-keiba-r2-catalog");
  expect(
    mocks.append.mock.calls[0]?.[0].records.map(
      (record: { ketto_toroku_bango: string }) => record.ketto_toroku_bango,
    ),
  ).toStrictEqual(["H2", "J1"]);
  expect(
    await env.SYNC_CACHE.get(
      "running-style-catalog:v1:running-style/predictions/by-day/raw-iceberg-v1/2026/10/20/nar/nar-v3.parquet",
    ),
  ).toBe("2026-10-20T10:30:00.000Z");
  expect(
    await env.SYNC_CACHE.get(
      "running-style-catalog:v1:running-style/predictions/by-day/raw-iceberg-v1/2026/10/21/jra/jra-v3.parquet",
    ),
  ).toBe("2026-10-21T01:00:00.000Z");
});

test("syncRunningStyleCatalog skips the commit when nothing is new", async () => {
  await Promise.all([
    env.FEATURES_ARCHIVE.put(
      "running-style/predictions/by-day/raw-iceberg-v1/2026/10/21/jra/jra-v3.parquet",
      "jra",
    ),
    env.SYNC_CACHE.put(
      "running-style-catalog:v1:running-style/predictions/by-day/raw-iceberg-v1/2026/10/21/jra/jra-v3.parquet",
      "2026-10-21T01:00:00.000Z",
    ),
  ]);
  mocks.readObjects.mockResolvedValue([
    {
      source: "jra",
      kaisai_nen: "2026",
      kaisai_tsukihi: "1021",
      keibajo_code: "05",
      race_bango: "01",
      ketto_toroku_bango: "J1",
      umaban: 1,
      p_nige: 0.25,
      p_senkou: 0.25,
      p_sashi: 0.25,
      p_oikomi: 0.25,
      predicted_label: "nige",
      predicted_class: 0,
      cell_model_key: null,
      cell_variant_id: null,
      model_version: "jra-v3",
      predicted_at: "2026-10-21T01:00:00.000Z",
    },
  ]);

  expect(await syncRunningStyleCatalog(env, "20261021")).toStrictEqual({
    appendedRows: 0,
    objects: 1,
    snapshotId: null,
  });
  expect(mocks.transaction).not.toHaveBeenCalled();
});

test("syncRunningStyleCatalog keeps the watermarks when the commit has no snapshot", async () => {
  await env.FEATURES_ARCHIVE.put(
    "running-style/predictions/by-day/raw-iceberg-v1/2026/10/21/jra/jra-v3.parquet",
    "jra",
  );
  mocks.readObjects.mockResolvedValue([
    {
      source: "jra",
      kaisai_nen: "2026",
      kaisai_tsukihi: "1021",
      keibajo_code: "05",
      race_bango: "01",
      ketto_toroku_bango: "J1",
      umaban: 1,
      p_nige: 0.25,
      p_senkou: 0.25,
      p_sashi: 0.25,
      p_oikomi: 0.25,
      predicted_label: "nige",
      predicted_class: 0,
      cell_model_key: null,
      cell_variant_id: null,
      model_version: "jra-v3",
      predicted_at: "2026-10-21T01:00:00.000Z",
    },
  ]);
  mocks.transaction.mockImplementation(async () => ({}));

  await expect(syncRunningStyleCatalog(env, "20261021")).rejects.toThrow(
    "Iceberg commit returned no current snapshot",
  );
  expect(
    await env.SYNC_CACHE.get(
      "running-style-catalog:v1:running-style/predictions/by-day/raw-iceberg-v1/2026/10/21/jra/jra-v3.parquet",
    ),
  ).toBe(null);
});

test("readObjectBatch fails when the listed object is gone", async () => {
  await expect(
    readObjectBatch(
      env,
      "running-style/predictions/by-day/raw-iceberg-v1/2026/10/21/jra/gone.parquet",
    ),
  ).rejects.toThrow(
    "Running-style Parquet object disappeared: running-style/predictions/by-day/raw-iceberg-v1/2026/10/21/jra/gone.parquet",
  );
});

test("syncRunningStyleCatalog fails on a truncated listing", async () => {
  const truncated = {
    ...env,
    FEATURES_ARCHIVE: {
      ...env.FEATURES_ARCHIVE,
      list: async () => ({ delimitedPrefixes: [], objects: [], truncated: true, cursor: "next" }),
    },
  };

  await expect(syncRunningStyleCatalog(truncated, "20261021")).rejects.toThrow(
    "Running-style Parquet listing is truncated: running-style/predictions/by-day/raw-iceberg-v1/2026/10/18/",
  );
});
