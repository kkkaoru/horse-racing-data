import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, test, vi } from "vitest";
import type { AsyncBuffer } from "hyparquet";

import {
  buildDayBaseRaceFoundationKey,
  buildDayBaseRaceManifestKey,
  materializeDayBasePerRaceCache,
} from "./day-base-race-materializer";

const fixture = new Uint8Array(
  readFileSync(join(import.meta.dirname, "scoring", "__fixtures__", "sample-cache.parquet")),
);

interface CapturedPut {
  body: Uint8Array;
  key: string;
  options: R2PutOptions;
}

interface BucketFixture {
  env: { FEATURES_CACHE: R2Bucket };
  get: ReturnType<typeof vi.fn>;
  head: ReturnType<typeof vi.fn>;
  puts: CapturedPut[];
}

const bodyBytes = async (value: unknown): Promise<Uint8Array> => {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (typeof value === "string") return new TextEncoder().encode(value);
  if (value instanceof Blob) return new Uint8Array(await value.arrayBuffer());
  throw new Error("unexpected put body");
};

const makeBucket = (
  bytes: Uint8Array = fixture,
  overrides: {
    get?: (key: string, options: R2GetOptions) => Promise<unknown>;
    head?: () => Promise<unknown>;
    put?: (key: string, value: unknown, options: R2PutOptions) => Promise<unknown>;
  } = {},
): BucketFixture => {
  const puts: CapturedPut[] = [];
  const head = vi.fn(
    overrides.head ??
      (async () => ({
        etag: "source-etag",
        size: bytes.byteLength,
        version: "source-version",
      })),
  );
  const get = vi.fn(
    overrides.get ??
      (async (_key: string, options: R2GetOptions) => {
        const range = "range" in options ? options.range : undefined;
        if (range === undefined || "suffix" in range) throw new Error("missing range");
        const offset = "offset" in range ? (range.offset ?? 0) : 0;
        const length = "length" in range ? (range.length ?? bytes.byteLength - offset) : 0;
        const selected = bytes.slice(offset, offset + length);
        return { arrayBuffer: async () => selected.buffer };
      }),
  );
  const put = vi.fn(
    overrides.put ??
      (async (key: string, value: unknown, options: R2PutOptions) => {
        puts.push({ body: await bodyBytes(value), key, options });
        return {};
      }),
  );
  return {
    env: { FEATURES_CACHE: { get, head, put } as unknown as R2Bucket },
    get,
    head,
    puts,
  };
};

const jsonBody = (put: CapturedPut | undefined): Record<string, unknown> => {
  if (put === undefined) throw new Error("put missing");
  return JSON.parse(new TextDecoder().decode(put.body)) as Record<string, unknown>;
};

const validRow = (overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
  ketto_toroku_bango: "2020100001",
  race_id: "jra:2026:0823:01:01",
  umaban: 1,
  value: 0.5,
  ...overrides,
});

const schemaFor = (featureNames: string[]) =>
  featureNames.map((name) => ({ convertedType: "UTF8", name, physicalType: "BYTE_ARRAY" }));

const injected = (rows: unknown[], featureNames = ["race_id", "ketto_toroku_bango", "umaban"]) => ({
  decodeDayBase: async () => ({ featureSchema: schemaFor(featureNames), rows }),
});

describe("day-base per-race foundation materializer", () => {
  test("uses a separate namespace from canonical final feature cache", () => {
    expect(buildDayBaseRaceFoundationKey("jra", "20260823", "1", "2")).toBe(
      "feat-daybase-race/catalog-v1/jra/20260823/01/02/foundation.json",
    );
    expect(buildDayBaseRaceManifestKey("ban-ei", "20260823")).toBe(
      "feat-daybase-race/catalog-v1/ban-ei/20260823/manifest.json",
    );
  });

  test("range-reads real parquet, writes each race, and publishes the manifest last", async () => {
    const bucket = makeBucket();
    const result = await materializeDayBasePerRaceCache({
      category: "jra",
      env: bucket.env,
      runYmd: "20260614",
    });

    expect(result).toMatchObject({ raceCount: 2, rowCount: 5, status: "materialized" });
    expect(bucket.get).toHaveBeenCalled();
    expect(bucket.puts).toHaveLength(3);
    expect(bucket.puts.at(-1)?.key).toBe(buildDayBaseRaceManifestKey("jra", "20260614"));
    const race = jsonBody(bucket.puts[0]);
    expect(race).toMatchObject({ raceId: "jra:2026:0614:05:11" });
    expect(race.contract).toMatchObject({ rowCount: 3, schemaVersion: "1" });
    expect(race.source).toStrictEqual({
      etag: "source-etag",
      key: "feat-daybase/catalog-v1/jra/20260614/features.parquet",
      version: "source-version",
    });
    const manifest = jsonBody(bucket.puts.at(-1));
    expect(manifest.contract).toMatchObject({ raceCount: 2, rowCount: 5, schemaVersion: "1" });
    expect(bucket.puts[0]?.options.customMetadata).toMatchObject({
      "row-count": "3",
      "schema-version": "1",
      "source-etag": "source-etag",
    });
  });

  test("returns an existing manifest for the same source without decoding or writing", async () => {
    const head = vi
      .fn()
      .mockResolvedValueOnce({ etag: "source-etag", size: 4, version: "source-version" })
      .mockResolvedValueOnce({
        customMetadata: {
          "contract-version": "day-base-race-foundation-v1",
          "feature-hash": "feature-hash",
          "race-count": "36",
          "row-count": "466",
          "schema-version": "1",
          "source-etag": "source-etag",
          "source-version": "source-version",
        },
      });
    const bucket = makeBucket(new Uint8Array([1, 2, 3, 4]), { head });
    const decodeDayBase = vi.fn(async () => ({ featureSchema: [], rows: [] }));
    await expect(
      materializeDayBasePerRaceCache(
        { category: "jra", env: bucket.env, runYmd: "20260823" },
        { decodeDayBase },
      ),
    ).resolves.toStrictEqual({
      featureHash: "feature-hash",
      manifestKey: buildDayBaseRaceManifestKey("jra", "20260823"),
      raceCount: 36,
      rowCount: 466,
      status: "materialized",
    });
    expect(decodeDayBase).not.toHaveBeenCalled();
    expect(bucket.puts).toHaveLength(0);
  });

  test.each([
    ["contract-version", "old"],
    ["schema-version", "0"],
    ["source-etag", "stale"],
    ["source-version", "stale"],
    ["feature-hash", ""],
    ["race-count", "NaN"],
    ["race-count", "0"],
    ["race-count", "65"],
    ["row-count", "NaN"],
    ["row-count", "0"],
    ["row-count", "1025"],
  ])("rebuilds when existing manifest metadata %s is stale", async (key, value) => {
    const metadata: Record<string, string> = {
      "contract-version": "day-base-race-foundation-v1",
      "feature-hash": "feature-hash",
      "race-count": "1",
      "row-count": "1",
      "schema-version": "1",
      "source-etag": "source-etag",
      "source-version": "source-version",
      [key]: value,
    };
    const head = vi
      .fn()
      .mockResolvedValueOnce({ etag: "source-etag", size: 1, version: "source-version" })
      .mockResolvedValueOnce({ customMetadata: metadata });
    const bucket = makeBucket(new Uint8Array([1]), { head });
    const decodeDayBase = vi.fn(injected([validRow()]).decodeDayBase);
    const result = await materializeDayBasePerRaceCache(
      { category: "jra", env: bucket.env, runYmd: "20260823" },
      { decodeDayBase },
    );
    expect(result.status).toBe("materialized");
    expect(decodeDayBase).toHaveBeenCalledTimes(1);
  });

  test("accepts a missing manifest source-version when the R2 source has no version", async () => {
    const head = vi
      .fn()
      .mockResolvedValueOnce({ etag: "source-etag", size: 1 })
      .mockResolvedValueOnce({
        customMetadata: {
          "contract-version": "day-base-race-foundation-v1",
          "feature-hash": "feature-hash",
          "race-count": "1",
          "row-count": "1",
          "schema-version": "1",
          "source-etag": "source-etag",
        },
      });
    const bucket = makeBucket(new Uint8Array([1]), { head });
    const decodeDayBase = vi.fn(injected([validRow()]).decodeDayBase);
    const result = await materializeDayBasePerRaceCache(
      { category: "jra", env: bucket.env, runYmd: "20260823" },
      { decodeDayBase },
    );
    expect(result.status).toBe("materialized");
    expect(decodeDayBase).not.toHaveBeenCalled();
  });

  test("normalizes bigint and Date, hashes entry sets independent of row order, and supports NAR", async () => {
    const first = makeBucket(new Uint8Array([1, 2, 3, 4]));
    const rows = [
      {
        ketto_toroku_bango: "b",
        race_id: "nar:2026:0823:83:1",
        timestamp: new Date("2026-08-23T00:00:00Z"),
        umaban: 2n,
      },
      {
        ketto_toroku_bango: "a",
        race_id: "nar:2026:0823:83:1",
        timestamp: null,
        umaban: "1",
      },
    ];
    const result = await materializeDayBasePerRaceCache(
      { category: "ban-ei", env: first.env, runYmd: "20260823" },
      injected(rows),
    );
    const second = makeBucket(new Uint8Array([1, 2, 3, 4]));
    await materializeDayBasePerRaceCache(
      { category: "ban-ei", env: second.env, runYmd: "20260823" },
      injected([...rows].reverse()),
    );

    expect(result.status).toBe("materialized");
    const firstRace = jsonBody(first.puts[0]);
    const secondRace = jsonBody(second.puts[0]);
    expect(firstRace.contract).toMatchObject(secondRace.contract as object);
    expect(firstRace.rows).toMatchObject([
      { timestamp: "2026-08-23T00:00:00.000Z", umaban: 2 },
      { timestamp: null, umaban: "1" },
    ]);
  });

  test("filters well-formed daysAhead races while materializing the requested day", async () => {
    const bucket = makeBucket(new Uint8Array([1, 2, 3, 4]));
    const result = await materializeDayBasePerRaceCache(
      { category: "ban-ei", env: bucket.env, runYmd: "20260823" },
      injected([
        {
          ketto_toroku_bango: "tomorrow",
          race_id: "nar:2026:0824:83:12",
          umaban: 1,
        },
        {
          ketto_toroku_bango: "today",
          race_id: "nar:2026:0823:83:01",
          umaban: 1,
        },
      ]),
    );

    expect(result).toMatchObject({ raceCount: 1, rowCount: 1, status: "materialized" });
    expect(jsonBody(bucket.puts[0])).toMatchObject({ raceId: "nar:2026:0823:83:01" });
  });

  test.each([
    ["invalid-run-ymd", "2026-08-23", [validRow()], ["race_id"]],
    ["unsupported-schema", "20260823", [validRow()], []],
    ["unsupported-schema", "20260823", [validRow()], ["race_id", "race_id"]],
    ["row-limit", "20260823", [], ["race_id"]],
    ["unsupported-cell", "20260823", [null], ["race_id"]],
    ["unsupported-cell", "20260823", [validRow({ value: Number.POSITIVE_INFINITY })], ["race_id"]],
    ["unsupported-cell", "20260823", [validRow({ value: undefined })], ["race_id"]],
    [
      "unsupported-cell",
      "20260823",
      [validRow({ value: BigInt(Number.MAX_SAFE_INTEGER) + 1n })],
      ["race_id"],
    ],
    ["invalid-race-id", "20260823", [validRow({ race_id: 1 })], ["race_id"]],
    ["invalid-race-id", "20260823", [validRow({ race_id: "jra:bad" })], ["race_id"]],
    ["invalid-race-id", "20260823", [validRow({ race_id: "nar:2026:0823:01:01" })], ["race_id"]],
    ["race-limit", "20260823", [validRow({ race_id: "jra:2026:0822:01:01" })], ["race_id"]],
    ["invalid-race-id", "20260823", [validRow({ race_id: "jra:2026:0823:abc:01" })], ["race_id"]],
    ["invalid-race-id", "20260823", [validRow({ race_id: "jra:2026:0823:01:abc" })], ["race_id"]],
    ["invalid-entry", "20260823", [validRow({ ketto_toroku_bango: "" })], ["race_id"]],
    ["invalid-entry", "20260823", [validRow({ umaban: 0 })], ["race_id"]],
    ["invalid-entry", "20260823", [validRow({ umaban: "x" })], ["race_id"]],
    ["invalid-entry", "20260823", [validRow({ umaban: true })], ["race_id"]],
    ["duplicate-entry", "20260823", [validRow(), validRow()], ["race_id"]],
  ])("fails closed for %s", async (reason, runYmd, rows, featureNames) => {
    const bucket = makeBucket(new Uint8Array([1]));
    await expect(
      materializeDayBasePerRaceCache(
        { category: "jra", env: bucket.env, runYmd },
        injected(rows, featureNames),
      ),
    ).resolves.toStrictEqual({ reason, status: "fallback" });
    expect(bucket.puts).toHaveLength(0);
  });

  test("rejects source misses and unsupported source sizes before decoding", async () => {
    const miss = makeBucket(fixture, { head: async () => null });
    await expect(
      materializeDayBasePerRaceCache({ category: "jra", env: miss.env, runYmd: "20260823" }),
    ).resolves.toStrictEqual({ reason: "day-base-miss", status: "fallback" });

    const empty = makeBucket(fixture, {
      head: async () => ({ etag: "x", size: 0, version: undefined }),
    });
    await expect(
      materializeDayBasePerRaceCache({ category: "jra", env: empty.env, runYmd: "20260823" }),
    ).resolves.toStrictEqual({ reason: "source-size-limit", status: "fallback" });

    const huge = makeBucket(fixture, {
      head: async () => ({ etag: "x", size: 17 * 1024 * 1024, version: undefined }),
    });
    await expect(
      materializeDayBasePerRaceCache({ category: "jra", env: huge.env, runYmd: "20260823" }),
    ).resolves.toStrictEqual({ reason: "source-size-limit", status: "fallback" });
  });

  test.each([
    ["unsupported-range", async (file: AsyncBuffer) => file.slice(-1)],
    ["range-out-of-bounds", async (file: AsyncBuffer) => file.slice(0, 5)],
  ])("fails closed when range reader reports %s", async (reason, exercise) => {
    const bucket = makeBucket(new Uint8Array([1, 2, 3, 4]));
    const result = await materializeDayBasePerRaceCache(
      { category: "jra", env: bucket.env, runYmd: "20260823" },
      {
        decodeDayBase: async (file) => {
          await exercise(file);
          return { featureSchema: schemaFor(["race_id"]), rows: [validRow()] };
        },
      },
    );
    expect(result).toStrictEqual({ reason, status: "fallback" });
  });

  test("handles an empty range without an R2 request", async () => {
    const bucket = makeBucket(new Uint8Array([1]));
    const result = await materializeDayBasePerRaceCache(
      { category: "jra", env: bucket.env, runYmd: "20260823" },
      {
        decodeDayBase: async (file) => {
          await expect(file.slice(0, 0)).resolves.toHaveProperty("byteLength", 0);
          return {
            featureSchema: schemaFor(["race_id", "ketto_toroku_bango", "umaban"]),
            rows: [validRow()],
          };
        },
      },
    );
    expect(result.status).toBe("materialized");
    expect(bucket.get).not.toHaveBeenCalled();
  });

  test("bounds range request count and cumulative bytes", async () => {
    const requestLimited = makeBucket(new Uint8Array([1, 2, 3, 4]));
    const requests = await materializeDayBasePerRaceCache(
      { category: "jra", env: requestLimited.env, runYmd: "20260823" },
      {
        decodeDayBase: async (file) => {
          const readAgain = async (remaining: number): Promise<void> => {
            if (remaining === 0) return;
            await file.slice(0, 1);
            await readAgain(remaining - 1);
          };
          await readAgain(257);
          return { featureSchema: schemaFor(["race_id"]), rows: [validRow()] };
        },
      },
    );
    expect(requests).toStrictEqual({ reason: "range-request-limit", status: "fallback" });

    const byteLimited = makeBucket(new Uint8Array(16 * 1024 * 1024));
    const bytes = await materializeDayBasePerRaceCache(
      { category: "jra", env: byteLimited.env, runYmd: "20260823" },
      {
        decodeDayBase: async (file) => {
          await file.slice(0, file.byteLength);
          await file.slice(0, file.byteLength);
          await file.slice(0, file.byteLength);
          await file.slice(0, 1);
          return { featureSchema: schemaFor(["race_id"]), rows: [validRow()] };
        },
      },
    );
    expect(bytes).toStrictEqual({ reason: "range-byte-limit", status: "fallback" });
  });

  test("fails closed when a range disappears or returns the wrong length", async () => {
    const missing = makeBucket(new Uint8Array([1]), { get: async () => null });
    const decodeByRange = {
      decodeDayBase: async (file: AsyncBuffer) => {
        await file.slice(0, 1);
        return { featureSchema: schemaFor(["race_id"]), rows: [validRow()] };
      },
    };
    await expect(
      materializeDayBasePerRaceCache(
        { category: "jra", env: missing.env, runYmd: "20260823" },
        decodeByRange,
      ),
    ).resolves.toStrictEqual({ reason: "source-disappeared", status: "fallback" });

    const wrong = makeBucket(new Uint8Array([1]), {
      get: async () => ({ arrayBuffer: async () => new ArrayBuffer(0) }),
    });
    await expect(
      materializeDayBasePerRaceCache(
        { category: "jra", env: wrong.env, runYmd: "20260823" },
        decodeByRange,
      ),
    ).resolves.toStrictEqual({ reason: "invalid-range-response", status: "fallback" });
  });

  test("enforces race and per-race row bounds", async () => {
    const tooManyRaces = Array.from({ length: 65 }, (_, index) =>
      validRow({ race_id: `jra:2026:0823:${index + 1}:01` }),
    );
    const raceBucket = makeBucket(new Uint8Array([1]));
    await expect(
      materializeDayBasePerRaceCache(
        { category: "jra", env: raceBucket.env, runYmd: "20260823" },
        injected(tooManyRaces),
      ),
    ).resolves.toStrictEqual({ reason: "race-limit", status: "fallback" });

    const tooManyEntries = Array.from({ length: 33 }, (_, index) =>
      validRow({ ketto_toroku_bango: String(index), umaban: index + 1 }),
    );
    const rowBucket = makeBucket(new Uint8Array([1]));
    await expect(
      materializeDayBasePerRaceCache(
        { category: "jra", env: rowBucket.env, runYmd: "20260823" },
        injected(tooManyEntries),
      ),
    ).resolves.toStrictEqual({ reason: "race-row-limit", status: "fallback" });
  });

  test("does not publish a manifest when a race write fails", async () => {
    const bucket = makeBucket(new Uint8Array([1]), {
      put: async () => {
        throw new Error("r2-write-failed");
      },
    });
    await expect(
      materializeDayBasePerRaceCache(
        { category: "jra", env: bucket.env, runYmd: "20260823" },
        injected([validRow()]),
      ),
    ).resolves.toStrictEqual({ reason: "r2-write-failed", status: "fallback" });
    expect(bucket.puts).toHaveLength(0);
  });

  test("uses an empty source version and writes more than one bounded PUT batch", async () => {
    const bytes = new Uint8Array([1]);
    const bucket = makeBucket(bytes, {
      head: async () => ({ etag: "source-etag", size: bytes.byteLength }),
    });
    const rows = Array.from({ length: 5 }, (_, index) =>
      validRow({ race_id: `jra:2026:0823:0${index + 1}:01` }),
    );
    const result = await materializeDayBasePerRaceCache(
      { category: "jra", env: bucket.env, runYmd: "20260823" },
      injected(rows),
    );
    expect(result).toMatchObject({ raceCount: 5, status: "materialized" });
    expect(jsonBody(bucket.puts[0]).source).toMatchObject({ version: "" });
    expect(bucket.puts).toHaveLength(6);
  });

  test("rejects oversized race and aggregate JSON before publishing a manifest", async () => {
    const raceBucket = makeBucket(new Uint8Array([1]));
    const oversizedRace = validRow({ payload: "x".repeat(2 * 1024 * 1024) });
    await expect(
      materializeDayBasePerRaceCache(
        { category: "jra", env: raceBucket.env, runYmd: "20260823" },
        injected([oversizedRace]),
      ),
    ).resolves.toStrictEqual({ reason: "race-json-size-limit", status: "fallback" });
    expect(raceBucket.puts).toHaveLength(0);

    const totalBucket = makeBucket(new Uint8Array([1]));
    const aggregateRows = Array.from({ length: 64 }, (_, index) =>
      validRow({
        payload: "x".repeat(525_000),
        race_id: `jra:2026:0823:${index + 1}:01`,
      }),
    );
    await expect(
      materializeDayBasePerRaceCache(
        { category: "jra", env: totalBucket.env, runYmd: "20260823" },
        injected(aggregateRows),
      ),
    ).resolves.toStrictEqual({ reason: "total-json-size-limit", status: "fallback" });
    expect(totalBucket.puts).toHaveLength(0);
  });

  test("normalizes unknown failures to a stable fallback reason", async () => {
    const bucket = makeBucket(new Uint8Array([1]));
    await expect(
      materializeDayBasePerRaceCache(
        { category: "jra", env: bucket.env, runYmd: "20260823" },
        { decodeDayBase: async () => Promise.reject("unknown") },
      ),
    ).resolves.toStrictEqual({ reason: "materialize-failed", status: "fallback" });
    await expect(
      materializeDayBasePerRaceCache(
        { category: "jra", env: bucket.env, runYmd: "20260823" },
        { decodeDayBase: async () => Promise.reject(new Error()) },
      ),
    ).resolves.toStrictEqual({ reason: "materialize-failed", status: "fallback" });
  });

  test("normalizes production missing-float NaN to nullable JSON while rejecting infinities", async () => {
    const bucket = makeBucket(new Uint8Array([1]));
    const result = await materializeDayBasePerRaceCache(
      { category: "jra", env: bucket.env, runYmd: "20260823" },
      injected([validRow({ weight_trend_5_1: Number.NaN })]),
    );
    expect(result.status).toBe("materialized");
    expect(jsonBody(bucket.puts[0]).rows).toMatchObject([{ weight_trend_5_1: null }]);
  });
});
