// run with: bun run test
import { Buffer } from "node:buffer";
import { Writable } from "node:stream";

import { ParquetSchema, ParquetWriter } from "@dsnp/parquetjs";
import { beforeEach, expect, it, vi } from "vitest";

import type { RunningStyleRaceParams } from "./running-style-features";
import {
  buildFinishPositionDayBaseKey,
  buildRunningStyleFoundationKey,
  clearFinishPositionDayBaseCache,
  loadRunningStyleFeaturesFromFinishPositionDayBase,
  toRunningStyleParquetNumberOrNull,
} from "./running-style-finish-feature-hit";

const RACE: RunningStyleRaceParams = {
  kaisaiNen: "2026",
  kaisaiTsukihi: "0822",
  keibajoCode: "4",
  raceBango: "3",
  source: "jra",
};

class MemorySink extends Writable {
  readonly chunks: Buffer[] = [];

  override _write(
    chunk: Buffer,
    _encoding: BufferEncoding,
    callback: (error?: Error | null) => void,
  ): void {
    this.chunks.push(Buffer.from(chunk));
    callback();
  }
}

const parquetBytes = async (
  rows: ReadonlyArray<Record<string, unknown>>,
  includeFeature = true,
): Promise<ArrayBuffer> => {
  const sink = new MemorySink();
  const schema = new ParquetSchema({
    source: { type: "UTF8" },
    kaisai_nen: { type: "UTF8" },
    kaisai_tsukihi: { type: "UTF8" },
    keibajo_code: { type: "UTF8" },
    race_bango: { type: "UTF8" },
    ketto_toroku_bango: { type: "UTF8" },
    umaban: { type: "INT32" },
    category: { optional: true, type: "UTF8" },
    kyori: { optional: true, type: "INT32" },
    track_code: { optional: true, type: "UTF8" },
    grade_code: { optional: true, type: "UTF8" },
    shusso_tosu: { optional: true, type: "INT32" },
    kyoso_joken_code: { optional: true, type: "UTF8" },
    nar_subclass: { optional: true, type: "UTF8" },
    ...(includeFeature ? { f1: { optional: true, type: "DOUBLE" } } : {}),
  });
  const writer = await ParquetWriter.openStream(schema, sink);
  for (const row of rows) await writer.appendRow(row);
  await writer.close();
  const bytes = Buffer.concat(sink.chunks);
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
};

const metadata = {
  "max-data-sakusei-nengappi": "20260822",
  "row-count": "12",
  "rs-predicted-at-max": "none",
  "rs-row-count": "0",
};

const rawRow = (overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
  category: "jra",
  f1: 1.25,
  grade_code: "A",
  kaisai_nen: "2026",
  kaisai_tsukihi: "0822",
  keibajo_code: "04",
  ketto_toroku_bango: "2023100001",
  kyori: 1600,
  kyoso_joken_code: "703",
  race_bango: "03",
  shusso_tosu: 12,
  source: "jra",
  track_code: "11",
  umaban: 1,
  ...overrides,
});

const bucketWith = (bytes: ArrayBuffer, etag = "etag-1") => {
  const head = vi.fn(async () => ({ customMetadata: metadata, etag, size: bytes.byteLength }));
  const get = vi.fn(async (_key: string, options: R2GetOptions) => ({
    arrayBuffer: vi.fn(async () => {
      const range = options.range;
      if (range instanceof Headers || range === undefined || !("offset" in range)) return bytes;
      const start = range.offset ?? 0;
      const end = range.length === undefined ? bytes.byteLength : start + range.length;
      return bytes.slice(start, end);
    }),
    customMetadata: metadata,
    etag,
    size: bytes.byteLength,
  }));
  return { bucket: { get, head } as unknown as R2Bucket, get, head };
};

beforeEach(() => {
  clearFinishPositionDayBaseCache();
});

it("builds the shared finish-position day-base key", () => {
  expect(buildFinishPositionDayBaseKey(RACE)).toBe(
    "feat-daybase/catalog-v1/jra/20260822/features.parquet",
  );
});

it("builds the RS-independent daily foundation key", () => {
  expect(buildRunningStyleFoundationKey(RACE)).toBe(
    "feat-running-style-base/catalog-v1/jra/20260822/features.parquet",
  );
});

it("normalizes every Parquet scalar representation without changing numeric values", () => {
  expect(toRunningStyleParquetNumberOrNull(null)).toBe(null);
  expect(toRunningStyleParquetNumberOrNull(undefined)).toBe(null);
  expect(toRunningStyleParquetNumberOrNull("")).toBe(null);
  expect(toRunningStyleParquetNumberOrNull(1.25)).toBe(1.25);
  expect(toRunningStyleParquetNumberOrNull(Number.POSITIVE_INFINITY)).toBe(null);
  expect(toRunningStyleParquetNumberOrNull(BigInt(2))).toBe(2);
  expect(toRunningStyleParquetNumberOrNull(true)).toBe(1);
  expect(toRunningStyleParquetNumberOrNull(false)).toBe(0);
  expect(toRunningStyleParquetNumberOrNull("3.5")).toBe(3.5);
  expect(toRunningStyleParquetNumberOrNull("invalid")).toBe(null);
  expect(toRunningStyleParquetNumberOrNull({ value: 1 })).toBe(null);
});

it("prefers the daily foundation and falls back to the final day-base", async () => {
  const bytes = await parquetBytes([rawRow()]);
  const foundationKey = buildRunningStyleFoundationKey(RACE);
  const dayBaseKey = buildFinishPositionDayBaseKey(RACE);
  const head = vi.fn(async (key: string) =>
    key === foundationKey
      ? null
      : { customMetadata: metadata, etag: "final-etag", size: bytes.byteLength },
  );
  const get = vi.fn(async (key: string, options: R2GetOptions) =>
    key === dayBaseKey
      ? {
          arrayBuffer: vi.fn(async () => {
            const range = options.range;
            if (range instanceof Headers || range === undefined || !("offset" in range))
              return bytes;
            const start = range.offset ?? 0;
            const end = range.length === undefined ? bytes.byteLength : start + range.length;
            return bytes.slice(start, end);
          }),
          customMetadata: metadata,
          etag: "final-etag",
          size: bytes.byteLength,
        }
      : null,
  );

  const rows = await loadRunningStyleFeaturesFromFinishPositionDayBase({
    bucket: { get, head } as unknown as R2Bucket,
    featureNames: ["f1"],
    race: RACE,
  });

  expect(rows).toHaveLength(1);
  expect(head.mock.calls.map(([key]) => key)).toStrictEqual([foundationKey, dayBaseKey]);
  expect(get.mock.calls[0]?.[0]).toBe("feat-daybase/catalog-v1/jra/20260822/features.parquet");
});

it("returns a complete race slice and reuses the etag cache", async () => {
  const bytes = await parquetBytes([
    rawRow(),
    rawRow({ keibajo_code: "05", ketto_toroku_bango: "2023100002", race_bango: "01" }),
  ]);
  const { bucket, get, head } = bucketWith(bytes);
  const first = await loadRunningStyleFeaturesFromFinishPositionDayBase({
    bucket,
    featureNames: ["f1"],
    race: RACE,
  });
  get.mockClear();
  const second = await loadRunningStyleFeaturesFromFinishPositionDayBase({
    bucket,
    featureNames: ["f1"],
    race: RACE,
  });
  expect(first).toStrictEqual([
    {
      bamei: null,
      category: "jra",
      gradeCode: "A",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0822",
      keibajoCode: "04",
      kettoTorokuBango: "2023100001",
      kyori: 1600,
      kyosoJokenCode: "703",
      narSubClass: null,
      peerInputs: {
        careerWinRate: null,
        kohan3fAvg5: null,
        pastCorner1NormAvg5: null,
        pastFirst3fAvg5: null,
        pastNigeRate: null,
        pastOikomiRate: null,
        pastSashiRate: null,
        pastSenkouRate: null,
        speedIndexAvg5: null,
        speedIndexBest5: null,
      },
      perHorseFeatures: { f1: 1.25 },
      raceBango: "03",
      raceKey: "jra:20260822:04:03",
      shussoTosu: 12,
      source: "jra",
      trackCode: "11",
      umaban: 1,
    },
  ]);
  expect(second).toStrictEqual(first);
  expect(head).toHaveBeenCalledTimes(2);
  expect(get).toHaveBeenCalledTimes(0);
});

it("reads non-contiguous rows for only the requested race in file order", async () => {
  const bytes = await parquetBytes([
    rawRow({ ketto_toroku_bango: "2023100001", umaban: 1 }),
    rawRow({ keibajo_code: "05", ketto_toroku_bango: "2023100002", race_bango: "01" }),
    rawRow({ ketto_toroku_bango: "2023100003", umaban: 3 }),
  ]);
  const { bucket } = bucketWith(bytes);

  const rows = await loadRunningStyleFeaturesFromFinishPositionDayBase({
    bucket,
    featureNames: ["f1"],
    race: RACE,
  });

  expect(rows?.map((row) => row.kettoTorokuBango)).toStrictEqual(["2023100001", "2023100003"]);
  expect(rows?.map((row) => row.umaban)).toStrictEqual([1, 3]);
});

it("evicts old race slices after the bounded cache reaches two entries", async () => {
  const bytes = await parquetBytes([
    rawRow({ ketto_toroku_bango: "2023100001", race_bango: "03" }),
    rawRow({ ketto_toroku_bango: "2023100002", race_bango: "04" }),
    rawRow({ ketto_toroku_bango: "2023100003", race_bango: "05" }),
  ]);
  const { bucket, get } = bucketWith(bytes);
  await loadRunningStyleFeaturesFromFinishPositionDayBase({
    bucket,
    featureNames: ["f1"],
    race: RACE,
  });
  await loadRunningStyleFeaturesFromFinishPositionDayBase({
    bucket,
    featureNames: ["f1"],
    race: { ...RACE, raceBango: "04" },
  });
  await loadRunningStyleFeaturesFromFinishPositionDayBase({
    bucket,
    featureNames: ["f1"],
    race: { ...RACE, raceBango: "05" },
  });
  get.mockClear();

  await loadRunningStyleFeaturesFromFinishPositionDayBase({
    bucket,
    featureNames: ["f1"],
    race: RACE,
  });

  expect(get.mock.calls.length).toBeGreaterThan(0);
});

it("misses when the binding, object, watermark, race, or requested feature is absent", async () => {
  await expect(
    loadRunningStyleFeaturesFromFinishPositionDayBase({
      bucket: undefined,
      featureNames: ["f1"],
      race: RACE,
    }),
  ).resolves.toBeNull();
  await expect(
    loadRunningStyleFeaturesFromFinishPositionDayBase({
      bucket: { head: vi.fn(async () => null) } as unknown as R2Bucket,
      featureNames: ["f1"],
      race: RACE,
    }),
  ).resolves.toBeNull();
  await expect(
    loadRunningStyleFeaturesFromFinishPositionDayBase({
      bucket: {
        head: vi.fn(async () => ({ customMetadata: {}, etag: "x" })),
      } as unknown as R2Bucket,
      featureNames: ["f1"],
      race: RACE,
    }),
  ).resolves.toBeNull();

  const wrongRace = bucketWith(await parquetBytes([rawRow({ race_bango: "04" })]));
  await expect(
    loadRunningStyleFeaturesFromFinishPositionDayBase({
      bucket: wrongRace.bucket,
      featureNames: ["f1"],
      race: RACE,
    }),
  ).resolves.toBeNull();
  wrongRace.get.mockClear();
  await expect(
    loadRunningStyleFeaturesFromFinishPositionDayBase({
      bucket: wrongRace.bucket,
      featureNames: ["f1"],
      race: RACE,
    }),
  ).resolves.toBeNull();
  expect(wrongRace.get).toHaveBeenCalledTimes(0);
  clearFinishPositionDayBaseCache();
  const missingFeature = bucketWith(await parquetBytes([rawRow()], false));
  await expect(
    loadRunningStyleFeaturesFromFinishPositionDayBase({
      bucket: missingFeature.bucket,
      featureNames: ["f1"],
      race: RACE,
    }),
  ).resolves.toBeNull();
});

it("misses invalid rows and rejects a vanished or inconsistent ranged body", async () => {
  const invalid = bucketWith(
    await parquetBytes([rawRow({ kaisai_nen: "26", source: "other", umaban: 0 })]),
  );
  await expect(
    loadRunningStyleFeaturesFromFinishPositionDayBase({
      bucket: invalid.bucket,
      featureNames: ["f1"],
      race: RACE,
    }),
  ).resolves.toBeNull();

  const vanished = {
    get: vi.fn(async () => null),
    head: vi.fn(async () => ({ customMetadata: metadata, etag: "x", size: 100 })),
  } as unknown as R2Bucket;
  await expect(
    loadRunningStyleFeaturesFromFinishPositionDayBase({
      bucket: vanished,
      featureNames: ["f1"],
      race: RACE,
    }),
  ).rejects.toThrow("R2 object changed while reading");

  const bytes = await parquetBytes([rawRow()]);
  const inconsistent = {
    get: vi.fn(async () => ({ etag: "x" })),
    head: vi.fn(async () => ({ customMetadata: metadata, etag: "x", size: bytes.byteLength })),
  } as unknown as R2Bucket;
  await expect(
    loadRunningStyleFeaturesFromFinishPositionDayBase({
      bucket: inconsistent,
      featureNames: ["f1"],
      race: RACE,
    }),
  ).rejects.toThrow("R2 object changed while reading");
});
