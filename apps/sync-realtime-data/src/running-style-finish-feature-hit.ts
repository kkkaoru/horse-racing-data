// Run with bun. Reads the shared finish-position day-base before rebuilding
// the same early-binding running-style inputs from Catalog/PostgreSQL.

import {
  parquetMetadataAsync,
  parquetReadObjects,
  parquetSchema,
  type AsyncBuffer,
} from "hyparquet";

import type { ByteRangeCache } from "./byte-range-cache";
import { deriveRunningStyleCategory } from "./running-style-cell-router";
import { buildRunningStyleRaceKey, type RunningStyleRaceParams } from "./running-style-features";
import { isRunningStyleDerivedFieldFeature } from "./running-style-field-features";
import type { RaceHorseFeatureRow } from "./running-style-r2";
import type { Env } from "./types";

interface R2AsyncBufferParams {
  bucket: R2Bucket;
  key: string;
  object: R2Object;
  byteRangeCache: ByteRangeCache | undefined;
}

const DAY_BASE_PREFIX = "feat-daybase/catalog-v1";
const RUNNING_STYLE_FOUNDATION_PREFIX = "feat-running-style-base/catalog-v1";
const DAY_BASE_FILE = "features.parquet";
const MAX_CACHED_RACES = 2;
const MANIFEST_PATH = "/api/internal/day-base-manifest";
// Running-style inference consumes the early-binding feature columns in the
// day-base artifact. The rs-* watermarks describe downstream running-style
// predictions and are necessarily absent before this consumer runs; requiring
// them here creates a generation deadlock where a valid day-base can never HIT.
const WATERMARK_METADATA_KEYS = ["max-data-sakusei-nengappi", "row-count"] as const;

const PEER_INPUT_FEATURES = {
  career_win_rate: "careerWinRate",
  kohan3f_avg_5: "kohan3fAvg5",
  past_corner_1_norm_avg_5: "pastCorner1NormAvg5",
  past_first_3f_avg_5: "pastFirst3fAvg5",
  past_nige_rate_self: "pastNigeRate",
  past_oikomi_rate_self: "pastOikomiRate",
  past_sashi_rate_self: "pastSashiRate",
  past_senkou_rate_self: "pastSenkouRate",
  speed_index_avg_5: "speedIndexAvg5",
  speed_index_best_5: "speedIndexBest5",
} as const;

interface CachedDayBase {
  etag: string;
  rows: ReadonlyArray<RaceHorseFeatureRow>;
}

const dayBaseCache = new Map<string, CachedDayBase>();

interface RowRange {
  end: number;
  start: number;
}

interface ReadRaceRowsParams {
  featureNames: ReadonlyArray<string>;
  file: AsyncBuffer;
  race: RunningStyleRaceParams;
}

interface ReadRangesParams {
  columns: string[];
  file: AsyncBuffer;
  metadata: Awaited<ReturnType<typeof parquetMetadataAsync>>;
  rangeIndex: number;
  ranges: ReadonlyArray<RowRange>;
}

const RACE_IDENTITY_COLUMNS = [
  "source",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
] satisfies string[];

const ROW_COLUMNS = [
  ...RACE_IDENTITY_COLUMNS,
  "ketto_toroku_bango",
  "umaban",
  "category",
  "bamei",
  "kyori",
  "track_code",
  "grade_code",
  "shusso_tosu",
  "kyoso_joken_code",
  "nar_subclass",
] satisfies string[];

export const toRunningStyleParquetNumberOrNull = (value: unknown): number | null => {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "boolean") return value ? 1 : 0;
  if (typeof value !== "string") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const toStringOrNull = (value: unknown): string | null => {
  if (typeof value !== "string" || value.length === 0) return null;
  return value;
};

const normalizedCode = (value: unknown): string => String(value).padStart(2, "0");

const hasFreshnessMetadata = (object: R2Object): boolean => {
  const metadata = object.customMetadata;
  if (metadata === undefined) return false;
  return WATERMARK_METADATA_KEYS.every((key) => {
    const value = metadata[key];
    return typeof value === "string" && value.length > 0;
  });
};

const resolveFinishPositionFeatureCategory = (params: RunningStyleRaceParams): string =>
  deriveRunningStyleCategory({ keibajoCode: params.keibajoCode, source: params.source });

export const buildFinishPositionDayBaseKey = (params: RunningStyleRaceParams): string =>
  `${DAY_BASE_PREFIX}/${resolveFinishPositionFeatureCategory(params)}/${params.kaisaiNen}${params.kaisaiTsukihi}/${DAY_BASE_FILE}`;

export const buildRunningStyleFoundationKey = (params: RunningStyleRaceParams): string =>
  `${RUNNING_STYLE_FOUNDATION_PREFIX}/${resolveFinishPositionFeatureCategory(params)}/${params.kaisaiNen}${params.kaisaiTsukihi}/${DAY_BASE_FILE}`;

const toFeatureRow = (
  raw: Record<string, unknown>,
  featureNames: ReadonlyArray<string>,
): RaceHorseFeatureRow | null => {
  const source = raw.source;
  if (source !== "jra" && source !== "nar") return null;
  const kaisaiNen = String(raw.kaisai_nen);
  const kaisaiTsukihi = String(raw.kaisai_tsukihi).padStart(4, "0");
  const keibajoCode = normalizedCode(raw.keibajo_code);
  const raceBango = normalizedCode(raw.race_bango);
  const kettoTorokuBango = String(raw.ketto_toroku_bango);
  const umaban = toRunningStyleParquetNumberOrNull(raw.umaban);
  if (kaisaiNen.length !== 4 || kaisaiTsukihi.length !== 4 || kettoTorokuBango.length === 0)
    return null;
  if (umaban === null) return null;

  const perHorseFeatures: Record<string, number | null> = {};
  for (const name of featureNames) {
    if (!Object.hasOwn(raw, name)) return null;
    perHorseFeatures[name] = toRunningStyleParquetNumberOrNull(raw[name]);
  }
  const peerInputs = {} as RaceHorseFeatureRow["peerInputs"];
  Object.entries(PEER_INPUT_FEATURES).forEach(([featureName, peerName]) => {
    peerInputs[peerName] = perHorseFeatures[featureName] ?? null;
  });
  return {
    bamei: toStringOrNull(raw.bamei),
    category: typeof raw.category === "string" ? raw.category : source,
    kaisaiNen,
    kaisaiTsukihi,
    keibajoCode,
    kettoTorokuBango,
    kyori: toRunningStyleParquetNumberOrNull(raw.kyori),
    kyosoJokenCode: toStringOrNull(raw.kyoso_joken_code),
    gradeCode: toStringOrNull(raw.grade_code),
    narSubClass: toStringOrNull(raw.nar_subclass),
    peerInputs,
    perHorseFeatures,
    raceBango,
    raceKey: `${source}:${kaisaiNen}${kaisaiTsukihi}:${keibajoCode}:${raceBango}`,
    shussoTosu: toRunningStyleParquetNumberOrNull(raw.shusso_tosu),
    source,
    trackCode: toStringOrNull(raw.track_code),
    umaban,
  };
};

const matchesRace = (raw: Record<string, unknown>, race: RunningStyleRaceParams): boolean =>
  raw.source === race.source &&
  String(raw.kaisai_nen) === race.kaisaiNen &&
  String(raw.kaisai_tsukihi).padStart(4, "0") === race.kaisaiTsukihi &&
  normalizedCode(raw.keibajo_code) === race.keibajoCode.padStart(2, "0") &&
  normalizedCode(raw.race_bango) === race.raceBango.padStart(2, "0");

const contiguousRanges = (indices: ReadonlyArray<number>): RowRange[] =>
  indices.reduce<RowRange[]>((ranges, index) => {
    const last = ranges.at(-1);
    if (last !== undefined && last.end === index) {
      last.end = index + 1;
      return ranges;
    }
    ranges.push({ end: index + 1, start: index });
    return ranges;
  }, []);

const readRanges = async (params: ReadRangesParams): Promise<Record<string, unknown>[]> => {
  const range = params.ranges[params.rangeIndex];
  if (range === undefined) return [];
  const rows = await parquetReadObjects({
    columns: params.columns,
    file: params.file,
    metadata: params.metadata,
    rowEnd: range.end,
    rowStart: range.start,
    useOffsetIndex: true,
  });
  const remaining = await readRanges({ ...params, rangeIndex: params.rangeIndex + 1 });
  return rows.concat(remaining);
};

const readRaceRows = async (
  params: ReadRaceRowsParams,
): Promise<ReadonlyArray<RaceHorseFeatureRow>> => {
  const metadata = await parquetMetadataAsync(params.file);
  const available = new Set(parquetSchema(metadata).children.map((child) => child.element.name));
  if (RACE_IDENTITY_COLUMNS.some((column) => !available.has(column))) return [];
  const identities = await parquetReadObjects({
    columns: RACE_IDENTITY_COLUMNS,
    file: params.file,
    metadata,
  });
  const indices = identities.flatMap((raw, index) =>
    matchesRace(raw, params.race) ? [index] : [],
  );
  if (indices.length === 0) return [];
  // Field-relative inputs are computed after all runners are loaded, not stored in the raw day base.
  const rawFeatureNames = params.featureNames.filter(
    (name) => !isRunningStyleDerivedFieldFeature(name),
  );
  if (rawFeatureNames.some((name) => !available.has(name))) return [];
  const columns = [...new Set([...ROW_COLUMNS, ...rawFeatureNames])].filter((name) =>
    available.has(name),
  );
  const decoded = await readRanges({
    columns,
    file: params.file,
    metadata,
    rangeIndex: 0,
    ranges: contiguousRanges(indices),
  });
  return decoded.flatMap((raw) => {
    const row = toFeatureRow(raw, rawFeatureNames);
    return row === null ? [] : [row];
  });
};

const r2AsyncBuffer = ({
  bucket,
  key,
  object,
  byteRangeCache,
}: R2AsyncBufferParams): AsyncBuffer => ({
  byteLength: object.size,
  slice: async (start, end) => {
    const rangeEnd: number = end === undefined ? object.size : end;
    const load = async (): Promise<ArrayBuffer> => {
      const ranged = await bucket.get(key, {
        onlyIf: { etagMatches: object.etag },
        range: { length: rangeEnd - start, offset: start },
      });
      if (ranged === null || !("arrayBuffer" in ranged)) {
        throw new Error(`R2 object changed while reading: ${key}`);
      }
      return ranged.arrayBuffer();
    };
    return byteRangeCache === undefined
      ? load()
      : byteRangeCache.read(JSON.stringify([key, object.etag, start, rangeEnd]), load);
  },
});

const rememberDayBase = (key: string, value: CachedDayBase): void => {
  dayBaseCache.delete(key);
  dayBaseCache.set(key, value);
  if (dayBaseCache.size <= MAX_CACHED_RACES) return;
  const oldest = dayBaseCache.keys().next().value;
  if (typeof oldest === "string") dayBaseCache.delete(oldest);
};

export const resolveFinishPositionDayBasePointer = async (params: {
  category: string;
  env: Pick<Env, "DAY_BASE_MANIFEST_REQUIRED" | "FINISH_POSITION_CRON" | "TRIGGER_TOKEN">;
  runYmd: string;
}): Promise<{ key: string; etag: string } | "legacy" | null> => {
  if (params.env.DAY_BASE_MANIFEST_REQUIRED === undefined) return "legacy";
  const { FINISH_POSITION_CRON: service, TRIGGER_TOKEN: token } = params.env;
  if (service === undefined || token === undefined || token.length === 0) {
    return params.env.DAY_BASE_MANIFEST_REQUIRED === "1" ? null : "legacy";
  }
  const { category, runYmd } = params;
  const url = new URL(MANIFEST_PATH, "https://finish-position-cron.internal");
  url.searchParams.set("category", category);
  url.searchParams.set("runYmd", runYmd);
  try {
    const response = await service.fetch(
      new Request(url, {
        headers: { Authorization: `Bearer ${token}` },
        signal: AbortSignal.timeout(5000),
      }),
    );
    if (response.status === 404 && params.env.DAY_BASE_MANIFEST_REQUIRED !== "1") return "legacy";
    if (!response.ok) return null;
    const value: unknown = await response.json();
    if (typeof value !== "object" || value === null || !("state" in value)) return null;
    if (value.state === "missing") {
      return params.env.DAY_BASE_MANIFEST_REQUIRED === "1" ? null : "legacy";
    }
    if (value.state !== "published" || !("key" in value) || !("etag" in value)) return null;
    const prefix = `${DAY_BASE_PREFIX}/${category}/${runYmd}/generations/`;
    if (
      typeof value.key !== "string" ||
      !value.key.startsWith(prefix) ||
      !/^[A-Za-z0-9_-]{1,128}\/features\.parquet$/.test(value.key.slice(prefix.length)) ||
      typeof value.etag !== "string" ||
      value.etag.length === 0
    )
      return null;
    return { key: value.key, etag: value.etag };
  } catch {
    return null;
  }
};

export const loadRunningStyleFeaturesFromFinishPositionDayBase = async (params: {
  // Only share this completed-byte cache within one invocation and one bucket.
  byteRangeCache?: ByteRangeCache;
  bucket: R2Bucket | undefined;
  env?: Pick<Env, "DAY_BASE_MANIFEST_REQUIRED" | "FINISH_POSITION_CRON" | "TRIGGER_TOKEN">;
  featureNames: ReadonlyArray<string>;
  race: RunningStyleRaceParams;
}): Promise<ReadonlyArray<RaceHorseFeatureRow> | null> => {
  if (params.bucket === undefined) return null;
  const raceKey = buildRunningStyleRaceKey(params.race);
  const pointer =
    params.env === undefined
      ? "legacy"
      : await resolveFinishPositionDayBasePointer({
          category: resolveFinishPositionFeatureCategory(params.race),
          env: params.env,
          runYmd: `${params.race.kaisaiNen}${params.race.kaisaiTsukihi}`,
        });
  const keys = [
    buildRunningStyleFoundationKey(params.race),
    ...(pointer === null
      ? []
      : [pointer === "legacy" ? buildFinishPositionDayBaseKey(params.race) : pointer.key]),
  ];
  for (const key of keys) {
    const cacheKey = `${key}\u0000${raceKey}\u0000${params.featureNames.join("\u0000")}`;
    const head = await params.bucket.head(key);
    if (
      head === null ||
      !hasFreshnessMetadata(head) ||
      (pointer !== null &&
        pointer !== "legacy" &&
        key === pointer.key &&
        head.etag !== pointer.etag)
    )
      continue;
    const cached = dayBaseCache.get(cacheKey);
    if (cached !== undefined && cached.etag === head.etag) {
      if (cached.rows.length > 0) return cached.rows;
      continue;
    }
    const rows = await readRaceRows({
      featureNames: params.featureNames,
      file: r2AsyncBuffer({
        bucket: params.bucket,
        key,
        object: head,
        byteRangeCache: params.byteRangeCache,
      }),
      race: params.race,
    });
    rememberDayBase(cacheKey, { etag: head.etag, rows });
    if (rows.length > 0) return rows;
  }
  return null;
};

export const clearFinishPositionDayBaseCache = (): void => {
  dayBaseCache.clear();
};
