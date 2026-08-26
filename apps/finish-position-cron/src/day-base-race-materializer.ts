// Materialize a completed day-base parquet into bounded, per-race foundation
// objects. These objects intentionally live outside the canonical feat-cache
// namespace: the Container still owns final feature parity and may ignore this
// optimization whenever the contract is missing or unsupported.

import {
  parquetMetadataAsync,
  parquetReadObjects,
  type AsyncBuffer,
  type FileMetaData,
} from "hyparquet";

import { buildDayBaseObjectKey } from "./day-base-object-key";
import type { Env, PredictCategory } from "./types";

const FOUNDATION_PREFIX = "feat-daybase-race";
const FOUNDATION_GENERATION = "catalog-v1";
const FOUNDATION_FILE = "foundation.json";
const MANIFEST_FILE = "manifest.json";
const CONTRACT_SCHEMA_VERSION = "1";
const FOUNDATION_CONTRACT_VERSION = "day-base-race-foundation-v1";
const MAX_DAY_BASE_BYTES = 16 * 1024 * 1024;
const MAX_UNCOMPRESSED_PARQUET_BYTES = 16 * 1024 * 1024;
const MAX_DAY_BASE_ROWS = 1_024;
const MAX_FEATURE_COLUMNS = 512;
const MAX_RACES = 64;
const MAX_ROWS_PER_RACE = 32;
const MAX_RANGE_REQUESTS = 256;
const MAX_RANGE_BYTES = 48 * 1024 * 1024;
const MAX_RACE_JSON_BYTES = 2 * 1024 * 1024;
const MAX_TOTAL_JSON_BYTES = 32 * 1024 * 1024;
const PUT_CONCURRENCY = 4;
const METADATA_INITIAL_FETCH_BYTES = 64 * 1024;
const RACE_ID_FIELD = "race_id";
const KETTO_FIELD = "ketto_toroku_bango";
const UMABAN_FIELD = "umaban";
const encoder = new TextEncoder();

type JsonScalar = boolean | number | string | null;
type FoundationRow = Record<string, JsonScalar>;

interface MaterializeParams {
  category: PredictCategory;
  env: Pick<Env, "FEATURES_CACHE">;
  force?: boolean;
  runYmd: string;
}

interface RaceFoundationReadinessParams extends MaterializeParams {
  raceNumber: string;
  venueCode: string;
}

interface RaceIdentity {
  raceId: string;
  raceNumber: string;
  venueCode: string;
}

interface RaceFoundation {
  entrySetHash: string;
  identity: RaceIdentity;
  rows: FoundationRow[];
}

interface DecodeResult {
  featureSchema: FoundationFeatureField[];
  rows: unknown[];
}

interface FoundationFeatureField {
  convertedType?: string;
  name: string;
  physicalType: string;
  precision?: number;
  scale?: number;
  typeLength?: number;
}

interface MaterializerDependencies {
  decodeDayBase: (file: AsyncBuffer) => Promise<DecodeResult>;
}

interface FoundationContract {
  contractVersion: string;
  entrySetHash: string;
  featureHash: string;
  generationId: string;
  rowCount: number;
  schemaVersion: string;
}

interface ManifestContract {
  contractVersion: string;
  featureHash: string;
  featureSchema: FoundationFeatureField[];
  generationId: string;
  raceCount: number;
  rowCount: number;
  schemaVersion: string;
}

interface FoundationEnvelope {
  contract: FoundationContract;
  raceId: string;
  rows: FoundationRow[];
  source: {
    etag: string;
    key: string;
    version: string;
  };
}

interface ManifestRace {
  entrySetHash: string;
  key: string;
  raceId: string;
  rowCount: number;
}

export type DayBaseRaceMaterializeResult =
  | {
      featureHash: string;
      manifestKey: string;
      raceCount: number;
      rowCount: number;
      status: "materialized";
    }
  | { reason: string; status: "fallback" };

export interface DayBaseRaceFoundationReadiness {
  ready: boolean;
  reason: string;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const hex = (bytes: ArrayBuffer): string =>
  [...new Uint8Array(bytes)].map((value) => value.toString(16).padStart(2, "0")).join("");

const sha256 = async (value: string): Promise<string> =>
  hex(await crypto.subtle.digest("SHA-256", encoder.encode(value)));

const normalizeCell = (value: unknown): JsonScalar | undefined => {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (Number.isNaN(value)) return null;
    return Number.isFinite(value) ? value : undefined;
  }
  if (typeof value === "bigint") {
    const normalized = Number(value);
    return Number.isSafeInteger(normalized) ? normalized : undefined;
  }
  if (value instanceof Date && !Number.isNaN(value.valueOf())) return value.toISOString();
  return undefined;
};

const normalizeRow = (value: unknown): FoundationRow | null => {
  if (!isRecord(value)) return null;
  const normalized: FoundationRow = {};
  const unsupported = Object.entries(value).some(([key, cell]) => {
    const normalizedCell = normalizeCell(cell);
    if (normalizedCell === undefined) return true;
    normalized[key] = normalizedCell;
    return false;
  });
  return unsupported ? null : normalized;
};

const positiveIntegerText = (value: JsonScalar | undefined): string | null => {
  if (typeof value !== "number" && typeof value !== "string") return null;
  const text = String(value).trim();
  if (!/^\d+$/.test(text)) return null;
  const parsed = Number(text);
  return Number.isSafeInteger(parsed) && parsed > 0 ? String(parsed) : null;
};

const parseRaceIdentity = (
  value: JsonScalar | undefined,
  category: PredictCategory,
  runYmd: string,
): RaceIdentity | null => {
  if (typeof value !== "string") return null;
  const parts = value.split(":");
  if (parts.length !== 5) return null;
  const [source, year, monthDay, venue, race] = parts;
  const expectedSource = category === "jra" ? "jra" : "nar";
  if (source !== expectedSource || `${year}${monthDay}` !== runYmd) return null;
  if (venue === undefined || race === undefined || !/^\d{1,2}$/.test(venue)) return null;
  if (!/^\d{1,2}$/.test(race)) return null;
  return {
    raceId: value,
    raceNumber: race.padStart(2, "0"),
    venueCode: venue.padStart(2, "0"),
  };
};

const isWellFormedOutOfScopeRace = (
  value: JsonScalar | undefined,
  category: PredictCategory,
  runYmd: string,
): boolean => {
  if (typeof value !== "string") return false;
  const parts = value.split(":");
  if (parts.length !== 5) return false;
  const [source, year, monthDay, venue, race] = parts;
  const expectedSource = category === "jra" ? "jra" : "nar";
  if (source !== expectedSource) return false;
  if (year === undefined || !/^\d{4}$/.test(year)) return false;
  if (monthDay === undefined || !/^\d{4}$/.test(monthDay)) return false;
  if (venue === undefined || !/^\d{1,2}$/.test(venue)) return false;
  if (race === undefined || !/^\d{1,2}$/.test(race)) return false;
  return `${year}${monthDay}` !== runYmd;
};

const buildFoundationRoot = (category: PredictCategory, runYmd: string): string =>
  `${FOUNDATION_PREFIX}/${FOUNDATION_GENERATION}/${category}/${runYmd}`;

export const buildDayBaseRaceFoundationKey = (
  category: PredictCategory,
  runYmd: string,
  venueCode: string,
  raceNumber: string,
): string =>
  `${buildFoundationRoot(category, runYmd)}/${venueCode.padStart(2, "0")}/${raceNumber.padStart(2, "0")}/${FOUNDATION_FILE}`;

export const buildDayBaseRaceManifestKey = (category: PredictCategory, runYmd: string): string =>
  `${buildFoundationRoot(category, runYmd)}/${MANIFEST_FILE}`;

class R2RangeBuffer implements AsyncBuffer {
  readonly byteLength: number;
  private requestedBytes = 0;
  private requests = 0;

  constructor(
    private readonly bucket: R2Bucket,
    private readonly key: string,
    size: number,
  ) {
    this.byteLength = size;
  }

  async slice(start: number, end = this.byteLength): Promise<ArrayBuffer> {
    if (!Number.isInteger(start) || !Number.isInteger(end) || start < 0 || end < start) {
      throw new Error("unsupported-range");
    }
    if (end > this.byteLength) throw new Error("range-out-of-bounds");
    const length = end - start;
    if (length === 0) return new ArrayBuffer(0);
    if (this.requests + 1 > MAX_RANGE_REQUESTS) throw new Error("range-request-limit");
    if (this.requestedBytes + length > MAX_RANGE_BYTES) throw new Error("range-byte-limit");
    this.requests += 1;
    this.requestedBytes += length;
    const object = await this.bucket.get(this.key, { range: { length, offset: start } });
    if (object === null) throw new Error("source-disappeared");
    const bytes = await object.arrayBuffer();
    if (bytes.byteLength !== length) throw new Error("invalid-range-response");
    return bytes;
  }
}

const defaultDecodeDayBase = async (file: AsyncBuffer): Promise<DecodeResult> => {
  const metadata = await parquetMetadataAsync(file, {
    initialFetchSize: METADATA_INITIAL_FETCH_BYTES,
  });
  if (metadata.num_rows > BigInt(MAX_DAY_BASE_ROWS)) throw new Error("row-limit");
  const featureSchema = metadata.schema.flatMap((element) =>
    element.type === undefined
      ? []
      : [
          {
            ...(element.converted_type === undefined
              ? {}
              : { convertedType: element.converted_type }),
            name: element.name,
            physicalType: element.type,
            ...(element.precision === undefined ? {} : { precision: element.precision }),
            ...(element.scale === undefined ? {} : { scale: element.scale }),
            ...(element.type_length === undefined ? {} : { typeLength: element.type_length }),
          },
        ],
  );
  if (featureSchema.length === 0 || featureSchema.length > MAX_FEATURE_COLUMNS) {
    throw new Error("unsupported-schema");
  }
  const uncompressedBytes = metadata.row_groups.reduce(
    (total, rowGroup) => total + rowGroup.total_byte_size,
    0n,
  );
  if (uncompressedBytes > BigInt(MAX_UNCOMPRESSED_PARQUET_BYTES)) {
    throw new Error("uncompressed-size-limit");
  }
  return {
    featureSchema,
    rows: await readParquetRows(file, metadata),
  };
};

const readParquetRows = async (file: AsyncBuffer, metadata: FileMetaData): Promise<unknown[]> =>
  parquetReadObjects({ file, metadata });

const groupRows = async (
  rows: unknown[],
  category: PredictCategory,
  runYmd: string,
): Promise<RaceFoundation[]> => {
  if (rows.length === 0 || rows.length > MAX_DAY_BASE_ROWS) throw new Error("row-limit");
  const byRace = new Map<string, { identity: RaceIdentity; rows: FoundationRow[] }>();
  rows.forEach((rawRow) => {
    const row = normalizeRow(rawRow);
    if (row === null) throw new Error("unsupported-cell");
    // The day-base builder may include daysAhead rows in the same artifact.
    // They are valid input but do not belong in this runYmd's immutable
    // manifest. Reject malformed or cross-source identities, while filtering
    // only well-formed races from another date.
    if (isWellFormedOutOfScopeRace(row[RACE_ID_FIELD], category, runYmd)) return;
    const identity = parseRaceIdentity(row[RACE_ID_FIELD], category, runYmd);
    if (identity === null) throw new Error("invalid-race-id");
    const current = byRace.get(identity.raceId);
    if (current === undefined) byRace.set(identity.raceId, { identity, rows: [row] });
    else current.rows.push(row);
  });
  if (byRace.size === 0 || byRace.size > MAX_RACES) throw new Error("race-limit");
  return Promise.all(
    [...byRace.values()].map(async ({ identity, rows: raceRows }) => {
      if (raceRows.length === 0 || raceRows.length > MAX_ROWS_PER_RACE) {
        throw new Error("race-row-limit");
      }
      const entries = raceRows.map((row) => {
        const ketto = row[KETTO_FIELD];
        const umaban = positiveIntegerText(row[UMABAN_FIELD]);
        if (typeof ketto !== "string" || ketto.trim() === "" || umaban === null) {
          throw new Error("invalid-entry");
        }
        return `${ketto.trim()}:${umaban}`;
      });
      if (new Set(entries).size !== entries.length) throw new Error("duplicate-entry");
      return {
        entrySetHash: await sha256([...entries].sort().join("\n")),
        identity,
        rows: raceRows,
      };
    }),
  );
};

const encodeJson = (value: unknown): Uint8Array => encoder.encode(JSON.stringify(value));

const putRaces = async (
  bucket: R2Bucket,
  objects: ReadonlyArray<{ body: Uint8Array; key: string; metadata: Record<string, string> }>,
): Promise<void> => {
  const chunks = Array.from({ length: Math.ceil(objects.length / PUT_CONCURRENCY) }, (_, index) =>
    objects.slice(index * PUT_CONCURRENCY, (index + 1) * PUT_CONCURRENCY),
  );
  const putChunk = async (index: number): Promise<void> => {
    const chunk = chunks[index];
    if (chunk === undefined) return;
    await Promise.all(
      chunk.map(({ body, key, metadata }) =>
        bucket.put(key, body, {
          customMetadata: metadata,
          httpMetadata: { contentType: "application/json" },
        }),
      ),
    );
    await putChunk(index + 1);
  };
  await putChunk(0);
};

const fallbackReason = (error: unknown): string =>
  error instanceof Error && error.message !== "" ? error.message : "materialize-failed";

const existingManifestResult = (
  object: R2Object | null,
  manifestKey: string,
  sourceEtag: string,
  sourceVersion: string,
): DayBaseRaceMaterializeResult | null => {
  const metadata = object?.customMetadata;
  if (metadata === undefined) return null;
  const raceCount = Number(metadata["race-count"]);
  const rowCount = Number(metadata["row-count"]);
  const featureHash = metadata["feature-hash"];
  if (metadata["contract-version"] !== FOUNDATION_CONTRACT_VERSION) return null;
  if (metadata["schema-version"] !== CONTRACT_SCHEMA_VERSION) return null;
  if (metadata["source-etag"] !== sourceEtag) return null;
  if ((metadata["source-version"] ?? "") !== sourceVersion) return null;
  if (typeof featureHash !== "string" || featureHash === "") return null;
  if (!Number.isSafeInteger(raceCount) || raceCount <= 0 || raceCount > MAX_RACES) return null;
  if (!Number.isSafeInteger(rowCount) || rowCount <= 0 || rowCount > MAX_DAY_BASE_ROWS) return null;
  return { featureHash, manifestKey, raceCount, rowCount, status: "materialized" };
};

const metadataString = (object: R2Object | null, key: string): string | null => {
  const value = object?.customMetadata?.[key]?.trim();
  return value === undefined || value.length === 0 ? null : value;
};

export const getDayBaseRaceFoundationReadiness = async (
  params: RaceFoundationReadinessParams,
): Promise<DayBaseRaceFoundationReadiness> => {
  if (!/^\d{8}$/.test(params.runYmd)) return { ready: false, reason: "invalid-run-ymd" };
  if (!/^\d{1,2}$/.test(params.venueCode) || !/^\d{1,2}$/.test(params.raceNumber)) {
    return { ready: false, reason: "invalid-race-scope" };
  }
  const [source, manifest, foundation] = await Promise.all([
    params.env.FEATURES_CACHE.head(buildDayBaseObjectKey(params)),
    params.env.FEATURES_CACHE.head(buildDayBaseRaceManifestKey(params.category, params.runYmd)),
    params.env.FEATURES_CACHE.head(
      buildDayBaseRaceFoundationKey(
        params.category,
        params.runYmd,
        params.venueCode,
        params.raceNumber,
      ),
    ),
  ]);
  if (source === null) return { ready: false, reason: "day-base-miss" };
  if (manifest === null) return { ready: false, reason: "manifest-miss" };
  if (foundation === null) return { ready: false, reason: "foundation-miss" };
  const generationId = metadataString(manifest, "generation-id");
  if (
    metadataString(manifest, "contract-version") !== FOUNDATION_CONTRACT_VERSION ||
    metadataString(manifest, "schema-version") !== CONTRACT_SCHEMA_VERSION ||
    metadataString(manifest, "source-etag") !== source.etag ||
    (manifest.customMetadata?.["source-version"] ?? "") !== (source.version ?? "") ||
    generationId === null
  ) {
    return { ready: false, reason: "manifest-attestation-mismatch" };
  }
  const rowCount = Number(metadataString(foundation, "row-count"));
  if (
    metadataString(foundation, "contract-version") !== FOUNDATION_CONTRACT_VERSION ||
    metadataString(foundation, "schema-version") !== CONTRACT_SCHEMA_VERSION ||
    metadataString(foundation, "source-etag") !== source.etag ||
    (foundation.customMetadata?.["source-version"] ?? "") !== (source.version ?? "") ||
    metadataString(foundation, "generation-id") !== generationId ||
    metadataString(foundation, "entry-set-hash") === null ||
    !Number.isSafeInteger(rowCount) ||
    rowCount <= 0 ||
    rowCount > MAX_ROWS_PER_RACE
  ) {
    return { ready: false, reason: "foundation-attestation-mismatch" };
  }
  return { ready: true, reason: "ready" };
};

export const materializeDayBasePerRaceCache = async (
  params: MaterializeParams,
  dependencies: MaterializerDependencies = { decodeDayBase: defaultDecodeDayBase },
): Promise<DayBaseRaceMaterializeResult> => {
  try {
    if (!/^\d{8}$/.test(params.runYmd)) throw new Error("invalid-run-ymd");
    const sourceKey = buildDayBaseObjectKey(params);
    const source = await params.env.FEATURES_CACHE.head(sourceKey);
    if (source === null) throw new Error("day-base-miss");
    if (source.size <= 0 || source.size > MAX_DAY_BASE_BYTES) throw new Error("source-size-limit");
    const sourceVersion = source.version ?? "";
    const manifestKey = buildDayBaseRaceManifestKey(params.category, params.runYmd);
    const existing = params.force
      ? null
      : existingManifestResult(
          await params.env.FEATURES_CACHE.head(manifestKey),
          manifestKey,
          source.etag,
          sourceVersion,
        );
    if (existing !== null) return existing;
    const decoded = await dependencies.decodeDayBase(
      new R2RangeBuffer(params.env.FEATURES_CACHE, sourceKey, source.size),
    );
    if (decoded.featureSchema.length === 0) throw new Error("unsupported-schema");
    const featureNames = decoded.featureSchema.map(({ name }) => name);
    if (
      featureNames.some((name) => name === "") ||
      new Set(featureNames).size !== featureNames.length
    ) {
      throw new Error("unsupported-schema");
    }
    const races = await groupRows(decoded.rows, params.category, params.runYmd);
    const targetRowCount = races.reduce((total, race) => total + race.rows.length, 0);
    const featureHash = await sha256(featureNames.join("\n"));
    const entryContract = races
      .map(({ entrySetHash, identity }) => `${identity.raceId}:${entrySetHash}`)
      .sort()
      .join("\n");
    const generationId = await sha256(
      [
        FOUNDATION_CONTRACT_VERSION,
        CONTRACT_SCHEMA_VERSION,
        source.etag,
        sourceVersion,
        featureHash,
        entryContract,
      ].join("\n"),
    );
    const raceObjects = races.map(({ entrySetHash, identity, rows: raceRows }) => {
      const contract: FoundationContract = {
        contractVersion: FOUNDATION_CONTRACT_VERSION,
        entrySetHash,
        featureHash,
        generationId,
        rowCount: raceRows.length,
        schemaVersion: CONTRACT_SCHEMA_VERSION,
      };
      const envelope: FoundationEnvelope = {
        contract,
        raceId: identity.raceId,
        rows: raceRows,
        source: { etag: source.etag, key: sourceKey, version: sourceVersion },
      };
      const body = encodeJson(envelope);
      if (body.byteLength > MAX_RACE_JSON_BYTES) throw new Error("race-json-size-limit");
      return {
        body,
        key: buildDayBaseRaceFoundationKey(
          params.category,
          params.runYmd,
          identity.venueCode,
          identity.raceNumber,
        ),
        metadata: {
          "contract-version": FOUNDATION_CONTRACT_VERSION,
          "entry-set-hash": entrySetHash,
          "feature-hash": featureHash,
          "generation-id": generationId,
          "row-count": String(raceRows.length),
          "schema-version": CONTRACT_SCHEMA_VERSION,
          "source-etag": source.etag,
          "source-version": sourceVersion,
        },
      };
    });
    const totalJsonBytes = raceObjects.reduce((total, object) => total + object.body.byteLength, 0);
    if (totalJsonBytes > MAX_TOTAL_JSON_BYTES) throw new Error("total-json-size-limit");
    await putRaces(params.env.FEATURES_CACHE, raceObjects);

    const manifestRaces: ManifestRace[] = races.map((race) => ({
      entrySetHash: race.entrySetHash,
      key: buildDayBaseRaceFoundationKey(
        params.category,
        params.runYmd,
        race.identity.venueCode,
        race.identity.raceNumber,
      ),
      raceId: race.identity.raceId,
      rowCount: race.rows.length,
    }));
    const manifestContract: ManifestContract = {
      contractVersion: FOUNDATION_CONTRACT_VERSION,
      featureHash,
      featureSchema: decoded.featureSchema,
      generationId,
      raceCount: races.length,
      rowCount: targetRowCount,
      schemaVersion: CONTRACT_SCHEMA_VERSION,
    };
    const manifestBody = encodeJson({
      contract: manifestContract,
      races: manifestRaces,
      source: { etag: source.etag, key: sourceKey, version: sourceVersion },
    });
    await params.env.FEATURES_CACHE.put(manifestKey, manifestBody, {
      customMetadata: {
        "contract-version": FOUNDATION_CONTRACT_VERSION,
        "feature-hash": featureHash,
        "generation-id": generationId,
        "race-count": String(races.length),
        "row-count": String(decoded.rows.length),
        "schema-version": CONTRACT_SCHEMA_VERSION,
        "source-etag": source.etag,
        "source-version": sourceVersion,
      },
      httpMetadata: { contentType: "application/json" },
    });
    return {
      featureHash,
      manifestKey,
      raceCount: races.length,
      rowCount: targetRowCount,
      status: "materialized",
    };
  } catch (error) {
    return { reason: fallbackReason(error), status: "fallback" };
  }
};
