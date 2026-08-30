// Run with bun. Attested producer for a Worker-enriched, per-race market-signal
// foundation. The artifact uses a namespace distinct from both canonical
// day-base foundations and directly-scoreable final feature caches. Consumers
// must validate every attestation field and retain the legacy Container market
// layer on any miss or mismatch.

import { buildDayBaseObjectKey } from "./day-base-object-key";
import {
  buildDayBaseRaceFoundationKey,
  buildDayBaseRaceManifestKey,
} from "./day-base-race-materializer";
import {
  materializeRaceMarketSignals,
  type MarketSignalCell,
  type MarketSignalFoundationRow,
} from "./race-chain-market-signal";
import type { RealtimeOdds } from "./scoring/rescore-realtime";
import type { PredictCategory } from "./types";

const FEATURE_PREFIX: string = "feat-racechain-market-signal";
const CATALOG_GENERATION: string = "catalog-v1";
const FEATURE_FILE: string = "foundation.json";
const CONTRACT_VERSION: string = "race-chain-market-signal-foundation-v1";
const SCHEMA_VERSION: string = "1";
const ENABLED_VALUE: string = "1";
const JRA_CATEGORY: PredictCategory = "jra";
const PAD_WIDTH: number = 2;
const MAX_SOURCE_BYTES: number = 16 * 1024 * 1024;
const MAX_FOUNDATION_BYTES: number = 2 * 1024 * 1024;
const MAX_MANIFEST_BYTES: number = 2 * 1024 * 1024;
const MAX_OUTPUT_BYTES: number = 2 * 1024 * 1024;
const MAX_ROWS: number = 32;
const MAX_BASE_FEATURES: number = 512;
const MAX_COMPUTE_MS: number = 250;
const KETTO_FIELD: string = "ketto_toroku_bango";
const HORSE_NUMBER_FIELD: string = "umaban";
const encoder: TextEncoder = new TextEncoder();

const MARKET_SIGNAL_ALWAYS_ADDED_COLUMNS: ReadonlyArray<string> = [
  "tansho_odds_raw",
  "tansho_ninkijun_raw",
  "inverse_odds_implied_prob",
  "inverse_odds_market_share",
  "inverse_odds_rank_in_race",
  "popularity_rank_in_race",
  "odds_score_diff_from_race_avg",
  "popularity_score_diff_from_race_avg",
  "popularity_odds_disagreement",
  "form_market_edge",
];

const MARKET_SIGNAL_NEAR_MISS_OVERWRITE_COLUMNS: ReadonlyArray<string> = [
  "field_dominant_favorite_indicator",
  "horse_popularity_vs_field",
];

export const MARKET_SIGNAL_ADDED_COLUMNS: ReadonlyArray<string> =
  MARKET_SIGNAL_ALWAYS_ADDED_COLUMNS;

export interface MarketSignalFoundationObject {
  customMetadata?: Record<string, string>;
  etag: string;
  size: number;
  version?: string;
}

export interface MarketSignalFoundationObjectBody extends MarketSignalFoundationObject {
  arrayBuffer: () => Promise<ArrayBuffer>;
}

export interface MarketSignalFoundationPutOptions {
  customMetadata: Record<string, string>;
  httpMetadata: { contentType: string };
}

export interface MarketSignalFoundationBucket {
  get: (key: string) => Promise<MarketSignalFoundationObjectBody | null>;
  head: (key: string) => Promise<MarketSignalFoundationObject | null>;
  put: (
    key: string,
    value: Uint8Array,
    options: MarketSignalFoundationPutOptions,
  ) => Promise<unknown>;
}

export interface MarketSignalFoundationEnv {
  FEATURES_CACHE: MarketSignalFoundationBucket;
  WORKER_MARKET_SIGNAL_FOUNDATION_ENABLED?: string;
}

export interface MaterializeMarketSignalFoundationInput {
  category: PredictCategory;
  env: MarketSignalFoundationEnv;
  keibajoCode: string;
  liveOddsByHorseNumber: ReadonlyMap<number, RealtimeOdds>;
  raceBango: string;
  runYmd: string;
}

interface MaterializedMarketSignalFoundationResult {
  artifactEtag: string;
  artifactVersion: string;
  baseGenerationId: string;
  cacheHit: boolean;
  computeMs: number;
  key: string;
  oddsSnapshotHash: string;
  rowCount: number;
  status: "materialized";
  totalMs: number;
}

interface FallbackMarketSignalFoundationResult {
  reason: string;
  status: "fallback";
}

interface SkippedMarketSignalFoundationResult {
  reason: "disabled";
  status: "skipped";
}

export type MarketSignalFoundationResult =
  | FallbackMarketSignalFoundationResult
  | MaterializedMarketSignalFoundationResult
  | SkippedMarketSignalFoundationResult;

interface ProducerDependencies {
  now: () => number;
}

interface ObjectIdentity {
  etag: string;
  key: string;
  version: string;
}

interface LoadedJson {
  identity: ObjectIdentity;
  value: unknown;
}

interface ValidatedBaseFoundation {
  entrySetHash: string;
  featureNames: string[];
  foundationIdentity: ObjectIdentity;
  foundationKey: string;
  generationId: string;
  inputFeatureHash: string;
  manifestIdentity: ObjectIdentity;
  manifestKey: string;
  rows: MarketSignalFoundationRow[];
  sourceIdentity: ObjectIdentity;
}

interface MarketSignalContract {
  addedColumns: ReadonlyArray<string>;
  overwrittenColumns: ReadonlyArray<string>;
  baseGenerationId: string;
  contractVersion: string;
  entrySetHash: string;
  inputFeatureHash: string;
  oddsSnapshotHash: string;
  outputFeatureHash: string;
  raceId: string;
  rowCount: number;
  schemaVersion: string;
}

interface MarketSignalBaseAttestation {
  foundationEtag: string;
  foundationKey: string;
  foundationVersion: string;
  manifestEtag: string;
  manifestKey: string;
  manifestVersion: string;
}

interface MarketSignalTelemetry {
  totalMs: number;
  workerComputeMs: number;
}

interface MarketSignalEnvelope {
  base: MarketSignalBaseAttestation;
  contract: MarketSignalContract;
  rows: MarketSignalFoundationRow[];
  source: ObjectIdentity;
  telemetry: MarketSignalTelemetry;
}

interface ArtifactExpectation {
  baseGenerationId: string;
  entrySetHash: string;
  oddsSnapshotHash: string;
  outputFeatureHash: string;
  raceId: string;
  rowCount: number;
  sourceEtag: string;
  sourceVersion: string;
}

interface ManifestRaceContract {
  entrySetHash: string;
  rowCount: number;
}

const defaultDependencies: ProducerDependencies = { now: () => performance.now() };

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isMarketSignalCell = (value: unknown): value is MarketSignalCell =>
  value === null ||
  typeof value === "boolean" ||
  typeof value === "number" ||
  typeof value === "string";

const isFoundationRow = (value: unknown): value is MarketSignalFoundationRow =>
  isRecord(value) && Object.values(value).every(isMarketSignalCell);

const requiredString = (record: Record<string, unknown>, key: string): string | null => {
  const value: unknown = record[key];
  return typeof value === "string" && value !== "" ? value : null;
};

const requiredPositiveInteger = (record: Record<string, unknown>, key: string): number | null => {
  const value: unknown = record[key];
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0 ? value : null;
};

const objectVersion = (object: MarketSignalFoundationObject): string => object.version ?? "";

const objectIdentity = (key: string, object: MarketSignalFoundationObject): ObjectIdentity => ({
  etag: object.etag,
  key,
  version: objectVersion(object),
});

const sameIdentity = (left: ObjectIdentity, right: ObjectIdentity): boolean =>
  left.etag === right.etag && left.key === right.key && left.version === right.version;

const hex = (bytes: ArrayBuffer): string =>
  [...new Uint8Array(bytes)].map((value) => value.toString(16).padStart(2, "0")).join("");

const sha256 = async (value: string): Promise<string> =>
  hex(await crypto.subtle.digest("SHA-256", encoder.encode(value)));

const readBoundedJson = async (
  bucket: MarketSignalFoundationBucket,
  key: string,
  maxBytes: number,
): Promise<LoadedJson | null> => {
  const object: MarketSignalFoundationObjectBody | null = await bucket.get(key);
  if (object === null || object.size <= 0 || object.size > maxBytes) return null;
  const bytes: ArrayBuffer = await object.arrayBuffer();
  if (bytes.byteLength !== object.size) return null;
  try {
    const value: unknown = JSON.parse(new TextDecoder().decode(bytes));
    return { identity: objectIdentity(key, object), value };
  } catch {
    return null;
  }
};

const sourceFrom = (value: unknown): ObjectIdentity | null => {
  if (!isRecord(value)) return null;
  const etag: string | null = requiredString(value, "etag");
  const key: string | null = requiredString(value, "key");
  const version: unknown = value.version;
  if (etag === null || key === null || typeof version !== "string") return null;
  return { etag, key, version };
};

const featureNamesFrom = (contract: Record<string, unknown>): string[] | null => {
  const schema: unknown = contract.featureSchema;
  if (!Array.isArray(schema) || schema.length === 0 || schema.length > MAX_BASE_FEATURES)
    return null;
  const names: Array<string | null> = schema.map((field) =>
    isRecord(field) ? requiredString(field, "name") : null,
  );
  if (names.some((name) => name === null)) return null;
  const validNames: string[] = names.filter((name): name is string => name !== null);
  return new Set(validNames).size === validNames.length ? validNames : null;
};

const manifestRaceFrom = (
  manifest: Record<string, unknown>,
  foundationKey: string,
  raceId: string,
): ManifestRaceContract | null => {
  if (!Array.isArray(manifest.races)) return null;
  const match: unknown = manifest.races.find(
    (race) => isRecord(race) && race.key === foundationKey && race.raceId === raceId,
  );
  if (!isRecord(match)) return null;
  const entrySetHash: string | null = requiredString(match, "entrySetHash");
  const rowCount: number | null = requiredPositiveInteger(match, "rowCount");
  return entrySetHash === null || rowCount === null ? null : { entrySetHash, rowCount };
};

const validManifestCounts = (
  manifest: Record<string, unknown>,
  contract: Record<string, unknown>,
): boolean => {
  const raceCount: number | null = requiredPositiveInteger(contract, "raceCount");
  const rowCount: number | null = requiredPositiveInteger(contract, "rowCount");
  if (raceCount === null || rowCount === null || !Array.isArray(manifest.races)) return false;
  const raceRowCounts: Array<number | null> = manifest.races.map((race) =>
    isRecord(race) ? requiredPositiveInteger(race, "rowCount") : null,
  );
  return (
    manifest.races.length === raceCount &&
    raceRowCounts.every((count) => count !== null) &&
    raceRowCounts.reduce((total, count) => total + (count ?? 0), 0) === rowCount
  );
};

const rowsMatchFeatureSchema = (
  rows: ReadonlyArray<MarketSignalFoundationRow>,
  featureNames: ReadonlyArray<string>,
): boolean =>
  rows.every(
    (row) =>
      Object.keys(row).length === featureNames.length && featureNames.every((name) => name in row),
  );

const normalizedEntryToken = (row: MarketSignalFoundationRow): string | null => {
  const ketto: MarketSignalCell | undefined = row[KETTO_FIELD];
  const rawHorseNumber: MarketSignalCell | undefined = row[HORSE_NUMBER_FIELD];
  if (typeof ketto !== "string" || ketto.trim() === "") return null;
  if (typeof rawHorseNumber !== "number" && typeof rawHorseNumber !== "string") return null;
  const text: string = String(rawHorseNumber).trim();
  if (!/^\d+$/.test(text)) return null;
  const horseNumber: number = Number(text);
  return Number.isSafeInteger(horseNumber) && horseNumber > 0
    ? `${ketto.trim()}:${horseNumber}`
    : null;
};

const entrySetHashFor = async (
  rows: ReadonlyArray<MarketSignalFoundationRow>,
): Promise<string | null> => {
  const tokens: Array<string | null> = rows.map(normalizedEntryToken);
  if (tokens.some((token) => token === null)) return null;
  const validTokens: string[] = tokens.filter((token): token is string => token !== null);
  if (new Set(validTokens).size !== validTokens.length) return null;
  return sha256(validTokens.toSorted().join("\n"));
};

const validateBaseFoundation = async (
  input: MaterializeMarketSignalFoundationInput,
  source: MarketSignalFoundationObject,
  manifest: LoadedJson,
  foundation: LoadedJson,
  raceId: string,
  foundationKey: string,
  manifestKey: string,
): Promise<ValidatedBaseFoundation | null> => {
  if (!isRecord(manifest.value) || !isRecord(foundation.value)) return null;
  const manifestContract: unknown = manifest.value.contract;
  const raceContract: unknown = foundation.value.contract;
  if (!isRecord(manifestContract) || !isRecord(raceContract)) return null;
  const sourceIdentity: ObjectIdentity = objectIdentity(buildDayBaseObjectKey(input), source);
  const manifestSource: ObjectIdentity | null = sourceFrom(manifest.value.source);
  const raceSource: ObjectIdentity | null = sourceFrom(foundation.value.source);
  if (
    manifestSource === null ||
    raceSource === null ||
    !sameIdentity(sourceIdentity, manifestSource) ||
    !sameIdentity(sourceIdentity, raceSource)
  ) {
    return null;
  }
  if (
    requiredString(manifestContract, "contractVersion") !== "day-base-race-foundation-v1" ||
    requiredString(manifestContract, "schemaVersion") !== SCHEMA_VERSION ||
    requiredString(raceContract, "contractVersion") !== "day-base-race-foundation-v1" ||
    requiredString(raceContract, "schemaVersion") !== SCHEMA_VERSION ||
    foundation.value.raceId !== raceId
  ) {
    return null;
  }
  const generationId: string | null = requiredString(manifestContract, "generationId");
  const inputFeatureHash: string | null = requiredString(manifestContract, "featureHash");
  const featureNames: string[] | null = featureNamesFrom(manifestContract);
  const manifestRace: ManifestRaceContract | null = manifestRaceFrom(
    manifest.value,
    foundationKey,
    raceId,
  );
  const rowsValue: unknown = foundation.value.rows;
  const actualInputFeatureHash: string | null =
    featureNames === null ? null : await sha256(featureNames.join("\n"));
  if (
    generationId === null ||
    inputFeatureHash === null ||
    featureNames === null ||
    actualInputFeatureHash !== inputFeatureHash ||
    manifestRace === null ||
    !validManifestCounts(manifest.value, manifestContract) ||
    !Array.isArray(rowsValue) ||
    rowsValue.length === 0 ||
    rowsValue.length > MAX_ROWS ||
    rowsValue.length !== manifestRace.rowCount ||
    rowsValue.some((row) => !isFoundationRow(row))
  ) {
    return null;
  }
  const rows: MarketSignalFoundationRow[] = rowsValue.filter(isFoundationRow);
  if (!rowsMatchFeatureSchema(rows, featureNames)) return null;
  const actualEntrySetHash: string | null = await entrySetHashFor(rows);
  if (
    actualEntrySetHash === null ||
    actualEntrySetHash !== manifestRace.entrySetHash ||
    requiredString(raceContract, "generationId") !== generationId ||
    requiredString(raceContract, "featureHash") !== inputFeatureHash ||
    requiredString(raceContract, "entrySetHash") !== manifestRace.entrySetHash ||
    requiredPositiveInteger(raceContract, "rowCount") !== rows.length
  ) {
    return null;
  }
  return {
    entrySetHash: actualEntrySetHash,
    featureNames,
    foundationIdentity: foundation.identity,
    foundationKey,
    generationId,
    inputFeatureHash,
    manifestIdentity: manifest.identity,
    manifestKey,
    rows,
    sourceIdentity,
  };
};

interface MarketSignalOutputContract {
  addedColumns: string[];
  featureNames: string[];
  overwrittenColumns: string[];
}

const outputContract = (
  baseFeatureNames: ReadonlyArray<string>,
): MarketSignalOutputContract | null => {
  if (MARKET_SIGNAL_ALWAYS_ADDED_COLUMNS.some((name) => baseFeatureNames.includes(name))) {
    return null;
  }
  const overwrittenColumns = MARKET_SIGNAL_NEAR_MISS_OVERWRITE_COLUMNS.filter((name) =>
    baseFeatureNames.includes(name),
  );
  const addedColumns = [...MARKET_SIGNAL_ALWAYS_ADDED_COLUMNS];
  return {
    addedColumns,
    featureNames: [...baseFeatureNames, ...addedColumns],
    overwrittenColumns,
  };
};

const oddsSnapshotHashFor = (odds: ReadonlyMap<number, RealtimeOdds>): Promise<string> =>
  sha256(
    [...odds.entries()]
      .toSorted(([left], [right]) => left - right)
      .map(([horseNumber, value]) => `${horseNumber}:${value.tanshoOdds}:${value.tanshoNinkijun}`)
      .join("\n"),
  );

const buildEnvelope = async (
  base: ValidatedBaseFoundation,
  raceId: string,
  rows: MarketSignalFoundationRow[],
  odds: ReadonlyMap<number, RealtimeOdds>,
  computeMs: number,
  totalMs: number,
): Promise<MarketSignalEnvelope | null> => {
  const output = outputContract(base.featureNames);
  if (output === null) return null;
  const projectedRows: MarketSignalFoundationRow[] = [];
  for (const row of rows) {
    if (output.featureNames.some((name) => !(name in row))) return null;
    projectedRows.push(
      Object.fromEntries(output.featureNames.map((name) => [name, row[name] ?? null])),
    );
  }
  return {
    base: {
      foundationEtag: base.foundationIdentity.etag,
      foundationKey: base.foundationKey,
      foundationVersion: base.foundationIdentity.version,
      manifestEtag: base.manifestIdentity.etag,
      manifestKey: base.manifestKey,
      manifestVersion: base.manifestIdentity.version,
    },
    contract: {
      addedColumns: output.addedColumns,
      overwrittenColumns: output.overwrittenColumns,
      baseGenerationId: base.generationId,
      contractVersion: CONTRACT_VERSION,
      entrySetHash: base.entrySetHash,
      inputFeatureHash: base.inputFeatureHash,
      oddsSnapshotHash: await oddsSnapshotHashFor(odds),
      outputFeatureHash: await sha256(output.featureNames.join("\n")),
      raceId,
      rowCount: projectedRows.length,
      schemaVersion: SCHEMA_VERSION,
    },
    rows: projectedRows,
    source: base.sourceIdentity,
    telemetry: { totalMs, workerComputeMs: computeMs },
  };
};

const artifactMetadata = (
  expectation: ArtifactExpectation,
  computeMs: number,
): Record<string, string> => ({
  "base-generation-id": expectation.baseGenerationId,
  "contract-version": CONTRACT_VERSION,
  "entry-set-hash": expectation.entrySetHash,
  "odds-snapshot-hash": expectation.oddsSnapshotHash,
  "output-feature-hash": expectation.outputFeatureHash,
  "race-id": expectation.raceId,
  "row-count": String(expectation.rowCount),
  "schema-version": SCHEMA_VERSION,
  "source-etag": expectation.sourceEtag,
  "source-version": expectation.sourceVersion,
  "worker-compute-ms": String(computeMs),
});

const artifactMatches = (
  object: MarketSignalFoundationObject | null,
  expectation: ArtifactExpectation,
): object is MarketSignalFoundationObject => {
  if (object === null || object.size <= 0 || object.size > MAX_OUTPUT_BYTES) return false;
  const metadata: Record<string, string> | undefined = object.customMetadata;
  return (
    metadata?.["base-generation-id"] === expectation.baseGenerationId &&
    metadata["contract-version"] === CONTRACT_VERSION &&
    metadata["entry-set-hash"] === expectation.entrySetHash &&
    metadata["odds-snapshot-hash"] === expectation.oddsSnapshotHash &&
    metadata["output-feature-hash"] === expectation.outputFeatureHash &&
    metadata["race-id"] === expectation.raceId &&
    metadata["row-count"] === String(expectation.rowCount) &&
    metadata["schema-version"] === SCHEMA_VERSION &&
    metadata["source-etag"] === expectation.sourceEtag &&
    metadata["source-version"] === expectation.sourceVersion
  );
};

const materializedResult = (
  object: MarketSignalFoundationObject,
  expectation: ArtifactExpectation,
  key: string,
  cacheHit: boolean,
  computeMs: number,
  totalMs: number,
): MaterializedMarketSignalFoundationResult => ({
  artifactEtag: object.etag,
  artifactVersion: objectVersion(object),
  baseGenerationId: expectation.baseGenerationId,
  cacheHit,
  computeMs,
  key,
  oddsSnapshotHash: expectation.oddsSnapshotHash,
  rowCount: expectation.rowCount,
  status: "materialized",
  totalMs,
});

export const buildMarketSignalFoundationKey = (
  category: PredictCategory,
  runYmd: string,
  keibajoCode: string,
  raceBango: string,
): string =>
  `${FEATURE_PREFIX}/${CATALOG_GENERATION}/${category}/${runYmd}/${keibajoCode
    .trim()
    .padStart(PAD_WIDTH, "0")}/${raceBango.trim().padStart(PAD_WIDTH, "0")}/${FEATURE_FILE}`;

export const marketSignalFoundationEnabled = (value: string | undefined): boolean =>
  value === ENABLED_VALUE;

export const materializeMarketSignalFoundation = async (
  input: MaterializeMarketSignalFoundationInput,
  dependencies: ProducerDependencies = defaultDependencies,
): Promise<MarketSignalFoundationResult> => {
  if (!marketSignalFoundationEnabled(input.env.WORKER_MARKET_SIGNAL_FOUNDATION_ENABLED)) {
    return { reason: "disabled", status: "skipped" };
  }
  if (input.category !== JRA_CATEGORY)
    return { reason: "unsupported-category", status: "fallback" };
  if (
    !/^\d{8}$/.test(input.runYmd) ||
    !/^\d{1,2}$/.test(input.keibajoCode) ||
    !/^\d{1,2}$/.test(input.raceBango)
  ) {
    return { reason: "invalid-race-identity", status: "fallback" };
  }
  if (input.liveOddsByHorseNumber.size === 0 || input.liveOddsByHorseNumber.size > MAX_ROWS) {
    return { reason: "invalid-live-board", status: "fallback" };
  }
  const totalStart: number = dependencies.now();
  const raceId: string = `jra:${input.runYmd.slice(0, 4)}:${input.runYmd.slice(4)}:${input.keibajoCode.padStart(PAD_WIDTH, "0")}:${input.raceBango.padStart(PAD_WIDTH, "0")}`;
  const sourceKey: string = buildDayBaseObjectKey(input);
  const foundationKey: string = buildDayBaseRaceFoundationKey(
    input.category,
    input.runYmd,
    input.keibajoCode,
    input.raceBango,
  );
  const manifestKey: string = buildDayBaseRaceManifestKey(input.category, input.runYmd);
  const [source, manifest, foundation] = await Promise.all([
    input.env.FEATURES_CACHE.head(sourceKey),
    readBoundedJson(input.env.FEATURES_CACHE, manifestKey, MAX_MANIFEST_BYTES),
    readBoundedJson(input.env.FEATURES_CACHE, foundationKey, MAX_FOUNDATION_BYTES),
  ]);
  if (source === null || source.size <= 0 || source.size > MAX_SOURCE_BYTES) {
    return { reason: "source-unavailable", status: "fallback" };
  }
  if (manifest === null || foundation === null) {
    return { reason: "foundation-unavailable", status: "fallback" };
  }
  const base: ValidatedBaseFoundation | null = await validateBaseFoundation(
    input,
    source,
    manifest,
    foundation,
    raceId,
    foundationKey,
    manifestKey,
  );
  if (base === null) return { reason: "foundation-attestation-mismatch", status: "fallback" };
  const computeStart: number = dependencies.now();
  const computed = materializeRaceMarketSignals({
    liveOddsByHorseNumber: input.liveOddsByHorseNumber,
    raceId,
    rows: base.rows,
  });
  const computeMs: number = dependencies.now() - computeStart;
  if (computed.status !== "ready") {
    return { reason: `market-signal-${computed.reason}`, status: "fallback" };
  }
  if (computeMs < 0 || computeMs > MAX_COMPUTE_MS) {
    return { reason: "compute-limit", status: "fallback" };
  }
  const totalMs: number = dependencies.now() - totalStart;
  const envelope: MarketSignalEnvelope | null = await buildEnvelope(
    base,
    raceId,
    computed.rows,
    input.liveOddsByHorseNumber,
    computeMs,
    totalMs,
  );
  if (envelope === null) return { reason: "output-feature-limit", status: "fallback" };
  const body: Uint8Array = encoder.encode(JSON.stringify(envelope));
  if (body.byteLength > MAX_OUTPUT_BYTES)
    return { reason: "output-size-limit", status: "fallback" };
  const key: string = buildMarketSignalFoundationKey(
    input.category,
    input.runYmd,
    input.keibajoCode,
    input.raceBango,
  );
  const expectation: ArtifactExpectation = {
    baseGenerationId: base.generationId,
    entrySetHash: base.entrySetHash,
    oddsSnapshotHash: envelope.contract.oddsSnapshotHash,
    outputFeatureHash: envelope.contract.outputFeatureHash,
    raceId,
    rowCount: computed.rows.length,
    sourceEtag: base.sourceIdentity.etag,
    sourceVersion: base.sourceIdentity.version,
  };
  const existing: MarketSignalFoundationObject | null = await input.env.FEATURES_CACHE.head(key);
  if (artifactMatches(existing, expectation)) {
    return materializedResult(existing, expectation, key, true, computeMs, totalMs);
  }
  await input.env.FEATURES_CACHE.put(key, body, {
    customMetadata: artifactMetadata(expectation, computeMs),
    httpMetadata: { contentType: "application/json" },
  });
  const written: MarketSignalFoundationObject | null = await input.env.FEATURES_CACHE.head(key);
  if (!artifactMatches(written, expectation)) {
    return { reason: "artifact-attestation-unavailable", status: "fallback" };
  }
  return materializedResult(written, expectation, key, false, computeMs, totalMs);
};
