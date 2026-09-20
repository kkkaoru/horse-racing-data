// Runs with bun; exact stored-data checks for a bounded corner-vector sample, not ANN recall.
export interface VectorSampleReceipt {
  verifiedVectors: number;
  sourceDimensions: 8;
  storageDimensions: 32;
}
export interface VectorSampleAssessment extends VectorSampleReceipt {
  exactMetadata: boolean;
  normalizedRoundingFields: number;
}
interface MetadataPair {
  key: string;
  expected: VectorizeVectorMetadata;
  actual: unknown;
}
const NORMALIZED_FIELDS: ReadonlySet<string> = new Set([
  "finishNorm",
  "corner1",
  "corner2",
  "corner3",
  "corner4",
]);
const DOUBLE_BYTES: number = 8;
const SOURCE_DIMENSIONS: number = 8;
const STORAGE_DIMENSIONS: number = 32;
const MAX_SAMPLE: number = 500;
const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const list = (value: unknown): value is readonly unknown[] => Array.isArray(value);

const adjacentNormalizedDouble = (pair: MetadataPair): boolean => {
  if (
    !NORMALIZED_FIELDS.has(pair.key) ||
    typeof pair.expected !== "number" ||
    typeof pair.actual !== "number" ||
    !(pair.expected > 0 && pair.expected < 1 && pair.actual > 0 && pair.actual < 1)
  )
    return false;
  const bits = new DataView(new ArrayBuffer(DOUBLE_BYTES));
  bits.setFloat64(0, pair.expected);
  const expected = bits.getBigUint64(0);
  bits.setFloat64(0, pair.actual);
  const difference = expected - bits.getBigUint64(0);
  return difference === 1n || difference === -1n;
};

const countNormalizedRounding = (
  expected: Record<string, VectorizeVectorMetadata>,
  actual: Record<string, unknown>,
): number => {
  const rounding: { fields: number } = { fields: 0 };
  for (const [key, value] of Object.entries(expected)) {
    if (actual[key] === value) continue;
    if (!adjacentNormalizedDouble({ key, expected: value, actual: actual[key] }))
      throw new Error("Vector reconciliation metadata mismatch");
    rounding.fields += 1;
  }
  return rounding.fields;
};

/** Report observed one-ULP normalized-metadata differences explicitly, never as exact parity. */
export const assessCornerVectorSample = (
  expected: readonly VectorizeVector[],
  actual: unknown,
): VectorSampleAssessment => {
  if (
    expected.length < 1 ||
    expected.length > MAX_SAMPLE ||
    new Set(expected.map((vector) => vector.id)).size !== expected.length ||
    !list(actual) ||
    actual.length !== expected.length
  )
    throw new Error("Invalid vector reconciliation count");
  const rounding: { fields: number } = { fields: 0 };
  const byId: Map<string, Record<string, unknown>> = new Map();
  for (const value of actual) {
    if (!record(value) || typeof value.id !== "string" || byId.has(value.id))
      throw new Error("Invalid vector reconciliation identity");
    byId.set(value.id, value);
  }
  for (const vector of expected) {
    const found = byId.get(vector.id);
    const metadata = vector.metadata;
    if (
      vector.id.length === 0 ||
      typeof vector.namespace !== "string" ||
      vector.namespace.length === 0 ||
      vector.values.length !== SOURCE_DIMENSIONS ||
      Array.from(vector.values).some((value) => !Number.isFinite(Math.fround(value))) ||
      metadata === undefined ||
      Object.values(metadata).some(
        (value) =>
          !["string", "number", "boolean"].includes(typeof value) ||
          (typeof value === "number" && !Number.isFinite(value)),
      )
    )
      throw new Error("Invalid corner reconciliation source");
    if (
      found === undefined ||
      found.namespace !== vector.namespace ||
      !list(found.values) ||
      found.values.length !== STORAGE_DIMENSIONS
    )
      throw new Error("Vector reconciliation isolation or dimensions mismatch");
    const values: number[] = [
      ...vector.values,
      ...Array.from({ length: STORAGE_DIMENSIONS - SOURCE_DIMENSIONS }, () => 0),
    ];
    if (
      found.values.some(
        (value, index) =>
          typeof value !== "number" ||
          Math.fround(value) !== Math.fround(Number(values[index])) ||
          (index >= SOURCE_DIMENSIONS && value !== 0),
      )
    )
      throw new Error("Vector reconciliation values mismatch");
    if (
      !record(found.metadata) ||
      Object.keys(found.metadata).length !== Object.keys(metadata).length
    )
      throw new Error("Vector reconciliation metadata mismatch");
    const restoredMetadata: Record<string, unknown> = found.metadata;
    rounding.fields += countNormalizedRounding(metadata, restoredMetadata);
  }
  return {
    verifiedVectors: expected.length,
    sourceDimensions: 8,
    storageDimensions: 32,
    exactMetadata: rounding.fields === 0,
    normalizedRoundingFields: rounding.fields,
  };
};

/** Strict entry point remains strict even when the assessment reports only one-ULP drift. */
export const verifyCornerVectorSample = (
  expected: readonly VectorizeVector[],
  actual: unknown,
): VectorSampleReceipt => {
  const assessment = assessCornerVectorSample(expected, actual);
  if (!assessment.exactMetadata) throw new Error("Vector reconciliation metadata mismatch");
  return {
    verifiedVectors: assessment.verifiedVectors,
    sourceDimensions: 8,
    storageDimensions: 32,
  };
};
