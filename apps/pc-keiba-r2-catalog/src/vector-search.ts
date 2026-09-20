// Runs with bun; uses the Cloudflare Vectorize V2 binding in Workers.

export interface HistoricalVectorQuery {
  dimensions: 8 | 256;
  earliestDate: string;
  filters: VectorizeVectorMetadataFilter;
  namespace: string;
  raceDate: string;
  source: "jra" | "nar";
  topK: number;
  values: readonly number[];
}

export interface HistoricalVectorNeighbor {
  distance: number;
  id: string;
  metadata: Record<string, VectorizeVectorMetadata>;
  weight: number;
}

export interface HistoricalVectorWrite {
  dimensions: 8 | 256;
  namespace: string;
  vectors: readonly VectorizeVector[];
}

const DATE_PATTERN: RegExp = /^\d{8}$/u;
const NAMESPACE_PATTERN: RegExp = /^[a-zA-Z0-9_-]{1,64}$/u;
const VECTORIZE_MIN_DIMENSIONS: number = 32;
const MAX_QUERY_NEIGHBORS: number = 100;
const HYDRATION_BATCH_SIZE: number = 20;
const MAX_WRITE_BATCH: number = 500;
const MAX_VECTOR_ID_BYTES: number = 64;
const MAX_METADATA_BYTES: number = 10 * 1024;
const MAX_FILTER_BYTES: number = 2048;
const RESERVED_FILTERS: ReadonlySet<string> = new Set(["source", "raceDate"]);
const encoder: TextEncoder = new TextEncoder();

const validDate = (value: string): boolean => {
  if (!DATE_PATTERN.test(value)) return false;
  const date: Date = new Date(
    `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}T00:00:00.000Z`,
  );
  return (
    Number.isFinite(date.getTime()) && date.toISOString().slice(0, 10).replaceAll("-", "") === value
  );
};

const validateValues = (values: readonly number[] | VectorFloatArray, dimensions: number): void => {
  if (values.length !== dimensions || Array.from(values).some((value) => !Number.isFinite(value)))
    throw new Error("Invalid vector dimensions or non-finite values");
};

const storageValues = (values: readonly number[] | VectorFloatArray): number[] => [
  ...values,
  ...Array.from({ length: Math.max(0, VECTORIZE_MIN_DIMENSIONS - values.length) }, () => 0),
];

const validateNamespace = (namespace: string): void => {
  if (!NAMESPACE_PATTERN.test(namespace)) throw new Error("Invalid vector namespace");
};

const queryFilters = (query: HistoricalVectorQuery): VectorizeVectorMetadataFilter => {
  validateNamespace(query.namespace);
  validateValues(query.values, query.dimensions);
  if (
    !validDate(query.raceDate) ||
    !validDate(query.earliestDate) ||
    query.earliestDate >= query.raceDate
  )
    throw new Error("Invalid historical vector date interval");
  if (!Number.isInteger(query.topK) || query.topK < 1 || query.topK > MAX_QUERY_NEIGHBORS)
    throw new Error("Invalid vector neighbor count");
  if (Object.keys(query.filters).some((key) => RESERVED_FILTERS.has(key)))
    throw new Error("Historical source and date filters cannot be overridden");
  const filters: VectorizeVectorMetadataFilter = {
    ...query.filters,
    source: query.source,
    raceDate: { $gte: query.earliestDate, $lt: query.raceDate },
  };
  if (encoder.encode(JSON.stringify(filters)).byteLength >= MAX_FILTER_BYTES)
    throw new Error("Vector metadata filter exceeds limit");
  return filters;
};

const neighborFromVector = (
  vector: VectorizeVector,
  query: HistoricalVectorQuery,
): HistoricalVectorNeighbor => {
  validateValues(vector.values, Math.max(VECTORIZE_MIN_DIMENSIONS, query.dimensions));
  if (
    Array.from(vector.values)
      .slice(query.dimensions)
      .some((value) => value !== 0)
  )
    throw new Error("Vector padding must preserve original L2 distance");
  const metadata: Record<string, VectorizeVectorMetadata> | undefined = vector.metadata;
  if (
    vector.namespace !== query.namespace ||
    metadata === undefined ||
    metadata.source !== query.source ||
    typeof metadata.raceDate !== "string" ||
    !validDate(metadata.raceDate) ||
    metadata.raceDate < query.earliestDate ||
    metadata.raceDate >= query.raceDate
  )
    throw new Error("Vector hydration violated historical query isolation");
  // Recompute exact L2 on hydrated values, never assume an ANN score's encoding.
  const distance: number = Math.hypot(
    ...query.values.map((value, index) => value - Number(vector.values[index])),
  );
  return { distance, id: vector.id, metadata, weight: 1 / (1 + distance) };
};

const compareNeighbors = (
  left: HistoricalVectorNeighbor,
  right: HistoricalVectorNeighbor,
): number => left.distance - right.distance || left.id.localeCompare(right.id);

/** ANN candidate retrieval, not an exact replacement for PostgreSQL's recency-capped search. */
export const queryHistoricalVectors = async (
  index: Pick<Vectorize, "getByIds" | "query">,
  query: HistoricalVectorQuery,
): Promise<HistoricalVectorNeighbor[]> => {
  const filter: VectorizeVectorMetadataFilter = queryFilters(query);
  // 80-neighbor finish searches exceed the limit for returnMetadata: all. Hydrate separately.
  const result: VectorizeMatches = await index.query(storageValues(query.values), {
    filter,
    namespace: query.namespace,
    returnMetadata: "none",
    returnValues: false,
    topK: query.topK,
  });
  const ids: string[] = result.matches.map((match) => match.id);
  if (ids.length === 0) return [];
  if (ids.length > query.topK || new Set(ids).size !== ids.length)
    throw new Error("Invalid Vectorize candidate identifiers");
  // Production bindings reject hydration above 20 IDs even when topK permits 100.
  const vectors: VectorizeVector[] = (
    await Promise.all(
      Array.from({ length: Math.ceil(ids.length / HYDRATION_BATCH_SIZE) }, (_, batch) =>
        index.getByIds(ids.slice(batch * HYDRATION_BATCH_SIZE, (batch + 1) * HYDRATION_BATCH_SIZE)),
      ),
    )
  ).flat();
  const byId: Map<string, VectorizeVector> = new Map(vectors.map((vector) => [vector.id, vector]));
  if (vectors.length !== ids.length || byId.size !== ids.length)
    throw new Error("Vectorize mutation is not fully visible");
  return ids
    .map((id) => {
      const vector: VectorizeVector | undefined = byId.get(id);
      if (vector === undefined) throw new Error("Vectorize hydration is incomplete");
      return neighborFromVector(vector, query);
    })
    .sort(compareNeighbors);
};

/** A receipt is acceptance only. Publish a cache revision only after visibility verification. */
export const upsertHistoricalVectors = async (
  index: Pick<Vectorize, "upsert"> | Pick<CatalogBindings["CORNER_VECTORS"], "upsert">,
  input: HistoricalVectorWrite,
): Promise<VectorizeAsyncMutation> => {
  validateNamespace(input.namespace);
  if (input.vectors.length < 1 || input.vectors.length > MAX_WRITE_BATCH)
    throw new Error("Invalid Vectorize mutation batch size");
  if (new Set(input.vectors.map((vector) => vector.id)).size !== input.vectors.length)
    throw new Error("Duplicate vector identifiers in mutation");
  input.vectors.forEach((vector) => {
    validateValues(vector.values, input.dimensions);
    if (vector.id.length === 0 || encoder.encode(vector.id).byteLength > MAX_VECTOR_ID_BYTES)
      throw new Error("Invalid vector identifier");
    if (vector.namespace !== input.namespace) throw new Error("Vector namespace mismatch");
    const metadata: Record<string, VectorizeVectorMetadata> | undefined = vector.metadata;
    if (
      metadata === undefined ||
      (metadata.source !== "jra" && metadata.source !== "nar") ||
      typeof metadata.raceDate !== "string" ||
      !validDate(metadata.raceDate)
    )
      throw new Error("Invalid historical vector metadata");
    if (encoder.encode(JSON.stringify(metadata)).byteLength > MAX_METADATA_BYTES)
      throw new Error("Vector metadata exceeds limit");
  });
  // Zero-padding 8D to the provider's minimum 32D preserves Euclidean distance exactly.
  const receipt = await index.upsert(
    input.vectors.map((vector) => ({ ...vector, values: storageValues(vector.values) })),
  );
  // Wrangler 4.100 generates a legacy binding type even for V2 indexes. Validate V2 at runtime.
  if (
    !("mutationId" in receipt) ||
    typeof receipt.mutationId !== "string" ||
    receipt.mutationId.length === 0
  )
    throw new Error("Vectorize V2 mutation receipt is required");
  return { mutationId: receipt.mutationId };
};
