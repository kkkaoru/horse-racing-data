// Runs with bun; offline migration mapping shared with the Vectorize publisher.
export interface CornerMigrationCursor {
  source: string;
  year: string;
  monthDay: string;
  venue: string;
  race: string;
  horse: string;
}

export const CORNER_MIGRATION_SQL: string = `
select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
       race_date, track_code, kyori, finish_position, finish_norm,
       corner1_norm, corner2_norm, corner3_norm, corner4_norm, feature_vector::text
from race_entry_corner_features
where (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    > ($1, $2, $3, $4, $5, $6)
order by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
limit $7`;

const VECTOR_DIMENSIONS: number = 8;
const OPTIONAL_NUMBERS: ReadonlyArray<readonly [string, string]> = [
  ["kyori", "distance"],
  ["finish_position", "finishPosition"],
  ["finish_norm", "finishNorm"],
  ["corner1_norm", "corner1"],
  ["corner2_norm", "corner2"],
  ["corner3_norm", "corner3"],
  ["corner4_norm", "corner4"],
];
const encoder: TextEncoder = new TextEncoder();

const requiredText = (row: Record<string, unknown>, key: string): string => {
  const value: unknown = row[key];
  if (typeof value !== "string" || value.trim().length === 0)
    throw new Error(`Missing migration key: ${key}`);
  return value;
};

export const cornerMigrationCursor = (row: Record<string, unknown>): CornerMigrationCursor => ({
  source: requiredText(row, "source"),
  year: requiredText(row, "kaisai_nen"),
  monthDay: requiredText(row, "kaisai_tsukihi"),
  venue: requiredText(row, "keibajo_code"),
  race: requiredText(row, "race_bango"),
  horse: requiredText(row, "ketto_toroku_bango"),
});

export const cornerMigrationParameters = (cursor: CornerMigrationCursor): string[] => [
  cursor.source,
  cursor.year,
  cursor.monthDay,
  cursor.venue,
  cursor.race,
  cursor.horse,
];

export const mapCornerMigrationRow = async (
  row: Record<string, unknown>,
  namespace: string,
): Promise<VectorizeVector> => {
  const cursor: CornerMigrationCursor = cornerMigrationCursor(row);
  if (cursor.source !== "jra" && cursor.source !== "nar")
    throw new Error("Unknown migration source");
  const rawVector: unknown = JSON.parse(requiredText(row, "feature_vector"));
  if (
    !Array.isArray(rawVector) ||
    rawVector.length !== VECTOR_DIMENSIONS ||
    !rawVector.every((value: unknown) => typeof value === "number" && Number.isFinite(value))
  )
    throw new Error("Invalid migration vector");
  const metadata: Record<string, VectorizeVectorMetadata> = {
    source: cursor.source,
    raceDate: requiredText(row, "race_date"),
    venue: cursor.venue,
    trackPrefix: typeof row.track_code === "string" ? row.track_code.slice(0, 1) : "",
    hasFinish: row.finish_norm !== null && row.finish_norm !== undefined,
  };
  OPTIONAL_NUMBERS.forEach(([column, field]) => {
    const raw: unknown = row[column];
    if (raw === null || raw === undefined) return;
    if ((typeof raw !== "number" && typeof raw !== "string") || String(raw).trim() === "")
      throw new Error(`Invalid migration numeric column: ${column}`);
    const value: number = Number(raw);
    if (!Number.isFinite(value)) throw new Error(`Invalid migration numeric column: ${column}`);
    metadata[field] = value;
  });
  // Vectorize identifiers are global across namespaces. Salt identity with the generation.
  const digest: ArrayBuffer = await crypto.subtle.digest(
    "SHA-256",
    encoder.encode(JSON.stringify([namespace, ...cornerMigrationParameters(cursor)])),
  );
  const id: string = Array.from(new Uint8Array(digest), (value) =>
    value.toString(16).padStart(2, "0"),
  ).join("");
  return { id, namespace, values: rawVector, metadata };
};
