// Run with bun. Computes a timestamp-independent fingerprint of the exact
// running-style feature cells consumed by the finish-position day-base build.

import type { PredictCategory } from "./types";

const MAX_RUNNING_STYLE_ROWS = 1_024;
const FLOAT64_BYTES = 8;
const SHA_256_HEX_LENGTH = 64;
const NONE_HASH = "none";

interface RunningStyleContentHashParams {
  category: PredictCategory;
  db: D1Database;
  runYmd: string;
}

interface RunningStyleHashRow {
  horse_number: number;
  ketto_toroku_bango: string;
  p_nige: number;
  p_oikomi: number;
  p_sashi: number;
  p_senkou: number;
  predicted_label: string;
  race_key: string;
}

export interface RunningStyleContentFingerprint {
  contentHash: string;
  rowCount: number;
}

const LABEL_CLASS: Readonly<Record<string, number>> = {
  nige: 0,
  oikomi: 3,
  sashi: 2,
  senkou: 1,
};

const encoder = new TextEncoder();

const hex = (bytes: ArrayBuffer): string =>
  [...new Uint8Array(bytes)].map((value) => value.toString(16).padStart(2, "0")).join("");

const sha256 = async (value: string): Promise<string> =>
  hex(await crypto.subtle.digest("SHA-256", encoder.encode(value)));

const float64Hex = (value: number): string => {
  const bytes = new ArrayBuffer(FLOAT64_BYTES);
  new DataView(bytes).setFloat64(0, value, false);
  return hex(bytes);
};

const normalizedRaceKey = (value: string): string | null => {
  const parts = value.split(":");
  if (parts.length !== 4) return null;
  const [source, runYmd, venueRaw, raceRaw] = parts;
  if (
    source === undefined ||
    runYmd === undefined ||
    venueRaw === undefined ||
    raceRaw === undefined
  )
    return null;
  const venue = Number(venueRaw);
  const race = Number(raceRaw);
  if (!/^(?:jra|nar)$/.test(source) || !/^\d{8}$/.test(runYmd)) return null;
  if (!Number.isSafeInteger(venue) || venue <= 0 || !Number.isSafeInteger(race) || race <= 0)
    return null;
  return `${source}:${runYmd}:${String(venue).padStart(2, "0")}:${String(race).padStart(2, "0")}`;
};

const canonicalRow = (row: RunningStyleHashRow): string | null => {
  const raceKey = normalizedRaceKey(row.race_key);
  const horseNumber = Number(row.horse_number);
  const ketto = row.ketto_toroku_bango.trim();
  const predictedClass = LABEL_CLASS[row.predicted_label];
  const probabilities = [row.p_nige, row.p_senkou, row.p_sashi, row.p_oikomi].map(Number);
  if (
    raceKey === null ||
    !Number.isSafeInteger(horseNumber) ||
    horseNumber <= 0 ||
    ketto.length === 0 ||
    predictedClass === undefined ||
    probabilities.some((value) => !Number.isFinite(value))
  ) {
    return null;
  }
  return JSON.stringify([
    raceKey,
    String(horseNumber),
    ketto,
    ...probabilities.map(float64Hex),
    String(predictedClass),
  ]);
};

const patterns = (
  category: PredictCategory,
  runYmd: string,
): { exclude: string; include: string } => {
  const source = category === "jra" ? "jra" : "nar";
  const includeVenue = category === "ban-ei" ? "83:" : "";
  const excludeVenue = category === "nar" ? "83:" : "__never__:";
  return {
    exclude: `${source}:${runYmd}:${excludeVenue}%`,
    include: `${source}:${runYmd}:${includeVenue}%`,
  };
};

export const computeRunningStyleContentFingerprint = async (
  params: RunningStyleContentHashParams,
): Promise<RunningStyleContentFingerprint> => {
  if (params.category === "ban-ei") return { contentHash: NONE_HASH, rowCount: 0 };
  const scope = patterns(params.category, params.runYmd);
  const result = await params.db
    .prepare(
      `select race_key, horse_number, ketto_toroku_bango,
              p_nige, p_senkou, p_sashi, p_oikomi, predicted_label
         from race_running_styles
        where race_key like ?1
          and race_key not like ?2
        order by race_key, horse_number
        limit 1025`,
    )
    .bind(scope.include, scope.exclude)
    .all<RunningStyleHashRow>();
  if (result.results.length > MAX_RUNNING_STYLE_ROWS) throw new Error("running-style-row-limit");
  const rows = result.results.map(canonicalRow);
  if (rows.some((row) => row === null)) throw new Error("running-style-row-invalid");
  const canonical = rows
    .filter((row): row is string => row !== null)
    .sort()
    .join("\n");
  const contentHash = await sha256(canonical);
  if (contentHash.length !== SHA_256_HEX_LENGTH) throw new Error("running-style-hash-invalid");
  return { contentHash, rowCount: rows.length };
};
