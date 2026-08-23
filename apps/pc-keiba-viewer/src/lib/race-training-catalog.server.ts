import "server-only";
import { safeGetCloudflareEnv } from "./cloudflare-context.server";
import type { Training } from "./race-types";

export interface RaceTrainingCatalogQuery {
  day: string;
  keibajoCode: string;
  month: string;
  raceBango: string;
  year: string;
}

const CATALOG_ORIGIN = "https://pc-keiba-r2-catalog.internal";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isNullableString = (value: unknown): value is string | null =>
  typeof value === "string" || value === null;

const isOptionalNullableString = (value: unknown): value is string | null | undefined =>
  value === undefined || isNullableString(value);

const isTrainingSource = (value: unknown): value is "jra" | "netkeiba" =>
  value === "jra" || value === "netkeiba";

const isCatalogTraining = (value: unknown): value is Training =>
  isRecord(value) &&
  isNullableString(value.umaban) &&
  isNullableString(value.bamei) &&
  typeof value.trainingType === "string" &&
  isNullableString(value.tracenKubun) &&
  typeof value.chokyoNengappi === "string" &&
  typeof value.chokyoJikoku === "string" &&
  isNullableString(value.course) &&
  isNullableString(value.babamawari) &&
  isNullableString(value.timeGokei10f) &&
  isNullableString(value.lapTime10f) &&
  isNullableString(value.timeGokei9f) &&
  isNullableString(value.lapTime9f) &&
  isNullableString(value.timeGokei8f) &&
  isNullableString(value.lapTime8f) &&
  isNullableString(value.timeGokei7f) &&
  isNullableString(value.lapTime7f) &&
  isNullableString(value.timeGokei6f) &&
  isNullableString(value.lapTime6f) &&
  isNullableString(value.timeGokei5f) &&
  isNullableString(value.lapTime5f) &&
  isNullableString(value.timeGokei4f) &&
  isNullableString(value.lapTime4f) &&
  isNullableString(value.timeGokei3f) &&
  isNullableString(value.lapTime3f) &&
  isNullableString(value.timeGokei2f) &&
  isNullableString(value.lapTime2f) &&
  isNullableString(value.lapTime1f) &&
  isOptionalNullableString(value.currentJockeyName) &&
  isOptionalNullableString(value.trainerName) &&
  isOptionalNullableString(value.trainingRiderName) &&
  isOptionalNullableString(value.premiumCommentText) &&
  isOptionalNullableString(value.premiumEvaluationGrade) &&
  isOptionalNullableString(value.premiumEvaluationText) &&
  isTrainingSource(value.trainingDataSource) &&
  (value.premiumWorkoutIndex === undefined ||
    (typeof value.premiumWorkoutIndex === "number" && Number.isInteger(value.premiumWorkoutIndex)));

const parseTraining = (value: unknown): Training | null =>
  isCatalogTraining(value) ? value : null;

export const buildRaceTrainingCatalogUrl = (query: RaceTrainingCatalogQuery): URL => {
  const url = new URL("/v1/race-trainings", CATALOG_ORIGIN);
  url.searchParams.set(
    "date",
    `${query.year}${query.month.padStart(2, "0")}${query.day.padStart(2, "0")}`,
  );
  url.searchParams.set("keibajoCode", query.keibajoCode.padStart(2, "0"));
  url.searchParams.set("raceBango", query.raceBango.padStart(2, "0"));
  return url;
};

export const fetchRaceTrainingsFromCatalog = async (
  query: RaceTrainingCatalogQuery,
): Promise<Training[] | null> => {
  const env = await safeGetCloudflareEnv();
  const catalog = env?.R2_CATALOG;
  if (!catalog) return null;

  try {
    const response = await catalog.fetch(buildRaceTrainingCatalogUrl(query).href);
    if (!response.ok) return null;
    const payload: unknown = await response.json();
    if (!isRecord(payload) || !Array.isArray(payload.rows)) return null;
    const rows = payload.rows.map(parseTraining);
    return rows.some((row) => row === null) ? null : rows.filter((row) => row !== null);
  } catch {
    return null;
  }
};
