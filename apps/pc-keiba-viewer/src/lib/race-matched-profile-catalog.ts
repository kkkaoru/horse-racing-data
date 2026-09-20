// Run with bun. Private Catalog reader for the matched-profile (target
// averages) route; mirrors the jra/nar history adapter's validation.
import "server-only";
import { readBoundedCatalogBody } from "./catalog-read-body";
import type { TimeScoreTargetProfile } from "./time-score-pipeline";

export interface CatalogRaceMatchedProfileQuery {
  source: string;
  date: string;
  keibajoCode: string;
  raceBango: string;
  kyori: string;
  kyosoShubetsuCode: string;
  kyosoJokenCode: string;
  kyosoJokenMeisho: string;
  trackCode: string;
  gradeCode: string;
  kyosomeiHondai: string;
  years: string;
  limit: number;
  flags: readonly string[];
  runnerCount: number | null;
}

export interface CatalogRaceMatchedProfileBinding {
  fetch(request: Request): Promise<Response>;
}

const REQUIRED_KEYS: readonly (keyof CatalogRaceMatchedProfileQuery)[] = [
  "source",
  "date",
  "keibajoCode",
  "raceBango",
  "kyori",
  "kyosoShubetsuCode",
  "kyosoJokenCode",
  "kyosoJokenMeisho",
  "trackCode",
  "gradeCode",
  "kyosomeiHondai",
  "years",
];
export const MATCHED_PROFILE_FLAGS: readonly string[] = [
  "includeVenue",
  "includeDistance",
  "includeAge",
  "includeClass",
  "includeConditionKey",
  "includeTrackCode",
  "includeGrade",
  "includeRaceTitle",
  "includeMonthWindow",
  "includeRunnerCount",
];
const PROFILE_FIELDS: readonly (keyof TimeScoreTargetProfile)[] = [
  "targetRaceTime",
  "targetLast3f",
  "targetBodyWeight",
  "targetCarriedWeight",
  "targetMargin",
];
const TIMEOUT_MS: number = 35_000;
const ENDPOINT: string = "https://pc-keiba-r2-catalog.internal/v1/race-matched-profile";
const FAILURE: string = "Catalog matched profile unavailable";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const parseNumberOrNull = (value: unknown): number | null => {
  if (value === null) return null;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value !== "" && Number.isFinite(Number(value)))
    return Number(value);
  throw new Error(FAILURE);
};

export const readCatalogRaceMatchedProfile = async (
  binding: CatalogRaceMatchedProfileBinding | undefined,
  query: CatalogRaceMatchedProfileQuery,
): Promise<TimeScoreTargetProfile> => {
  if (
    binding === undefined ||
    REQUIRED_KEYS.some((key) => query[key] === "") ||
    !Number.isSafeInteger(query.limit) ||
    query.limit <= 0 ||
    query.flags.some((flag) => !MATCHED_PROFILE_FLAGS.includes(flag)) ||
    (query.runnerCount !== null &&
      (!Number.isSafeInteger(query.runnerCount) || query.runnerCount <= 0))
  )
    throw new Error(FAILURE);
  const params: URLSearchParams = new URLSearchParams();
  for (const key of REQUIRED_KEYS) params.set(key, String(query[key]));
  for (const flag of MATCHED_PROFILE_FLAGS)
    params.set(flag, query.flags.includes(flag) ? "1" : "0");
  if (query.runnerCount !== null) params.set("runnerCount", String(query.runnerCount));
  params.set("limit", String(query.limit));
  try {
    const response: Response = await binding.fetch(
      new Request(`${ENDPOINT}?${params.toString()}`, {
        method: "GET",
        redirect: "manual",
        signal: AbortSignal.timeout(TIMEOUT_MS),
      }),
    );
    if (response.status !== 200 || response.headers.get("cache-control") !== "no-store") {
      await response.body?.cancel();
      throw new Error(FAILURE);
    }
    const value: unknown = await readBoundedCatalogBody(response);
    if (!isRecord(value) || !isRecord(value.profile)) throw new Error(FAILURE);
    if (Object.keys(value.profile).length !== PROFILE_FIELDS.length) throw new Error(FAILURE);
    const profile: Record<string, unknown> = value.profile;
    return {
      targetRaceTime: parseNumberOrNull(profile.targetRaceTime),
      targetLast3f: parseNumberOrNull(profile.targetLast3f),
      targetBodyWeight: parseNumberOrNull(profile.targetBodyWeight),
      targetCarriedWeight: parseNumberOrNull(profile.targetCarriedWeight),
      targetMargin: parseNumberOrNull(profile.targetMargin),
    };
  } catch {
    throw new Error(FAILURE);
  }
};
