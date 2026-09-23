// Runs with bun; this handler is reachable only through the trusted binding.
import { boundedAuditFetch } from "./d1-audit";
import { notifyReadFailure } from "./read-failure-alert";
import {
  buildMatchedRacesSql,
  readTargetProfile,
  type MatchedProfileInput,
  type MatchedProfileSettings,
  type TargetProfile,
} from "./race-matched-profile-read";
import { executeR2Sql, R2SqlQueryError } from "./r2-sql";
import type { R2SqlCatalogConfig } from "./types";

const REQUIRED_PARAMETERS: readonly string[] = [
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
  "limit",
];
const FLAG_PARAMETERS: readonly string[] = [
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
const OPTIONAL_PARAMETERS: readonly string[] = ["runnerCount"];
const ALL_PARAMETERS: readonly string[] = [
  ...REQUIRED_PARAMETERS,
  ...FLAG_PARAMETERS,
  ...OPTIONAL_PARAMETERS,
];
const DEFAULT_LIMIT: number = 5000;
const json = (value: unknown, status: number): Response =>
  Response.json(value, { status, headers: { "Cache-Control": "no-store" } });

const readFlag = (value: string | null): boolean | null => {
  if (value === "0") return false;
  if (value === "1") return true;
  return null;
};

const readOptionalNumber = (value: string | null): number | null | undefined => {
  if (value === null || value === "") return null;
  const parsed: number = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : undefined;
};

const emptyToNull = (value: string): string | null => (value === "" ? null : value);

// A sanitized failure class: never the provider message (it can carry private
// detail), only enough to tell a timeout from an R2 SQL error or a bad result.
export const classifyMatchedProfileFailure = (error: unknown): string => {
  if (error instanceof R2SqlQueryError)
    return `r2_sql:${String(error.status ?? "-")}:${String(error.code ?? "-")}`;
  if (error instanceof DOMException) return `abort:${error.name}`;
  if (error instanceof Error && error.message.startsWith("Invalid target profile"))
    return "invalid_result";
  if (error instanceof Error && error.message.includes("byte limit")) return "byte_limit";
  return "unknown";
};

export const handleRaceMatchedProfileRead = async (
  request: Request,
  env: R2SqlCatalogConfig,
): Promise<Response> => {
  if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
  const params: URLSearchParams = new URL(request.url).searchParams;
  const flags: Record<string, boolean | null> = {};
  for (const name of FLAG_PARAMETERS) flags[name] = readFlag(params.get(name));
  const years: number | null | undefined = readOptionalNumber(params.get("years"));
  const limit: number | null | undefined = readOptionalNumber(params.get("limit"));
  const runnerCount: number | null | undefined = readOptionalNumber(params.get("runnerCount"));
  if (
    REQUIRED_PARAMETERS.some((key) => params.getAll(key).length !== 1) ||
    FLAG_PARAMETERS.some((key) => params.getAll(key).length !== 1) ||
    OPTIONAL_PARAMETERS.some((key) => params.getAll(key).length > 1) ||
    [...params.keys()].some((key) => !ALL_PARAMETERS.includes(key)) ||
    Object.values(flags).some((value) => value === null) ||
    years === undefined ||
    limit === undefined ||
    runnerCount === undefined
  )
    return json({ error: "Invalid matched profile request" }, 400);
  const source: string | null = params.get("source");
  if (source !== "jra" && source !== "nar")
    return json({ error: "Invalid matched profile request" }, 400);
  const settings: MatchedProfileSettings = {
    includeVenue: flags.includeVenue === true,
    includeDistance: flags.includeDistance === true,
    includeAge: flags.includeAge === true,
    includeClass: flags.includeClass === true,
    includeConditionKey: flags.includeConditionKey === true,
    includeTrackCode: flags.includeTrackCode === true,
    includeGrade: flags.includeGrade === true,
    includeRaceTitle: flags.includeRaceTitle === true,
    includeMonthWindow: flags.includeMonthWindow === true,
    includeRunnerCount: flags.includeRunnerCount === true,
    runnerCount: runnerCount ?? null,
  };
  const input: MatchedProfileInput = {
    namespace: env.R2_SQL_NAMESPACE,
    source,
    raceDate: params.get("date") ?? "",
    years: years ?? null,
    race: {
      keibajoCode: params.get("keibajoCode") ?? "",
      kyori: emptyToNull(params.get("kyori") ?? ""),
      kyosoShubetsuCode: emptyToNull(params.get("kyosoShubetsuCode") ?? ""),
      kyosoJokenCode: emptyToNull(params.get("kyosoJokenCode") ?? ""),
      kyosoJokenMeisho: emptyToNull(params.get("kyosoJokenMeisho") ?? ""),
      trackCode: emptyToNull(params.get("trackCode") ?? ""),
      gradeCode: emptyToNull(params.get("gradeCode") ?? ""),
      kyosomeiHondai: emptyToNull(params.get("kyosomeiHondai") ?? ""),
    },
    settings,
    limit: limit ?? DEFAULT_LIMIT,
  };
  try {
    buildMatchedRacesSql(input);
  } catch {
    return json({ error: "Invalid matched profile request" }, 400);
  }
  try {
    const profile: TargetProfile = await readTargetProfile(input, async (sql) =>
      executeR2Sql(env, sql, boundedAuditFetch(fetch)),
    );
    return json({ profile }, 200);
  } catch (error) {
    console.error(
      JSON.stringify({
        event: "race_matched_profile_read_failed",
        reason: classifyMatchedProfileFailure(error),
      }),
    );
    await notifyReadFailure(env.INGESTION_ALERTS, { event: "race_matched_profile_read_failed" });
    return json({ error: "Catalog matched profile unavailable" }, 503);
  }
};
