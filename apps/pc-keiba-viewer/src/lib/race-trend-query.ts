export const RACE_TREND_TARGET_QUERY_PARAM = "raceTrendTargets";

export const RACE_TREND_SCORE_CONDITIONS_QUERY_PARAM = "raceTrendScoreConditions";

const RACE_TREND_TARGET_QUERY_PARAM_ALIASES = ["trendTargets", "trend"] as const;

export const RACE_TREND_TARGET_KEYS = ["runningStyle", "frame", "jockey", "raceNumber"] as const;

export const RACE_TREND_SCORE_CONDITION_QUERY_KEYS = [
  "frame",
  "jockey",
  "frameRunningStyle",
] as const;

export type RaceTrendTargetKey = (typeof RACE_TREND_TARGET_KEYS)[number];

export type RaceTrendScoreConditionKey = (typeof RACE_TREND_SCORE_CONDITION_QUERY_KEYS)[number];

export type RaceTrendTargets = Record<RaceTrendTargetKey, boolean>;

export type RaceTrendScoreConditionsQuery = Record<RaceTrendScoreConditionKey, boolean>;

export const DEFAULT_RACE_TREND_TARGETS: RaceTrendTargets = {
  runningStyle: false,
  frame: true,
  jockey: false,
  raceNumber: false,
};

export const DEFAULT_RACE_TREND_SCORE_CONDITIONS_QUERY: RaceTrendScoreConditionsQuery = {
  frame: true,
  jockey: false,
  frameRunningStyle: false,
};

const EMPTY_RACE_TREND_TARGETS: RaceTrendTargets = {
  runningStyle: false,
  frame: false,
  jockey: false,
  raceNumber: false,
};

const EMPTY_RACE_TREND_SCORE_CONDITIONS_QUERY: RaceTrendScoreConditionsQuery = {
  frame: false,
  jockey: false,
  frameRunningStyle: false,
};

const TARGET_TOKEN_ALIASES: Record<string, RaceTrendTargetKey> = {
  frame: "frame",
  jockey: "jockey",
  race: "raceNumber",
  raceNumber: "raceNumber",
  runningStyle: "runningStyle",
  style: "runningStyle",
};

const SCORE_CONDITION_TOKEN_ALIASES: Record<string, RaceTrendScoreConditionKey> = {
  frame: "frame",
  jockey: "jockey",
  frameRunningStyle: "frameRunningStyle",
};

type SearchParamRecord = Record<string, string | string[] | undefined>;

const getSearchParamValue = (
  searchParams: URLSearchParams | SearchParamRecord,
  name: string,
): string | null => {
  if (searchParams instanceof URLSearchParams) {
    return searchParams.get(name);
  }
  const value = searchParams[name];
  if (Array.isArray(value)) {
    return value[0] ?? null;
  }
  return value ?? null;
};

export const getRaceTrendTargetQueryValue = (
  searchParams: URLSearchParams | SearchParamRecord,
): string | null =>
  getSearchParamValue(searchParams, RACE_TREND_TARGET_QUERY_PARAM) ??
  RACE_TREND_TARGET_QUERY_PARAM_ALIASES.map((name) => getSearchParamValue(searchParams, name)).find(
    (value): value is string => value !== null,
  ) ??
  null;

export const parseRaceTrendTargets = (value: string | null): RaceTrendTargets | null => {
  if (value === null) {
    return null;
  }
  const normalized = value.trim();
  if (normalized === "" || normalized === "none") {
    return { ...EMPTY_RACE_TREND_TARGETS };
  }

  const targets = { ...EMPTY_RACE_TREND_TARGETS };
  let hasKnownToken = false;
  for (const token of normalized.split(",")) {
    const key = TARGET_TOKEN_ALIASES[token.trim()];
    if (!key) {
      continue;
    }
    targets[key] = true;
    hasKnownToken = true;
  }
  return hasKnownToken ? targets : null;
};

export const getRaceTrendTargetsFromSearchParams = (
  searchParams: URLSearchParams | SearchParamRecord,
): RaceTrendTargets =>
  parseRaceTrendTargets(getRaceTrendTargetQueryValue(searchParams)) ?? {
    ...DEFAULT_RACE_TREND_TARGETS,
  };

export const serializeRaceTrendTargets = (targets: RaceTrendTargets): string => {
  const selectedKeys = RACE_TREND_TARGET_KEYS.filter((key) => targets[key]);
  return selectedKeys.length === 0 ? "none" : selectedKeys.join(",");
};

export const isSameRaceTrendTargets = (left: RaceTrendTargets, right: RaceTrendTargets): boolean =>
  RACE_TREND_TARGET_KEYS.every((key) => left[key] === right[key]);

export const isDefaultRaceTrendTargets = (targets: RaceTrendTargets): boolean =>
  isSameRaceTrendTargets(targets, DEFAULT_RACE_TREND_TARGETS);

export const clearRaceTrendTargetQueryParams = (searchParams: URLSearchParams): void => {
  searchParams.delete(RACE_TREND_TARGET_QUERY_PARAM);
  for (const name of RACE_TREND_TARGET_QUERY_PARAM_ALIASES) {
    searchParams.delete(name);
  }
};

export const getRaceTrendScoreConditionsQueryValue = (
  searchParams: URLSearchParams | SearchParamRecord,
): string | null => getSearchParamValue(searchParams, RACE_TREND_SCORE_CONDITIONS_QUERY_PARAM);

export const parseRaceTrendScoreConditionsQuery = (
  value: string | null,
): RaceTrendScoreConditionsQuery | null => {
  if (value === null) {
    return null;
  }
  const normalized = value.trim();
  if (normalized === "" || normalized === "none") {
    return { ...EMPTY_RACE_TREND_SCORE_CONDITIONS_QUERY };
  }

  const conditions = { ...EMPTY_RACE_TREND_SCORE_CONDITIONS_QUERY };
  let hasKnownToken = false;
  for (const token of normalized.split(",")) {
    const key = SCORE_CONDITION_TOKEN_ALIASES[token.trim()];
    if (!key) {
      continue;
    }
    conditions[key] = true;
    hasKnownToken = true;
  }
  return hasKnownToken ? conditions : null;
};

export const getRaceTrendScoreConditionsFromSearchParams = (
  searchParams: URLSearchParams | SearchParamRecord,
): RaceTrendScoreConditionsQuery =>
  parseRaceTrendScoreConditionsQuery(getRaceTrendScoreConditionsQueryValue(searchParams)) ?? {
    ...DEFAULT_RACE_TREND_SCORE_CONDITIONS_QUERY,
  };

export const serializeRaceTrendScoreConditionsQuery = (
  conditions: RaceTrendScoreConditionsQuery,
): string => {
  const selectedKeys = RACE_TREND_SCORE_CONDITION_QUERY_KEYS.filter((key) => conditions[key]);
  return selectedKeys.length === 0 ? "none" : selectedKeys.join(",");
};

export const isSameRaceTrendScoreConditionsQuery = (
  left: RaceTrendScoreConditionsQuery,
  right: RaceTrendScoreConditionsQuery,
): boolean => RACE_TREND_SCORE_CONDITION_QUERY_KEYS.every((key) => left[key] === right[key]);

export const isDefaultRaceTrendScoreConditionsQuery = (
  conditions: RaceTrendScoreConditionsQuery,
): boolean =>
  isSameRaceTrendScoreConditionsQuery(conditions, DEFAULT_RACE_TREND_SCORE_CONDITIONS_QUERY);

export const clearRaceTrendScoreConditionsQueryParam = (searchParams: URLSearchParams): void => {
  searchParams.delete(RACE_TREND_SCORE_CONDITIONS_QUERY_PARAM);
};
