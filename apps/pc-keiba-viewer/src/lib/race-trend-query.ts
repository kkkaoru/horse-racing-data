export const RACE_TREND_TARGET_QUERY_PARAM = "raceTrendTargets";

export const RACE_TREND_SCORE_CONDITIONS_QUERY_PARAM = "raceTrendScoreConditions";

export const RACE_TREND_SORT_QUERY_PARAM = "raceTrendSortBy";

export const RACE_TREND_SCORE_LINK_QUERY_PARAM = "raceTrendScoreLinkToWinRate";

export const DEFAULT_RACE_TREND_SCORE_LINK_TO_WIN_RATE: boolean = false;

const RACE_TREND_SCORE_LINK_TRUE_TOKENS: Set<string> = new Set(["1", "true"]);

const RACE_TREND_SCORE_LINK_FALSE_TOKENS: Set<string> = new Set(["0", "false"]);

const RACE_TREND_SCORE_LINK_TRUE_STRING: string = "1";

const RACE_TREND_SCORE_LINK_FALSE_STRING: string = "0";

const RACE_TREND_TARGET_QUERY_PARAM_ALIASES = ["trendTargets", "trend"] as const;

export const RACE_TREND_TARGET_KEYS = [
  "runningStyle",
  "frame",
  "jockey",
  "trainer",
  "raceNumber",
] as const;

export const RACE_TREND_SCORE_CONDITION_QUERY_KEYS = [
  "frame",
  "jockey",
  "trainer",
  "frameRunningStyle",
] as const;

export type RaceTrendTargetKey = (typeof RACE_TREND_TARGET_KEYS)[number];

export type RaceTrendScoreConditionKey = (typeof RACE_TREND_SCORE_CONDITION_QUERY_KEYS)[number];

export type RaceTrendSortKey = "score" | "showRate" | "quinellaRate" | "winRate";

export const RACE_TREND_SORT_KEYS = [
  "score",
  "showRate",
  "quinellaRate",
  "winRate",
] satisfies readonly RaceTrendSortKey[];

export const DEFAULT_RACE_TREND_SORT_KEY: RaceTrendSortKey = "showRate";

const RACE_TREND_SORT_KEY_SET: Set<string> = new Set(RACE_TREND_SORT_KEYS);

export type RaceTrendTargets = Record<RaceTrendTargetKey, boolean>;

export type RaceTrendScoreConditionsQuery = Record<RaceTrendScoreConditionKey, boolean>;

export const DEFAULT_RACE_TREND_TARGETS: RaceTrendTargets = {
  runningStyle: false,
  frame: false,
  jockey: true,
  trainer: false,
  raceNumber: false,
};

export const DEFAULT_RACE_TREND_SCORE_CONDITIONS_QUERY: RaceTrendScoreConditionsQuery = {
  frame: true,
  jockey: true,
  trainer: true,
  frameRunningStyle: false,
};

const EMPTY_RACE_TREND_TARGETS: RaceTrendTargets = {
  runningStyle: false,
  frame: false,
  jockey: false,
  trainer: false,
  raceNumber: false,
};

const EMPTY_RACE_TREND_SCORE_CONDITIONS_QUERY: RaceTrendScoreConditionsQuery = {
  frame: false,
  jockey: false,
  trainer: false,
  frameRunningStyle: false,
};

const TARGET_TOKEN_ALIASES: Record<string, RaceTrendTargetKey> = {
  frame: "frame",
  jockey: "jockey",
  trainer: "trainer",
  race: "raceNumber",
  raceNumber: "raceNumber",
  runningStyle: "runningStyle",
  style: "runningStyle",
};

const SCORE_CONDITION_TOKEN_ALIASES: Record<string, RaceTrendScoreConditionKey> = {
  frame: "frame",
  jockey: "jockey",
  trainer: "trainer",
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

const isRaceTrendSortKey = (value: string): value is RaceTrendSortKey =>
  RACE_TREND_SORT_KEY_SET.has(value);

export const parseRaceTrendSortKeyQuery = (value: string | null): RaceTrendSortKey => {
  if (value === null) return DEFAULT_RACE_TREND_SORT_KEY;
  const normalized = value.trim();
  return isRaceTrendSortKey(normalized) ? normalized : DEFAULT_RACE_TREND_SORT_KEY;
};

export const serializeRaceTrendSortKeyQuery = (key: RaceTrendSortKey): string => key;

export const getRaceTrendSortKeyQueryValue = (
  searchParams: URLSearchParams | SearchParamRecord,
): string | null => getSearchParamValue(searchParams, RACE_TREND_SORT_QUERY_PARAM);

export const getRaceTrendSortKeyFromSearchParams = (
  searchParams: URLSearchParams | SearchParamRecord,
): RaceTrendSortKey => parseRaceTrendSortKeyQuery(getRaceTrendSortKeyQueryValue(searchParams));

export const isDefaultRaceTrendSortKey = (key: RaceTrendSortKey): boolean =>
  key === DEFAULT_RACE_TREND_SORT_KEY;

export const clearRaceTrendSortKeyQueryParam = (searchParams: URLSearchParams): void => {
  searchParams.delete(RACE_TREND_SORT_QUERY_PARAM);
};

export const parseRaceTrendScoreLinkQuery = (value: string | null): boolean => {
  if (value === null) return DEFAULT_RACE_TREND_SCORE_LINK_TO_WIN_RATE;
  const normalized = value.trim().toLowerCase();
  if (RACE_TREND_SCORE_LINK_TRUE_TOKENS.has(normalized)) return true;
  if (RACE_TREND_SCORE_LINK_FALSE_TOKENS.has(normalized)) return false;
  return DEFAULT_RACE_TREND_SCORE_LINK_TO_WIN_RATE;
};

export const serializeRaceTrendScoreLinkQuery = (linked: boolean): string =>
  linked ? RACE_TREND_SCORE_LINK_TRUE_STRING : RACE_TREND_SCORE_LINK_FALSE_STRING;

export const getRaceTrendScoreLinkQueryValue = (
  searchParams: URLSearchParams | SearchParamRecord,
): string | null => getSearchParamValue(searchParams, RACE_TREND_SCORE_LINK_QUERY_PARAM);

export const getRaceTrendScoreLinkFromSearchParams = (
  searchParams: URLSearchParams | SearchParamRecord,
): boolean => parseRaceTrendScoreLinkQuery(getRaceTrendScoreLinkQueryValue(searchParams));

export const isDefaultRaceTrendScoreLinkToWinRate = (linked: boolean): boolean =>
  linked === DEFAULT_RACE_TREND_SCORE_LINK_TO_WIN_RATE;

export const clearRaceTrendScoreLinkQuery = (searchParams: URLSearchParams): void => {
  searchParams.delete(RACE_TREND_SCORE_LINK_QUERY_PARAM);
};
