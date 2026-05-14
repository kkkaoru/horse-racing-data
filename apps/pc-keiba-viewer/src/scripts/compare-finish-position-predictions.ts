import { readFile } from "node:fs/promises";

import { Pool } from "pg";

import { getConnectionString, loadEnv } from "./compare-corner-predictions";

type Category = "all" | "ban-ei" | "jra" | "nar";

type Options = {
  breakdown: boolean;
  category: Category;
  changedRaceLimit: number;
  changedRaces: boolean;
  concurrency: number;
  ensembleMode: "auto" | "mixed" | "off" | "vote" | "weighted";
  fromDate: string;
  historyWeightMultiplier: number;
  oddsWeightMultiplier: number;
  popularityWeightMultiplier: number;
  recentWeightMultiplier: number;
  sameDayJockeyWeight: number;
  target: "local" | "neon";
  tuningConfig?: FinishPredictionTuningConfig;
  tuningConfigPath: string | null;
  toDate: string;
};

type ScoreComponentKey =
  | "avgFinish"
  | "lightgbm"
  | "lstm"
  | "odds"
  | "popularity"
  | "recentFinish"
  | "sameDayJockey"
  | "transformer";

type ConditionMatcher = {
  conditionBands?: string[];
  distanceBands?: string[];
  fromDate?: string;
  gradeBands?: string[];
  keibajoCodes?: string[];
  raceBangos?: string[];
  sources?: string[];
  toDate?: string;
};

type ScoreWeightRule = {
  multiply?: Partial<Record<ScoreComponentKey, number>>;
  set?: Partial<Record<ScoreComponentKey, number>>;
  when?: ConditionMatcher;
};

type EnsembleRule = {
  mixedWeightedShare?: number;
  mode?: "mixed" | "off" | "vote" | "weighted";
  weights?: Partial<Record<"lightgbm" | "lstm" | "transformer", number>>;
  when?: ConditionMatcher;
};

type ComponentModelConfig = {
  fallback?: number;
  weights?: Partial<
    Record<"avgFinish" | "odds" | "popularity" | "recentFinish" | "sameDayJockey", number>
  >;
};

type FinishPredictionTuningConfig = {
  baneiScoreWeights?: Partial<Record<"odds" | "popularity" | "sameDayJockey", number>>;
  componentModels?: {
    lstmLike?: ComponentModelConfig;
    transformerLike?: ComponentModelConfig;
  };
  ensemble?: {
    defaultMode?: "auto" | "mixed" | "off" | "vote" | "weighted";
    mixedWeightedShare?: number;
    rules?: EnsembleRule[];
    weights?: Partial<Record<"lightgbm" | "lstm" | "transformer", number>>;
  };
  scoreWeights?: {
    base?: Partial<
      Record<"avgFinish" | "odds" | "popularity" | "recentFinish" | "sameDayJockey", number>
    >;
    rules?: ScoreWeightRule[];
  };
  version?: string;
};

const scoreComponentKeys: ScoreComponentKey[] = [
  "avgFinish",
  "lightgbm",
  "lstm",
  "odds",
  "popularity",
  "recentFinish",
  "sameDayJockey",
  "transformer",
];

type RaceKey = {
  source: string;
  race_date: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
};

type Prediction = {
  actual: number;
  conditionBand: string;
  distanceBand: string;
  gradeBand: string;
  horseNumber: number;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  lightgbmScore: number;
  lstmScore: number;
  predictedRank: number;
  raceBango: string;
  raceKey: string;
  raceUrl: string;
  score: number;
  source: string;
  transformerScore: number;
};

type EvaluationSummary = {
  place1Accuracy: number;
  place2Accuracy: number;
  place3Accuracy: number;
  top3BoxAccuracy: number;
  top3ExactOrderAccuracy: number;
  top3PlaceRelation: number;
  top3WinnerCapture: number;
  top5WinnerCapture: number;
};

type PredictionQueryRow = RaceKey & {
  avg_finish: string | null;
  finish_position: number;
  grade_code: string | null;
  horseNumber: number;
  kyori: number | null;
  kyoso_joken_code: string | null;
  odds_score: string | null;
  popularity_score: string | null;
  recent_finish: string | null;
  same_day_jockey_win_score: string | null;
};

type BaneiPredictionQueryRow = {
  finish_position: number;
  horseNumber: number;
  odds_score: string | null;
  popularity_score: string | null;
  race_key: string;
  same_day_jockey_win_score: string | null;
};

const today = new Date();
const defaultToDate = today.toISOString().slice(0, 10).replaceAll("-", "");
const defaultFromDate = new Date(today);
defaultFromDate.setFullYear(defaultFromDate.getFullYear() - 10);

const parseArgs = (args: string[]): Options => {
  const options: Options = {
    breakdown: false,
    category: "all",
    changedRaceLimit: 40,
    changedRaces: false,
    concurrency: 6,
    ensembleMode: "off",
    fromDate: defaultFromDate.toISOString().slice(0, 10).replaceAll("-", ""),
    historyWeightMultiplier: 1,
    oddsWeightMultiplier: 1,
    popularityWeightMultiplier: 1,
    recentWeightMultiplier: 1,
    sameDayJockeyWeight: 0.02,
    target: "local",
    tuningConfigPath: null,
    toDate: defaultToDate,
  };
  for (let index = 0; index < args.length; index += 1) {
    const name = args[index];
    const value = args[index + 1];
    if (name === "--target") {
      if (value !== "local" && value !== "neon") {
        throw new Error("--target must be local or neon.");
      }
      options.target = value;
      index += 1;
    } else if (name === "--category") {
      if (value !== "all" && value !== "jra" && value !== "nar" && value !== "ban-ei") {
        throw new Error("--category must be all, jra, nar, or ban-ei.");
      }
      options.category = value;
      index += 1;
    } else if (name === "--from-date") {
      if (value === undefined) {
        throw new Error("--from-date requires a value.");
      }
      options.fromDate = value.replaceAll("-", "");
      index += 1;
    } else if (name === "--to-date") {
      if (value === undefined) {
        throw new Error("--to-date requires a value.");
      }
      options.toDate = value.replaceAll("-", "");
      index += 1;
    } else if (name === "--from-year") {
      options.fromDate = `${value}0101`;
      index += 1;
    } else if (name === "--to-year") {
      options.toDate = `${value}1231`;
      index += 1;
    } else if (name === "--concurrency") {
      options.concurrency = Math.max(1, Number(value));
      index += 1;
    } else if (name === "--ensemble-mode") {
      if (
        value !== "off" &&
        value !== "weighted" &&
        value !== "vote" &&
        value !== "mixed" &&
        value !== "auto"
      ) {
        throw new Error("--ensemble-mode must be off, weighted, vote, mixed, or auto.");
      }
      options.ensembleMode = value;
      index += 1;
    } else if (name === "--same-day-jockey-weight") {
      options.sameDayJockeyWeight = Math.max(0, Number(value));
      index += 1;
    } else if (name === "--history-weight-multiplier") {
      options.historyWeightMultiplier = Math.max(0, Number(value));
      index += 1;
    } else if (name === "--recent-weight-multiplier") {
      options.recentWeightMultiplier = Math.max(0, Number(value));
      index += 1;
    } else if (name === "--popularity-weight-multiplier") {
      options.popularityWeightMultiplier = Math.max(0, Number(value));
      index += 1;
    } else if (name === "--odds-weight-multiplier") {
      options.oddsWeightMultiplier = Math.max(0, Number(value));
      index += 1;
    } else if (name === "--tuning-config") {
      if (value === undefined) {
        throw new Error("--tuning-config requires a file path.");
      }
      options.tuningConfigPath = value;
      index += 1;
    } else if (name === "--breakdown") {
      options.breakdown = true;
    } else if (name === "--changed-races") {
      options.changedRaces = true;
    } else if (name === "--changed-race-limit") {
      options.changedRaceLimit = Math.max(1, Number(value));
      index += 1;
    } else if (name === "--help" || name === "-h") {
      console.log(`Usage:
  bun run src/scripts/compare-finish-position-predictions.ts [options]

Options:
  --target local|neon
  --category all|jra|nar|ban-ei
  --from-date YYYYMMDD
  --to-date YYYYMMDD
  --from-year YYYY
  --to-year YYYY
  --concurrency N
  --ensemble-mode off|weighted|vote|mixed|auto
  --same-day-jockey-weight N
  --history-weight-multiplier N
  --recent-weight-multiplier N
  --popularity-weight-multiplier N
  --odds-weight-multiplier N
  --tuning-config path/to/config.json
  --breakdown
  --changed-races
  --changed-race-limit N

Default range is the latest 10 years.`);
      process.exit(0);
    } else {
      throw new Error(`Unknown argument: ${name}`);
    }
  }
  return options;
};

const categoryFilter = (category: Category): string => {
  if (category === "jra") {
    return "and source = 'jra'";
  }
  if (category === "nar") {
    return "and source = 'nar' and keibajo_code <> '83'";
  }
  if (category === "ban-ei") {
    return "and source = 'nar' and keibajo_code = '83'";
  }
  return "";
};

const toNumber = (value: string | null): number | null => {
  if (value === null) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const readTuningConfig = async (
  path: string | null,
): Promise<FinishPredictionTuningConfig | undefined> => {
  if (path === null) {
    return undefined;
  }
  const raw = await readFile(path, "utf8");
  const parsed: unknown = JSON.parse(raw);
  return typeof parsed === "object" && parsed !== null ? parsed : {};
};

const gradedRaceCodes = new Set(["A", "B", "C", "D", "E", "F", "G", "H", "L"]);

const isNarClassRace = (row: PredictionQueryRow): boolean => {
  const code = row.kyoso_joken_code?.trim() ?? "";
  return code === "000" || /^[ABC]\d?/u.test(code);
};

const getDistanceBand = (distance: number | null): "long" | "middle" | "sprint" | "unknown" => {
  if (distance === null || !Number.isFinite(distance)) {
    return "unknown";
  }
  if (distance <= 1400) {
    return "sprint";
  }
  if (distance >= 2000) {
    return "long";
  }
  return "middle";
};

const getGradeBand = (gradeCode: string | null | undefined): string =>
  gradedRaceCodes.has(gradeCode?.trim() ?? "") ? "graded" : "non_graded";

const getConditionBand = (row: PredictionQueryRow): string => {
  if (row.source === "jra") {
    return getGradeBand(row.grade_code);
  }
  if (row.keibajo_code === "83") {
    return "ban_ei";
  }
  return isNarClassRace(row) ? "nar_class" : "nar_non_class";
};

const arrayIncludes = (values: string[] | undefined, value: string): boolean =>
  values === undefined || values.includes(value);

const dateInRange = (matcher: ConditionMatcher | undefined, date: string): boolean => {
  if (matcher?.fromDate !== undefined && date < matcher.fromDate.replaceAll("-", "")) {
    return false;
  }
  if (matcher?.toDate !== undefined && date > matcher.toDate.replaceAll("-", "")) {
    return false;
  }
  return true;
};

const matchesRowCondition = (
  matcher: ConditionMatcher | undefined,
  row: PredictionQueryRow,
): boolean => {
  if (matcher === undefined) {
    return true;
  }
  return (
    arrayIncludes(
      matcher.sources,
      row.source === "nar" && row.keibajo_code === "83" ? "ban-ei" : row.source,
    ) &&
    arrayIncludes(matcher.distanceBands, getDistanceBand(row.kyori)) &&
    arrayIncludes(matcher.gradeBands, getGradeBand(row.grade_code)) &&
    arrayIncludes(matcher.conditionBands, getConditionBand(row)) &&
    arrayIncludes(matcher.keibajoCodes, row.keibajo_code) &&
    arrayIncludes(matcher.raceBangos, row.race_bango) &&
    dateInRange(matcher, row.race_date)
  );
};

const matchesPredictionCondition = (
  matcher: ConditionMatcher | undefined,
  row: Prediction,
): boolean => {
  if (matcher === undefined) {
    return true;
  }
  return (
    arrayIncludes(matcher.sources, row.source) &&
    arrayIncludes(matcher.distanceBands, row.distanceBand) &&
    arrayIncludes(matcher.gradeBands, row.gradeBand) &&
    arrayIncludes(matcher.conditionBands, row.conditionBand) &&
    arrayIncludes(matcher.keibajoCodes, row.keibajoCode) &&
    arrayIncludes(matcher.raceBangos, row.raceBango) &&
    dateInRange(matcher, `${row.kaisaiNen}${row.kaisaiTsukihi}`)
  );
};

const applyWeightRules = (
  weights: Partial<Record<ScoreComponentKey, number>>,
  row: PredictionQueryRow,
  rules: ScoreWeightRule[] | undefined,
): Partial<Record<ScoreComponentKey, number>> => {
  if (rules === undefined) {
    return weights;
  }
  for (const rule of rules) {
    if (!matchesRowCondition(rule.when, row)) {
      continue;
    }
    for (const key of scoreComponentKeys) {
      const value = rule.multiply?.[key];
      const current = weights[key];
      if (value !== undefined && current !== undefined) {
        weights[key] = current * value;
      }
    }
    for (const key of scoreComponentKeys) {
      const value = rule.set?.[key];
      if (value !== undefined) {
        weights[key] = value;
      }
    }
  }
  return weights;
};

const getScoreWeights = (row: PredictionQueryRow, options: Options) => {
  const distanceBand = getDistanceBand(row.kyori);
  const gradeBand = getGradeBand(row.grade_code);
  const appliesNarRefinedMarket =
    row.source === "nar" && distanceBand !== "sprint" && gradeBand !== "graded";
  const appliesRequestedMultipliers =
    row.source !== "nar" || (distanceBand !== "sprint" && gradeBand !== "graded");
  const marketPopularityMultiplier = 1.1;
  const marketOddsMultiplier = row.source === "jra" ? 1.1 : 1;
  const historyMultiplier = row.source === "jra" ? 0.9 : 1;
  const recentMultiplier = row.source === "jra" ? 0.9 : 1;
  const jraHistoryMultiplier = row.source === "jra" ? 0.95 : 1;
  const jraRecentMultiplier = row.source === "jra" ? 0.95 : 1;
  const jraPopularityMultiplier = row.source === "jra" ? 1.05 : 1;
  const jraOddsMultiplier = row.source === "jra" ? 1.05 : 1;
  const narHistoryMultiplier = appliesNarRefinedMarket ? 0.95 : 1;
  const narRecentMultiplier = appliesNarRefinedMarket ? 0.95 : 1;
  const narPopularityMultiplier = appliesNarRefinedMarket ? 1.2 : 1;
  const narOddsMultiplier = appliesNarRefinedMarket ? 1.1 : 1;
  const baseWeights = options.tuningConfig?.scoreWeights?.base;
  const weights = {
    avgFinish:
      (baseWeights?.avgFinish ?? 0.18) *
      historyMultiplier *
      jraHistoryMultiplier *
      narHistoryMultiplier *
      (appliesRequestedMultipliers ? options.historyWeightMultiplier : 1),
    odds:
      (baseWeights?.odds ?? 0.12) *
      marketOddsMultiplier *
      jraOddsMultiplier *
      narOddsMultiplier *
      (appliesRequestedMultipliers ? options.oddsWeightMultiplier : 1),
    popularity:
      (baseWeights?.popularity ?? 0.6) *
      marketPopularityMultiplier *
      jraPopularityMultiplier *
      narPopularityMultiplier *
      (appliesRequestedMultipliers ? options.popularityWeightMultiplier : 1),
    recentFinish:
      (baseWeights?.recentFinish ?? 0.1) *
      recentMultiplier *
      jraRecentMultiplier *
      narRecentMultiplier *
      (appliesRequestedMultipliers ? options.recentWeightMultiplier : 1),
    sameDayJockey:
      row.source === "nar"
        ? (baseWeights?.sameDayJockey ?? options.sameDayJockeyWeight)
        : (baseWeights?.sameDayJockey ?? 0),
  };
  const graded = gradedRaceCodes.has(row.grade_code?.trim() ?? "");
  const narClass = row.source === "nar" && isNarClassRace(row);

  if (graded) {
    weights.sameDayJockey *= row.source === "nar" ? 0.7 : 0;
  } else if (narClass) {
    weights.sameDayJockey *= 0.8;
  }

  if (distanceBand === "sprint") {
    weights.sameDayJockey *= 1.2;
  } else if (distanceBand === "long") {
    weights.sameDayJockey *= 0.75;
  }

  return applyWeightRules(weights, row, options.tuningConfig?.scoreWeights?.rules);
};

const scorePrediction = (row: PredictionQueryRow, options: Options): number => {
  const weights = getScoreWeights(row, options);
  const values = [
    { value: toNumber(row.avg_finish), weight: weights.avgFinish },
    { value: toNumber(row.recent_finish), weight: weights.recentFinish },
    { value: toNumber(row.popularity_score), weight: weights.popularity },
    { value: toNumber(row.odds_score), weight: weights.odds },
    {
      value: row.source === "nar" ? toNumber(row.same_day_jockey_win_score) : null,
      weight: weights.sameDayJockey,
    },
  ].filter((item): item is { value: number; weight: number } => item.value !== null);
  if (values.length === 0) {
    return 0.5;
  }
  const weightTotal = values.reduce((total, item) => total + item.weight, 0);
  return values.reduce((total, item) => total + item.value * item.weight, 0) / weightTotal;
};

const weightedComponentScore = (
  values: Array<{ value: number | null; weight: number }>,
  fallback = 0.5,
): number => {
  const available = values.filter(
    (item): item is { value: number; weight: number } => item.value !== null && item.weight > 0,
  );
  if (available.length === 0) {
    return fallback;
  }
  const weightTotal = available.reduce((total, item) => total + item.weight, 0);
  return available.reduce((total, item) => total + item.value * item.weight, 0) / weightTotal;
};

const getLstmLikeScore = (row: PredictionQueryRow, options: Options): number => {
  const configured = options.tuningConfig?.componentModels?.lstmLike;
  return weightedComponentScore(
    [
      {
        value: toNumber(row.recent_finish),
        weight: configured?.weights?.recentFinish ?? (row.source === "jra" ? 0.52 : 0.46),
      },
      {
        value: toNumber(row.avg_finish),
        weight: configured?.weights?.avgFinish ?? (row.source === "jra" ? 0.2 : 0.24),
      },
      {
        value: toNumber(row.popularity_score),
        weight: configured?.weights?.popularity ?? (row.source === "jra" ? 0.18 : 0.2),
      },
      { value: toNumber(row.odds_score), weight: configured?.weights?.odds ?? 0.1 },
      {
        value: toNumber(row.same_day_jockey_win_score),
        weight: configured?.weights?.sameDayJockey ?? 0,
      },
    ],
    configured?.fallback ?? 0.5,
  );
};

const getTransformerLikeScore = (row: PredictionQueryRow, options: Options): number => {
  const distanceBand = getDistanceBand(row.kyori);
  const gradeBand = getGradeBand(row.grade_code);
  const marketWeight = row.source === "jra" || distanceBand !== "sprint" ? 1.1 : 0.95;
  const conditionMarketWeight = gradeBand === "graded" ? 0.9 : 1;
  const configured = options.tuningConfig?.componentModels?.transformerLike;
  return weightedComponentScore(
    [
      {
        value: toNumber(row.popularity_score),
        weight: configured?.weights?.popularity ?? 0.4 * marketWeight * conditionMarketWeight,
      },
      { value: toNumber(row.odds_score), weight: configured?.weights?.odds ?? 0.32 * marketWeight },
      { value: toNumber(row.avg_finish), weight: configured?.weights?.avgFinish ?? 0.11 },
      { value: toNumber(row.recent_finish), weight: configured?.weights?.recentFinish ?? 0.1 },
      {
        value: toNumber(row.same_day_jockey_win_score),
        weight: configured?.weights?.sameDayJockey ?? (row.source === "nar" ? 0.07 : 0),
      },
    ],
    configured?.fallback ?? 0.5,
  );
};

const getEnsembleWeights = (
  source: string,
  distanceBand: string,
  gradeBand: string,
  options: Options,
) => {
  const configured = options.tuningConfig?.ensemble?.weights;
  if (configured !== undefined) {
    return {
      lightgbm: configured.lightgbm ?? 0.45,
      lstm: configured.lstm ?? 0.16,
      transformer: configured.transformer ?? 0.39,
    };
  }
  if (source === "ban-ei") {
    return { lightgbm: 0.42, lstm: 0.08, transformer: 0.5 };
  }
  if (source === "jra") {
    return gradeBand === "graded"
      ? { lightgbm: 0.5, lstm: 0.18, transformer: 0.32 }
      : { lightgbm: 0.45, lstm: 0.16, transformer: 0.39 };
  }
  if (distanceBand === "sprint" || gradeBand === "graded") {
    return { lightgbm: 0.55, lstm: 0.16, transformer: 0.29 };
  }
  return { lightgbm: 0.38, lstm: 0.14, transformer: 0.48 };
};

const getScoreRanks = (
  rows: Prediction[],
  key: "lightgbmScore" | "lstmScore" | "transformerScore",
) =>
  new Map(
    rows
      .toSorted((left, right) => left[key] - right[key] || left.horseNumber - right.horseNumber)
      .map((row, index) => [row.horseNumber, index + 1] as const),
  );

const getEffectiveEnsembleMode = (
  requestedMode: Options["ensembleMode"],
  rows: Prediction[],
  options: Options,
): "mixed" | "off" | "vote" | "weighted" => {
  const first = rows[0];
  const matchingRule = options.tuningConfig?.ensemble?.rules?.find((rule) =>
    first === undefined ? false : matchesPredictionCondition(rule.when, first),
  );
  if (matchingRule?.mode !== undefined) {
    return matchingRule.mode;
  }
  const configuredDefault = options.tuningConfig?.ensemble?.defaultMode;
  if (configuredDefault !== undefined && configuredDefault !== "auto") {
    return configuredDefault;
  }
  if (requestedMode !== "auto") {
    return requestedMode;
  }
  if (first?.source === "jra" && first.distanceBand === "sprint" && first.gradeBand === "graded") {
    return "mixed";
  }
  return "off";
};

const applyEnsembleRanking = (rows: Prediction[], options: Options): Prediction[] => {
  if (rows.length === 0) {
    return rows;
  }
  const ensembleMode = getEffectiveEnsembleMode(options.ensembleMode, rows, options);
  if (ensembleMode === "off") {
    return rows
      .toSorted(
        (left, right) =>
          left.lightgbmScore - right.lightgbmScore || left.horseNumber - right.horseNumber,
      )
      .map((row, index) =>
        Object.assign(row, { predictedRank: index + 1, score: row.lightgbmScore }),
      );
  }
  const lightgbmRanks = getScoreRanks(rows, "lightgbmScore");
  const lstmRanks = getScoreRanks(rows, "lstmScore");
  const transformerRanks = getScoreRanks(rows, "transformerScore");
  const denominator = Math.max(1, rows.length - 1);
  return rows
    .map((row) => {
      const matchingRule = options.tuningConfig?.ensemble?.rules?.find((rule) =>
        matchesPredictionCondition(rule.when, row),
      );
      const defaultWeights = getEnsembleWeights(
        row.source,
        row.distanceBand,
        row.gradeBand,
        options,
      );
      const weights =
        matchingRule?.weights === undefined
          ? defaultWeights
          : {
              lightgbm: matchingRule.weights.lightgbm ?? defaultWeights.lightgbm,
              lstm: matchingRule.weights.lstm ?? defaultWeights.lstm,
              transformer: matchingRule.weights.transformer ?? defaultWeights.transformer,
            };
      const weightedScore =
        row.lightgbmScore * weights.lightgbm +
        row.lstmScore * weights.lstm +
        row.transformerScore * weights.transformer;
      const voteRank =
        ((lightgbmRanks.get(row.horseNumber) ?? rows.length) * weights.lightgbm +
          (lstmRanks.get(row.horseNumber) ?? rows.length) * weights.lstm +
          (transformerRanks.get(row.horseNumber) ?? rows.length) * weights.transformer) /
        (weights.lightgbm + weights.lstm + weights.transformer);
      const voteScore = (voteRank - 1) / denominator;
      const score =
        ensembleMode === "weighted"
          ? weightedScore
          : ensembleMode === "vote"
            ? voteScore
            : weightedScore *
                (matchingRule?.mixedWeightedShare ??
                  options.tuningConfig?.ensemble?.mixedWeightedShare ??
                  0.72) +
              voteScore *
                (1 -
                  (matchingRule?.mixedWeightedShare ??
                    options.tuningConfig?.ensemble?.mixedWeightedShare ??
                    0.72));
      return Object.assign(row, { score });
    })
    .toSorted((left, right) => left.score - right.score || left.horseNumber - right.horseNumber)
    .map((row, index) => Object.assign(row, { predictedRank: index + 1 }));
};

const raceKey = (row: RaceKey): string =>
  [
    row.source,
    row.race_date,
    row.kaisai_nen,
    row.kaisai_tsukihi,
    row.keibajo_code,
    row.race_bango,
  ].join(":");

const roundPercent = (value: number): number => Math.round(value * 10000) / 100;

const calculateEvaluationSummary = (evaluated: Prediction[][]): EvaluationSummary => {
  if (evaluated.length === 0) {
    return {
      place1Accuracy: 0,
      place2Accuracy: 0,
      place3Accuracy: 0,
      top3BoxAccuracy: 0,
      top3ExactOrderAccuracy: 0,
      top3PlaceRelation: 0,
      top3WinnerCapture: 0,
      top5WinnerCapture: 0,
    };
  }

  let place1Hits = 0;
  let place2Hits = 0;
  let place3Hits = 0;
  let top3WinnerHits = 0;
  let top5WinnerHits = 0;
  let top3ExactOrderHits = 0;
  let top3BoxHits = 0;
  let top3PlaceRelationTotal = 0;

  for (const rows of evaluated) {
    if (rows[0]?.actual === 1) {
      place1Hits += 1;
    }
    if (rows[1]?.actual === 2) {
      place2Hits += 1;
    }
    if (rows[2]?.actual === 3) {
      place3Hits += 1;
    }
    if (rows.slice(0, 3).some((row) => row.actual === 1)) {
      top3WinnerHits += 1;
    }
    if (rows.slice(0, 5).some((row) => row.actual === 1)) {
      top5WinnerHits += 1;
    }

    const predictedTop3 = rows.slice(0, 3);
    const predictedTop3Actuals = predictedTop3.map((row) => row.actual);
    if (
      predictedTop3Actuals[0] === 1 &&
      predictedTop3Actuals[1] === 2 &&
      predictedTop3Actuals[2] === 3
    ) {
      top3ExactOrderHits += 1;
    }

    const actualTop3Set = new Set([1, 2, 3]);
    const matchedTop3Count = predictedTop3.filter((row) => actualTop3Set.has(row.actual)).length;
    if (matchedTop3Count === 3) {
      top3BoxHits += 1;
    }
    top3PlaceRelationTotal += matchedTop3Count / 3;
  }

  return {
    place1Accuracy: roundPercent(place1Hits / evaluated.length),
    place2Accuracy: roundPercent(place2Hits / evaluated.length),
    place3Accuracy: roundPercent(place3Hits / evaluated.length),
    top3BoxAccuracy: roundPercent(top3BoxHits / evaluated.length),
    top3ExactOrderAccuracy: roundPercent(top3ExactOrderHits / evaluated.length),
    top3PlaceRelation: roundPercent(top3PlaceRelationTotal / evaluated.length),
    top3WinnerCapture: roundPercent(top3WinnerHits / evaluated.length),
    top5WinnerCapture: roundPercent(top5WinnerHits / evaluated.length),
  };
};

const calculateBreakdowns = (evaluated: Prediction[][]) => {
  const groups = new Map<string, Prediction[][]>();
  for (const rows of evaluated) {
    const first = rows[0];
    if (!first) {
      continue;
    }
    const keys = [
      `source:${first.source}`,
      `grade:${first.gradeBand}`,
      `distance:${first.distanceBand}`,
      `condition:${first.conditionBand}`,
      `condition_distance:${first.conditionBand}:${first.distanceBand}`,
    ];
    for (const key of keys) {
      groups.set(key, [...(groups.get(key) ?? []), rows]);
    }
  }
  return [...groups.entries()]
    .map(([key, rows]) =>
      Object.assign({ key, raceCount: rows.length }, calculateEvaluationSummary(rows)),
    )
    .filter((row) => row.raceCount >= 200)
    .toSorted(
      (left, right) => left.key.localeCompare(right.key) || right.raceCount - left.raceCount,
    );
};

const isExactTop3 = (rows: Prediction[]): boolean =>
  rows[0]?.actual === 1 && rows[1]?.actual === 2 && rows[2]?.actual === 3;

const getTop3Actuals = (rows: Prediction[]): number[] => rows.slice(0, 3).map((row) => row.actual);

const getTop3PlaceMatchCount = (rows: Prediction[]): number => {
  const actualTop3Set = new Set([1, 2, 3]);
  return rows.slice(0, 3).filter((row) => actualTop3Set.has(row.actual)).length;
};

const getTop3PlaceMatchRate = (rows: Prediction[]): number => getTop3PlaceMatchCount(rows) / 3;

const getTop3HorseNumbers = (rows: Prediction[]): number[] =>
  rows.slice(0, 3).map((row) => row.horseNumber);

const formatRacePath = (row: {
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}): string =>
  `/races/${row.kaisaiNen}/${row.kaisaiTsukihi.slice(0, 2)}/${row.kaisaiTsukihi.slice(2, 4)}/${row.keibajoCode}/${row.raceBango}`;

const countChangedGroups = (
  rows: Array<{ conditionBand: string; distanceBand: string; gradeBand: string; outcome: string }>,
) => {
  const groups = new Map<string, { improved: number; worsened: number }>();
  for (const row of rows) {
    const keys = [
      `grade:${row.gradeBand}`,
      `distance:${row.distanceBand}`,
      `condition:${row.conditionBand}`,
      `condition_distance:${row.conditionBand}:${row.distanceBand}`,
    ];
    for (const key of keys) {
      const current = groups.get(key) ?? { improved: 0, worsened: 0 };
      if (row.outcome === "improved") {
        current.improved += 1;
      } else {
        current.worsened += 1;
      }
      groups.set(key, current);
    }
  }
  return [...groups.entries()]
    .map(([key, value]) => Object.assign({ key }, value, { net: value.improved - value.worsened }))
    .toSorted((left, right) => right.net - left.net || right.improved - left.improved);
};

const calculateChangedRaces = (
  baseline: Prediction[][],
  candidate: Prediction[][],
  limit: number,
) => {
  const baselineByKey = new Map(
    baseline.flatMap((rows) => (rows[0] ? [[rows[0].raceKey, rows] as const] : [])),
  );
  const changed = candidate.flatMap((candidateRows) => {
    const first = candidateRows[0];
    if (!first) {
      return [];
    }
    const baselineRows = baselineByKey.get(first.raceKey);
    if (!baselineRows) {
      return [];
    }
    const baselineExact = isExactTop3(baselineRows);
    const candidateExact = isExactTop3(candidateRows);
    const baselineTop3PlaceMatchRate = getTop3PlaceMatchRate(baselineRows);
    const candidateTop3PlaceMatchRate = getTop3PlaceMatchRate(candidateRows);
    const top3PlaceMatchRateDelta = candidateTop3PlaceMatchRate - baselineTop3PlaceMatchRate;
    if (baselineExact === candidateExact && top3PlaceMatchRateDelta === 0) {
      return [];
    }
    return [
      {
        baselineTop3Actuals: getTop3Actuals(baselineRows),
        baselineTop3HorseNumbers: getTop3HorseNumbers(baselineRows),
        baselineTop3PlaceMatchRate: roundPercent(baselineTop3PlaceMatchRate),
        candidateTop3Actuals: getTop3Actuals(candidateRows),
        candidateTop3HorseNumbers: getTop3HorseNumbers(candidateRows),
        candidateTop3PlaceMatchRate: roundPercent(candidateTop3PlaceMatchRate),
        conditionBand: first.conditionBand,
        distanceBand: first.distanceBand,
        gradeBand: first.gradeBand,
        isNewlyExactTop3: candidateExact && !baselineExact,
        kaisaiNen: first.kaisaiNen,
        kaisaiTsukihi: first.kaisaiTsukihi,
        keibajoCode: first.keibajoCode,
        outcome:
          candidateExact && !baselineExact
            ? "improved"
            : !candidateExact && baselineExact
              ? "worsened"
              : top3PlaceMatchRateDelta > 0
                ? "improved"
                : "worsened",
        raceBango: first.raceBango,
        raceKey: first.raceKey,
        racePath: formatRacePath(first),
        raceUrl: `http://localhost:3000${formatRacePath(first)}`,
        source: first.source,
        top3PlaceMatchRateDelta: roundPercent(top3PlaceMatchRateDelta),
      },
    ];
  });
  const improved = changed.filter((row) => row.outcome === "improved");
  const worsened = changed.filter((row) => row.outcome === "worsened");
  const newlyExactTop3 = changed.filter((row) => row.isNewlyExactTop3);
  const top3PlaceMatchRateDecreased = changed
    .filter((row) => row.top3PlaceMatchRateDelta < 0)
    .toSorted((left, right) => left.top3PlaceMatchRateDelta - right.top3PlaceMatchRateDelta);
  return {
    groups: countChangedGroups(changed),
    improvedCount: improved.length,
    improvedSamples: improved.slice(0, limit),
    newlyExactTop3Count: newlyExactTop3.length,
    newlyExactTop3Samples: newlyExactTop3.slice(0, limit),
    top3PlaceMatchRateDecreasedCount: top3PlaceMatchRateDecreased.length,
    top3PlaceMatchRateDecreasedSamples: top3PlaceMatchRateDecreased.slice(0, limit),
    worsenedCount: worsened.length,
    worsenedSamples: worsened.slice(0, limit),
  };
};

const loadPredictions = async (pool: Pool, options: Options): Promise<Prediction[][]> => {
  if (options.category === "ban-ei") {
    const result = await pool.query<BaneiPredictionQueryRow>(
      `
        with target as (
          select
            ra.kaisai_nen || ra.kaisai_tsukihi || ':' || ra.keibajo_code || ':' || ra.race_bango race_key,
            nullif(se.umaban, '')::integer umaban,
            nullif(se.kakutei_chakujun, '00')::integer finish_position,
            nullif(se.tansho_ninkijun, '00')::integer tansho_ninkijun,
            nullif(se.tansho_odds, '0000')::numeric / 10 tansho_odds,
            coalesce(nullif(btrim(se.kishumei_ryakusho, ' 　'), ''), '-') jockey_name,
            count(*) over (
              partition by ra.kaisai_nen, ra.kaisai_tsukihi, ra.keibajo_code, ra.race_bango
            ) runner_count
          from nvd_se se
          join nvd_ra ra
            on ra.kaisai_nen = se.kaisai_nen
            and ra.kaisai_tsukihi = se.kaisai_tsukihi
            and ra.keibajo_code = se.keibajo_code
            and ra.race_bango = se.race_bango
          where ra.keibajo_code = '83'
            and ra.kaisai_nen || ra.kaisai_tsukihi between $1 and $2
            and nullif(se.kakutei_chakujun, '00') is not null
        ),
        winner_rows as (
          select
            race_key,
            split_part(race_key, ':', 1) kaisai_key,
            split_part(race_key, ':', 2) keibajo_code,
            split_part(race_key, ':', 3) race_bango,
            jockey_name
          from target
          where finish_position = 1
        ),
        same_day_jockey_wins as (
          select
            target.race_key,
            target.umaban,
            count(winner_rows.*) win_count
          from target
          left join winner_rows
            on winner_rows.kaisai_key = split_part(target.race_key, ':', 1)
            and winner_rows.keibajo_code = split_part(target.race_key, ':', 2)
            and winner_rows.race_bango::integer < split_part(target.race_key, ':', 3)::integer
            and winner_rows.jockey_name = target.jockey_name
          group by target.race_key, target.umaban
        )
        select
          target.race_key,
          target.umaban "horseNumber",
          target.finish_position,
          case
            when target.runner_count > 1 and target.tansho_ninkijun is not null
            then greatest(0, least(1, (target.tansho_ninkijun - 1)::numeric / nullif(target.runner_count - 1, 0)))::text
            else null
          end popularity_score,
          case
            when target.tansho_odds is not null and target.tansho_odds > 0
            then greatest(0, least(1, ln(greatest(target.tansho_odds, 1)) / ln(300)))::text
            else null
          end odds_score,
          case
            when same_day_jockey_wins.win_count > 0
            then greatest(0, least(1, 0.28 - least(3, same_day_jockey_wins.win_count) * 0.07))::text
            else null
          end same_day_jockey_win_score
        from target
        left join same_day_jockey_wins
          on same_day_jockey_wins.race_key = target.race_key
          and same_day_jockey_wins.umaban = target.umaban
        where target.runner_count >= 5
        order by target.race_key, target.umaban
      `,
      [options.fromDate, options.toDate],
    );
    const grouped = new Map<string, BaneiPredictionQueryRow[]>();
    for (const row of result.rows) {
      grouped.set(row.race_key, [...(grouped.get(row.race_key) ?? []), row]);
    }
    return [...grouped.values()].map((rows) =>
      applyEnsembleRanking(
        rows.map((row) => {
          const values = [
            {
              value: toNumber(row.popularity_score),
              weight: 0.72 * options.popularityWeightMultiplier,
            },
            { value: toNumber(row.odds_score), weight: 0.28 * options.oddsWeightMultiplier },
            { value: null, weight: 0 },
          ].filter((item): item is { value: number; weight: number } => item.value !== null);
          const weightTotal = values.reduce((total, item) => total + item.weight, 0);
          const score =
            weightTotal > 0
              ? values.reduce((total, item) => total + item.value * item.weight, 0) / weightTotal
              : 0.5;
          const lstmScore = weightedComponentScore([
            { value: toNumber(row.popularity_score), weight: 0.62 },
            { value: toNumber(row.odds_score), weight: 0.38 },
          ]);
          const transformerScore = weightedComponentScore([
            { value: toNumber(row.popularity_score), weight: 0.52 },
            { value: toNumber(row.odds_score), weight: 0.48 },
          ]);
          return {
            actual: row.finish_position,
            conditionBand: "ban_ei",
            distanceBand: "unknown",
            gradeBand: "non_graded",
            horseNumber: row.horseNumber,
            kaisaiNen: row.race_key.slice(0, 4),
            kaisaiTsukihi: row.race_key.slice(4, 8),
            keibajoCode: row.race_key.split(":")[1] ?? "",
            lightgbmScore: score,
            lstmScore,
            predictedRank: 0,
            raceBango: row.race_key.split(":")[2] ?? "",
            raceKey: row.race_key,
            raceUrl: `http://localhost:3000${formatRacePath({
              kaisaiNen: row.race_key.slice(0, 4),
              kaisaiTsukihi: row.race_key.slice(4, 8),
              keibajoCode: row.race_key.split(":")[1] ?? "",
              raceBango: row.race_key.split(":")[2] ?? "",
            })}`,
            score,
            source: "ban-ei",
            transformerScore,
          };
        }),
        options,
      ),
    );
  }

  const result = await pool.query<PredictionQueryRow>(
    `
      with target as (
        select
          source,
          race_date,
          kaisai_nen,
          kaisai_tsukihi,
          keibajo_code,
          race_bango,
          grade_code,
          nullif(btrim(kyori::text), '')::integer kyori,
          kyoso_joken_code,
          ketto_toroku_bango,
          umaban,
          finish_position,
          finish_norm,
          tansho_ninkijun,
          tansho_odds,
          coalesce(nullif(btrim(kishumei_ryakusho, ' 　'), ''), '-') jockey_name,
          count(*) over (
            partition by source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
          ) runner_count
        from race_entry_corner_features
        where race_date between $1 and $2
          ${categoryFilter(options.category)}
          and finish_position is not null
      ),
      history as (
        select
          target.source,
          target.race_date,
          target.kaisai_nen,
          target.kaisai_tsukihi,
          target.keibajo_code,
          target.race_bango,
          target.umaban,
          history.finish_norm,
          row_number() over (
            partition by
              target.source,
              target.race_date,
              target.kaisai_nen,
              target.kaisai_tsukihi,
              target.keibajo_code,
              target.race_bango,
              target.umaban
            order by history.race_date desc
          ) recent_rank
        from target
        join race_entry_corner_features history
          on history.source = target.source
          and history.ketto_toroku_bango = target.ketto_toroku_bango
          and history.race_date < target.race_date
          and history.race_date >= (target.race_date::integer - 100000)::text
          and history.finish_norm is not null
      ),
      history_summary as (
        select
          source,
          race_date,
          kaisai_nen,
          kaisai_tsukihi,
          keibajo_code,
          race_bango,
          umaban,
          avg(finish_norm)::text avg_finish,
          avg(finish_norm) filter (where recent_rank <= 5)::text recent_finish
        from history
        group by source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban
      ),
      winner_rows as (
        select
          source,
          race_date,
          keibajo_code,
          race_bango,
          jockey_name
        from target
        where finish_position = 1
      ),
      same_day_jockey_wins as (
        select
          target.source,
          target.race_date,
          target.kaisai_nen,
          target.kaisai_tsukihi,
          target.keibajo_code,
          target.race_bango,
          target.umaban,
          count(winner_rows.*) win_count
        from target
        left join winner_rows
          on winner_rows.source = target.source
          and winner_rows.race_date = target.race_date
          and winner_rows.keibajo_code = target.keibajo_code
          and winner_rows.race_bango::integer < target.race_bango::integer
          and winner_rows.jockey_name = target.jockey_name
        group by
          target.source,
          target.race_date,
          target.kaisai_nen,
          target.kaisai_tsukihi,
          target.keibajo_code,
          target.race_bango,
          target.umaban
      )
      select
        target.source,
        target.race_date,
        target.kaisai_nen,
        target.kaisai_tsukihi,
        target.keibajo_code,
        target.race_bango,
        target.grade_code,
        target.kyori,
        target.kyoso_joken_code,
        target.umaban "horseNumber",
        target.finish_position,
        history_summary.avg_finish,
        history_summary.recent_finish,
        case
          when target.runner_count > 1 and target.tansho_ninkijun is not null
          then greatest(0, least(1, (target.tansho_ninkijun - 1)::numeric / nullif(target.runner_count - 1, 0)))::text
          else null
        end popularity_score,
        case
          when target.tansho_odds is not null and target.tansho_odds > 0
          then greatest(0, least(1, ln(greatest(target.tansho_odds, 1)) / ln(300)))::text
          else null
        end odds_score,
        case
          when same_day_jockey_wins.win_count > 0
          then greatest(0, least(1, 0.28 - least(3, same_day_jockey_wins.win_count) * 0.07))::text
          else null
        end same_day_jockey_win_score
      from target
      left join history_summary
        on history_summary.source = target.source
        and history_summary.race_date = target.race_date
        and history_summary.kaisai_nen = target.kaisai_nen
        and history_summary.kaisai_tsukihi = target.kaisai_tsukihi
        and history_summary.keibajo_code = target.keibajo_code
        and history_summary.race_bango = target.race_bango
        and history_summary.umaban = target.umaban
      left join same_day_jockey_wins
        on same_day_jockey_wins.source = target.source
        and same_day_jockey_wins.race_date = target.race_date
        and same_day_jockey_wins.kaisai_nen = target.kaisai_nen
        and same_day_jockey_wins.kaisai_tsukihi = target.kaisai_tsukihi
        and same_day_jockey_wins.keibajo_code = target.keibajo_code
        and same_day_jockey_wins.race_bango = target.race_bango
        and same_day_jockey_wins.umaban = target.umaban
      where target.runner_count >= 5
      order by target.race_date desc, target.source, target.keibajo_code, target.race_bango, target.umaban
    `,
    [options.fromDate, options.toDate],
  );
  const grouped = new Map<string, PredictionQueryRow[]>();
  for (const row of result.rows) {
    const key = raceKey(row);
    grouped.set(key, [...(grouped.get(key) ?? []), row]);
  }
  return [...grouped.values()].map((rows) =>
    applyEnsembleRanking(
      rows.map((row) => {
        const score = scorePrediction(row, options);
        return {
          actual: row.finish_position,
          conditionBand: getConditionBand(row),
          distanceBand: getDistanceBand(row.kyori),
          gradeBand: getGradeBand(row.grade_code),
          horseNumber: row.horseNumber,
          kaisaiNen: row.kaisai_nen,
          kaisaiTsukihi: row.kaisai_tsukihi,
          keibajoCode: row.keibajo_code,
          lightgbmScore: score,
          lstmScore: getLstmLikeScore(row, options),
          predictedRank: 0,
          raceBango: row.race_bango,
          raceKey: raceKey(row),
          raceUrl: `http://localhost:3000${formatRacePath({
            kaisaiNen: row.kaisai_nen,
            kaisaiTsukihi: row.kaisai_tsukihi,
            keibajoCode: row.keibajo_code,
            raceBango: row.race_bango,
          })}`,
          score,
          source: row.source === "nar" && row.keibajo_code === "83" ? "ban-ei" : row.source,
          transformerScore: getTransformerLikeScore(row, options),
        };
      }),
      options,
    ),
  );
};

const main = async () => {
  const options = parseArgs(process.argv.slice(2));
  await loadEnv();
  options.tuningConfig = await readTuningConfig(options.tuningConfigPath);
  const pool = new Pool({ connectionString: getConnectionString(options.target) });
  try {
    const evaluated = (await loadPredictions(pool, options)).filter((rows) => rows.length > 0);
    const evaluationSummary = calculateEvaluationSummary(evaluated);
    const pairScores = evaluated.map((rows) => {
      let correct = 0;
      let total = 0;
      for (let left = 0; left < rows.length; left += 1) {
        for (let right = left + 1; right < rows.length; right += 1) {
          const leftRow = rows[left];
          const rightRow = rows[right];
          if (leftRow === undefined || rightRow === undefined) {
            continue;
          }
          total += 1;
          const predicted = leftRow.predictedRank < rightRow.predictedRank;
          const actual = leftRow.actual < rightRow.actual;
          if (predicted === actual) {
            correct += 1;
          }
        }
      }
      return total > 0 ? correct / total : 0;
    });
    const pairScore =
      pairScores.length > 0
        ? pairScores.reduce((total, score) => total + score, 0) / pairScores.length
        : 0;
    const output = {
      breakdowns: options.breakdown ? calculateBreakdowns(evaluated) : undefined,
      category: options.category,
      changedRaces: options.changedRaces
        ? calculateChangedRaces(
            await loadPredictions(pool, {
              ...options,
              historyWeightMultiplier: 1,
              ensembleMode: "off",
              oddsWeightMultiplier: 1,
              popularityWeightMultiplier: 1,
              recentWeightMultiplier: 1,
              sameDayJockeyWeight: 0.02,
              tuningConfig: undefined,
              tuningConfigPath: null,
            }),
            evaluated,
            options.changedRaceLimit,
          )
        : undefined,
      fromDate: options.fromDate,
      pairScore: Math.round(pairScore * 10000) / 100,
      place1Accuracy: evaluationSummary.place1Accuracy,
      place2Accuracy: evaluationSummary.place2Accuracy,
      place3Accuracy: evaluationSummary.place3Accuracy,
      raceCount: evaluated.length,
      target: options.target,
      toDate: options.toDate,
      top1Accuracy: evaluationSummary.place1Accuracy,
      top3BoxAccuracy: evaluationSummary.top3BoxAccuracy,
      top3ExactOrderAccuracy: evaluationSummary.top3ExactOrderAccuracy,
      top3PlaceRelation: evaluationSummary.top3PlaceRelation,
      top3WinnerCapture: evaluationSummary.top3WinnerCapture,
      top5WinnerCapture: evaluationSummary.top5WinnerCapture,
    };
    console.log(JSON.stringify(output, null, 2));
  } finally {
    await pool.end();
  }
};

main().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
