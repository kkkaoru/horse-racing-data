import type { RaceSource } from "./codes";
import { cleanText } from "./format";
import { isSameJockeyName } from "./jockey-name";
import type {
  FinishPositionModelPredictionFeature,
  FinishPositionSimilarityFeature,
  FinishPredictionDetail,
  FinishPredictionRow,
  HorseRaceResult,
  Runner,
  SameDayVenueJockeyWinFeature,
} from "./race-types";
import { isBanEiKeibajoCode } from "./runner-format";

type FinishPredictionCategory = "ban-ei" | "jra" | "nar";

export interface FinishPredictionMarketOverride {
  odds: number | null;
  popularity: number | null;
}

export interface FinishPredictionBuildInputs {
  currentDistance: string | null | undefined;
  currentGradeCode?: string | null;
  currentKeibajoCode: string | null | undefined;
  currentKyosoJokenCode?: string | null;
  currentKyosoJokenMeisho?: string | null;
  currentRaceDate: string;
  currentSource: RaceSource;
  currentTrackCode?: string | null;
  modelPredictionFeatures?: FinishPositionModelPredictionFeature[];
  results: HorseRaceResult[];
  runners: Runner[];
  sameDayVenueJockeyWins?: SameDayVenueJockeyWinFeature[];
  similarityFeatures?: FinishPositionSimilarityFeature[];
}

interface BuildFinishPredictionRowsParams extends FinishPredictionBuildInputs {
  marketOverrides?: ReadonlyMap<string, FinishPredictionMarketOverride>;
}

type FinishPredictionConfig = {
  decayDays: number;
  distanceBandMeters: number;
  distanceScale: number;
  horseWeight: number;
  jockeyWeight: number;
  minDistanceWeight: number;
  modelWeight: number;
  oddsWeight: number;
  popularityWeight: number;
  recentWeight: number;
  sameDayJockeyWeight: number;
  similarityWeight: number;
  trainerWeight: number;
};

type ScoreCandidate = {
  label: string;
  reason: string;
  value: number | null;
  weight: number;
};

export const RACE_FINISH_PREDICTION_RESULTS_EVENT = "pc-keiba:finish-prediction-results";

export const buildFinishPredictionMarketOverrides = (
  tanshoRows: ReadonlyArray<{
    combination: string;
    odds?: number | null;
    rank?: number | null;
  }>,
): Map<string, FinishPredictionMarketOverride> =>
  new Map(
    tanshoRows.map((row) => [
      row.combination.replace(/^0+/u, "") || row.combination,
      {
        odds: row.odds ?? null,
        popularity: row.rank ?? null,
      },
    ]),
  );

const CATEGORY_CONFIG: Record<FinishPredictionCategory, FinishPredictionConfig> = {
  "ban-ei": {
    decayDays: 90,
    distanceBandMeters: 100,
    distanceScale: 300,
    horseWeight: 0.36,
    jockeyWeight: 0.08,
    minDistanceWeight: 0.4,
    modelWeight: 0.08,
    oddsWeight: 0.1,
    popularityWeight: 0.01,
    recentWeight: 0.18,
    sameDayJockeyWeight: 0,
    similarityWeight: 0.1,
    trainerWeight: 0.06,
  },
  jra: {
    decayDays: 75,
    distanceBandMeters: 400,
    distanceScale: 500,
    horseWeight: 0.162,
    jockeyWeight: 0.04,
    minDistanceWeight: 0.25,
    modelWeight: 0.08,
    oddsWeight: 0.066,
    popularityWeight: 0.01,
    recentWeight: 0.09,
    sameDayJockeyWeight: 0.03,
    similarityWeight: 0.04,
    trainerWeight: 0.02,
  },
  nar: {
    decayDays: 35,
    distanceBandMeters: 400,
    distanceScale: 400,
    horseWeight: 0.22,
    jockeyWeight: 0.08,
    minDistanceWeight: 0.25,
    modelWeight: 0.06,
    oddsWeight: 0.05,
    popularityWeight: 0.01,
    recentWeight: 0.12,
    sameDayJockeyWeight: 0.02,
    similarityWeight: 0.06,
    trainerWeight: 0.04,
  },
};

const GRADED_RACE_CODES = new Set(["A", "B", "C", "D", "E", "F", "G", "H", "L"]);

const NEW_HORSE_MAIDEN_CODE = "701";

const NEW_HORSE_MAIDEN_MODEL_BOOST = 2;

const NEW_HORSE_MAIDEN_POPULARITY_DAMP = 0.5;

const ODDS_RESTORE_MULTIPLIER_NON_BANEI = 2;

const ODDS_RESTORE_MULTIPLIER_BANEI = 1;

const clampScore = (value: number): number => Math.max(0, Math.min(1, value));

const roundScore = (value: number): number => Math.round(value * 100) / 100;

const parseStoredNumber = (value: string | null | undefined, emptyValue: string): number | null => {
  const cleaned = cleanText(value, "");
  if (!cleaned || cleaned === emptyValue || /^0+$/u.test(cleaned)) {
    return null;
  }
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
};

const parseRunnerCount = (value: string | null | undefined): number | null => {
  const parsed = parseStoredNumber(value, "00");
  return parsed === null || parsed <= 1 ? null : parsed;
};

const parseFinishPosition = (value: string | null | undefined): number | null => {
  const parsed = parseStoredNumber(value, "00");
  return parsed === null || parsed <= 0 ? null : parsed;
};

const parseOdds = (value: string | null | undefined): number | null => {
  const parsed = parseStoredNumber(value, "0000");
  return parsed === null || parsed <= 0 ? null : parsed / 10;
};

const parseDistance = (value: string | null | undefined): number | null => {
  const parsed = parseStoredNumber(value, "0000");
  return parsed === null || parsed <= 0 ? null : parsed;
};

const getRaceDateValue = (value: string | null | undefined): number => {
  const cleaned = cleanText(value, "");
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : 0;
};

const toJstDate = (raceDate: string | null | undefined): Date | null => {
  const cleaned = cleanText(raceDate, "");
  if (!/^\d{8}$/u.test(cleaned)) {
    return null;
  }
  return new Date(
    `${cleaned.slice(0, 4)}-${cleaned.slice(4, 6)}-${cleaned.slice(6, 8)}T00:00:00+09:00`,
  );
};

const getDateWeight = (
  raceDate: string | null | undefined,
  currentRaceDate: string,
  decayDays: number,
): number => {
  const currentDate = toJstDate(currentRaceDate);
  const pastDate = toJstDate(raceDate);
  if (currentDate === null || pastDate === null) {
    const diff = Math.max(0, getRaceDateValue(currentRaceDate) - getRaceDateValue(raceDate));
    return 1 / (1 + diff / 10000);
  }
  const elapsedDays = Math.max(0, (currentDate.getTime() - pastDate.getTime()) / 86_400_000);
  return 1 / (1 + elapsedDays / Math.max(1, decayDays));
};

const getDistanceWeight = (
  distance: string | null | undefined,
  currentDistance: string | null | undefined,
  config: Pick<
    FinishPredictionConfig,
    "distanceBandMeters" | "distanceScale" | "minDistanceWeight"
  >,
): number => {
  const parsedDistance = parseDistance(distance);
  const parsedCurrentDistance = parseDistance(currentDistance);
  if (parsedDistance === null || parsedCurrentDistance === null) {
    return 0.75;
  }
  return Math.max(
    config.minDistanceWeight,
    1 -
      Math.abs(parsedDistance - parsedCurrentDistance) /
        Math.max(parsedCurrentDistance * config.distanceScale, config.distanceBandMeters),
  );
};

const getTrackWeight = (
  trackCode: string | null | undefined,
  currentTrackCode: string | null | undefined,
): number => {
  const track = cleanText(trackCode, "");
  const currentTrack = cleanText(currentTrackCode, "");
  if (!track || !currentTrack) {
    return 0.85;
  }
  if (track === currentTrack) {
    return 1.15;
  }
  return track.slice(0, 1) === currentTrack.slice(0, 1) ? 1 : 0.58;
};

const getCategory = ({
  keibajoCode,
  source,
}: {
  keibajoCode: string | null | undefined;
  source: RaceSource;
}): FinishPredictionCategory => {
  if (source === "nar" && isBanEiKeibajoCode(keibajoCode)) {
    return "ban-ei";
  }
  return source === "jra" ? "jra" : "nar";
};

const isGradedRace = (gradeCode: string | null | undefined): boolean =>
  GRADED_RACE_CODES.has(cleanText(gradeCode, ""));

const isNarClassRace = ({
  conditionCode,
  conditionName,
}: {
  conditionCode: string | null | undefined;
  conditionName: string | null | undefined;
}): boolean => {
  const code = cleanText(conditionCode, "");
  const condition = cleanText(conditionName, "");
  return code === "000" || /^[ABC]\d?/u.test(condition);
};

const getDistanceBand = (
  distance: string | null | undefined,
): "long" | "middle" | "sprint" | "unknown" => {
  const parsed = parseDistance(distance);
  if (parsed === null) {
    return "unknown";
  }
  if (parsed <= 1400) {
    return "sprint";
  }
  if (parsed >= 2000) {
    return "long";
  }
  return "middle";
};

const getConditionAdjustedConfig = ({
  baseConfig,
  category,
  currentDistance,
  currentGradeCode,
  currentKyosoJokenCode,
  currentKyosoJokenMeisho,
}: {
  baseConfig: FinishPredictionConfig;
  category: FinishPredictionCategory;
  currentDistance: string | null | undefined;
  currentGradeCode: string | null | undefined;
  currentKyosoJokenCode: string | null | undefined;
  currentKyosoJokenMeisho: string | null | undefined;
}): FinishPredictionConfig => {
  const distanceBand = getDistanceBand(currentDistance);
  const graded = isGradedRace(currentGradeCode);
  const narClass = isNarClassRace({
    conditionCode: currentKyosoJokenCode,
    conditionName: currentKyosoJokenMeisho,
  });
  const config = { ...baseConfig };

  if (category === "ban-ei") {
    return config;
  }

  if (graded) {
    config.sameDayJockeyWeight *= category === "nar" ? 0.7 : 0;
  } else if (category === "nar" && narClass) {
    config.sameDayJockeyWeight *= 0.8;
  }

  if (category === "jra") {
    config.sameDayJockeyWeight = 0;
    config.horseWeight *= 0.95;
    config.oddsWeight *= 1.05;
    config.popularityWeight *= 1.05;
    config.recentWeight *= 0.95;
  }

  if (category === "nar" && !graded && distanceBand !== "sprint") {
    config.horseWeight *= 0.95;
    config.oddsWeight *= 1.1;
    config.popularityWeight *= 1.2;
    config.recentWeight *= 0.95;
  }

  if (distanceBand === "sprint") {
    config.sameDayJockeyWeight *= 1.2;
  } else if (distanceBand === "long") {
    config.sameDayJockeyWeight *= 0.75;
  }

  if (cleanText(currentKyosoJokenCode, "") === NEW_HORSE_MAIDEN_CODE) {
    config.modelWeight = config.modelWeight * NEW_HORSE_MAIDEN_MODEL_BOOST;
    config.popularityWeight = Math.max(
      0,
      config.popularityWeight * NEW_HORSE_MAIDEN_POPULARITY_DAMP,
    );
  }

  return config;
};

const getHorseHistoryAdjustedConfig = (
  baseConfig: FinishPredictionConfig,
  horseResultsCount: number,
  category: FinishPredictionCategory,
): FinishPredictionConfig => {
  if (horseResultsCount <= 1) {
    const oddsRestoreMultiplier =
      category === "ban-ei" ? ODDS_RESTORE_MULTIPLIER_BANEI : ODDS_RESTORE_MULTIPLIER_NON_BANEI;
    return {
      ...baseConfig,
      horseWeight: Math.max(0.12, baseConfig.horseWeight - 0.04),
      jockeyWeight: baseConfig.jockeyWeight + 0.025,
      oddsWeight: baseConfig.oddsWeight * oddsRestoreMultiplier + 0.015,
      popularityWeight: baseConfig.popularityWeight + 0.035,
      sameDayJockeyWeight: baseConfig.sameDayJockeyWeight + 0.015,
      trainerWeight: baseConfig.trainerWeight + 0.015,
    };
  }
  if (horseResultsCount >= 6) {
    return {
      ...baseConfig,
      horseWeight: baseConfig.horseWeight + 0.04,
      jockeyWeight: Math.max(0.02, baseConfig.jockeyWeight - 0.015),
      oddsWeight: Math.max(0.04, baseConfig.oddsWeight - 0.01),
      popularityWeight: Math.max(0, baseConfig.popularityWeight - 0.025),
      sameDayJockeyWeight: Math.max(0, baseConfig.sameDayJockeyWeight - 0.01),
      trainerWeight: Math.max(0.02, baseConfig.trainerWeight - 0.01),
    };
  }
  return baseConfig;
};

const normalizeFinish = ({
  finishPosition,
  runnerCount,
}: {
  finishPosition: number | null;
  runnerCount: number | null;
}): number | null => {
  if (finishPosition === null || runnerCount === null || runnerCount <= 1) {
    return null;
  }
  return clampScore((finishPosition - 1) / (runnerCount - 1));
};

const weightedAverage = (
  results: HorseRaceResult[],
  params: {
    currentDistance: string | null | undefined;
    currentRaceDate: string;
    currentTrackCode: string | null | undefined;
    config: Pick<
      FinishPredictionConfig,
      "decayDays" | "distanceBandMeters" | "distanceScale" | "minDistanceWeight"
    >;
  },
): { count: number; value: number | null } => {
  let total = 0;
  let weightTotal = 0;
  for (const result of results) {
    const finishNorm = normalizeFinish({
      finishPosition: parseFinishPosition(result.kakuteiChakujun),
      runnerCount: parseRunnerCount(result.shussoTosu),
    });
    if (finishNorm === null) {
      continue;
    }
    const weight =
      getDateWeight(
        `${result.kaisaiNen}${result.kaisaiTsukihi}`,
        params.currentRaceDate,
        params.config.decayDays,
      ) *
      getDistanceWeight(result.kyori, params.currentDistance, params.config) *
      getTrackWeight(result.trackCode, params.currentTrackCode);
    total += finishNorm * weight;
    weightTotal += weight;
  }
  return {
    count: results.length,
    value: weightTotal > 0 ? total / weightTotal : null,
  };
};

const normalizeParsedPopularity = (
  popularity: number | null,
  runnerCount: number,
): number | null => {
  if (popularity === null || runnerCount <= 1) {
    return null;
  }
  return clampScore((popularity - 1) / (runnerCount - 1));
};

const normalizeParsedOdds = (odds: number | null): number | null => {
  if (odds === null) {
    return null;
  }
  return clampScore(Math.log(Math.max(odds, 1)) / Math.log(300));
};

const toDetail = (candidate: ScoreCandidate): FinishPredictionDetail => ({
  label: candidate.label,
  reason: candidate.reason,
  value: candidate.value === null ? null : roundScore(candidate.value),
  weight: roundScore(candidate.weight),
});

const calculateScore = (candidates: ScoreCandidate[]): { confidence: number; score: number } => {
  let total = 0;
  let weightTotal = 0;
  for (const candidate of candidates) {
    if (candidate.value === null || candidate.weight <= 0) {
      continue;
    }
    total += candidate.value * candidate.weight;
    weightTotal += candidate.weight;
  }
  return {
    confidence: roundScore(clampScore(weightTotal)),
    score: weightTotal > 0 ? clampScore(total / weightTotal) : 0.5,
  };
};

const getModelKind = (modelVersion: string): "LightGBM" | "LSTM" | "Transformer" | "モデル" => {
  const normalized = modelVersion.toLowerCase();
  if (normalized.includes("lightgbm")) {
    return "LightGBM";
  }
  if (normalized.includes("lstm")) {
    return "LSTM";
  }
  if (normalized.includes("transformer")) {
    return "Transformer";
  }
  return "モデル";
};

const getModelBaseWeight = (modelVersion: string): number => {
  const kind = getModelKind(modelVersion);
  if (kind === "LightGBM") {
    return 0.5;
  }
  if (kind === "Transformer") {
    return 0.3;
  }
  if (kind === "LSTM") {
    return 0.2;
  }
  return 1;
};

const getModelCandidates = (
  models: FinishPositionModelPredictionFeature[],
  modelWeight: number,
): ScoreCandidate[] => {
  const usableModels = models.filter((model) => model.predictedFinishNorm !== null);
  if (usableModels.length === 0) {
    return [
      {
        label: "モデル",
        reason: "モデル予測なし",
        value: null,
        weight: modelWeight,
      },
    ];
  }
  const baseWeightTotal = usableModels.reduce(
    (total, model) => total + getModelBaseWeight(model.modelVersion),
    0,
  );
  return usableModels.map((model) => {
    const kind = getModelKind(model.modelVersion);
    return {
      label: kind === "モデル" ? "モデル" : `${kind}モデル`,
      reason: `${model.modelVersion} の予測値をモデルアンサンブルに利用`,
      value: model.predictedFinishNorm,
      weight: (modelWeight * getModelBaseWeight(model.modelVersion)) / baseWeightTotal,
    };
  });
};

const getModelProbability = (
  models: FinishPositionModelPredictionFeature[],
  key: "showProbability" | "winProbability",
): number | null => {
  const usableModels = models.filter((model) => model[key] !== null);
  if (usableModels.length === 0) {
    return null;
  }
  const baseWeightTotal = usableModels.reduce(
    (total, model) => total + getModelBaseWeight(model.modelVersion),
    0,
  );
  return (
    usableModels.reduce(
      (total, model) => total + (model[key] ?? 0) * getModelBaseWeight(model.modelVersion),
      0,
    ) / baseWeightTotal
  );
};

const getSameDayJockeyScore = (
  jockeyName: string | null | undefined,
  sameDayVenueJockeyWins: SameDayVenueJockeyWinFeature[],
): { feature: SameDayVenueJockeyWinFeature; value: number } | null => {
  const matched = sameDayVenueJockeyWins
    .filter((feature) => isSameJockeyName(jockeyName, feature.jockeyName))
    .toSorted((left, right) => right.winCount - left.winCount)[0];
  if (!matched) {
    return null;
  }
  return {
    feature: matched,
    value: clampScore(0.28 - Math.min(3, matched.winCount) * 0.07),
  };
};

export const buildFinishPredictionRowsFromInputs = (
  inputs: FinishPredictionBuildInputs,
  marketOverrides?: ReadonlyMap<string, FinishPredictionMarketOverride>,
): FinishPredictionRow[] => buildFinishPredictionRowsFromResults({ ...inputs, marketOverrides });

export const buildFinishPredictionRowsFromResults = ({
  currentDistance,
  currentGradeCode,
  currentKeibajoCode,
  currentKyosoJokenCode,
  currentKyosoJokenMeisho,
  currentRaceDate,
  currentSource,
  currentTrackCode,
  marketOverrides,
  modelPredictionFeatures = [],
  results,
  runners,
  sameDayVenueJockeyWins = [],
  similarityFeatures = [],
}: BuildFinishPredictionRowsParams): FinishPredictionRow[] => {
  const category = getCategory({ keibajoCode: currentKeibajoCode, source: currentSource });
  const config = getConditionAdjustedConfig({
    baseConfig: CATEGORY_CONFIG[category],
    category,
    currentDistance,
    currentGradeCode,
    currentKyosoJokenCode,
    currentKyosoJokenMeisho,
  });
  const runnerCount = Math.max(2, runners.length);
  const resultsByHorse = new Map<string, HorseRaceResult[]>();
  for (const result of results) {
    const horseNumber = cleanText(result.currentUmaban, "").replace(/^0+/u, "");
    if (!horseNumber) {
      continue;
    }
    resultsByHorse.set(horseNumber, [...(resultsByHorse.get(horseNumber) ?? []), result]);
  }
  const similarityByHorse = new Map(
    similarityFeatures.map((feature) => [
      cleanText(feature.horseNumber, "").replace(/^0+/u, ""),
      feature,
    ]),
  );
  const modelsByHorse = new Map<string, FinishPositionModelPredictionFeature[]>();
  for (const feature of modelPredictionFeatures) {
    const horseNumber = cleanText(feature.horseNumber, "").replace(/^0+/u, "");
    if (!horseNumber) {
      continue;
    }
    modelsByHorse.set(horseNumber, [...(modelsByHorse.get(horseNumber) ?? []), feature]);
  }

  const provisionalRows = runners.map((runner) => {
    const horseNumber = cleanText(runner.umaban, "").replace(/^0+/u, "");
    const horseResults = (resultsByHorse.get(horseNumber) ?? []).toSorted(
      (left, right) =>
        getRaceDateValue(`${right.kaisaiNen}${right.kaisaiTsukihi}`) -
        getRaceDateValue(`${left.kaisaiNen}${left.kaisaiTsukihi}`),
    );
    const recentResults = horseResults.slice(0, Math.min(5, horseResults.length));
    const jockeyResults = horseResults.filter(
      (result) =>
        cleanText(result.kishumeiRyakusho, "") &&
        cleanText(result.kishumeiRyakusho, "") === cleanText(runner.kishumeiRyakusho, ""),
    );
    const trainerResults = horseResults.filter(
      (result) =>
        cleanText(result.chokyoshimeiRyakusho, "") &&
        cleanText(result.chokyoshimeiRyakusho, "") === cleanText(runner.chokyoshimeiRyakusho, ""),
    );
    const runnerConfig = getHorseHistoryAdjustedConfig(config, horseResults.length, category);
    const averageParams = {
      config: runnerConfig,
      currentDistance,
      currentRaceDate,
      currentTrackCode,
    };
    const horseAverage = weightedAverage(horseResults, averageParams);
    const recentAverage = weightedAverage(recentResults, averageParams);
    const jockeyAverage = weightedAverage(jockeyResults, averageParams);
    const trainerAverage = weightedAverage(trainerResults, averageParams);
    const similarity = similarityByHorse.get(horseNumber);
    const models = modelsByHorse.get(horseNumber) ?? [];
    const sameDayJockey = getSameDayJockeyScore(runner.kishumeiRyakusho, sameDayVenueJockeyWins);
    const marketOverride = marketOverrides?.get(horseNumber);
    const storedPopularity =
      marketOverride?.popularity ?? parseStoredNumber(runner.tanshoNinkijun, "00");
    const storedOdds = marketOverride?.odds ?? parseOdds(runner.tanshoOdds);
    const candidates: ScoreCandidate[] = [
      {
        label: "競走成績",
        reason: `${category}向け重みで過去${horseAverage.count}走の着順、距離、馬場、日付を評価`,
        value: horseAverage.value,
        weight: runnerConfig.horseWeight,
      },
      {
        label: "近走",
        reason: `直近${recentAverage.count}走を強めに評価`,
        value: recentAverage.value,
        weight: runnerConfig.recentWeight,
      },
      {
        label: "騎手",
        reason: `今回騎手と一致する過去${jockeyAverage.count}走を評価`,
        value: jockeyAverage.value,
        weight: runnerConfig.jockeyWeight,
      },
      {
        label: "調教師",
        reason: `今回調教師と一致する過去${trainerAverage.count}走を評価`,
        value: trainerAverage.value,
        weight: runnerConfig.trainerWeight,
      },
      {
        label: "人気",
        reason: marketOverride
          ? "リアルタイムの人気順を出走頭数で正規化"
          : "最新の人気順を出走頭数で正規化",
        value: normalizeParsedPopularity(storedPopularity, runnerCount),
        weight: runnerConfig.popularityWeight,
      },
      {
        label: "単勝",
        reason: marketOverride
          ? "リアルタイムの単勝オッズを対数で正規化"
          : "最新の単勝オッズを対数で正規化",
        value: normalizeParsedOdds(storedOdds),
        weight: runnerConfig.oddsWeight,
      },
      {
        label: "同日同場の騎手勝利",
        reason: sameDayJockey
          ? `${sameDayJockey.feature.jockeyName}騎手が同じ競馬場の当日${sameDayJockey.feature.latestRaceNumber}Rまでに${sameDayJockey.feature.winCount}勝`
          : "同じ競馬場の当日勝利情報なし",
        value: sameDayJockey?.value ?? null,
        weight: runnerConfig.sameDayJockeyWeight,
      },
      {
        label: "類似レース",
        reason: similarity
          ? `10年範囲の類似${similarity.neighborCount}件から着順を推定`
          : "類似レース特徴量なし",
        value:
          similarity?.averageFinishPosition === null ||
          similarity?.averageFinishPosition === undefined
            ? null
            : clampScore((similarity.averageFinishPosition - 1) / (runnerCount - 1)),
        weight: config.similarityWeight,
      },
      ...getModelCandidates(models, config.modelWeight),
    ];
    const { confidence, score } = calculateScore(candidates);
    return {
      confidence,
      details: candidates.map(toDetail),
      horseName: cleanText(runner.bamei, ""),
      horseNumber,
      jockeyName: cleanText(runner.kishumeiRyakusho, ""),
      predictedRank: 0,
      score,
      showProbability: 0,
      storedOdds,
      storedPopularity,
      winProbability: 0,
    };
  });

  const sortedRows = provisionalRows.toSorted(
    (left, right) =>
      left.score - right.score || Number(left.horseNumber) - Number(right.horseNumber),
  );
  const winWeights = sortedRows.map((row) => Math.exp((1 - row.score) * 4));
  const winWeightTotal = winWeights.reduce((total, value) => total + value, 0);

  return sortedRows.map((row, index) => {
    const rank = index + 1;
    const baseWinProbability =
      winWeightTotal > 0 ? (winWeights[index] ?? 0) / winWeightTotal : 1 / runnerCount;
    const models = modelsByHorse.get(row.horseNumber) ?? [];
    const modelWinProbability = getModelProbability(models, "winProbability");
    const modelShowProbability = getModelProbability(models, "showProbability");
    const winProbability =
      modelWinProbability === null || modelWinProbability === undefined
        ? baseWinProbability
        : baseWinProbability * 0.55 + modelWinProbability * 0.45;
    const rankShowProbability = clampScore((runnerCount - rank + 1) / runnerCount);
    const showProbability =
      modelShowProbability === null || modelShowProbability === undefined
        ? rankShowProbability
        : rankShowProbability * 0.55 + modelShowProbability * 0.45;
    return {
      confidence: row.confidence,
      details: row.details,
      horseName: row.horseName,
      horseNumber: row.horseNumber,
      jockeyName: row.jockeyName,
      predictedRank: rank,
      score: roundScore(1 - row.score),
      showProbability: roundScore(clampScore(showProbability)),
      storedOdds: row.storedOdds,
      storedPopularity: row.storedPopularity,
      winProbability: roundScore(clampScore(winProbability)),
    };
  });
};
