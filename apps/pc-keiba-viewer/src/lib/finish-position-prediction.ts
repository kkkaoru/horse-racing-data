import type { RaceSource } from "./codes";
import { cleanText } from "./format";
import type {
  FinishPositionModelPredictionFeature,
  FinishPositionSimilarityFeature,
  FinishPredictionDetail,
  FinishPredictionRow,
  HorseRaceResult,
  Runner,
} from "./race-types";
import { isBanEiKeibajoCode } from "./runner-format";

type FinishPredictionCategory = "ban-ei" | "jra" | "nar";

interface BuildFinishPredictionRowsParams {
  currentDistance: string | null | undefined;
  currentKeibajoCode: string | null | undefined;
  currentRaceDate: string;
  currentSource: RaceSource;
  currentTrackCode?: string | null;
  modelPredictionFeatures?: FinishPositionModelPredictionFeature[];
  results: HorseRaceResult[];
  runners: Runner[];
  similarityFeatures?: FinishPositionSimilarityFeature[];
}

type FinishPredictionConfig = {
  distanceScale: number;
  horseWeight: number;
  jockeyWeight: number;
  modelWeight: number;
  oddsWeight: number;
  popularityWeight: number;
  recentWeight: number;
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

const CATEGORY_CONFIG: Record<FinishPredictionCategory, FinishPredictionConfig> = {
  "ban-ei": {
    distanceScale: 300,
    horseWeight: 0.36,
    jockeyWeight: 0.08,
    modelWeight: 0.08,
    oddsWeight: 0.1,
    popularityWeight: 0.14,
    recentWeight: 0.18,
    similarityWeight: 0.1,
    trainerWeight: 0.06,
  },
  jra: {
    distanceScale: 500,
    horseWeight: 0.18,
    jockeyWeight: 0.04,
    modelWeight: 0.08,
    oddsWeight: 0.12,
    popularityWeight: 0.42,
    recentWeight: 0.1,
    similarityWeight: 0.04,
    trainerWeight: 0.02,
  },
  nar: {
    distanceScale: 400,
    horseWeight: 0.22,
    jockeyWeight: 0.08,
    modelWeight: 0.06,
    oddsWeight: 0.1,
    popularityWeight: 0.32,
    recentWeight: 0.12,
    similarityWeight: 0.06,
    trainerWeight: 0.04,
  },
};

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

const getDateWeight = (raceDate: string | null | undefined, currentRaceDate: string): number => {
  const diff = Math.max(0, getRaceDateValue(currentRaceDate) - getRaceDateValue(raceDate));
  return 1 / (1 + diff / 10000);
};

const getDistanceWeight = (
  distance: string | null | undefined,
  currentDistance: string | null | undefined,
  scale: number,
): number => {
  const parsedDistance = parseDistance(distance);
  const parsedCurrentDistance = parseDistance(currentDistance);
  if (parsedDistance === null || parsedCurrentDistance === null) {
    return 0.75;
  }
  return 1 / (1 + Math.abs(parsedDistance - parsedCurrentDistance) / scale);
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
  return track.slice(0, 1) === currentTrack.slice(0, 1) ? 1 : 0.72;
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
    distanceScale: number;
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
      getDateWeight(`${result.kaisaiNen}${result.kaisaiTsukihi}`, params.currentRaceDate) *
      getDistanceWeight(result.kyori, params.currentDistance, params.distanceScale) *
      getTrackWeight(result.trackCode, params.currentTrackCode);
    total += finishNorm * weight;
    weightTotal += weight;
  }
  return {
    count: results.length,
    value: weightTotal > 0 ? total / weightTotal : null,
  };
};

const normalizePopularity = (
  value: string | null | undefined,
  runnerCount: number,
): number | null => {
  const popularity = parseStoredNumber(value, "00");
  if (popularity === null || runnerCount <= 1) {
    return null;
  }
  return clampScore((popularity - 1) / (runnerCount - 1));
};

const normalizeOdds = (value: string | null | undefined): number | null => {
  const odds = parseOdds(value);
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

export const buildFinishPredictionRowsFromResults = ({
  currentDistance,
  currentKeibajoCode,
  currentRaceDate,
  currentSource,
  currentTrackCode,
  modelPredictionFeatures = [],
  results,
  runners,
  similarityFeatures = [],
}: BuildFinishPredictionRowsParams): FinishPredictionRow[] => {
  const category = getCategory({ keibajoCode: currentKeibajoCode, source: currentSource });
  const config = CATEGORY_CONFIG[category];
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
  const modelByHorse = new Map(
    modelPredictionFeatures.map((feature) => [
      cleanText(feature.horseNumber, "").replace(/^0+/u, ""),
      feature,
    ]),
  );

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
    const averageParams = {
      currentDistance,
      currentRaceDate,
      currentTrackCode,
      distanceScale: config.distanceScale,
    };
    const horseAverage = weightedAverage(horseResults, averageParams);
    const recentAverage = weightedAverage(recentResults, averageParams);
    const jockeyAverage = weightedAverage(jockeyResults, averageParams);
    const trainerAverage = weightedAverage(trainerResults, averageParams);
    const similarity = similarityByHorse.get(horseNumber);
    const model = modelByHorse.get(horseNumber);
    const candidates: ScoreCandidate[] = [
      {
        label: "競走成績",
        reason: `${category}向け重みで過去${horseAverage.count}走の着順、距離、馬場、日付を評価`,
        value: horseAverage.value,
        weight: config.horseWeight,
      },
      {
        label: "近走",
        reason: `直近${recentAverage.count}走を強めに評価`,
        value: recentAverage.value,
        weight: config.recentWeight,
      },
      {
        label: "騎手",
        reason: `今回騎手と一致する過去${jockeyAverage.count}走を評価`,
        value: jockeyAverage.value,
        weight: config.jockeyWeight,
      },
      {
        label: "調教師",
        reason: `今回調教師と一致する過去${trainerAverage.count}走を評価`,
        value: trainerAverage.value,
        weight: config.trainerWeight,
      },
      {
        label: "人気",
        reason: "最新の人気順を出走頭数で正規化",
        value: normalizePopularity(runner.tanshoNinkijun, runnerCount),
        weight: config.popularityWeight,
      },
      {
        label: "単勝",
        reason: "最新の単勝オッズを対数で正規化",
        value: normalizeOdds(runner.tanshoOdds),
        weight: config.oddsWeight,
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
      {
        label: "モデル",
        reason: model ? `モデル ${model.modelVersion} の予測値` : "モデル予測なし",
        value: model?.predictedFinishNorm ?? null,
        weight: config.modelWeight,
      },
    ];
    const { confidence, score } = calculateScore(candidates);
    return {
      confidence,
      details: candidates.map(toDetail),
      horseName: cleanText(runner.bamei, ""),
      horseNumber,
      predictedRank: 0,
      score,
      showProbability: 0,
      storedOdds: parseOdds(runner.tanshoOdds),
      storedPopularity: parseStoredNumber(runner.tanshoNinkijun, "00"),
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
    const model = modelByHorse.get(row.horseNumber);
    const winProbability =
      model?.winProbability === null || model?.winProbability === undefined
        ? baseWinProbability
        : baseWinProbability * 0.55 + model.winProbability * 0.45;
    const rankShowProbability = clampScore((runnerCount - rank + 1) / runnerCount);
    const modelShow = model?.showProbability;
    const showProbability =
      modelShow === null || modelShow === undefined
        ? rankShowProbability
        : rankShowProbability * 0.55 + modelShow * 0.45;
    return {
      confidence: row.confidence,
      details: row.details,
      horseName: row.horseName,
      horseNumber: row.horseNumber,
      predictedRank: rank,
      score: roundScore(1 - row.score),
      showProbability: roundScore(clampScore(showProbability)),
      storedOdds: row.storedOdds,
      storedPopularity: row.storedPopularity,
      winProbability: roundScore(clampScore(winProbability)),
    };
  });
};
