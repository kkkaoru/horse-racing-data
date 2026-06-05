import type {
  RaceTrendRunningStyleCache,
  RaceTrendStarterRow,
} from "horse-racing-realtime/race-trend-daily-track-types";

import type { RaceSource } from "./codes";

export interface RaceDaySummary {
  year: string;
  month: string;
  day: string;
  jraCount: number;
  narCount: number;
}

export interface RaceYearSummary {
  year: string;
  raceCount: number;
  dayCount: number;
}

export interface RaceListItem extends Record<string, unknown> {
  source: RaceSource;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  kyosomeiHondai: string | null;
  kyosomeiFukudai: string | null;
  gradeCode: string | null;
  kyosoShubetsuCode: string | null;
  kyosoKigoCode: string | null;
  juryoShubetsuCode: string | null;
  jockeyNames?: string[];
  kyosoJokenCode: string | null;
  kyosoJokenMeisho: string | null;
  kyori: string | null;
  trackCode: string | null;
  hassoJikoku: string | null;
  shussoTosu: string | null;
}

export interface RaceDetail extends RaceListItem {
  kaisaiKai: string | null;
  kaisaiNichime: string | null;
  kyosomeiKakkonai: string | null;
  torokuTosu: string | null;
  tenkoCode: string | null;
  babajotaiCodeShiba: string | null;
  babajotaiCodeDirt: string | null;
}

export interface Runner {
  wakuban: string | null;
  umaban: string | null;
  kettoTorokuBango: string | null;
  bamei: string | null;
  moshokuCode?: string | null;
  seibetsuCode: string | null;
  barei: string | null;
  futanJuryo: string | null;
  kishumeiRyakusho: string | null;
  chokyoshimeiRyakusho: string | null;
  banushimei: string | null;
  bataiju: string | null;
  zogenFugo: string | null;
  zogenSa: string | null;
  kakuteiChakujun: string | null;
  tanshoOdds: string | null;
  tanshoNinkijun: string | null;
  sohaTime: string | null;
  timeSa: string | null;
  corner1: string | null;
  corner2: string | null;
  corner3: string | null;
  corner4: string | null;
  kohan3f: string | null;
}

export type RaceTrendRunningStyle = "nige" | "senkou" | "sashi" | "oikomi";

export interface RaceTrendDetail {
  source: RaceSource;
  date: string;
  keibajoCode: string;
  raceNumber: string;
  raceName: string | null;
  runningStyle: RaceTrendRunningStyle | null;
  frameNumber: string | null;
  horseNumber: string | null;
  horseName: string | null;
  jockeyName: string | null;
  // Optional: populated only for rows whose source carried a trainer name.
  // Detail rendering falls back to "-" when missing.
  trainerName?: string | null;
  popularity: number | null;
  winOdds: number | null;
  finishPosition: number;
  time: string | null;
  horseWeight: number | null;
  horseWeightDelta: number | null;
}

export interface RaceTrendCurrentRunningStyle {
  horseNumber: string;
  predictedLabel: RaceTrendRunningStyle;
}

export interface RaceTrendRunnerSummary {
  frameNumber: string | null;
  horseNumber: string | null;
  jockeyName: string | null;
  // Trainer name (optional for backward compatibility with cached payloads
  // produced before the trainer column was added). The aggregator skips
  // trainer matching when this is missing.
  trainerName?: string | null;
}

export interface RaceTrendRawPayload {
  raceContext: {
    keibajoCode: string;
    raceBango: string;
    source: RaceSource;
  };
  runners: RaceTrendRunnerSummary[];
  starterRows: RaceTrendStarterRow[];
  currentRunningStyles: RaceTrendCurrentRunningStyle[];
  historicalRunningStyles: RaceTrendRunningStyleCache[];
}

export interface RaceTrendRateRow {
  key: string;
  label: string;
  targetHorseNumber?: string | null;
  targetPopularity?: number | null;
  targetWinOdds?: number | null;
  starts: number;
  showRate: number;
  quinellaRate: number;
  winRate: number;
  finishPositionMedian?: number | null;
  details: RaceTrendDetail[];
}

export interface RaceTrendRunningStyleRow {
  key: string;
  targetHorseNumbers: string[];
  runningStyle: RaceTrendRunningStyle | null;
  frameNumber?: string | null;
  jockeyName?: string | null;
  // Optional: populated for the current race row even when the trainer
  // grouping target is off, so the table can always display the trainer
  // name in its column. The aggregator nulls this only when the trainer
  // target is off AND the row has no current-race trainer available.
  trainerName?: string | null;
  raceNumber?: string | null;
  starts: number;
  showRate: number;
  quinellaRate: number;
  winRate: number;
  finishPositionAverage: number | null;
  popularityMedian: number | null;
  winOddsMedian: number | null;
  finishPositionMedian: number | null;
  details: RaceTrendDetail[];
}

export interface RaceTrendPayload {
  /**
   * @deprecated Kept for cache backward compatibility. The viewer only renders
   * `runningStyleRows`; new responses omit this array to keep the JSON small.
   */
  frameRows?: RaceTrendRateRow[];
  /**
   * @deprecated Kept for cache backward compatibility. The viewer only renders
   * `runningStyleRows`; new responses omit this array to keep the JSON small.
   */
  jockeyRows?: RaceTrendRateRow[];
  raceCount: number;
  runningStyleRows: RaceTrendRunningStyleRow[];
}

export interface HorseRaceResult extends Record<string, unknown> {
  currentJockey: string | null;
  currentBarei: string | null;
  currentSeibetsuCode: string | null;
  currentUmaban: string | null;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  kyosomeiHondai: string | null;
  kyosomeiFukudai: string | null;
  kyosomeiKakkonai: string | null;
  gradeCode: string | null;
  kyosoShubetsuCode: string | null;
  kyosoKigoCode: string | null;
  juryoShubetsuCode: string | null;
  kyosoJokenCode: string | null;
  kyosoJokenMeisho: string | null;
  kyori: string | null;
  trackCode: string | null;
  hassoJikoku: string | null;
  shussoTosu?: string | null;
  tenkoCode: string | null;
  babajotaiCodeShiba: string | null;
  babajotaiCodeDirt: string | null;
  wakuban: string | null;
  umaban: string | null;
  kettoTorokuBango: string | null;
  bamei: string | null;
  seibetsuCode: string | null;
  barei: string | null;
  futanJuryo: string | null;
  kishumeiRyakusho: string | null;
  chokyoshimeiRyakusho: string | null;
  banushimei: string | null;
  bataiju: string | null;
  zogenFugo: string | null;
  zogenSa: string | null;
  kakuteiChakujun: string | null;
  tanshoOdds: string | null;
  tanshoNinkijun: string | null;
  sohaTime: string | null;
  timeSa: string | null;
  corner1: string | null;
  corner2: string | null;
  corner3: string | null;
  corner4: string | null;
  kohan3f: string | null;
}

export interface CourseInfo {
  courseKaishuNengappi: string;
  courseSetsumei: string | null;
}

export interface Training extends Record<string, unknown> {
  umaban: string | null;
  bamei: string | null;
  currentJockeyName?: string | null;
  trainerName?: string | null;
  trainingRiderName?: string | null;
  trainingType: string;
  tracenKubun: string | null;
  chokyoNengappi: string;
  chokyoJikoku: string;
  course: string | null;
  babamawari: string | null;
  timeGokei10f: string | null;
  lapTime10f: string | null;
  timeGokei9f: string | null;
  lapTime9f: string | null;
  timeGokei8f: string | null;
  lapTime8f: string | null;
  timeGokei7f: string | null;
  lapTime7f: string | null;
  timeGokei6f: string | null;
  lapTime6f: string | null;
  timeGokei5f: string | null;
  lapTime5f: string | null;
  timeGokei4f: string | null;
  lapTime4f: string | null;
  timeGokei3f: string | null;
  lapTime3f: string | null;
  timeGokei2f: string | null;
  lapTime2f: string | null;
  lapTime1f: string | null;
  premiumCommentText?: string | null;
  premiumEvaluationGrade?: string | null;
  premiumEvaluationText?: string | null;
}

export interface StableComment extends Record<string, unknown> {
  commentText: string;
  evaluationGrade: number | null;
  evaluationText: string | null;
  fetchedAt: string;
  frameNumber: string | null;
  horseName: string | null;
  horseNumber: string;
}

export interface PremiumPaddockBulletin extends Record<string, unknown> {
  commentText: string | null;
  evaluationText: string | null;
  fetchedAt: string;
  frameNumber: string | null;
  groupKey: "favorite" | "value";
  horseName: string | null;
  horseNumber: string;
}

export interface PremiumDataTopHorse extends Record<string, unknown> {
  fetchedAt: string;
  horseName: string | null;
  horseNumber: string;
  jockeyName: string | null;
  rank: number;
  reasons: string[];
  storedOdds: number | null;
  storedPopularity: number | null;
}

export interface AbilityTest extends Record<string, unknown> {
  currentUmaban: string | null;
  currentBamei: string | null;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  umaban: string | null;
  kettoTorokuBango: string | null;
  bamei: string | null;
  seibetsuCode: string | null;
  barei: string | null;
  chokyoshimeiRyakusho: string | null;
  futanJuryo: string | null;
  kishumeiRyakusho: string | null;
  bataiju: string | null;
  zogenFugo: string | null;
  zogenSa: string | null;
  ijoKubunCode: string | null;
  juni: string | null;
  sohaTime: string | null;
  chakusaCode1: string | null;
  chakusaCode2: string | null;
  chakusaCode3: string | null;
  noryokuShikenCode: string | null;
  gohiCode: string | null;
  riyuCode: string | null;
  gohiNengappi: string | null;
  ashiiroCode: string | null;
  corner1: string | null;
  corner2: string | null;
  corner3: string | null;
  corner4: string | null;
  kohan4f: string | null;
  kohan3f: string | null;
  aiteumaJoho1: string | null;
  aiteumaJoho2: string | null;
  aiteumaJoho3: string | null;
  kyakushitsuHantei: string | null;
  kyori: string | null;
  trackCode: string | null;
  hassoJikoku: string | null;
  tenkoCode: string | null;
  babajotaiCodeDirt: string | null;
}

export interface SimilarRaceStatsRow extends Record<string, unknown> {
  category: "jockey" | "owner" | "trainer";
  currentHorseNumbers: string;
  name: string;
  details: StatsDetail[];
  starts: number;
  horseCount: number;
  winCount: number;
  quinellaCount: number;
  showCount: number;
  winRate: number;
  quinellaRate: number;
  showRate: number;
}

export interface BloodlineStatsRow extends Record<string, unknown> {
  category: "damSire" | "sire" | "sireSire";
  currentHorseNumbers: string;
  name: string;
  details: StatsDetail[];
  starts: number;
  horseCount: number;
  winCount: number;
  quinellaCount: number;
  showCount: number;
  winRate: number;
  quinellaRate: number;
  showRate: number;
}

export interface StatsDetail extends Record<string, unknown> {
  damSireName?: string;
  date: string;
  keibajoCode: string;
  raceNumber: string;
  raceName: string;
  sireName?: string;
  sireSireName?: string;
  horseName: string;
  frameNumber: string;
  horseNumber: string;
  jockeyName: string;
  popularity: string;
  rank: string;
  raceTime: string;
  winOdds: string;
}

export interface PayoutStatsDetail extends Record<string, unknown> {
  date: string;
  keibajoCode: string;
  raceNumber: string;
  raceName: string;
  payout: number;
}

export interface SimilarRaceStatsSettings {
  classConditionName: string | null;
  includeAge: boolean;
  includeBloodlineAncestors: boolean;
  includeClass: boolean;
  includeDistance: boolean;
  includeFrame: boolean;
  includeMonthWindow: boolean;
  includeNarOnly: boolean;
  includeRaceNumber: boolean;
  includeRaceSubtitle: boolean;
  includeRaceTitle: boolean;
  includeRunnerCount: boolean;
  includeSex: boolean;
  includeSurface: boolean;
  includeTurn: boolean;
  includeVenue: boolean;
  includeWeight: boolean;
  runnerCount: number | null;
  sourceScope: RaceSource | "all";
  years: number | null;
}

export interface EntityListQuery {
  date: string;
  dateFrom: string;
  dateTo: string;
  distanceMax: string;
  distanceMin: string;
  jockeyName: string;
  keibajoCode: string;
  last3fMax: string;
  last3fMin: string;
  order: string;
  oddsMax: string;
  oddsMin: string;
  popularityMax: string;
  popularityMin: string;
  q: string;
  rank: string;
  raceNumber: string;
  raceTimeMax: string;
  raceTimeMin: string;
  source: RaceSource | "all";
  surface: string;
  trainerName: string;
  turn: string;
}

export interface HorseListRow extends Record<string, unknown> {
  kettoTorokuBango: string;
  bamei: string;
  starts: number;
  winCount: number;
  showCount: number;
  winRate: number;
  showRate: number;
  latestDate: string;
  latestKeibajoCode: string;
  latestRaceBango: string;
  latestRaceName: string;
  latestSource: RaceSource;
  primarySource: RaceSource;
}

export interface PersonListRow extends Record<string, unknown> {
  name: string;
  starts: number;
  winCount: number;
  showCount: number;
  winRate: number;
  showRate: number;
  latestDate: string;
  latestKeibajoCode: string;
  latestRaceBango: string;
  latestRaceName: string;
  latestSource: RaceSource;
  primarySource: RaceSource;
}

export interface EntityRaceResult extends Record<string, unknown> {
  source: RaceSource;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  raceName: string;
  hassoJikoku: string | null;
  kyori: string | null;
  trackCode: string | null;
  kettoTorokuBango: string | null;
  horseName: string;
  jockeyName: string;
  trainerName: string;
  ownerName: string;
  horseNumber: string | null;
  frameNumber: string | null;
  rank: string | null;
  popularity: string | null;
  winOdds: string | null;
  raceTime: string | null;
  last3f: string | null;
  corner1: string | null;
  corner2: string | null;
  corner3: string | null;
  corner4: string | null;
  isUpcoming: boolean;
}

export interface EntityDetailSummary extends Record<string, unknown> {
  name: string;
  starts: number;
  winCount: number;
  quinellaCount: number;
  showCount: number;
  winRate: number;
  quinellaRate: number;
  showRate: number;
  averagePopularity: number | null;
  averageOdds: number | null;
}

export interface TopRaceSummary extends RaceListItem {
  raceStartAt: string;
}

export interface RaceTimeTargetRace extends Record<string, unknown> {
  date: string;
  horseName: string;
  horseNumber: string;
  jockeyName: string;
  keibajoCode: string;
  kohan3f: string;
  ownerName: string;
  popularity: string;
  raceName: string;
  raceNumber: string;
  raceTime: string;
  trainerName: string;
}

export interface ConditionCorrelationDetail extends Record<string, unknown> {
  key:
    | "horseShow"
    | "horseWin"
    | "jockeyShow"
    | "odds"
    | "ownerShow"
    | "popularity"
    | "trainerShow";
  label: string;
  reason: string;
  score: number;
  target: number | null;
  value: number | null;
  weight: number;
}

export interface ConditionCorrelationRow extends Record<string, unknown> {
  details: ConditionCorrelationDetail[];
  horseName: string;
  horseNumber: string;
  score: number;
}

export interface TimeScoreDetail extends Record<string, unknown> {
  label: string;
  reason: string;
  score: number;
  target: number | null;
  value: number | null;
  weight: number;
}

export interface TimeScoreRow extends Record<string, unknown> {
  details: TimeScoreDetail[];
  horseName: string;
  horseNumber: string;
  jockeyName: string;
  score: number;
}

export interface OverallScoreDetail extends Record<string, unknown> {
  label: string;
  reason: string;
  score: number;
  weight: number;
}

export interface OverallScoreRow extends Record<string, unknown> {
  details: OverallScoreDetail[];
  horseName: string;
  horseNumber: string;
  jockeyName: string;
  score: number;
  storedOdds: number | null;
  storedPopularity: number | null;
}

export interface RacePacePredictionDetail extends Record<string, unknown> {
  label: string;
  reason: string;
  value: number | null;
  weight: number;
}

export interface RacePaceSimilarityFeature extends Record<string, unknown> {
  corner1: number | null;
  corner2: number | null;
  corner3: number | null;
  corner4: number | null;
  horseNumber: string;
  neighborCount: number;
  similarityScore: number;
}

export interface RacePaceModelPredictionFeature extends Record<string, unknown> {
  corner1: number | null;
  corner2: number | null;
  corner3: number | null;
  corner4: number | null;
  horseNumber: string;
  modelVersion: string;
}

export interface RacePacePredictionRow extends Record<string, unknown> {
  confidence: number;
  corner1: number | null;
  corner2: number | null;
  corner3: number | null;
  corner4: number | null;
  details: RacePacePredictionDetail[];
  horseName: string;
  horseNumber: string;
  predictedCorners: string;
}

export interface FinishPredictionDetail extends Record<string, unknown> {
  label: string;
  reason: string;
  value: number | null;
  weight: number;
}

export interface FinishPositionSimilarityFeature extends Record<string, unknown> {
  averageFinishPosition: number | null;
  horseNumber: string;
  neighborCount: number;
  showRate: number | null;
  similarityScore: number;
  winRate: number | null;
}

export interface FinishPositionModelPredictionFeature extends Record<string, unknown> {
  horseNumber: string;
  modelVersion: string;
  predictedFinishNorm: number | null;
  showProbability: number | null;
  winProbability: number | null;
}

export interface SameDayVenueJockeyWinFeature extends Record<string, unknown> {
  jockeyName: string;
  latestRaceNumber: string;
  winCount: number;
}

export interface FinishPredictionRow extends Record<string, unknown> {
  confidence: number;
  details: FinishPredictionDetail[];
  horseName: string;
  horseNumber: string;
  jockeyName: string;
  predictedRank: number;
  score: number;
  showProbability: number;
  storedOdds: number | null;
  storedPopularity: number | null;
  winProbability: number;
}

export interface RaceTimeStats extends Record<string, unknown> {
  raceCount: number;
  fastestRaceTime: number | null;
  fastestKohan3f: number | null;
  averageRaceTime: number | null;
  averageKohan3f: number | null;
  medianRaceTime: number | null;
  medianKohan3f: number | null;
  fastestDetail: StatsDetail | null;
  correlationRows: ConditionCorrelationRow[];
  targetRaces: RaceTimeTargetRace[];
}

export interface PayoutStatsRow extends Record<string, unknown> {
  betType: string;
  count: number;
  minPayout: number | null;
  maxPayout: number | null;
  averagePayout: number | null;
  medianPayout: number | null;
  details: PayoutStatsDetail[];
}

export interface FinishPositionStatsRow extends Record<string, unknown> {
  finishPosition: number;
  count: number;
  averagePopularity: number | null;
  medianPopularity: number | null;
  averageOdds: number | null;
  medianOdds: number | null;
  details: StatsDetail[];
}

export interface FrameStatsRow extends Record<string, unknown> {
  frameNumber: string;
  runnerCount: number | null;
  count: number;
  score: number;
  averageFinish: number | null;
  medianFinish: number | null;
  averagePopularity: number | null;
  medianPopularity: number | null;
  details: StatsDetail[];
}
