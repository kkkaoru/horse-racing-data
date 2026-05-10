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
  kyosoJokenCode: string | null;
  kyosoJokenMeisho: string | null;
  kyori: string | null;
  trackCode: string | null;
  hassoJikoku: string | null;
  shussoTosu: string | null;
}

export interface RaceDetail extends RaceListItem {
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
  kohan3f: string | null;
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
  kohan3f: string | null;
}

export interface CourseInfo {
  courseKaishuNengappi: string;
  courseSetsumei: string | null;
}

export interface Training extends Record<string, unknown> {
  umaban: string | null;
  bamei: string | null;
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
  includeClass: boolean;
  includeDistance: boolean;
  includeFrame: boolean;
  includeMonthWindow: boolean;
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
  years: number | null;
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
