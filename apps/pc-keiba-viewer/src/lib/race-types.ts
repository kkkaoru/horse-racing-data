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

export interface SimilarRaceStatsRow extends Record<string, unknown> {
  category: "jockey" | "owner" | "trainer";
  currentHorseNumbers: string;
  name: string;
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
  starts: number;
  horseCount: number;
  winCount: number;
  quinellaCount: number;
  showCount: number;
  winRate: number;
  quinellaRate: number;
  showRate: number;
}

export interface SimilarRaceStatsSettings {
  classConditionName: string | null;
  includeAge: boolean;
  includeClass: boolean;
  includeDistance: boolean;
  includeFrame: boolean;
  includeRaceNumber: boolean;
  includeRaceSubtitle: boolean;
  includeRaceTitle: boolean;
  includeSex: boolean;
  includeSurface: boolean;
  includeTurn: boolean;
  includeVenue: boolean;
  years: number | null;
}
