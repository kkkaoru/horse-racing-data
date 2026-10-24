// Run with bun.
import type { PartnershipStatsRow } from "./heatmap-partnership-stats";
import type { ForeignOwnerMetadata } from "./foreign-owner-reader";
export type CatalogSource = "jra" | "nar";
export type RunningStyleSourceScope = "ban-ei" | "jra" | "nar";
export type SourceScope = "all" | "ban-ei" | "jra" | "nar";

export interface DailyRaceEntryRow {
  source: CatalogSource;
  race_date: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  ketto_toroku_bango: string;
  wakuban: string | null;
  umaban: number | null;
  bamei: string | null;
  race_name: string | null;
  hasso_jikoku: string | null;
  track_code: string | null;
  grade_code?: string | null;
  kyoso_shubetsu_code: string | null;
  juryo_shubetsu_code: string | null;
  kyoso_joken_code: string | null;
  babajotai_code_shiba: string | null;
  babajotai_code_dirt: string | null;
  kyori: number | null;
  shusso_tosu: number | null;
  seibetsu_code: string | null;
  barei: number | null;
  futan_juryo: number | null;
  kishumei_ryakusho: string | null;
  chokyoshimei_ryakusho: string | null;
  banushimei: string | null;
  finish_position: number | null;
  finish_norm: number | null;
  tansho_ninkijun: number | null;
  tansho_odds: number | null;
  soha_time: number | null;
  time_sa: number | null;
  kohan_3f: number | null;
  corner1_norm: number | null;
  corner2_norm: number | null;
  corner3_norm: number | null;
  corner4_norm: number | null;
  corner_1: number | null;
  corner_2: number | null;
  corner_3: number | null;
  corner_4: number | null;
  bataiju: number | null;
  zogen_fugo: string | null;
  zogen_sa: number | null;
}

export interface CatalogRaceKeyRow {
  source: CatalogSource;
  grade_code?: string | null;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
}

export interface KvStore {
  delete(key: string): Promise<void>;
  get(key: string): Promise<string | null>;
  put(key: string, value: string, options: { expirationTtl: number }): Promise<void>;
}

export interface CacheStore {
  delete(request: Request): Promise<boolean>;
  match(request: Request): Promise<Response | undefined>;
  put(request: Request, response: Response): Promise<void>;
}

export interface ObjectBody {
  body: ReadableStream<Uint8Array>;
  size: number;
}

export interface ObjectRange {
  length: number;
  offset: number;
}

export interface ObjectStore {
  get(key: string, options?: { range: ObjectRange }): Promise<ObjectBody | null>;
}

export type Fetcher = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

export interface R2SqlCatalogConfig {
  R2_SQL_ACCOUNT_ID: string;
  R2_SQL_BUCKET_NAME: string;
  R2_SQL_NAMESPACE: string;
  R2_SQL_TOKEN: string;
  INGESTION_ALERTS?: { send(message: unknown): Promise<void> };
}

export interface Env extends R2SqlCatalogConfig {
  ADMIN_TOKEN?: string;
  // Account-wide cap on concurrent heavy R2 SQL queries (src/r2-sql-gate.ts).
  R2_SQL_GATE?: import("./r2-sql-gate").R2SqlGateNamespace;
  CACHE_TTL_SECONDS?: string;
  CATALOG_KV: KvStore;
  CATALOG_OBJECTS?: ObjectStore;
  FINISH_POSITION_ATTESTATION_TOKEN?: string;
  INGESTION_TOKEN?: string;
  KV_TTL_SECONDS?: string;
  RACE_ENTITY_CURSOR_SECRET?: string;
  RACE_ENTITY_WARM_TOKEN?: string;
}

export interface WorkerDependencies {
  cache: CacheStore;
  fetchImpl: Fetcher;
  // ExecutionContext.waitUntil: keeps work alive after the client disconnects.
  waitUntil?: (promise: Promise<unknown>) => void;
}

export interface RaceFeatureFilters {
  date: string;
  keibajoCode?: string;
  raceBango?: string;
  source: SourceScope;
}

export interface FreshRaceEntryFilters {
  date: string;
  keibajoCode: string;
  raceBango: string;
  source: RunningStyleSourceScope;
}

export interface FreshRaceEntry {
  kettoTorokuBango: string;
  umaban: number;
}

export interface BulkFreshRaceEntryFilters {
  date: string;
  source: RunningStyleSourceScope;
}

export interface BulkFreshRaceEntry extends FreshRaceEntry {
  keibajoCode: string;
  raceBango: string;
  source: RunningStyleSourceScope;
}

// raceBango is optional so one query can cover every race at a venue on a
// day. The 10-year history CTEs depend only on date + source (see
// running-style-sql.ts::historyPredicates), so a venue-level build amortises
// that decade scan across ~12 races instead of repeating it per race.
export interface RunningStyleFeatureFilters {
  date: string;
  keibajoCode: string;
  raceBango?: string;
  source: RunningStyleSourceScope;
  umaban?: number;
  gradeCode?: string | null;
}

export interface RaceTrainingFilters {
  date: string;
  keibajoCode: string;
  raceBango: string;
}

export type HorseRaceResultsSourceScope = "all" | "jra" | "nar";

export interface HorseRaceResultsFilters {
  date: string;
  keibajoCode: string;
  raceBango: string;
  source: CatalogSource;
  sourceScope: HorseRaceResultsSourceScope;
}

export type RaceEntityType = "horse" | "jockey" | "trainer" | "owner";

export interface RaceEntityRecentResultsFilters {
  cursor: string | null;
  date: string;
  entityType: RaceEntityType;
  horseNumber: string;
  keibajoCode: string;
  limit: number;
  raceBango: string;
  source: CatalogSource;
}

export interface HorseRaceResultRow {
  babajotaiCodeDirt: string | null;
  babajotaiCodeShiba: string | null;
  bamei: string | null;
  banushimei: string | null;
  barei: string | null;
  bataiju: string | null;
  blinkerShiyoKubun: string | null;
  chokyoshimeiRyakusho: string | null;
  corner1: string | null;
  corner2: string | null;
  corner3: string | null;
  corner4: string | null;
  currentBarei: string | null;
  currentJockey: string | null;
  currentSeibetsuCode: string | null;
  currentUmaban: string | null;
  futanJuryo: string | null;
  gradeCode: string | null;
  hassoJikoku: string | null;
  juryoShubetsuCode: string | null;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  kakuteiChakujun: string | null;
  keibajoCode: string;
  kettoTorokuBango: string | null;
  kishumeiRyakusho: string | null;
  kohan3f: string | null;
  kyori: string | null;
  kyosoJokenCode: string | null;
  kyosoJokenMeisho: string | null;
  kyosoKigoCode: string | null;
  kyosomeiFukudai: string | null;
  kyosomeiHondai: string | null;
  kyosomeiKakkonai: string | null;
  kyosoShubetsuCode: string | null;
  raceBango: string;
  seibetsuCode: string | null;
  shussoTosu: string | null;
  sohaTime: string | null;
  tanshoNinkijun: string | null;
  tanshoOdds: string | null;
  tenkoCode: string | null;
  timeSa: string | null;
  trackCode: string | null;
  umaban: string | null;
  wakuban: string | null;
  zogenFugo: string | null;
  zogenSa: string | null;
}

export interface ConditionFrameStatsRow {
  averageFinish: number | null;
  averagePopularity: number | null;
  count: number;
  details: [];
  frameNumber: string;
  medianFinish: number | null;
  medianPopularity: number | null;
  quinellaCount: number;
  quinellaRate: number;
  runnerCount: number | null;
  score: number;
  showCount: number;
  showRate: number;
  winCount: number;
  winRate: number;
}

export interface ConditionWeightClassStatsRow {
  key: string;
  quinellaCount: number;
  quinellaRate: number;
  showCount: number;
  showRate: number;
  starts: number;
  winCount: number;
  winRate: number;
}

export interface ConditionFinishPositionDetail {
  date: string;
  frameNumber: string;
  horseName: string;
  horseNumber: string;
  jockeyName: string;
  keibajoCode: string;
  popularity: string;
  raceName: string;
  raceNumber: string;
  raceTime: string;
  rank: string;
  winOdds: string;
}

export interface ConditionFinishPositionStatsRow {
  averageOdds: number | null;
  averagePopularity: number | null;
  count: number;
  details: ConditionFinishPositionDetail[];
  finishPosition: number;
  medianOdds: number | null;
  medianPopularity: number | null;
}

export interface ConditionTargetRace {
  date: string;
  horseName: string;
  horseNumber: string;
  jockeyName: string;
  keibajoCode: string;
  kohan3f: string;
  kyori: string;
  ownerName: string;
  popularity: string;
  raceName: string;
  raceNumber: string;
  raceTime: string;
  trainerName: string;
}

export type ConditionCorrelationKey =
  | "horseShow"
  | "horseWin"
  | "jockeyShow"
  | "odds"
  | "ownerShow"
  | "popularity"
  | "trainerShow";

export interface ConditionCorrelationDetail {
  key: ConditionCorrelationKey;
  label: string;
  reason: string;
  score: number;
  target: number | null;
  value: number | null;
  weight: number;
}

export interface ConditionCorrelationRow {
  details: ConditionCorrelationDetail[];
  horseName: string;
  horseNumber: string;
  score: number;
}

export interface ConditionRaceTimeStats {
  averageKohan3f: number | null;
  averageRaceTime: number | null;
  correlationRows: ConditionCorrelationRow[];
  fastestDetail: null;
  fastestKohan3f: number | null;
  fastestRaceTime: number | null;
  medianKohan3f: number | null;
  medianRaceTime: number | null;
  raceCount: number;
  targetRaces: ConditionTargetRace[];
}

export interface ConditionHistoryStatsPayload {
  carriedWeightClassStats: ConditionWeightClassStatsRow[];
  finishPositionStats: ConditionFinishPositionStatsRow[];
  frameStats: ConditionFrameStatsRow[];
  raceTimeStats: ConditionRaceTimeStats;
  weightChangeClassStats: ConditionWeightClassStatsRow[];
  weightClassStats: ConditionWeightClassStatsRow[];
}

export type WinRateHeatmapBloodlineCategory =
  | "damDamSire"
  | "damSire"
  | "damSireSire"
  | "sire"
  | "sireDamSire"
  | "sireSire"
  | "sireSireSire";

export type WinRateHeatmapSimilarKind = "jockey" | "jockeyFrame" | "owner" | "trainer";

export interface WinRateHeatmapStatsFilters {
  date: string;
  includeAge?: boolean;
  includeClass?: boolean;
  includeConditionKey?: boolean;
  includeDistance: boolean;
  includeGrade?: boolean;
  includeJockeyFrame?: boolean;
  includeOwner?: boolean;
  includeForeignSourceStats?: boolean;
  includeRaceTitle?: boolean;
  includeSurface: boolean;
  includeTrackCode?: boolean;
  includeTurn: boolean;
  includeVenue: boolean;
  keibajoCode: string;
  raceBango: string;
  source: CatalogSource;
  years: number;
}

export interface WinRateHeatmapBloodlineRow {
  category: WinRateHeatmapBloodlineCategory;
  details: [];
  name: string;
  places: number;
  shows: number;
  starts: number;
  umaban: number;
  wins: number;
}

export interface WinRateHeatmapSimilarRow {
  statsSource?: "netkeiba";
  statsScope?: "stored-results-10y" | "stored-results-race-10y" | "stored-results-venue-30y";
  sourcePersonId?: string;
  details: [];
  kind: WinRateHeatmapSimilarKind;
  name: string;
  places: number;
  shows: number;
  starts: number;
  umaban: number;
  wins: number;
}

/** Qualified Arc rows use fixed rolling windows per kind (10Y people/horse/frame,
 * 30Y horse-jockey/triple/owner-venue, 3Y jockey-venue, 10Y jockey-trainer-venue).
 * Their since/before/scope are authoritative, independent of generic condition filters.
 * Only rows and partial coverage cross the HTTP boundary; the raw ledger stays private.
 */
export interface WinRateHeatmapStatsPayload {
  arcQualifiedStats?: Pick<
    import("./arc-qualified-people").ArcQualifiedResult,
    "rows" | "coverage"
  >;
  foreignOwnerVenueRows?: (PartnershipStatsRow & ForeignOwnerMetadata)[];
  bloodlineRows: WinRateHeatmapBloodlineRow[];
  similarRows: WinRateHeatmapSimilarRow[];
}

export interface RaceTrainingRow {
  babamawari: string | null;
  bamei: string | null;
  chokyoJikoku: string;
  chokyoNengappi: string;
  course: string | null;
  currentJockeyName: string | null;
  lapTime10f: string | null;
  lapTime1f: string | null;
  lapTime2f: string | null;
  lapTime3f: string | null;
  lapTime4f: string | null;
  lapTime5f: string | null;
  lapTime6f: string | null;
  lapTime7f: string | null;
  lapTime8f: string | null;
  lapTime9f: string | null;
  premiumCommentText?: string | null;
  premiumEvaluationGrade?: string | null;
  premiumEvaluationText?: string | null;
  premiumWorkoutIndex?: number;
  timeGokei10f: string | null;
  timeGokei2f: string | null;
  timeGokei3f: string | null;
  timeGokei4f: string | null;
  timeGokei5f: string | null;
  timeGokei6f: string | null;
  timeGokei7f: string | null;
  timeGokei8f: string | null;
  timeGokei9f: string | null;
  tracenKubun: string | null;
  trainerName: string | null;
  trainingDataSource: "jra" | "netkeiba";
  trainingRiderName: string | null;
  trainingType: string;
  umaban: string | null;
}
