// This file runs with Bun.

export type RaceGrade = "G1" | "G2" | "G3" | null;
export type RaceDirection = "右" | "左" | "直線" | string;
export type RaceSurface = "芝" | "ダート" | string;

export interface ParsedRunner {
  horseNumber: number;
  gate: number;
  horseName: string;
  sex: string;
  age: number;
  coatColour: string;
  weightCarriedKg: number;
  jockeyAbbrev: string;
  trainerAbbrev: string;
  trainerCountry: string;
  owner: string;
  winOdds: number | null;
  popularity: number | null;
  formRecord: string;
  sire: string;
  dam: string;
  damsire: string;
}

export interface ParsedRace {
  raceName: string;
  grade: RaceGrade;
  date: string;
  venue: string;
  country: string;
  distanceMetres: number;
  surface: RaceSurface;
  direction: RaceDirection;
  startTime: string;
  runners: readonly ParsedRunner[];
}

export interface ResolvedEntityCodes {
  horseRegistrationNumber: string | null;
  horseName: string | null;
  jockeyCode: string | null;
  jockeyName: string | null;
  trainerCode: string | null;
  trainerName: string | null;
  ownerCode: string | null;
  ownerName: string | null;
  /** JV east/west affiliation: 1=Miho, 2=Ritto, 3=local, 4=overseas; "0" when unresolved. */
  tozaiShozokuCode: string | null;
}

export interface EntityResolutionRequest {
  race: ParsedRace;
  secondaryRace: SecondaryRaceData;
}

export interface EntityCodeResolver {
  resolve(request: EntityResolutionRequest): Promise<ReadonlyMap<number, ResolvedEntityCodes>>;
}

export interface SecondaryRunnerIdentity {
  horseNumber: number;
  horseName: string;
  horseRegistrationNumber: string | null;
  jockeyName: string;
  trainerName: string;
  ownerName: string;
}

export interface SecondaryRaceData {
  raceId: string;
  runners: readonly SecondaryRunnerIdentity[];
}

export interface SecondarySourceAdapter {
  parse(html: string, raceId: string): SecondaryRaceData;
}

export interface RaceStorageIdentity {
  venueCode: string;
  raceNumber: string;
}

export interface JvdRaRow extends Record<string, string> {
  record_id: string;
  data_kubun: string;
  data_sakusei_nengappi: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  kaisai_kai: string;
  kaisai_nichime: string;
  race_bango: string;
  yobi_code: string;
  tokubetsu_kyoso_bango: string;
  kyosomei_hondai: string;
  kyosomei_fukudai: string;
  kyosomei_kakkonai: string;
  kyosomei_hondai_eur: string;
  kyosomei_fukudai_eur: string;
  kyosomei_kakkonai_eur: string;
  kyosomei_ryakusho_10: string;
  kyosomei_ryakusho_6: string;
  kyosomei_ryakusho_3: string;
  kyosomei_kubun: string;
  jusho_kaiji: string;
  grade_code: string;
  grade_code_henkomae: string;
  kyoso_shubetsu_code: string;
  kyoso_kigo_code: string;
  juryo_shubetsu_code: string;
  kyoso_joken_code_2sai: string;
  kyoso_joken_code_3sai: string;
  kyoso_joken_code_4sai: string;
  kyoso_joken_code_5sai_ijo: string;
  kyoso_joken_code: string;
  kyoso_joken_meisho: string;
  kyori: string;
  kyori_henkomae: string;
  track_code: string;
  track_code_henkomae: string;
  course_kubun: string;
  course_kubun_henkomae: string;
  honshokin: string;
  honshokin_henkomae: string;
  fukashokin: string;
  fukashokin_henkomae: string;
  hasso_jikoku: string;
  hasso_jikoku_henkomae: string;
  toroku_tosu: string;
  shusso_tosu: string;
  nyusen_tosu: string;
  tenko_code: string;
  babajotai_code_shiba: string;
  babajotai_code_dirt: string;
  lap_time: string;
  shogai_mile_time: string;
  zenhan_3f: string;
  zenhan_4f: string;
  kohan_3f: string;
  kohan_4f: string;
  corner_tsuka_juni_1: string;
  corner_tsuka_juni_2: string;
  corner_tsuka_juni_3: string;
  corner_tsuka_juni_4: string;
  record_koshin_kubun: string;
}

export interface JvdSeRow extends Record<string, string> {
  record_id: string;
  data_kubun: string;
  data_sakusei_nengappi: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  kaisai_kai: string;
  kaisai_nichime: string;
  race_bango: string;
  wakuban: string;
  umaban: string;
  ketto_toroku_bango: string;
  bamei: string;
  umakigo_code: string;
  seibetsu_code: string;
  hinshu_code: string;
  moshoku_code: string;
  barei: string;
  tozai_shozoku_code: string;
  chokyoshi_code: string;
  chokyoshimei_ryakusho: string;
  banushi_code: string;
  banushimei: string;
  fukushoku_hyoji: string;
  yobi_1: string;
  futan_juryo: string;
  futan_juryo_henkomae: string;
  blinker_shiyo_kubun: string;
  yobi_2: string;
  kishu_code: string;
  kishu_code_henkomae: string;
  kishumei_ryakusho: string;
  kishumei_ryakusho_henkomae: string;
  kishu_minarai_code: string;
  kishu_minarai_code_henkomae: string;
  bataiju: string;
  zogen_fugo: string;
  zogen_sa: string;
  ijo_kubun_code: string;
  nyusen_juni: string;
  kakutei_chakujun: string;
  dochaku_kubun: string;
  dochaku_tosu: string;
  soha_time: string;
  chakusa_code_1: string;
  chakusa_code_2: string;
  chakusa_code_3: string;
  corner_1: string;
  corner_2: string;
  corner_3: string;
  corner_4: string;
  tansho_odds: string;
  tansho_ninkijun: string;
  kakutoku_honshokin: string;
  kakutoku_fukashokin: string;
  yobi_3: string;
  yobi_4: string;
  kohan_4f: string;
  kohan_3f: string;
  aiteuma_joho_1: string;
  aiteuma_joho_2: string;
  aiteuma_joho_3: string;
  time_sa: string;
  record_koshin_kubun: string;
  mining_kubun: string;
  yoso_soha_time: string;
  yoso_gosa_plus: string;
  yoso_gosa_minus: string;
  yoso_juni: string;
  kyakushitsu_hantei: string;
}

export interface JvdRows {
  race: JvdRaRow;
  runners: readonly JvdSeRow[];
}

export interface SqlStatement {
  text: string;
  values: readonly string[];
}

export interface ParsedJraRace extends ParsedRace {
  localStartTime: string;
}
