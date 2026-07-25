// This file runs with Bun.
//
// Builds complete fixed-width JV master rows (jvd_um / jvd_ks / jvd_ch) for
// numeric-only overseas backfill. Matches foreign-visitor conventions:
// massho_kubun=1, tozai_shozoku_code=4, unknown dates = 00000000, packed stats
// zeroed to column width. Pedigree name slots follow real JV 3-gen layout:
// 01b=sire, 02b=dam, 05b=damsire (codes *a remain zero placeholders).

import { isValidTrainerCode, TRAINER_CODE_PLACEHOLDER } from "./entity-resolver";
import { padToJvByteWidth } from "./jvd-mapper";
import type { MasterBackfillCandidate } from "./master-backfill";

const FULL_WIDTH_SPACE: string = "　";
const ASCII_SPACE: string = " ";
const ZERO_DATE: string = "00000000";
const ZERO_HORSE_CODE: string = "0000000000";
const ZERO_OWNER_CODE: string = "000000";
const ZERO_PRODUCER_CODE: string = "00000000";
const PERSON_NAME_BYTE_WIDTH: number = 8;
const BAMEI_BYTE_WIDTH: number = 36;
const BAMEI_EUR_BYTE_WIDTH: number = 60;
const BAMEI_KANA_BYTE_WIDTH: number = 36;
const PERSON_FULL_BYTE_WIDTH: number = 34;
const PERSON_KANA_BYTE_WIDTH: number = 30;
const PERSON_EUR_BYTE_WIDTH: number = 80;
const BANUSHIMEI_BYTE_WIDTH: number = 64;
const SEISANSHA_BYTE_WIDTH: number = 72;
const SANCHIMEI_BYTE_WIDTH: number = 20;
const SHOTAI_BYTE_WIDTH: number = 20;
const YOBI1_UM_WIDTH: number = 19;
const CHAKUKAISU_WIDTH: number = 18;
const KYAKUSHITSU_WIDTH: number = 12;
const PRIZE_WIDTH: number = 9;
const TOROKU_RACE_SU_WIDTH: number = 3;
const SEISEKI_JOHO_WIDTH: number = 1052;
const JUSHOSHORI_WIDTH: number = 163;
const HATSUKIJO_WIDTH: number = 67;
const HATSUSHORI_WIDTH: number = 64;
const TOZAI_OVERSEAS: string = "4";
const MASSHO_OVERSEAS_VISITOR: string = "1";
const DATA_KUBUN_ACTIVE: string = "1";
const HINSHU_THOROUGHBRED: string = "1";
const PERSON_WHITESPACE_PATTERN: RegExp = /\s+/g;

const SEX_CODES: ReadonlyMap<string, string> = new Map([
  ["牡", "1"],
  ["牝", "2"],
  ["せん", "3"],
]);
const COAT_CODES: ReadonlyMap<string, string> = new Map([
  ["栗", "01"],
  ["栃栗", "02"],
  ["鹿", "03"],
  ["黒鹿", "04"],
  ["青鹿", "05"],
  ["青", "06"],
  ["芦", "07"],
  ["栗粕", "08"],
  ["白", "09"],
]);

const zeros = (width: number): string => "0".repeat(width);
const spaces = (width: number): string => ASCII_SPACE.repeat(width);
const fullWidthSpaces = (charCount: number): string => FULL_WIDTH_SPACE.repeat(charCount);

const abbreviatedPersonName = (value: string): string => {
  const dotIndex: number = value.lastIndexOf(".");
  const withoutInitial: string = dotIndex < 0 ? value : value.slice(dotIndex + 1);
  return padToJvByteWidth(
    withoutInitial.replace(PERSON_WHITESPACE_PATTERN, ""),
    PERSON_NAME_BYTE_WIDTH,
  );
};

const pedigreeName = (value: string): string => padToJvByteWidth(value, BAMEI_BYTE_WIDTH);

const emptyPedigreePair = (): { readonly a: string; readonly b: string } => ({
  a: ZERO_HORSE_CODE,
  b: spaces(BAMEI_BYTE_WIDTH),
});

export type MasterTableName = "jvd_um" | "jvd_ks" | "jvd_ch";

export interface BuiltMasterRow {
  readonly table: MasterTableName;
  readonly primaryKeyColumn: string;
  readonly primaryKeyValue: string;
  readonly row: Readonly<Record<string, string>>;
}

const buildHorseRow = (candidate: MasterBackfillCandidate): BuiltMasterRow => {
  const date: string = candidate.raceDate.length === 8 ? candidate.raceDate : ZERO_DATE;
  const trainerCode: string =
    candidate.trainerCode !== null && isValidTrainerCode(candidate.trainerCode)
      ? candidate.trainerCode
      : TRAINER_CODE_PLACEHOLDER;
  const emptyPedigree = emptyPedigreePair();

  const row: Record<string, string> = {
    record_id: "UM",
    data_kubun: DATA_KUBUN_ACTIVE,
    data_sakusei_nengappi: date,
    ketto_toroku_bango: candidate.code,
    massho_kubun: MASSHO_OVERSEAS_VISITOR,
    toroku_nengappi: ZERO_DATE,
    massho_nengappi: ZERO_DATE,
    seinengappi: ZERO_DATE,
    bamei: padToJvByteWidth(candidate.horseName, BAMEI_BYTE_WIDTH),
    bamei_hankaku_kana: spaces(BAMEI_KANA_BYTE_WIDTH),
    bamei_eur: spaces(BAMEI_EUR_BYTE_WIDTH),
    zaikyu_flag: "0",
    yobi_1: spaces(YOBI1_UM_WIDTH),
    umakigo_code: "00",
    seibetsu_code: SEX_CODES.get(candidate.sex) ?? "0",
    hinshu_code: HINSHU_THOROUGHBRED,
    moshoku_code: COAT_CODES.get(candidate.coatColour) ?? "00",
    ketto_joho_01a: ZERO_HORSE_CODE,
    ketto_joho_01b: pedigreeName(candidate.sire),
    ketto_joho_02a: ZERO_HORSE_CODE,
    ketto_joho_02b: pedigreeName(candidate.dam),
    ketto_joho_03a: emptyPedigree.a,
    ketto_joho_03b: emptyPedigree.b,
    ketto_joho_04a: emptyPedigree.a,
    ketto_joho_04b: emptyPedigree.b,
    ketto_joho_05a: ZERO_HORSE_CODE,
    ketto_joho_05b: pedigreeName(candidate.damsire),
    ketto_joho_06a: emptyPedigree.a,
    ketto_joho_06b: emptyPedigree.b,
    ketto_joho_07a: emptyPedigree.a,
    ketto_joho_07b: emptyPedigree.b,
    ketto_joho_08a: emptyPedigree.a,
    ketto_joho_08b: emptyPedigree.b,
    ketto_joho_09a: emptyPedigree.a,
    ketto_joho_09b: emptyPedigree.b,
    ketto_joho_10a: emptyPedigree.a,
    ketto_joho_10b: emptyPedigree.b,
    ketto_joho_11a: emptyPedigree.a,
    ketto_joho_11b: emptyPedigree.b,
    ketto_joho_12a: emptyPedigree.a,
    ketto_joho_12b: emptyPedigree.b,
    ketto_joho_13a: emptyPedigree.a,
    ketto_joho_13b: emptyPedigree.b,
    ketto_joho_14a: emptyPedigree.a,
    ketto_joho_14b: emptyPedigree.b,
    tozai_shozoku_code: TOZAI_OVERSEAS,
    chokyoshi_code: trainerCode,
    chokyoshimei_ryakusho: abbreviatedPersonName(candidate.trainerAbbrev),
    shotai_chiikimei: fullWidthSpaces(SHOTAI_BYTE_WIDTH / 2),
    seisansha_code: ZERO_PRODUCER_CODE,
    seisanshamei: fullWidthSpaces(SEISANSHA_BYTE_WIDTH / 2),
    sanchimei: fullWidthSpaces(SANCHIMEI_BYTE_WIDTH / 2),
    banushi_code: ZERO_OWNER_CODE,
    banushimei: padToJvByteWidth(candidate.owner, BANUSHIMEI_BYTE_WIDTH),
    heichi_honshokin_ruikei: zeros(PRIZE_WIDTH),
    shogai_honshokin_ruikei: zeros(PRIZE_WIDTH),
    heichi_fukashokin_ruikei: zeros(PRIZE_WIDTH),
    shogai_fukashokin_ruikei: zeros(PRIZE_WIDTH),
    heichi_shutokushokin_ruikei: zeros(PRIZE_WIDTH),
    shogai_shutokushokin_ruikei: zeros(PRIZE_WIDTH),
    sogo: zeros(CHAKUKAISU_WIDTH),
    chuo_gokei: zeros(CHAKUKAISU_WIDTH),
    shiba_choku: zeros(CHAKUKAISU_WIDTH),
    shiba_migi: zeros(CHAKUKAISU_WIDTH),
    shiba_hidari: zeros(CHAKUKAISU_WIDTH),
    dirt_choku: zeros(CHAKUKAISU_WIDTH),
    dirt_migi: zeros(CHAKUKAISU_WIDTH),
    dirt_hidari: zeros(CHAKUKAISU_WIDTH),
    shogai: zeros(CHAKUKAISU_WIDTH),
    shiba_ryo: zeros(CHAKUKAISU_WIDTH),
    shiba_yayaomo: zeros(CHAKUKAISU_WIDTH),
    shiba_omo: zeros(CHAKUKAISU_WIDTH),
    shiba_furyo: zeros(CHAKUKAISU_WIDTH),
    dirt_ryo: zeros(CHAKUKAISU_WIDTH),
    dirt_yayaomo: zeros(CHAKUKAISU_WIDTH),
    dirt_omo: zeros(CHAKUKAISU_WIDTH),
    dirt_furyo: zeros(CHAKUKAISU_WIDTH),
    shogai_ryo: zeros(CHAKUKAISU_WIDTH),
    shogai_yayaomo: zeros(CHAKUKAISU_WIDTH),
    shogai_omo: zeros(CHAKUKAISU_WIDTH),
    shogai_furyo: zeros(CHAKUKAISU_WIDTH),
    shiba_short: zeros(CHAKUKAISU_WIDTH),
    shiba_middle: zeros(CHAKUKAISU_WIDTH),
    shiba_long: zeros(CHAKUKAISU_WIDTH),
    dirt_short: zeros(CHAKUKAISU_WIDTH),
    dirt_middle: zeros(CHAKUKAISU_WIDTH),
    dirt_long: zeros(CHAKUKAISU_WIDTH),
    kyakushitsu_keiko: zeros(KYAKUSHITSU_WIDTH),
    toroku_race_su: zeros(TOROKU_RACE_SU_WIDTH),
  };

  return {
    table: "jvd_um",
    primaryKeyColumn: "ketto_toroku_bango",
    primaryKeyValue: candidate.code,
    row,
  };
};

const buildJockeyRow = (candidate: MasterBackfillCandidate): BuiltMasterRow => {
  const date: string = candidate.raceDate.length === 8 ? candidate.raceDate : ZERO_DATE;
  const ryakusho: string = abbreviatedPersonName(candidate.jockeyAbbrev);

  const row: Record<string, string> = {
    record_id: "KS",
    data_kubun: DATA_KUBUN_ACTIVE,
    data_sakusei_nengappi: date,
    kishu_code: candidate.code,
    massho_kubun: MASSHO_OVERSEAS_VISITOR,
    menkyo_kofu_nengappi: ZERO_DATE,
    menkyo_massho_nengappi: ZERO_DATE,
    seinengappi: ZERO_DATE,
    kishumei: padToJvByteWidth(candidate.jockeyAbbrev, PERSON_FULL_BYTE_WIDTH),
    yobi_1: spaces(PERSON_FULL_BYTE_WIDTH),
    kishumei_hankaku_kana: spaces(PERSON_KANA_BYTE_WIDTH),
    kishumei_ryakusho: ryakusho,
    kishumei_eur: spaces(PERSON_EUR_BYTE_WIDTH),
    seibetsu_kubun: "1",
    kijo_shikaku_code: "0",
    kishu_minarai_code: "0",
    tozai_shozoku_code: TOZAI_OVERSEAS,
    shotai_chiikimei: fullWidthSpaces(SHOTAI_BYTE_WIDTH / 2),
    chokyoshi_code: TRAINER_CODE_PLACEHOLDER,
    chokyoshimei_ryakusho: fullWidthSpaces(PERSON_NAME_BYTE_WIDTH / 2),
    hatsukijo_joho_1: spaces(HATSUKIJO_WIDTH),
    hatsukijo_joho_2: spaces(HATSUKIJO_WIDTH),
    hatsushori_joho_1: spaces(HATSUSHORI_WIDTH),
    hatsushori_joho_2: spaces(HATSUSHORI_WIDTH),
    jushoshori_joho_1: spaces(JUSHOSHORI_WIDTH),
    jushoshori_joho_2: spaces(JUSHOSHORI_WIDTH),
    jushoshori_joho_3: spaces(JUSHOSHORI_WIDTH),
    seiseki_joho_1: zeros(SEISEKI_JOHO_WIDTH),
    seiseki_joho_2: zeros(SEISEKI_JOHO_WIDTH),
    seiseki_joho_3: zeros(SEISEKI_JOHO_WIDTH),
  };

  return {
    table: "jvd_ks",
    primaryKeyColumn: "kishu_code",
    primaryKeyValue: candidate.code,
    row,
  };
};

const buildTrainerRow = (candidate: MasterBackfillCandidate): BuiltMasterRow => {
  const date: string = candidate.raceDate.length === 8 ? candidate.raceDate : ZERO_DATE;
  const ryakusho: string = abbreviatedPersonName(candidate.trainerAbbrev);

  const row: Record<string, string> = {
    record_id: "CH",
    data_kubun: DATA_KUBUN_ACTIVE,
    data_sakusei_nengappi: date,
    chokyoshi_code: candidate.code,
    massho_kubun: MASSHO_OVERSEAS_VISITOR,
    menkyo_kofu_nengappi: ZERO_DATE,
    menkyo_massho_nengappi: ZERO_DATE,
    seinengappi: ZERO_DATE,
    chokyoshimei: padToJvByteWidth(candidate.trainerAbbrev, PERSON_FULL_BYTE_WIDTH),
    chokyoshimei_hankaku_kana: spaces(PERSON_KANA_BYTE_WIDTH),
    chokyoshimei_ryakusho: ryakusho,
    chokyoshimei_eur: spaces(PERSON_EUR_BYTE_WIDTH),
    seibetsu_kubun: "1",
    tozai_shozoku_code: TOZAI_OVERSEAS,
    shotai_chiikimei: fullWidthSpaces(SHOTAI_BYTE_WIDTH / 2),
    jushoshori_joho_1: spaces(JUSHOSHORI_WIDTH),
    jushoshori_joho_2: spaces(JUSHOSHORI_WIDTH),
    jushoshori_joho_3: spaces(JUSHOSHORI_WIDTH),
    seiseki_joho_1: zeros(SEISEKI_JOHO_WIDTH),
    seiseki_joho_2: zeros(SEISEKI_JOHO_WIDTH),
    seiseki_joho_3: zeros(SEISEKI_JOHO_WIDTH),
  };

  return {
    table: "jvd_ch",
    primaryKeyColumn: "chokyoshi_code",
    primaryKeyValue: candidate.code,
    row,
  };
};

export const buildMasterRow = (candidate: MasterBackfillCandidate): BuiltMasterRow => {
  switch (candidate.kind) {
    case "horse":
      return buildHorseRow(candidate);
    case "jockey":
      return buildJockeyRow(candidate);
    case "trainer":
      return buildTrainerRow(candidate);
  }
};

export const buildMasterRows = (
  candidates: readonly MasterBackfillCandidate[],
): readonly BuiltMasterRow[] => candidates.map(buildMasterRow);
