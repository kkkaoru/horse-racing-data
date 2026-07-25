// This file runs with Bun.
// Resolved entities use their JV master's canonical name. When resolution fails or the master's
// name field is blank, the published JRA page name is deliberately retained rather than blanked so
// the overseas record preserves all available source information.
import type {
  JvdRaRow,
  JvdRows,
  JvdSeRow,
  ParsedRace,
  ParsedRunner,
  RaceStorageIdentity,
  ResolvedEntityCodes,
} from "./types";

interface MapJvdRowsInput {
  race: ParsedRace;
  storageIdentity: RaceStorageIdentity;
  resolvedCodes: ReadonlyMap<number, ResolvedEntityCodes>;
}

interface RunnerMappingInput {
  race: ParsedRace;
  runner: ParsedRunner;
  storageIdentity: RaceStorageIdentity;
  codes: ResolvedEntityCodes | undefined;
}

const FULL_WIDTH_SPACE: string = "　";
const ASCII_SPACE: string = " ";
const ZERO_HORSE_CODE: string = "0000000000";
const ZERO_JOCKEY_CODE: string = "00000";
const ZERO_TRAINER_CODE: string = "00000";
const ZERO_OWNER_CODE: string = "000000";
const TOZAI_SHOZOKU_CODE_PLACEHOLDER: string = "0";
// JV fixed field widths are specified in Shift-JIS bytes. Full-width characters consume 2 bytes;
// half-width/ASCII characters consume 1 byte. Character pad lengths for pure full-width text are
// therefore half the byte width (bamei 36→18, banushimei 64→32, race name 60→30).
const BAMEI_BYTE_WIDTH: number = 36;
const BANUSHIMEI_BYTE_WIDTH: number = 64;
const RACE_NAME_BYTE_WIDTH: number = 60;
const PERSON_NAME_BYTE_WIDTH: number = 8;
const RACE_NAME_SHORT_BYTE_WIDTH: number = 20;
const RACE_NAME_MEDIUM_BYTE_WIDTH: number = 12;
const RACE_NAME_MINIMUM_BYTE_WIDTH: number = 6;
const EUROPEAN_RACE_NAME_LENGTH: number = 120;
const CORNER_VALUE_LENGTH: number = 72;
const AITEUMA_VALUE_LENGTH: number = 46;
const LAP_TIME_LENGTH: number = 75;
const HONSHOKIN_LENGTH: number = 56;
const HONSHOKIN_BEFORE_LENGTH: number = 40;
const FUKASHOKIN_LENGTH: number = 24;
const DISTANCE_LENGTH: number = 4;
const HALF_WIDTH_UNIT: number = 1;
const FULL_WIDTH_UNIT: number = 2;
const ASCII_MAX_CODE_POINT: number = 0x7f;
const HALFWIDTH_KATAKANA_START: number = 0xff61;
const HALFWIDTH_KATAKANA_END: number = 0xff9f;
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
const GRADE_CODES: ReadonlyMap<string, string> = new Map([
  ["G1", "A"],
  ["G2", "B"],
  ["G3", "C"],
]);
const TRACK_CODES: ReadonlyMap<string, string> = new Map([["芝・右", "17"]]);

const isHalfWidthCodePoint = (codePoint: number): boolean =>
  codePoint <= ASCII_MAX_CODE_POINT ||
  (codePoint >= HALFWIDTH_KATAKANA_START && codePoint <= HALFWIDTH_KATAKANA_END);

const sjisDisplayWidth = (value: string): number =>
  Array.from(value).reduce((width: number, character: string): number => {
    const codePoint: number = character.codePointAt(0) ?? 0;
    return width + (isHalfWidthCodePoint(codePoint) ? HALF_WIDTH_UNIT : FULL_WIDTH_UNIT);
  }, 0);

const isPureHalfWidth = (value: string): boolean =>
  Array.from(value).every((character: string): boolean => {
    const codePoint: number = character.codePointAt(0) ?? 0;
    return isHalfWidthCodePoint(codePoint);
  });

const truncateToJvByteWidth = (value: string, byteWidth: number): string => {
  const characters: readonly string[] = Array.from(value);
  const truncateState = characters.reduce(
    (
      state: { readonly width: number; readonly chars: readonly string[]; readonly done: boolean },
      character: string,
    ): { readonly width: number; readonly chars: readonly string[]; readonly done: boolean } => {
      if (state.done) {
        return state;
      }
      const codePoint: number = character.codePointAt(0) ?? 0;
      const unit: number = isHalfWidthCodePoint(codePoint) ? HALF_WIDTH_UNIT : FULL_WIDTH_UNIT;
      if (state.width + unit > byteWidth) {
        return { ...state, done: true };
      }
      return {
        width: state.width + unit,
        chars: [...state.chars, character],
        done: false,
      };
    },
    { width: 0, chars: [], done: false },
  );
  return truncateState.chars.join("");
};

/** Pad a JV text field to its Shift-JIS byte width using ASCII or full-width spaces. */
export const padToJvByteWidth = (value: string, byteWidth: number): string => {
  const truncated: string = truncateToJvByteWidth(value, byteWidth);
  const currentWidth: number = sjisDisplayWidth(truncated);
  if (currentWidth >= byteWidth) {
    return truncated;
  }
  const useAsciiPad: boolean = truncated.length === 0 || isPureHalfWidth(truncated);
  const padChar: string = useAsciiPad ? ASCII_SPACE : FULL_WIDTH_SPACE;
  const padUnit: number = useAsciiPad ? HALF_WIDTH_UNIT : FULL_WIDTH_UNIT;
  const padCount: number = Math.floor((byteWidth - currentWidth) / padUnit);
  return `${truncated}${padChar.repeat(padCount)}`;
};

const padAscii = (value: string, length: number): string =>
  value.slice(0, length).padEnd(length, ASCII_SPACE);

const padNumber = (value: number, length: number): string => String(value).padStart(length, "0");

const compactDate = (date: string): string => date.replaceAll("-", "");

const abbreviatedPersonName = (value: string): string => {
  const dotIndex: number = value.lastIndexOf(".");
  const withoutInitial: string = dotIndex < 0 ? value : value.slice(dotIndex + 1);
  return padToJvByteWidth(
    withoutInitial.replace(PERSON_WHITESPACE_PATTERN, ""),
    PERSON_NAME_BYTE_WIDTH,
  );
};

const resolvedEntityName = (
  canonicalName: string | null | undefined,
  publishedName: string,
): string =>
  canonicalName === null || canonicalName === undefined || canonicalName.length === 0
    ? publishedName
    : canonicalName;

const resolvedPersonName = (
  canonicalName: string | null | undefined,
  publishedName: string,
): string =>
  canonicalName === null || canonicalName === undefined || canonicalName.length === 0
    ? abbreviatedPersonName(publishedName)
    : padToJvByteWidth(canonicalName, PERSON_NAME_BYTE_WIDTH);

const mapRace = (race: ParsedRace, storageIdentity: RaceStorageIdentity): JvdRaRow => {
  const date: string = compactDate(race.date);
  const year: string = date.slice(0, 4);
  const monthDay: string = date.slice(4);
  const runnerCount: string = padNumber(race.runners.length, 2);
  const trackKey: string = `${race.surface}・${race.direction}`;

  return {
    record_id: "RA",
    data_kubun: "B",
    data_sakusei_nengappi: date,
    kaisai_nen: year,
    kaisai_tsukihi: monthDay,
    keibajo_code: storageIdentity.venueCode,
    kaisai_kai: "00",
    kaisai_nichime: "00",
    race_bango: storageIdentity.raceNumber.padStart(2, "0"),
    yobi_code: "0",
    tokubetsu_kyoso_bango: "0000",
    kyosomei_hondai: padToJvByteWidth(race.raceName, RACE_NAME_BYTE_WIDTH),
    kyosomei_fukudai: FULL_WIDTH_SPACE.repeat(RACE_NAME_BYTE_WIDTH / FULL_WIDTH_UNIT),
    kyosomei_kakkonai: FULL_WIDTH_SPACE.repeat(RACE_NAME_BYTE_WIDTH / FULL_WIDTH_UNIT),
    kyosomei_hondai_eur: ASCII_SPACE.repeat(EUROPEAN_RACE_NAME_LENGTH),
    kyosomei_fukudai_eur: ASCII_SPACE.repeat(EUROPEAN_RACE_NAME_LENGTH),
    kyosomei_kakkonai_eur: ASCII_SPACE.repeat(EUROPEAN_RACE_NAME_LENGTH),
    kyosomei_ryakusho_10: padToJvByteWidth(race.raceName, RACE_NAME_SHORT_BYTE_WIDTH),
    kyosomei_ryakusho_6: padToJvByteWidth(race.raceName, RACE_NAME_MEDIUM_BYTE_WIDTH),
    kyosomei_ryakusho_3: padToJvByteWidth(race.raceName, RACE_NAME_MINIMUM_BYTE_WIDTH),
    kyosomei_kubun: "0",
    jusho_kaiji: "000",
    grade_code: race.grade === null ? " " : (GRADE_CODES.get(race.grade) ?? " "),
    grade_code_henkomae: " ",
    kyoso_shubetsu_code: "00",
    kyoso_kigo_code: "000",
    juryo_shubetsu_code: "0",
    kyoso_joken_code_2sai: "000",
    kyoso_joken_code_3sai: "000",
    kyoso_joken_code_4sai: "000",
    kyoso_joken_code_5sai_ijo: "000",
    kyoso_joken_code: "999",
    kyoso_joken_meisho: FULL_WIDTH_SPACE.repeat(RACE_NAME_BYTE_WIDTH / FULL_WIDTH_UNIT),
    kyori: padNumber(race.distanceMetres, DISTANCE_LENGTH),
    kyori_henkomae: "0000",
    track_code: TRACK_CODES.get(trackKey) ?? "00",
    track_code_henkomae: "00",
    course_kubun: "  ",
    course_kubun_henkomae: "  ",
    honshokin: "0".repeat(HONSHOKIN_LENGTH),
    honshokin_henkomae: "0".repeat(HONSHOKIN_BEFORE_LENGTH),
    fukashokin: "0".repeat(FUKASHOKIN_LENGTH),
    fukashokin_henkomae: "0000",
    hasso_jikoku: "0000",
    hasso_jikoku_henkomae: "0000",
    toroku_tosu: "00",
    shusso_tosu: runnerCount,
    nyusen_tosu: "00",
    tenko_code: "0",
    babajotai_code_shiba: "0",
    babajotai_code_dirt: "0",
    lap_time: "0".repeat(LAP_TIME_LENGTH),
    shogai_mile_time: "0000",
    zenhan_3f: "000",
    zenhan_4f: "000",
    kohan_3f: "000",
    kohan_4f: "000",
    corner_tsuka_juni_1: padAscii("00", CORNER_VALUE_LENGTH),
    corner_tsuka_juni_2: padAscii("00", CORNER_VALUE_LENGTH),
    corner_tsuka_juni_3: padAscii("00", CORNER_VALUE_LENGTH),
    corner_tsuka_juni_4: padAscii("00", CORNER_VALUE_LENGTH),
    record_koshin_kubun: "0",
  };
};

const mapRunner = ({ race, runner, storageIdentity, codes }: RunnerMappingInput): JvdSeRow => {
  const date: string = compactDate(race.date);
  const horseCode: string = codes?.horseRegistrationNumber ?? ZERO_HORSE_CODE;
  const jockeyCode: string = codes?.jockeyCode ?? ZERO_JOCKEY_CODE;
  const trainerCode: string = codes?.trainerCode ?? ZERO_TRAINER_CODE;
  const ownerCode: string = codes?.ownerCode ?? ZERO_OWNER_CODE;
  const tozaiShozokuCode: string = codes?.tozaiShozokuCode ?? TOZAI_SHOZOKU_CODE_PLACEHOLDER;
  const horseName: string = resolvedEntityName(codes?.horseName, runner.horseName);
  const ownerName: string = resolvedEntityName(codes?.ownerName, runner.owner);

  return {
    record_id: "SE",
    data_kubun: "B",
    data_sakusei_nengappi: date,
    kaisai_nen: date.slice(0, 4),
    kaisai_tsukihi: date.slice(4),
    keibajo_code: storageIdentity.venueCode,
    kaisai_kai: "00",
    kaisai_nichime: "00",
    race_bango: storageIdentity.raceNumber.padStart(2, "0"),
    wakuban: "0",
    umaban: padNumber(runner.horseNumber, 2),
    ketto_toroku_bango: horseCode,
    bamei: padToJvByteWidth(horseName, BAMEI_BYTE_WIDTH),
    umakigo_code: "00",
    seibetsu_code: SEX_CODES.get(runner.sex) ?? "0",
    hinshu_code: "1",
    moshoku_code: COAT_CODES.get(runner.coatColour) ?? "00",
    barei: padNumber(runner.age, 2),
    tozai_shozoku_code: tozaiShozokuCode,
    chokyoshi_code: trainerCode,
    chokyoshimei_ryakusho: resolvedPersonName(codes?.trainerName, runner.trainerAbbrev),
    banushi_code: ownerCode,
    banushimei: padToJvByteWidth(ownerName, BANUSHIMEI_BYTE_WIDTH),
    fukushoku_hyoji: FULL_WIDTH_SPACE.repeat(RACE_NAME_BYTE_WIDTH / FULL_WIDTH_UNIT),
    yobi_1: FULL_WIDTH_SPACE.repeat(RACE_NAME_BYTE_WIDTH / FULL_WIDTH_UNIT),
    futan_juryo: padNumber(Math.round(runner.weightCarriedKg * 10), 3),
    futan_juryo_henkomae: "000",
    blinker_shiyo_kubun: "0",
    yobi_2: "0",
    kishu_code: jockeyCode,
    kishu_code_henkomae: ZERO_JOCKEY_CODE,
    kishumei_ryakusho: resolvedPersonName(codes?.jockeyName, runner.jockeyAbbrev),
    kishumei_ryakusho_henkomae: FULL_WIDTH_SPACE.repeat(PERSON_NAME_BYTE_WIDTH / FULL_WIDTH_UNIT),
    kishu_minarai_code: "0",
    kishu_minarai_code_henkomae: "0",
    bataiju: "   ",
    zogen_fugo: " ",
    zogen_sa: "   ",
    ijo_kubun_code: "0",
    nyusen_juni: "00",
    kakutei_chakujun: "00",
    dochaku_kubun: "0",
    dochaku_tosu: "0",
    soha_time: "0000",
    chakusa_code_1: "   ",
    chakusa_code_2: "   ",
    chakusa_code_3: "   ",
    corner_1: "00",
    corner_2: "00",
    corner_3: "00",
    corner_4: "00",
    tansho_odds: "0000",
    tansho_ninkijun: "00",
    kakutoku_honshokin: "00000000",
    kakutoku_fukashokin: "00000000",
    yobi_3: "000",
    yobi_4: "000",
    kohan_4f: "000",
    kohan_3f: "000",
    aiteuma_joho_1: padAscii(ZERO_HORSE_CODE, AITEUMA_VALUE_LENGTH),
    aiteuma_joho_2: padAscii(ZERO_HORSE_CODE, AITEUMA_VALUE_LENGTH),
    aiteuma_joho_3: padAscii(ZERO_HORSE_CODE, AITEUMA_VALUE_LENGTH),
    time_sa: "0000",
    record_koshin_kubun: "0",
    mining_kubun: "0",
    yoso_soha_time: "00000",
    yoso_gosa_plus: "0000",
    yoso_gosa_minus: "0000",
    yoso_juni: "00",
    kyakushitsu_hantei: "0",
  };
};

export const mapJvdRows = ({ race, storageIdentity, resolvedCodes }: MapJvdRowsInput): JvdRows => ({
  race: mapRace(race, storageIdentity),
  runners: race.runners.map(
    (runner: ParsedRunner): JvdSeRow =>
      mapRunner({
        race,
        runner,
        storageIdentity,
        codes: resolvedCodes.get(runner.horseNumber),
      }),
  ),
});
