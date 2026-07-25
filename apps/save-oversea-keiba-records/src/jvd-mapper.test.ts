// This test runs with Bun and Vitest.
import { expect, test } from "vitest";
import { mapJvdRows, padToJvByteWidth } from "./jvd-mapper";
import type { ParsedRace, ResolvedEntityCodes } from "./types";

const FULL_WIDTH_SPACE: string = "　";
const ASCII_SPACE: string = " ";
const BAMEI_BYTE_WIDTH: number = 36;
const BANUSHIMEI_BYTE_WIDTH: number = 64;
const RACE_NAME_BYTE_WIDTH: number = 60;

const RACE: ParsedRace = {
  raceName: "テストステークス",
  grade: "G1",
  date: "2026-07-25",
  venue: "アスコット",
  country: "イギリス",
  distanceMetres: 2390,
  surface: "芝",
  direction: "右",
  startTime: "23:35",
  runners: [
    {
      horseNumber: 1,
      gate: 7,
      horseName: "テストホース",
      sex: "せん",
      age: 5,
      coatColour: "鹿",
      weightCarriedKg: 61,
      jockeyAbbrev: "M.ジョッキー",
      trainerAbbrev: "F.トレーナー",
      trainerCountry: "FR",
      owner: "TEST OWNER",
      winOdds: 1.6,
      popularity: 1,
      formRecord: "10.5.1.1",
      sire: "Test Sire",
      dam: "Test Dam",
      damsire: "Test Damsire",
    },
    {
      horseNumber: 2,
      gate: 3,
      horseName: "サンプルホース",
      sex: "unknown",
      age: 4,
      coatColour: "unknown",
      weightCarriedKg: 56.5,
      jockeyAbbrev: "C.キーン",
      trainerAbbrev: "手塚 貴久",
      trainerCountry: "",
      owner: "SAMPLE OWNER",
      winOdds: null,
      popularity: null,
      formRecord: "2.3.4.5",
      sire: "Sample Sire",
      dam: "Sample Dam",
      damsire: "Sample Damsire",
    },
  ],
};

const RESOLVED_CODES: ReadonlyMap<number, ResolvedEntityCodes> = new Map([
  [
    1,
    {
      horseRegistrationNumber: "2021190001",
      horseName: "マスターホース",
      jockeyCode: "05504",
      jockeyName: "騎手名",
      trainerCode: "05701",
      trainerName: "調教師",
      ownerCode: "166803",
      ownerName: "吉田　照哉",
      tozaiShozokuCode: "4",
    },
  ],
]);

test("maps parsed data and resolved codes to JV rows with fixed-width placeholders", () => {
  const rows = mapJvdRows({
    race: RACE,
    storageIdentity: { venueCode: "A6", raceNumber: "5" },
    resolvedCodes: RESOLVED_CODES,
  });

  expect({
    record: rows.race.record_id,
    dataKind: rows.race.data_kubun,
    created: rows.race.data_sakusei_nengappi,
    year: rows.race.kaisai_nen,
    monthDay: rows.race.kaisai_tsukihi,
    venue: rows.race.keibajo_code,
    raceNumber: rows.race.race_bango,
    grade: rows.race.grade_code,
    distance: rows.race.kyori,
    track: rows.race.track_code,
    start: rows.race.hasso_jikoku,
    runnerCount: rows.race.shusso_tosu,
  }).toStrictEqual({
    record: "RA",
    dataKind: "B",
    created: "20260725",
    year: "2026",
    monthDay: "0725",
    venue: "A6",
    raceNumber: "05",
    grade: "A",
    distance: "2390",
    track: "17",
    start: "0000",
    runnerCount: "02",
  });
  expect(rows.race.kyosomei_hondai.trim()).toBe("テストステークス");
  expect(rows.race.kyosomei_hondai.length).toBe(30);
  expect([
    rows.race.kyosomei_ryakusho_10,
    rows.race.kyosomei_ryakusho_6,
    rows.race.kyosomei_ryakusho_3,
  ]).toStrictEqual(["テストステークス　　", "テストステー", "テスト"]);
  expect(Object.keys(rows.race)).toHaveLength(62);

  expect({
    horseNumber: rows.runners[0]?.umaban,
    gate: rows.runners[0]?.wakuban,
    horseCode: rows.runners[0]?.ketto_toroku_bango,
    sex: rows.runners[0]?.seibetsu_code,
    coat: rows.runners[0]?.moshoku_code,
    age: rows.runners[0]?.barei,
    trainerCode: rows.runners[0]?.chokyoshi_code,
    ownerCode: rows.runners[0]?.banushi_code,
    affiliation: rows.runners[0]?.tozai_shozoku_code,
    carriedWeight: rows.runners[0]?.futan_juryo,
    jockeyCode: rows.runners[0]?.kishu_code,
    odds: rows.runners[0]?.tansho_odds,
    popularity: rows.runners[0]?.tansho_ninkijun,
    finish: rows.runners[0]?.kakutei_chakujun,
  }).toStrictEqual({
    horseNumber: "01",
    gate: "0",
    horseCode: "2021190001",
    sex: "3",
    coat: "03",
    age: "05",
    trainerCode: "05701",
    ownerCode: "166803",
    affiliation: "4",
    carriedWeight: "610",
    jockeyCode: "05504",
    odds: "0000",
    popularity: "00",
    finish: "00",
  });
  expect(rows.runners[0]?.bamei.trim()).toBe("マスターホース");
  expect(rows.runners[0]?.bamei.length).toBe(18);
  expect(rows.runners[0]?.bamei.endsWith(FULL_WIDTH_SPACE)).toBe(true);
  expect(rows.runners[0]?.banushimei).toBe(`吉田　照哉${FULL_WIDTH_SPACE.repeat(27)}`);
  expect(rows.runners[0]?.banushimei.length).toBe(32);
  expect(rows.runners[0]?.kishumei_ryakusho).toBe("騎手名　");
  expect(rows.runners[0]?.chokyoshimei_ryakusho).toBe("調教師　");
  expect(Object.keys(rows.runners[0] ?? {})).toHaveLength(70);

  expect({
    horseCode: rows.runners[1]?.ketto_toroku_bango,
    sex: rows.runners[1]?.seibetsu_code,
    coat: rows.runners[1]?.moshoku_code,
    trainerCode: rows.runners[1]?.chokyoshi_code,
    ownerCode: rows.runners[1]?.banushi_code,
    affiliation: rows.runners[1]?.tozai_shozoku_code,
    jockeyCode: rows.runners[1]?.kishu_code,
    jockeyName: rows.runners[1]?.kishumei_ryakusho,
    trainerName: rows.runners[1]?.chokyoshimei_ryakusho,
  }).toStrictEqual({
    horseCode: "0000000000",
    sex: "0",
    coat: "00",
    trainerCode: "00000",
    ownerCode: "000000",
    affiliation: "0",
    jockeyCode: "00000",
    jockeyName: "キーン　",
    trainerName: "手塚貴久",
  });
});

test("uses zero placeholders for explicitly unresolved codes and unsupported race metadata", () => {
  const unresolvedCodes: ReadonlyMap<number, ResolvedEntityCodes> = new Map([
    [
      1,
      {
        horseRegistrationNumber: null,
        horseName: null,
        jockeyCode: null,
        jockeyName: null,
        trainerCode: null,
        trainerName: null,
        ownerCode: null,
        ownerName: null,
        tozaiShozokuCode: null,
      },
    ],
  ]);
  const unsupportedRace: ParsedRace = {
    ...RACE,
    grade: null,
    direction: "左",
  };
  const rows = mapJvdRows({
    race: unsupportedRace,
    storageIdentity: { venueCode: "A6", raceNumber: "05" },
    resolvedCodes: unresolvedCodes,
  });

  expect({
    grade: rows.race.grade_code,
    track: rows.race.track_code,
    horseCode: rows.runners[0]?.ketto_toroku_bango,
    jockeyCode: rows.runners[0]?.kishu_code,
    trainerCode: rows.runners[0]?.chokyoshi_code,
    ownerCode: rows.runners[0]?.banushi_code,
    affiliation: rows.runners[0]?.tozai_shozoku_code,
  }).toStrictEqual({
    grade: " ",
    track: "00",
    horseCode: "0000000000",
    jockeyCode: "00000",
    trainerCode: "00000",
    ownerCode: "000000",
    affiliation: "0",
  });
});

test("emits resolved tozai affiliation and falls back to placeholder when affiliation is absent", () => {
  const codes: ReadonlyMap<number, ResolvedEntityCodes> = new Map([
    [
      1,
      {
        horseRegistrationNumber: "2021190001",
        horseName: null,
        jockeyCode: "05504",
        jockeyName: null,
        trainerCode: "01038",
        trainerName: null,
        ownerCode: "166803",
        ownerName: null,
        tozaiShozokuCode: "1",
      },
    ],
    [
      2,
      {
        horseRegistrationNumber: "2020190005",
        horseName: null,
        jockeyCode: "05271",
        jockeyName: null,
        trainerCode: "01073",
        trainerName: null,
        ownerCode: "000000",
        ownerName: null,
        tozaiShozokuCode: null,
      },
    ],
  ]);
  const rows = mapJvdRows({
    race: RACE,
    storageIdentity: { venueCode: "A6", raceNumber: "05" },
    resolvedCodes: codes,
  });

  expect(rows.runners[0]?.tozai_shozoku_code).toBe("1");
  expect(rows.runners[1]?.tozai_shozoku_code).toBe("0");
});

test("keeps published names when entity resolution or a master name is unavailable", () => {
  const codes: ReadonlyMap<number, ResolvedEntityCodes> = new Map([
    [
      1,
      {
        horseRegistrationNumber: "0000000000",
        horseName: "",
        jockeyCode: "00000",
        jockeyName: "",
        trainerCode: "00000",
        trainerName: "",
        ownerCode: "000000",
        ownerName: "",
        tozaiShozokuCode: "0",
      },
    ],
  ]);
  const rows = mapJvdRows({
    race: RACE,
    storageIdentity: { venueCode: "A6", raceNumber: "05" },
    resolvedCodes: codes,
  });

  expect(rows.runners[0]?.bamei.trim()).toBe("テストホース");
  expect(rows.runners[0]?.kishumei_ryakusho).toBe("ジョッキ");
  expect(rows.runners[0]?.chokyoshimei_ryakusho).toBe("トレーナ");
  expect(rows.runners[0]?.banushimei).toBe(`TEST OWNER${ASCII_SPACE.repeat(54)}`);
});

test("pads Japanese names to half the JV byte width with full-width spaces", () => {
  expect(padToJvByteWidth("カランダガン", BAMEI_BYTE_WIDTH)).toBe(
    `カランダガン${FULL_WIDTH_SPACE.repeat(12)}`,
  );
  expect(padToJvByteWidth("カランダガン", BAMEI_BYTE_WIDTH).length).toBe(18);
  expect(padToJvByteWidth("アガ・カーン・スタッズ", BANUSHIMEI_BYTE_WIDTH)).toBe(
    `アガ・カーン・スタッズ${FULL_WIDTH_SPACE.repeat(21)}`,
  );
  expect(padToJvByteWidth("アガ・カーン・スタッズ", BANUSHIMEI_BYTE_WIDTH).length).toBe(32);
  expect(padToJvByteWidth("テストステークス", RACE_NAME_BYTE_WIDTH).length).toBe(30);
});

test("pads ASCII names to the full JV byte width with ASCII spaces", () => {
  expect(padToJvByteWidth("COMMANDMENT", BAMEI_BYTE_WIDTH)).toBe(
    `COMMANDMENT${ASCII_SPACE.repeat(25)}`,
  );
  expect(padToJvByteWidth("COMMANDMENT", BAMEI_BYTE_WIDTH).length).toBe(36);
  expect(padToJvByteWidth("JUDDMONTE", BANUSHIMEI_BYTE_WIDTH)).toBe(
    `JUDDMONTE${ASCII_SPACE.repeat(55)}`,
  );
  expect(padToJvByteWidth("JUDDMONTE", BANUSHIMEI_BYTE_WIDTH).length).toBe(64);
});

test("leaves a name already at full JV width unchanged", () => {
  const fullJapaneseBamei: string = `マスカレードボール${FULL_WIDTH_SPACE.repeat(9)}`;
  expect(fullJapaneseBamei.length).toBe(18);
  expect(padToJvByteWidth(fullJapaneseBamei, BAMEI_BYTE_WIDTH)).toBe(fullJapaneseBamei);

  const fullAsciiBamei: string = `GOLDEN TEMPO${ASCII_SPACE.repeat(24)}`;
  expect(fullAsciiBamei.length).toBe(36);
  expect(padToJvByteWidth(fullAsciiBamei, BAMEI_BYTE_WIDTH)).toBe(fullAsciiBamei);

  const overlongJapanese: string = "ベンヴェヌートチェッリーニ追加文字一二";
  expect(overlongJapanese.length).toBe(19);
  expect(padToJvByteWidth(overlongJapanese, BAMEI_BYTE_WIDTH)).toBe(
    "ベンヴェヌートチェッリーニ追加文字一",
  );
  expect(padToJvByteWidth(overlongJapanese, BAMEI_BYTE_WIDTH).length).toBe(18);
});
