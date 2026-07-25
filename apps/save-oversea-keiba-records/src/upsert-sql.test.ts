// This test runs with Bun and Vitest.
import { expect, test } from "vitest";
import { mapJvdRows } from "./jvd-mapper";
import type { JvdSeRow, ParsedRace } from "./types";
import { buildJvdRaUpsert, buildJvdSeUpsert } from "./upsert-sql";

const RACE: ParsedRace = {
  raceName: "PARAMETER TEST",
  grade: "G1",
  date: "2026-07-25",
  venue: "Ascot",
  country: "GB",
  distanceMetres: 2390,
  surface: "芝",
  direction: "右",
  startTime: "23:35",
  runners: [
    {
      horseNumber: 1,
      gate: 7,
      horseName: "TEST HORSE",
      sex: "牡",
      age: 4,
      coatColour: "栗",
      weightCarriedKg: 61,
      jockeyAbbrev: "JOCKEY",
      trainerAbbrev: "TRAINER",
      trainerCountry: "GB",
      owner: "OWNER",
      winOdds: 2.5,
      popularity: 1,
      formRecord: "1.2.3.4",
      sire: "SIRE",
      dam: "DAM",
      damsire: "DAMSIRE",
    },
  ],
};

test("builds a fully parameterized jvd_ra upsert with the verified column order", () => {
  const rows = mapJvdRows({
    race: RACE,
    storageIdentity: { venueCode: "A6", raceNumber: "05" },
    resolvedCodes: new Map(),
  });
  const statement = buildJvdRaUpsert(rows.race);

  expect(Object.keys(rows.race)).toStrictEqual([
    "record_id",
    "data_kubun",
    "data_sakusei_nengappi",
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "kaisai_kai",
    "kaisai_nichime",
    "race_bango",
    "yobi_code",
    "tokubetsu_kyoso_bango",
    "kyosomei_hondai",
    "kyosomei_fukudai",
    "kyosomei_kakkonai",
    "kyosomei_hondai_eur",
    "kyosomei_fukudai_eur",
    "kyosomei_kakkonai_eur",
    "kyosomei_ryakusho_10",
    "kyosomei_ryakusho_6",
    "kyosomei_ryakusho_3",
    "kyosomei_kubun",
    "jusho_kaiji",
    "grade_code",
    "grade_code_henkomae",
    "kyoso_shubetsu_code",
    "kyoso_kigo_code",
    "juryo_shubetsu_code",
    "kyoso_joken_code_2sai",
    "kyoso_joken_code_3sai",
    "kyoso_joken_code_4sai",
    "kyoso_joken_code_5sai_ijo",
    "kyoso_joken_code",
    "kyoso_joken_meisho",
    "kyori",
    "kyori_henkomae",
    "track_code",
    "track_code_henkomae",
    "course_kubun",
    "course_kubun_henkomae",
    "honshokin",
    "honshokin_henkomae",
    "fukashokin",
    "fukashokin_henkomae",
    "hasso_jikoku",
    "hasso_jikoku_henkomae",
    "toroku_tosu",
    "shusso_tosu",
    "nyusen_tosu",
    "tenko_code",
    "babajotai_code_shiba",
    "babajotai_code_dirt",
    "lap_time",
    "shogai_mile_time",
    "zenhan_3f",
    "zenhan_4f",
    "kohan_3f",
    "kohan_4f",
    "corner_tsuka_juni_1",
    "corner_tsuka_juni_2",
    "corner_tsuka_juni_3",
    "corner_tsuka_juni_4",
    "record_koshin_kubun",
  ]);
  expect(statement.text.match(/^INSERT INTO jvd_ra \(/g)).toStrictEqual(["INSERT INTO jvd_ra ("]);
  expect(statement.text.match(/\$\d+/g)?.at(-1)).toBe("$62");
  expect(
    statement.text.match(/ON CONFLICT \(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango\)/g),
  ).toStrictEqual(["ON CONFLICT (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)"]);
  expect(
    statement.text.match(
      /hasso_jikoku = CASE WHEN NULLIF\(btrim\(excluded\.hasso_jikoku, ' 　'\), ''\) IS NULL OR btrim\(excluded\.hasso_jikoku, ' 　'\) ~ '\^0\+\$' THEN jvd_ra\.hasso_jikoku ELSE excluded\.hasso_jikoku END/g,
    ),
  ).toStrictEqual([
    "hasso_jikoku = CASE WHEN NULLIF(btrim(excluded.hasso_jikoku, ' 　'), '') IS NULL OR btrim(excluded.hasso_jikoku, ' 　') ~ '^0+$' THEN jvd_ra.hasso_jikoku ELSE excluded.hasso_jikoku END",
  ]);
  expect(statement.text.match(/DROP TABLE/g)).toStrictEqual(null);
  expect(statement.values).toHaveLength(62);
  expect(statement.values[0]).toBe("RA");
});

test("builds a fully parameterized non-destructive jvd_se upsert", () => {
  const rows = mapJvdRows({
    race: RACE,
    storageIdentity: { venueCode: "A6", raceNumber: "05" },
    resolvedCodes: new Map(),
  });
  const runner = rows.runners[0];
  if (runner === undefined) {
    throw new Error("Test fixture must include one runner.");
  }
  const statement = buildJvdSeUpsert(runner);

  expect(Object.keys(runner)).toStrictEqual([
    "record_id",
    "data_kubun",
    "data_sakusei_nengappi",
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "kaisai_kai",
    "kaisai_nichime",
    "race_bango",
    "wakuban",
    "umaban",
    "ketto_toroku_bango",
    "bamei",
    "umakigo_code",
    "seibetsu_code",
    "hinshu_code",
    "moshoku_code",
    "barei",
    "tozai_shozoku_code",
    "chokyoshi_code",
    "chokyoshimei_ryakusho",
    "banushi_code",
    "banushimei",
    "fukushoku_hyoji",
    "yobi_1",
    "futan_juryo",
    "futan_juryo_henkomae",
    "blinker_shiyo_kubun",
    "yobi_2",
    "kishu_code",
    "kishu_code_henkomae",
    "kishumei_ryakusho",
    "kishumei_ryakusho_henkomae",
    "kishu_minarai_code",
    "kishu_minarai_code_henkomae",
    "bataiju",
    "zogen_fugo",
    "zogen_sa",
    "ijo_kubun_code",
    "nyusen_juni",
    "kakutei_chakujun",
    "dochaku_kubun",
    "dochaku_tosu",
    "soha_time",
    "chakusa_code_1",
    "chakusa_code_2",
    "chakusa_code_3",
    "corner_1",
    "corner_2",
    "corner_3",
    "corner_4",
    "tansho_odds",
    "tansho_ninkijun",
    "kakutoku_honshokin",
    "kakutoku_fukashokin",
    "yobi_3",
    "yobi_4",
    "kohan_4f",
    "kohan_3f",
    "aiteuma_joho_1",
    "aiteuma_joho_2",
    "aiteuma_joho_3",
    "time_sa",
    "record_koshin_kubun",
    "mining_kubun",
    "yoso_soha_time",
    "yoso_gosa_plus",
    "yoso_gosa_minus",
    "yoso_juni",
    "kyakushitsu_hantei",
  ]);
  expect(statement.text.match(/^INSERT INTO jvd_se \(/g)).toStrictEqual(["INSERT INTO jvd_se ("]);
  expect(statement.text.match(/\$\d+/g)?.at(-1)).toBe("$70");
  expect(
    statement.text.match(
      /ON CONFLICT \(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban, ketto_toroku_bango\)/g,
    ),
  ).toStrictEqual([
    "ON CONFLICT (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban, ketto_toroku_bango)",
  ]);
  expect(
    statement.text.match(
      /kyakushitsu_hantei = CASE WHEN NULLIF\(btrim\(excluded\.kyakushitsu_hantei, ' 　'\), ''\) IS NULL OR btrim\(excluded\.kyakushitsu_hantei, ' 　'\) ~ '\^0\+\$' THEN jvd_se\.kyakushitsu_hantei ELSE excluded\.kyakushitsu_hantei END$/g,
    ),
  ).toStrictEqual([
    "kyakushitsu_hantei = CASE WHEN NULLIF(btrim(excluded.kyakushitsu_hantei, ' 　'), '') IS NULL OR btrim(excluded.kyakushitsu_hantei, ' 　') ~ '^0+$' THEN jvd_se.kyakushitsu_hantei ELSE excluded.kyakushitsu_hantei END",
  ]);
  expect(statement.values).toHaveLength(70);
  expect(statement.values[0]).toBe("SE");
});

test("incoming real owner code replaces an existing placeholder through the ELSE branch", () => {
  const rows = mapJvdRows({
    race: RACE,
    storageIdentity: { venueCode: "A6", raceNumber: "05" },
    resolvedCodes: new Map(),
  });
  const runner = rows.runners[0];
  if (runner === undefined) {
    throw new Error("Test fixture must include one runner.");
  }
  const incomingReal: JvdSeRow = { ...runner, banushi_code: "147803" };
  const statement = buildJvdSeUpsert(incomingReal);

  expect(
    statement.text.match(
      /banushi_code = CASE WHEN NULLIF\(btrim\(excluded\.banushi_code, ' 　'\), ''\) IS NULL OR btrim\(excluded\.banushi_code, ' 　'\) ~ '\^0\+\$' THEN jvd_se\.banushi_code ELSE excluded\.banushi_code END/g,
    ),
  ).toStrictEqual([
    "banushi_code = CASE WHEN NULLIF(btrim(excluded.banushi_code, ' 　'), '') IS NULL OR btrim(excluded.banushi_code, ' 　') ~ '^0+$' THEN jvd_se.banushi_code ELSE excluded.banushi_code END",
  ]);
  expect(statement.values.slice(21, 22)).toStrictEqual(["147803"]);
});

test("incoming placeholder owner code preserves an existing real value through the THEN branch", () => {
  const rows = mapJvdRows({
    race: RACE,
    storageIdentity: { venueCode: "A6", raceNumber: "05" },
    resolvedCodes: new Map(),
  });
  const runner = rows.runners[0];
  if (runner === undefined) {
    throw new Error("Test fixture must include one runner.");
  }
  const incomingPlaceholder: JvdSeRow = { ...runner, banushi_code: "000000" };
  const statement = buildJvdSeUpsert(incomingPlaceholder);

  expect(
    statement.text.match(
      /banushi_code = CASE WHEN NULLIF\(btrim\(excluded\.banushi_code, ' 　'\), ''\) IS NULL OR btrim\(excluded\.banushi_code, ' 　'\) ~ '\^0\+\$' THEN jvd_se\.banushi_code ELSE excluded\.banushi_code END/g,
    ),
  ).toStrictEqual([
    "banushi_code = CASE WHEN NULLIF(btrim(excluded.banushi_code, ' 　'), '') IS NULL OR btrim(excluded.banushi_code, ' 　') ~ '^0+$' THEN jvd_se.banushi_code ELSE excluded.banushi_code END",
  ]);
  expect(statement.values.slice(21, 22)).toStrictEqual(["000000"]);
});

test("incoming real owner code replaces a different existing real value through the ELSE branch", () => {
  const rows = mapJvdRows({
    race: RACE,
    storageIdentity: { venueCode: "A6", raceNumber: "05" },
    resolvedCodes: new Map(),
  });
  const runner = rows.runners[0];
  if (runner === undefined) {
    throw new Error("Test fixture must include one runner.");
  }
  const incomingReal: JvdSeRow = { ...runner, banushi_code: "147803" };
  const statement = buildJvdSeUpsert(incomingReal);

  expect(
    statement.text.match(
      /banushi_code = CASE WHEN NULLIF\(btrim\(excluded\.banushi_code, ' 　'\), ''\) IS NULL OR btrim\(excluded\.banushi_code, ' 　'\) ~ '\^0\+\$' THEN jvd_se\.banushi_code ELSE excluded\.banushi_code END/g,
    ),
  ).toStrictEqual([
    "banushi_code = CASE WHEN NULLIF(btrim(excluded.banushi_code, ' 　'), '') IS NULL OR btrim(excluded.banushi_code, ' 　') ~ '^0+$' THEN jvd_se.banushi_code ELSE excluded.banushi_code END",
  ]);
  expect(statement.values.slice(21, 22)).toStrictEqual(["147803"]);
});

test("incoming full-width-space-padded owner name preserves the existing name", () => {
  const rows = mapJvdRows({
    race: RACE,
    storageIdentity: { venueCode: "A6", raceNumber: "05" },
    resolvedCodes: new Map(),
  });
  const runner = rows.runners[0];
  if (runner === undefined) {
    throw new Error("Test fixture must include one runner.");
  }
  const incomingPlaceholder: JvdSeRow = { ...runner, banushimei: "　　　　　　　　" };
  const statement = buildJvdSeUpsert(incomingPlaceholder);

  expect(
    statement.text.match(
      /banushimei = CASE WHEN NULLIF\(btrim\(excluded\.banushimei, ' 　'\), ''\) IS NULL OR btrim\(excluded\.banushimei, ' 　'\) ~ '\^0\+\$' THEN jvd_se\.banushimei ELSE excluded\.banushimei END/g,
    ),
  ).toStrictEqual([
    "banushimei = CASE WHEN NULLIF(btrim(excluded.banushimei, ' 　'), '') IS NULL OR btrim(excluded.banushimei, ' 　') ~ '^0+$' THEN jvd_se.banushimei ELSE excluded.banushimei END",
  ]);
  expect(statement.values.slice(22, 23)).toStrictEqual(["　　　　　　　　"]);
});

test("rejects a runtime row that omits a fixed allowlisted column", () => {
  const rows = mapJvdRows({
    race: RACE,
    storageIdentity: { venueCode: "A6", raceNumber: "05" },
    resolvedCodes: new Map(),
  });
  const runner = rows.runners[0];
  if (runner === undefined) {
    throw new Error("Test fixture must include one runner.");
  }
  Reflect.deleteProperty(runner, "banushi_code");

  expect(() => buildJvdSeUpsert(runner)).toThrow("Missing required UPSERT column: banushi_code");
});
