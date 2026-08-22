import { expect, it } from "vitest";

import {
  buildHorseRaceResultsQuery,
  normaliseHorseRaceResultRow,
  uniqueHorseRaceResults,
} from "./horse-race-results";
import type { HorseRaceResultRow, R2SqlCatalogConfig } from "./types";

const config: R2SqlCatalogConfig = {
  R2_SQL_ACCOUNT_ID: "account",
  R2_SQL_BUCKET_NAME: "bucket",
  R2_SQL_NAMESPACE: "pc_keiba",
  R2_SQL_TOKEN: "token",
};

const sampleRow: HorseRaceResultRow = {
  babajotaiCodeDirt: "01",
  babajotaiCodeShiba: "01",
  bamei: "Deep",
  banushimei: "Owner",
  barei: "04",
  bataiju: "480",
  blinkerShiyoKubun: "1",
  chokyoshimeiRyakusho: "Trainer",
  corner1: "01",
  corner2: "02",
  corner3: "03",
  corner4: "04",
  currentBarei: "05",
  currentJockey: "Take",
  currentSeibetsuCode: "1",
  currentUmaban: "01",
  futanJuryo: "550",
  gradeCode: "A",
  hassoJikoku: "1510",
  juryoShubetsuCode: "3",
  kaisaiNen: "2025",
  kaisaiTsukihi: "0715",
  kakuteiChakujun: "01",
  keibajoCode: "05",
  kettoTorokuBango: "2023100001",
  kishumeiRyakusho: "Take",
  kohan3f: "351",
  kyori: "1600",
  kyosoJokenCode: "005",
  kyosoJokenMeisho: "3勝",
  kyosoKigoCode: "A",
  kyosomeiFukudai: null,
  kyosomeiHondai: "テスト",
  kyosomeiKakkonai: null,
  kyosoShubetsuCode: "12",
  raceBango: "11",
  seibetsuCode: "1",
  shussoTosu: "16",
  sohaTime: "1345",
  tanshoNinkijun: "01",
  tanshoOdds: "0250",
  tenkoCode: "1",
  timeSa: "002",
  trackCode: "17",
  umaban: "03",
  wakuban: "2",
  zogenFugo: "+",
  zogenSa: "004",
};

it("builds domestic JRA+NAR history SQL for sourceScope=all", () => {
  const sql = buildHorseRaceResultsQuery(config, {
    date: "20260715",
    keibajoCode: "05",
    raceBango: "01",
    source: "jra",
    sourceScope: "all",
  });
  expect(sql).toMatch("FROM pc_keiba.jvd_se");
  expect(sql).toMatch("FROM pc_keiba.jvd_se se");
  expect(sql).toMatch("INNER JOIN pc_keiba.jvd_ra ra");
  expect(sql).toMatch("FROM pc_keiba.nvd_se se");
  expect(sql).toMatch("INNER JOIN pc_keiba.nvd_ra ra");
  expect(sql).toMatch("UNION ALL");
  expect(sql).toMatch("kaisai_nen = '2026'");
  expect(sql).toMatch("kaisai_tsukihi = '0715'");
  expect(sql).toMatch("keibajo_code = '05'");
  expect(sql).toMatch("race_bango = '01'");
  expect(sql).toMatch("concat(kaisai_nen, kaisai_tsukihi) < '20260715'");
  expect(sql).toMatch("replace(btrim(coalesce(ketto_toroku_bango, '')), '0', '') <> ''");
  expect(sql).toMatch("blinker_shiyo_kubun");
  expect(sql).not.toMatch("jsonb_agg");
  expect(sql).not.toMatch("regexp_replace");
  expect(sql).not.toMatch("oversea_");
});

it("uses only JRA history when sourceScope=jra", () => {
  const sql = buildHorseRaceResultsQuery(config, {
    date: "20260715",
    keibajoCode: "05",
    raceBango: "01",
    source: "jra",
    sourceScope: "jra",
  });
  expect(sql).toMatch("FROM pc_keiba.jvd_se se");
  expect(sql).toMatch("INNER JOIN pc_keiba.jvd_ra ra");
  expect(sql).not.toMatch("nvd_se");
  expect(sql).not.toMatch("UNION ALL");
});

it("uses NAR current runners and NAR-only history when both source and sourceScope are nar", () => {
  const sql = buildHorseRaceResultsQuery(config, {
    date: "20260715",
    keibajoCode: "83",
    raceBango: "09",
    source: "nar",
    sourceScope: "nar",
  });
  expect(sql).toMatch("FROM pc_keiba.nvd_se");
  expect(sql).toMatch("FROM pc_keiba.nvd_se se");
  expect(sql).toMatch("INNER JOIN pc_keiba.nvd_ra ra");
  expect(sql).not.toMatch("jvd_se");
  expect(sql).not.toMatch("UNION ALL");
});

it("keeps JRA current identity with NAR-only history when sourceScope=nar", () => {
  const sql = buildHorseRaceResultsQuery(config, {
    date: "20260715",
    keibajoCode: "05",
    raceBango: "01",
    source: "jra",
    sourceScope: "nar",
  });
  expect(sql).toMatch("FROM pc_keiba.jvd_se");
  expect(sql).toMatch("FROM pc_keiba.nvd_se se");
  expect(sql).toMatch("INNER JOIN pc_keiba.nvd_ra ra");
  expect(sql).not.toMatch("jvd_ra");
});

it("rejects unsafe namespace, dates, and codes", () => {
  expect(() =>
    buildHorseRaceResultsQuery(
      { ...config, R2_SQL_NAMESPACE: "pc_keiba;drop" },
      {
        date: "20260715",
        keibajoCode: "05",
        raceBango: "01",
        source: "jra",
        sourceScope: "all",
      },
    ),
  ).toThrow("R2_SQL_NAMESPACE must be an unquoted SQL identifier");
  expect(() =>
    buildHorseRaceResultsQuery(config, {
      date: "2026-07-15",
      keibajoCode: "05",
      raceBango: "01",
      source: "jra",
      sourceScope: "all",
    }),
  ).toThrow("date must match YYYYMMDD");
  expect(() =>
    buildHorseRaceResultsQuery(config, {
      date: "20260231",
      keibajoCode: "05",
      raceBango: "01",
      source: "jra",
      sourceScope: "all",
    }),
  ).toThrow("date must be a valid calendar date");
  expect(() =>
    buildHorseRaceResultsQuery(config, {
      date: "20260715",
      keibajoCode: "5",
      raceBango: "01",
      source: "jra",
      sourceScope: "all",
    }),
  ).toThrow("keibajoCode must contain two digits");
  expect(() =>
    buildHorseRaceResultsQuery(config, {
      date: "20260715",
      keibajoCode: "05",
      raceBango: "1;",
      source: "jra",
      sourceScope: "all",
    }),
  ).toThrow("raceBango must contain two digits");
});

it("maps camelCase HorseRaceResult fields from snake_case R2 rows", () => {
  expect(
    normaliseHorseRaceResultRow({
      babajotai_code_dirt: "01",
      babajotai_code_shiba: "01",
      bamei: "Deep",
      banushimei: "Owner",
      barei: 4,
      bataiju: 480n,
      blinker_shiyo_kubun: true,
      chokyoshimei_ryakusho: "Trainer",
      corner_1: "01",
      corner_2: "02",
      corner_3: "03",
      corner_4: "04",
      current_barei: "05",
      current_jockey: "Take",
      current_seibetsu_code: "1",
      current_umaban: "01",
      futan_juryo: "550",
      grade_code: "A",
      hasso_jikoku: "1510",
      juryo_shubetsu_code: "3",
      kaisai_nen: "2025",
      kaisai_tsukihi: "0715",
      kakutei_chakujun: "01",
      keibajo_code: "05",
      ketto_toroku_bango: "2023100001",
      kishumei_ryakusho: "Take",
      kohan_3f: "351",
      kyori: "1600",
      kyoso_joken_code: "005",
      kyoso_joken_meisho: "3勝",
      kyoso_kigo_code: "A",
      kyosomei_fukudai: null,
      kyosomei_hondai: "テスト",
      kyosomei_kakkonai: { note: "x" },
      kyoso_shubetsu_code: "12",
      race_bango: 11,
      seibetsu_code: "1",
      shusso_tosu: "16",
      soha_time: "1345",
      tansho_ninkijun: "01",
      tansho_odds: "0250",
      tenko_code: "1",
      time_sa: "002",
      track_code: "17",
      umaban: "03",
      wakuban: "2",
      zogen_fugo: "+",
      zogen_sa: "004",
    }),
  ).toStrictEqual({
    babajotaiCodeDirt: "01",
    babajotaiCodeShiba: "01",
    bamei: "Deep",
    banushimei: "Owner",
    barei: "4",
    bataiju: "480",
    blinkerShiyoKubun: "true",
    chokyoshimeiRyakusho: "Trainer",
    corner1: "01",
    corner2: "02",
    corner3: "03",
    corner4: "04",
    currentBarei: "05",
    currentJockey: "Take",
    currentSeibetsuCode: "1",
    currentUmaban: "01",
    futanJuryo: "550",
    gradeCode: "A",
    hassoJikoku: "1510",
    juryoShubetsuCode: "3",
    kaisaiNen: "2025",
    kaisaiTsukihi: "0715",
    kakuteiChakujun: "01",
    keibajoCode: "05",
    kettoTorokuBango: "2023100001",
    kishumeiRyakusho: "Take",
    kohan3f: "351",
    kyori: "1600",
    kyosoJokenCode: "005",
    kyosoJokenMeisho: "3勝",
    kyosoKigoCode: "A",
    kyosomeiFukudai: null,
    kyosomeiHondai: "テスト",
    kyosomeiKakkonai: '{"note":"x"}',
    kyosoShubetsuCode: "12",
    raceBango: "11",
    seibetsuCode: "1",
    shussoTosu: "16",
    sohaTime: "1345",
    tanshoNinkijun: "01",
    tanshoOdds: "0250",
    tenkoCode: "1",
    timeSa: "002",
    trackCode: "17",
    umaban: "03",
    wakuban: "2",
    zogenFugo: "+",
    zogenSa: "004",
  });
});

it("rejects rows missing race identity fields", () => {
  expect(() =>
    normaliseHorseRaceResultRow({
      kaisai_nen: "",
      kaisai_tsukihi: "0715",
      keibajo_code: "05",
      race_bango: "01",
    }),
  ).toThrow("R2 SQL row is missing kaisai_nen");
  expect(() =>
    normaliseHorseRaceResultRow({
      kaisai_nen: "2025",
      kaisai_tsukihi: null,
      keibajo_code: "05",
      race_bango: "01",
    }),
  ).toThrow("R2 SQL row is missing kaisai_tsukihi");
  expect(() =>
    normaliseHorseRaceResultRow({
      kaisai_nen: "2025",
      kaisai_tsukihi: "0715",
      keibajo_code: "",
      race_bango: "01",
    }),
  ).toThrow("R2 SQL row is missing keibajo_code");
  expect(() =>
    normaliseHorseRaceResultRow({
      kaisai_nen: "2025",
      kaisai_tsukihi: "0715",
      keibajo_code: "05",
      race_bango: null,
    }),
  ).toThrow("R2 SQL row is missing race_bango");
});

it("keeps the first row for a duplicated horse-race key", () => {
  expect(
    uniqueHorseRaceResults([
      sampleRow,
      { ...sampleRow, bamei: "Duplicate" },
      { ...sampleRow, currentUmaban: "02", kaisaiNen: "2024" },
    ]),
  ).toStrictEqual([
    {
      babajotaiCodeDirt: "01",
      babajotaiCodeShiba: "01",
      bamei: "Deep",
      banushimei: "Owner",
      barei: "04",
      bataiju: "480",
      blinkerShiyoKubun: "1",
      chokyoshimeiRyakusho: "Trainer",
      corner1: "01",
      corner2: "02",
      corner3: "03",
      corner4: "04",
      currentBarei: "05",
      currentJockey: "Take",
      currentSeibetsuCode: "1",
      currentUmaban: "01",
      futanJuryo: "550",
      gradeCode: "A",
      hassoJikoku: "1510",
      juryoShubetsuCode: "3",
      kaisaiNen: "2025",
      kaisaiTsukihi: "0715",
      kakuteiChakujun: "01",
      keibajoCode: "05",
      kettoTorokuBango: "2023100001",
      kishumeiRyakusho: "Take",
      kohan3f: "351",
      kyori: "1600",
      kyosoJokenCode: "005",
      kyosoJokenMeisho: "3勝",
      kyosoKigoCode: "A",
      kyosomeiFukudai: null,
      kyosomeiHondai: "テスト",
      kyosomeiKakkonai: null,
      kyosoShubetsuCode: "12",
      raceBango: "11",
      seibetsuCode: "1",
      shussoTosu: "16",
      sohaTime: "1345",
      tanshoNinkijun: "01",
      tanshoOdds: "0250",
      tenkoCode: "1",
      timeSa: "002",
      trackCode: "17",
      umaban: "03",
      wakuban: "2",
      zogenFugo: "+",
      zogenSa: "004",
    },
    {
      babajotaiCodeDirt: "01",
      babajotaiCodeShiba: "01",
      bamei: "Deep",
      banushimei: "Owner",
      barei: "04",
      bataiju: "480",
      blinkerShiyoKubun: "1",
      chokyoshimeiRyakusho: "Trainer",
      corner1: "01",
      corner2: "02",
      corner3: "03",
      corner4: "04",
      currentBarei: "05",
      currentJockey: "Take",
      currentSeibetsuCode: "1",
      currentUmaban: "02",
      futanJuryo: "550",
      gradeCode: "A",
      hassoJikoku: "1510",
      juryoShubetsuCode: "3",
      kaisaiNen: "2024",
      kaisaiTsukihi: "0715",
      kakuteiChakujun: "01",
      keibajoCode: "05",
      kettoTorokuBango: "2023100001",
      kishumeiRyakusho: "Take",
      kohan3f: "351",
      kyori: "1600",
      kyosoJokenCode: "005",
      kyosoJokenMeisho: "3勝",
      kyosoKigoCode: "A",
      kyosomeiFukudai: null,
      kyosomeiHondai: "テスト",
      kyosomeiKakkonai: null,
      kyosoShubetsuCode: "12",
      raceBango: "11",
      seibetsuCode: "1",
      shussoTosu: "16",
      sohaTime: "1345",
      tanshoNinkijun: "01",
      tanshoOdds: "0250",
      tenkoCode: "1",
      timeSa: "002",
      trackCode: "17",
      umaban: "03",
      wakuban: "2",
      zogenFugo: "+",
      zogenSa: "004",
    },
  ]);
});
