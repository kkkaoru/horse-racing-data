import { expect, it } from "vitest";

import { buildRaceTrainingsQuery, normaliseRaceTrainingRow } from "./race-training";
import type { R2SqlCatalogConfig } from "./types";

const config: R2SqlCatalogConfig = {
  R2_SQL_ACCOUNT_ID: "account",
  R2_SQL_BUCKET_NAME: "bucket",
  R2_SQL_NAMESPACE: "pc_keiba",
  R2_SQL_TOKEN: "token",
};

it("builds the year-crossing official and netkeiba workout union", () => {
  const sql = buildRaceTrainingsQuery(config, {
    date: "20260101",
    keibajoCode: "05",
    raceBango: "01",
  });
  expect(sql).toMatch("FROM pc_keiba.jvd_se");
  expect(sql).toMatch("INNER JOIN pc_keiba.jvd_hc w");
  expect(sql).toMatch("INNER JOIN pc_keiba.jvd_wc w");
  expect(sql).toMatch("FROM pc_keiba.netkeiba_training_workouts n");
  expect(sql).toMatch("SELECT '20251218' AS start_date, '20260101' AS end_date");
  expect(sql).toMatch("w.chokyo_nengappi BETWEEN ww.start_date AND ww.end_date");
  expect(sql).toMatch("n.kaisai_nen = '2026'");
  expect(sql).toMatch("n.kaisai_tsukihi = '0101'");
  expect(sql).toMatch("n.keibajo_code = '05'");
  expect(sql).toMatch("n.race_bango = '01'");
});

it("gives official rows priority only for complete signatures", () => {
  const sql = buildRaceTrainingsQuery(config, {
    date: "20260822",
    keibajoCode: "01",
    raceBango: "11",
  });
  expect(sql).not.toMatch("official_rank");
  expect(sql).toMatch("'jra' AS training_data_source");
  expect(sql).toMatch("0 AS source_priority");
  expect(sql).toMatch("'netkeiba' AS training_data_source");
  expect(sql).toMatch("1 AS source_priority");
  expect(sql).toMatch(
    "ketto_toroku_bango, training_type, tracen_kubun, chokyo_nengappi, chokyo_jikoku",
  );
  expect(sql).toMatch("course, babamawari");
  expect(sql).toMatch("ORDER BY source_priority");
  expect(sql).toMatch("WHERE signature_rank = 1");
  expect(sql).not.toMatch("EXCLUDE");
});

it("keeps distinct workouts and emits a placeholder only for runners without actual rows", () => {
  const sql = buildRaceTrainingsQuery(config, {
    date: "20260822",
    keibajoCode: "01",
    raceBango: "12",
  });
  expect(sql).toMatch("combined_workouts AS");
  expect(sql).toMatch("source_priority FROM netkeiba_workouts");
  expect(sql).not.toMatch("SELECT *");
  expect(sql).toMatch("placeholder_rows AS");
  expect(sql).toMatch("'-' AS training_type");
  expect(sql).toMatch("'' AS chokyo_nengappi, '' AS chokyo_jikoku");
  expect(sql).toMatch("WHERE NOT EXISTS");
  expect(sql).toMatch("w.ketto_toroku_bango = r.ketto_toroku_bango");
});

it("rejects unsafe or invalid training filters", () => {
  expect(() =>
    buildRaceTrainingsQuery(
      { ...config, R2_SQL_NAMESPACE: "pc_keiba;drop" },
      {
        date: "20260822",
        keibajoCode: "01",
        raceBango: "01",
      },
    ),
  ).toThrow("R2_SQL_NAMESPACE must be an unquoted SQL identifier");
  expect(() =>
    buildRaceTrainingsQuery(config, {
      date: "2026-08-22",
      keibajoCode: "01",
      raceBango: "01",
    }),
  ).toThrow("date must match YYYYMMDD");
  expect(() =>
    buildRaceTrainingsQuery(config, {
      date: "20260230",
      keibajoCode: "01",
      raceBango: "01",
    }),
  ).toThrow("date must be a valid calendar date");
  expect(() =>
    buildRaceTrainingsQuery(config, {
      date: "20260822",
      keibajoCode: "1",
      raceBango: "01",
    }),
  ).toThrow("keibajoCode must contain two digits");
  expect(() =>
    buildRaceTrainingsQuery(config, {
      date: "20260822",
      keibajoCode: "01",
      raceBango: "1",
    }),
  ).toThrow("raceBango must contain two digits");
});

it("normalises a netkeiba workout to the Training contract", () => {
  expect(
    normaliseRaceTrainingRow({
      bamei: "テストホース",
      chokyo_jikoku: "0630",
      chokyo_nengappi: "20260820",
      premium_comment_text: "好調",
      premium_evaluation_grade: "A",
      premium_evaluation_text: "上々",
      premium_workout_index: "2",
      time_gokei_4f: 512,
      training_data_source: "netkeiba",
      training_rider_name: "調教助手",
      training_type: "ウッド",
      umaban: 7,
    }),
  ).toStrictEqual({
    babamawari: null,
    bamei: "テストホース",
    chokyoJikoku: "0630",
    chokyoNengappi: "20260820",
    course: null,
    currentJockeyName: null,
    lapTime10f: null,
    lapTime1f: null,
    lapTime2f: null,
    lapTime3f: null,
    lapTime4f: null,
    lapTime5f: null,
    lapTime6f: null,
    lapTime7f: null,
    lapTime8f: null,
    lapTime9f: null,
    premiumCommentText: "好調",
    premiumEvaluationGrade: "A",
    premiumEvaluationText: "上々",
    premiumWorkoutIndex: 2,
    timeGokei10f: null,
    timeGokei2f: null,
    timeGokei3f: null,
    timeGokei4f: "512",
    timeGokei5f: null,
    timeGokei6f: null,
    timeGokei7f: null,
    timeGokei8f: null,
    timeGokei9f: null,
    tracenKubun: null,
    trainerName: null,
    trainingDataSource: "netkeiba",
    trainingRiderName: "調教助手",
    trainingType: "ウッド",
    umaban: "7",
  });
});

it("normalises placeholders without a premium index and validates required row fields", () => {
  expect(
    normaliseRaceTrainingRow({
      chokyo_jikoku: "",
      chokyo_nengappi: "",
      premium_workout_index: "invalid",
      training_data_source: "jra",
      training_type: "-",
      umaban: "03",
    }),
  ).toMatchObject({
    chokyoJikoku: "",
    chokyoNengappi: "",
    trainingDataSource: "jra",
    trainingType: "-",
    umaban: "03",
  });
  expect(() =>
    normaliseRaceTrainingRow({
      chokyo_nengappi: "",
      training_data_source: "jra",
      training_type: "-",
    }),
  ).toThrow("R2 SQL row is missing chokyo_jikoku");
  expect(() =>
    normaliseRaceTrainingRow({
      chokyo_jikoku: "",
      chokyo_nengappi: "",
      training_data_source: "other",
      training_type: "-",
    }),
  ).toThrow("R2 SQL row has invalid training_data_source: other");
});
