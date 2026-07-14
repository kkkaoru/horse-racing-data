import { expect, it } from "vitest";

import { normaliseRunningStyleRows } from "./running-style-response";

const row = (): Record<string, unknown> => ({
  bamei: "  テストホース  ",
  career_win_rate: "0.25",
  category: "jra",
  grade_code: null,
  kaisai_nen: 2026,
  kaisai_tsukihi: "0715",
  keibajo_code: "5",
  ketto_toroku_bango: "2023100001",
  kohan3f_avg_5: Number.NaN,
  kyori: "1600",
  kyoso_joken_code: "010",
  nar_subclass: undefined,
  past_corner_1_norm_avg_5: 0.2,
  past_first_3f_avg_5: true,
  past_nige_rate_self: 0.1,
  past_oikomi_rate_self: 0.4,
  past_sashi_rate_self: 0.3,
  past_senkou_rate_self: 0.2,
  race_bango: 1,
  race_date: "20260715",
  self_field_feature: 99,
  shusso_tosu: 18,
  source: "jra",
  speed_index_avg_5: "bad",
  speed_index_best_5: "1.5",
  target_corner_1_norm: 0.4,
  track_code: "11",
  umaban: "7",
});

it("normalises flat R2 SQL rows into the catalog consumer contract", () => {
  const body = normaliseRunningStyleRows([row()]);
  expect(body.generation).toBe("raw-iceberg-v1");
  expect(body.featureNames).toContain("career_win_rate");
  expect(body.featureNames).not.toContain("target_corner_1_norm");
  expect(body.featureNames).not.toContain("self_field_feature");
  expect(body.rows[0]).toMatchObject({
    bamei: "テストホース",
    category: "jra",
    gradeCode: null,
    kaisaiNen: "2026",
    kaisaiTsukihi: "0715",
    keibajoCode: "05",
    kettoTorokuBango: "2023100001",
    kyori: 1600,
    kyosoJokenCode: "010",
    narSubClass: null,
    raceBango: "01",
    raceKey: "jra:20260715:05:01",
    shussoTosu: 18,
    source: "jra",
    trackCode: "11",
    umaban: 7,
  });
  expect(body.rows[0]?.perHorseFeatures).toMatchObject({
    career_win_rate: 0.25,
    kohan3f_avg_5: null,
    past_first_3f_avg_5: null,
    speed_index_avg_5: null,
    speed_index_best_5: 1.5,
  });
  expect(body.rows[0]?.peerInputs).toStrictEqual({
    careerWinRate: 0.25,
    kohan3fAvg5: null,
    pastCorner1NormAvg5: 0.2,
    pastFirst3fAvg5: null,
    pastNigeRate: 0.1,
    pastOikomiRate: 0.4,
    pastSashiRate: 0.3,
    pastSenkouRate: 0.2,
    speedIndexAvg5: null,
    speedIndexBest5: 1.5,
  });
});

it("handles empty results and nullable display values", () => {
  expect(normaliseRunningStyleRows([])).toStrictEqual({
    featureNames: [],
    generation: "raw-iceberg-v1",
    rows: [],
  });
  expect(
    normaliseRunningStyleRows([
      {
        ...row(),
        bamei: " ",
        category: null,
        source: "nar",
        umaban: null,
      },
    ]).rows[0],
  ).toMatchObject({ bamei: null, category: "nar", source: "nar", umaban: 0 });
});

it("rejects rows missing required identity fields", () => {
  expect(() => normaliseRunningStyleRows([{ ...row(), source: {} }])).toThrow(
    "R2 SQL row is missing source",
  );
});
