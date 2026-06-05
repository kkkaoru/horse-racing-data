import { expect, test } from "vitest";

import {
  buildRelationshipHistoryUpdateSql,
  HISTORY_LOOKBACK_DAYS_YYYYMMDD,
  JRA_RUNNER_TABLE,
  NAR_RUNNER_TABLE,
  RECENT_HISTORY_WINDOW_SIZE,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
} from "./build-relationship-history-sql";

test("SOURCE_FEATURE_TABLE points to the aggregated source", () => {
  expect(SOURCE_FEATURE_TABLE).toStrictEqual("race_entry_corner_features");
});

test("TARGET_FEATURE_TABLE points to the finish-position features", () => {
  expect(TARGET_FEATURE_TABLE).toStrictEqual("race_finish_position_features");
});

test("JRA_RUNNER_TABLE points to jvd_se", () => {
  expect(JRA_RUNNER_TABLE).toStrictEqual("jvd_se");
});

test("NAR_RUNNER_TABLE points to nvd_se", () => {
  expect(NAR_RUNNER_TABLE).toStrictEqual("nvd_se");
});

test("HISTORY_LOOKBACK_DAYS_YYYYMMDD reuses the ten-year window constant", () => {
  expect(HISTORY_LOOKBACK_DAYS_YYYYMMDD).toStrictEqual(100000);
});

test("RECENT_HISTORY_WINDOW_SIZE reuses the five-race window constant", () => {
  expect(RECENT_HISTORY_WINDOW_SIZE).toStrictEqual(5);
});

test("buildRelationshipHistoryUpdateSql narrows JRA target category", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch("target.category = 'jra'");
});

test("buildRelationshipHistoryUpdateSql narrows JRA history source", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch("history.source = 'jra'");
});

test("buildRelationshipHistoryUpdateSql narrows NAR excluding ban-ei keibajo 83", () => {
  expect(buildRelationshipHistoryUpdateSql("nar")).toMatch(
    "history.source = 'nar' and history.keibajo_code <> '83'",
  );
});

test("buildRelationshipHistoryUpdateSql narrows NAR target category", () => {
  expect(buildRelationshipHistoryUpdateSql("nar")).toMatch("target.category = 'nar'");
});

test("buildRelationshipHistoryUpdateSql narrows ban-ei history to keibajo 83", () => {
  expect(buildRelationshipHistoryUpdateSql("ban-ei")).toMatch(
    "history.source = 'nar' and history.keibajo_code = '83'",
  );
});

test("buildRelationshipHistoryUpdateSql narrows ban-ei target category", () => {
  expect(buildRelationshipHistoryUpdateSql("ban-ei")).toMatch("target.category = 'ban-ei'");
});

test("buildRelationshipHistoryUpdateSql with category all skips source filter", () => {
  expect(buildRelationshipHistoryUpdateSql("all")).toMatch("where true");
});

test("buildRelationshipHistoryUpdateSql with category all skips target filter", () => {
  expect(buildRelationshipHistoryUpdateSql("all")).toMatch("and true");
});

test("buildRelationshipHistoryUpdateSql enforces strict less-than race_date for leak prevention", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch("history.race_date < target.race_date");
});

test("buildRelationshipHistoryUpdateSql bounds history lookback to ten years", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch(
    "history.race_date >= (target.race_date::integer - 100000)::text",
  );
});

test("buildRelationshipHistoryUpdateSql partitions history by source", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch(
    "row_number() over (\n          partition by\n            target.source",
  );
});

test("buildRelationshipHistoryUpdateSql orders history by race_date desc as recent_rank", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch(
    "order by history.race_date desc\n        ) as recent_rank",
  );
});

test("buildRelationshipHistoryUpdateSql joins jvd_se on history side for bataiju", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch("left join jvd_se hj");
});

test("buildRelationshipHistoryUpdateSql joins nvd_se on history side for bataiju", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch("left join nvd_se hn");
});

test("buildRelationshipHistoryUpdateSql safely casts JRA bataiju values", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch(
    "trim(coalesce(hj.bataiju::text, '')) ~ '^-?[0-9]+$'",
  );
});

test("buildRelationshipHistoryUpdateSql safely casts NAR bataiju values", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch(
    "trim(coalesce(hn.bataiju::text, '')) ~ '^-?[0-9]+$'",
  );
});

test("buildRelationshipHistoryUpdateSql filters history where kyori is not null", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch("history.kyori is not null");
});

test("buildRelationshipHistoryUpdateSql filters history where kyori is positive to avoid divide-by-zero", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch("history.kyori > 0");
});

test("buildRelationshipHistoryUpdateSql filters history where soha_time is not null", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch("history.soha_time is not null");
});

test("buildRelationshipHistoryUpdateSql filters history where finish_position is not null", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch("history.finish_position is not null");
});

test("buildRelationshipHistoryUpdateSql derives past_speed_kg_normalized_avg5 as avg of soha/kyori times bataiju", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch(
    "avg(hist_soha_time / hist_kyori * hist_bataiju)\n          filter (where recent_rank <= 5 and hist_bataiju is not null)\n          as past_speed_kg_normalized_avg5",
  );
});

test("buildRelationshipHistoryUpdateSql derives past_speed_futan_normalized_avg5 as avg of soha/kyori times futan_juryo", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch(
    "avg(hist_soha_time / hist_kyori * hist_futan_juryo)\n          filter (where recent_rank <= 5 and hist_futan_juryo is not null)\n          as past_speed_futan_normalized_avg5",
  );
});

test("buildRelationshipHistoryUpdateSql derives past_speed_age_adjusted_avg5 with nullif on barei", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch(
    "avg((hist_soha_time / hist_kyori) / nullif(hist_barei, 0))\n          filter (where recent_rank <= 5 and hist_barei is not null)\n          as past_speed_age_adjusted_avg5",
  );
});

test("buildRelationshipHistoryUpdateSql derives past_speed_volatility_5 with stddev_pop on speed", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch(
    "stddev_pop(hist_soha_time / hist_kyori)\n          filter (where recent_rank <= 5)\n          as past_speed_volatility_5",
  );
});

test("buildRelationshipHistoryUpdateSql derives past_finish_position_volatility_5 with stddev_pop on finish_position", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch(
    "stddev_pop(hist_finish_position::double precision)\n          filter (where recent_rank <= 5)\n          as past_finish_position_volatility_5",
  );
});

test("buildRelationshipHistoryUpdateSql uses stddev_pop so a single past race yields NULL natively", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch("stddev_pop(");
});

test("buildRelationshipHistoryUpdateSql refreshes updated_at", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch("updated_at = now()");
});

test("buildRelationshipHistoryUpdateSql joins target.source on history_agg", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch("target.source = history_agg.source");
});

test("buildRelationshipHistoryUpdateSql joins target.kaisai_nen on history_agg", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch(
    "target.kaisai_nen = history_agg.kaisai_nen",
  );
});

test("buildRelationshipHistoryUpdateSql joins target.kaisai_tsukihi on history_agg", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch(
    "target.kaisai_tsukihi = history_agg.kaisai_tsukihi",
  );
});

test("buildRelationshipHistoryUpdateSql joins target.keibajo_code on history_agg", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch(
    "target.keibajo_code = history_agg.keibajo_code",
  );
});

test("buildRelationshipHistoryUpdateSql joins target.race_bango on history_agg", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch(
    "target.race_bango = history_agg.race_bango",
  );
});

test("buildRelationshipHistoryUpdateSql joins target.ketto_toroku_bango on history_agg", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch(
    "target.ketto_toroku_bango = history_agg.ketto_toroku_bango",
  );
});

test("buildRelationshipHistoryUpdateSql parameterizes target.race_date between $1 and $2", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch("target.race_date between $1 and $2");
});

test("buildRelationshipHistoryUpdateSql reads from race_entry_corner_features on history side", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch(
    "join race_entry_corner_features history",
  );
});

test("buildRelationshipHistoryUpdateSql updates race_finish_position_features", () => {
  expect(buildRelationshipHistoryUpdateSql("jra")).toMatch(
    "update race_finish_position_features target",
  );
});
