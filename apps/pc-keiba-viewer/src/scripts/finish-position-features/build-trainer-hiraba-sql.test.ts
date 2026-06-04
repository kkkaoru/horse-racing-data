import { expect, test } from "vitest";

import {
  buildTrainerHirabaIndexSqls,
  buildTrainerHirabaUpdateSql,
  HIRABA_KYOSO_JOKEN_CODES,
  HISTORY_LOOKBACK_DAYS_YYYYMMDD,
  PARTNER_COLUMN,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
} from "./build-trainer-hiraba-sql";

test("SOURCE_FEATURE_TABLE points to the aggregated source", () => {
  expect(SOURCE_FEATURE_TABLE).toBe("race_entry_corner_features");
});

test("TARGET_FEATURE_TABLE points to the finish-position features", () => {
  expect(TARGET_FEATURE_TABLE).toBe("race_finish_position_features");
});

test("HISTORY_LOOKBACK_DAYS_YYYYMMDD is ten years in YYYYMMDD arithmetic", () => {
  expect(HISTORY_LOOKBACK_DAYS_YYYYMMDD).toBe(100000);
});

test("PARTNER_COLUMN identifies trainer name as the partner key", () => {
  expect(PARTNER_COLUMN).toBe("chokyoshimei_ryakusho");
});

test("HIRABA_KYOSO_JOKEN_CODES enumerates JRA non-graded class codes only", () => {
  expect(HIRABA_KYOSO_JOKEN_CODES).toStrictEqual(["000", "005", "010", "016"]);
});

test("buildTrainerHirabaUpdateSql narrows JRA target and history", () => {
  const sql = buildTrainerHirabaUpdateSql("jra");
  expect(sql).toContain("target.category = 'jra'");
  expect(sql).toContain("history.source = 'jra'");
});

test("buildTrainerHirabaUpdateSql narrows NAR excluding ban-ei", () => {
  const sql = buildTrainerHirabaUpdateSql("nar");
  expect(sql).toContain("target.category = 'nar'");
  expect(sql).toContain("history.source = 'nar' and history.keibajo_code <> '83'");
});

test("buildTrainerHirabaUpdateSql narrows ban-ei to keibajo 83", () => {
  const sql = buildTrainerHirabaUpdateSql("ban-ei");
  expect(sql).toContain("target.category = 'ban-ei'");
  expect(sql).toContain("history.source = 'nar' and history.keibajo_code = '83'");
});

test("buildTrainerHirabaUpdateSql joins history by chokyoshimei_ryakusho", () => {
  expect(buildTrainerHirabaUpdateSql("jra")).toContain(
    "history.chokyoshimei_ryakusho = target.target_partner",
  );
});

test("buildTrainerHirabaUpdateSql excludes the current race via strict less-than", () => {
  expect(buildTrainerHirabaUpdateSql("jra")).toContain("history.race_date < target.race_date");
});

test("buildTrainerHirabaUpdateSql bounds history lookback to ten years", () => {
  expect(buildTrainerHirabaUpdateSql("jra")).toContain(
    "history.race_date >= (target.race_date::integer - 100000)::text",
  );
});

test("buildTrainerHirabaUpdateSql filters history to hiraba kyoso_joken_codes only", () => {
  expect(buildTrainerHirabaUpdateSql("jra")).toContain(
    "history.kyoso_joken_code in ('000', '005', '010', '016')",
  );
});

test("buildTrainerHirabaUpdateSql excludes history rows without finish_position", () => {
  expect(buildTrainerHirabaUpdateSql("jra")).toContain("history.finish_position is not null");
});

test("buildTrainerHirabaUpdateSql derives trainer_hiraba_win_rate as avg over filtered set", () => {
  expect(buildTrainerHirabaUpdateSql("jra")).toContain(
    "avg(case when finish_position = 1 then 1 else 0 end) as trainer_hiraba_win_rate",
  );
});

test("buildTrainerHirabaUpdateSql requires non-empty trainer name in target", () => {
  const sql = buildTrainerHirabaUpdateSql("jra");
  expect(sql).toContain("rec.chokyoshimei_ryakusho is not null");
  expect(sql).toContain("btrim(rec.chokyoshimei_ryakusho) <> ''");
});

test("buildTrainerHirabaUpdateSql does not emit other trainer columns", () => {
  const sql = buildTrainerHirabaUpdateSql("jra");
  expect(sql).not.toContain("trainer_career_win_rate");
  expect(sql).not.toContain("trainer_keibajo_win_rate");
  expect(sql).not.toContain("trainer_distance_win_rate");
  expect(sql).not.toContain("trainer_horse_win_rate");
});

test("buildTrainerHirabaUpdateSql does not emit jockey columns", () => {
  const sql = buildTrainerHirabaUpdateSql("jra");
  expect(sql).not.toContain("kishumei_ryakusho");
  expect(sql).not.toContain("jockey_career_win_rate");
});

test("buildTrainerHirabaUpdateSql binds the race_date window to two parameters", () => {
  expect(buildTrainerHirabaUpdateSql("jra")).toContain("target.race_date between $1 and $2");
});

test("buildTrainerHirabaUpdateSql refreshes updated_at on hit rows", () => {
  expect(buildTrainerHirabaUpdateSql("jra")).toContain("updated_at = now()");
});

test("buildTrainerHirabaUpdateSql writes into race_finish_position_features", () => {
  expect(buildTrainerHirabaUpdateSql("jra")).toContain(
    "update race_finish_position_features target",
  );
});

test("buildTrainerHirabaUpdateSql joins source_agg back to target on all primary key columns", () => {
  const sql = buildTrainerHirabaUpdateSql("jra");
  expect(sql).toContain("target.source = source_agg.source");
  expect(sql).toContain("target.kaisai_nen = source_agg.kaisai_nen");
  expect(sql).toContain("target.kaisai_tsukihi = source_agg.kaisai_tsukihi");
  expect(sql).toContain("target.keibajo_code = source_agg.keibajo_code");
  expect(sql).toContain("target.race_bango = source_agg.race_bango");
  expect(sql).toContain("target.ketto_toroku_bango = source_agg.ketto_toroku_bango");
});

test("buildTrainerHirabaUpdateSql widens the all category to no category filter", () => {
  const sql = buildTrainerHirabaUpdateSql("all");
  expect(sql).not.toContain("target.category = 'jra'");
  expect(sql).not.toContain("target.category = 'nar'");
  expect(sql).not.toContain("target.category = 'ban-ei'");
});

test("buildTrainerHirabaIndexSqls returns exactly one idempotent index statement", () => {
  expect(buildTrainerHirabaIndexSqls().length).toBe(1);
});

test("buildTrainerHirabaIndexSqls produces an idempotent create statement", () => {
  expect(buildTrainerHirabaIndexSqls()[0]).toContain("if not exists");
});

test("buildTrainerHirabaIndexSqls names the trainer hiraba index", () => {
  expect(buildTrainerHirabaIndexSqls()[0]).toContain(
    "race_entry_corner_features_trainer_hiraba_date_idx",
  );
});

test("buildTrainerHirabaIndexSqls covers source, trainer name, and race_date", () => {
  expect(buildTrainerHirabaIndexSqls()[0]).toContain("(source, chokyoshimei_ryakusho, race_date)");
});

test("buildTrainerHirabaIndexSqls narrows the index to hiraba kyoso_joken_codes", () => {
  expect(buildTrainerHirabaIndexSqls()[0]).toContain(
    "kyoso_joken_code in ('000', '005', '010', '016')",
  );
});

test("buildTrainerHirabaIndexSqls excludes null trainer names and unsettled finish positions", () => {
  const sql = buildTrainerHirabaIndexSqls()[0];
  expect(sql).toContain("chokyoshimei_ryakusho is not null");
  expect(sql).toContain("finish_position is not null");
});
