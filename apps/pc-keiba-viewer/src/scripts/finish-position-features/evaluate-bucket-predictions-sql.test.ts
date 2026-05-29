// Run with: bunx vitest run src/scripts/finish-position-features/evaluate-bucket-predictions-sql.test.ts
import { expect, test } from "vitest";

import {
  BUCKET_LOOKUP_INDEX_COLUMNS,
  BUCKET_RACE_NAME_INDEX_SQL,
  BUCKET_TABLE,
  BUCKET_UNIQUE_INDEX_COLUMNS,
  CATEGORY_BAN_EI,
  CATEGORY_JRA,
  CATEGORY_NAR,
  CONDITION_LABEL_PAIRS,
  JRA_RA_RACE_KEY_BUCKET_INDEX_SQL,
  JRA_RA_TABLE,
  NAR_RA_RACE_KEY_BUCKET_INDEX_SQL,
  NAR_RA_TABLE,
  PREDICTIONS_TEMP_TABLE,
  RACE_ENTRY_CORNER_FEATURES_BUCKETING_INDEX_SQL,
  SOURCE_TABLE,
  buildAnalyzeSqls,
  buildBucketAggregateSql,
  buildBucketEvaluationsDdl,
  buildBucketUpsertSql,
  buildCategoryActualsFilter,
  buildCategoryRaceSourceFilter,
  buildConcurrentIndexSqls,
  buildConditionCaseSql,
  buildRaceNameExpressionSql,
  escapeSqlLiteral,
} from "./evaluate-bucket-predictions-sql";

test("BUCKET_TABLE matches the new bucket evaluations table name", () => {
  expect(BUCKET_TABLE).toBe("model_prediction_bucket_evaluations");
});

test("PREDICTIONS_TEMP_TABLE matches the temp predictions table name", () => {
  expect(PREDICTIONS_TEMP_TABLE).toBe("bucket_predictions_loaded");
});

test("SOURCE_TABLE points to race_entry_corner_features", () => {
  expect(SOURCE_TABLE).toBe("race_entry_corner_features");
});

test("JRA_RA_TABLE is jvd_ra", () => {
  expect(JRA_RA_TABLE).toBe("jvd_ra");
});

test("NAR_RA_TABLE is nvd_ra", () => {
  expect(NAR_RA_TABLE).toBe("nvd_ra");
});

test("CATEGORY_JRA exposes jra slug", () => {
  expect(CATEGORY_JRA).toBe("jra");
});

test("CATEGORY_NAR exposes nar slug", () => {
  expect(CATEGORY_NAR).toBe("nar");
});

test("CATEGORY_BAN_EI exposes ban-ei slug", () => {
  expect(CATEGORY_BAN_EI).toBe("ban-ei");
});

test("BUCKET_UNIQUE_INDEX_COLUMNS lists 15 unique columns in stable order", () => {
  expect(BUCKET_UNIQUE_INDEX_COLUMNS).toStrictEqual([
    "model_version",
    "running_style_feature_version",
    "finish_position_version",
    "category",
    "evaluation_window_from",
    "evaluation_window_to",
    "source",
    "keibajo_code",
    "kyori",
    "kyoso_shubetsu_code",
    "coalesce(kyoso_joken_code,'')",
    "coalesce(condition_key,'')",
    "coalesce(track_code,'')",
    "coalesce(grade_code,'')",
    "coalesce(race_name,'')",
  ]);
});

test("BUCKET_LOOKUP_INDEX_COLUMNS lists viewer lookup keys in stable order", () => {
  expect(BUCKET_LOOKUP_INDEX_COLUMNS).toStrictEqual([
    "model_version",
    "running_style_feature_version",
    "finish_position_version",
    "category",
    "source",
    "keibajo_code",
    "kyori",
    "kyoso_shubetsu_code",
    "kyoso_joken_code",
    "condition_key",
    "track_code",
    "grade_code",
  ]);
});

test("CONDITION_LABEL_PAIRS embeds CONDITION_LABELS from race-classification", () => {
  expect(CONDITION_LABEL_PAIRS).toStrictEqual([
    ["005", "1勝クラス"],
    ["010", "2勝クラス"],
    ["016", "3勝クラス"],
    ["701", "新馬"],
    ["702", "未出走"],
    ["703", "未勝利"],
    ["999", "オープン"],
  ]);
});

test("escapeSqlLiteral doubles single quotes", () => {
  expect(escapeSqlLiteral("it's")).toBe("it''s");
});

test("escapeSqlLiteral returns plain string unchanged", () => {
  expect(escapeSqlLiteral("plain")).toBe("plain");
});

test("buildRaceNameExpressionSql returns CASE WHEN filtered on grade A/F", () => {
  expect(buildRaceNameExpressionSql("ra.grade_cd", "ra.kyosomei_hondai")).toBe(
    "case when ra.grade_cd in ('A','F') then trim(ra.kyosomei_hondai) else null end",
  );
});

test("buildConditionCaseSql embeds all 7 CONDITION_LABELS WHEN clauses", () => {
  expect(buildConditionCaseSql("ra.kyoso_joken_code", "ra.kyoso_joken_meisho")).toBe(
    `case
        when ra.kyoso_joken_code = '005' then '1勝クラス'
        when ra.kyoso_joken_code = '010' then '2勝クラス'
        when ra.kyoso_joken_code = '016' then '3勝クラス'
        when ra.kyoso_joken_code = '701' then '新馬'
        when ra.kyoso_joken_code = '702' then '未出走'
        when ra.kyoso_joken_code = '703' then '未勝利'
        when ra.kyoso_joken_code = '999' then 'オープン'
        else nullif(split_part(trim(ra.kyoso_joken_meisho), ' ', 1), '')
      end`,
  );
});

test("buildCategoryRaceSourceFilter returns jvd_ra for jra without filter", () => {
  expect(buildCategoryRaceSourceFilter("jra")).toStrictEqual({
    table: "jvd_ra",
    filter: "true",
  });
});

test("buildCategoryRaceSourceFilter returns nvd_ra excluding keibajo 83 for nar", () => {
  expect(buildCategoryRaceSourceFilter("nar")).toStrictEqual({
    table: "nvd_ra",
    filter: "ra.keibajo_code <> '83'",
  });
});

test("buildCategoryRaceSourceFilter returns nvd_ra restricted to keibajo 83 for ban-ei", () => {
  expect(buildCategoryRaceSourceFilter("ban-ei")).toStrictEqual({
    table: "nvd_ra",
    filter: "ra.keibajo_code = '83'",
  });
});

test("buildCategoryRaceSourceFilter throws for unknown category", () => {
  expect(() => buildCategoryRaceSourceFilter("oops")).toThrowError("Unknown category: oops");
});

test("buildCategoryActualsFilter narrows actuals to jra source", () => {
  expect(buildCategoryActualsFilter("jra")).toBe("rec.source = 'jra'");
});

test("buildCategoryActualsFilter narrows actuals to nar excluding ban-ei", () => {
  expect(buildCategoryActualsFilter("nar")).toBe("rec.source = 'nar' and rec.keibajo_code <> '83'");
});

test("buildCategoryActualsFilter narrows actuals to ban-ei keibajo 83", () => {
  expect(buildCategoryActualsFilter("ban-ei")).toBe(
    "rec.source = 'nar' and rec.keibajo_code = '83'",
  );
});

test("buildCategoryActualsFilter throws on unknown category", () => {
  expect(() => buildCategoryActualsFilter("xxx")).toThrowError("Unknown category: xxx");
});

test("BUCKET_RACE_NAME_INDEX_SQL is a partial index on race_name NOT NULL", () => {
  expect(BUCKET_RACE_NAME_INDEX_SQL).toBe(
    `create index if not exists model_prediction_bucket_evaluations_race_name
      on model_prediction_bucket_evaluations (category, source, race_name, keibajo_code, kyori)
      where race_name is not null`,
  );
});

test("RACE_ENTRY_CORNER_FEATURES_BUCKETING_INDEX_SQL is concurrent with INCLUDE columns", () => {
  expect(RACE_ENTRY_CORNER_FEATURES_BUCKETING_INDEX_SQL).toBe(
    `create index concurrently if not exists race_entry_corner_features_bucketing_idx
      on race_entry_corner_features (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
      include (ketto_toroku_bango, finish_position, finish_norm, shusso_tosu)`,
  );
});

test("JRA_RA_RACE_KEY_BUCKET_INDEX_SQL covers jvd_ra race key with INCLUDE", () => {
  expect(JRA_RA_RACE_KEY_BUCKET_INDEX_SQL).toBe(
    `create index concurrently if not exists jvd_ra_race_key_bucket_idx
      on jvd_ra (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
      include (kyori, grade_cd, kyoso_shubetsu_code, kyoso_joken_code, kyoso_joken_meisho, track_code, kyosomei_hondai)`,
  );
});

test("NAR_RA_RACE_KEY_BUCKET_INDEX_SQL covers nvd_ra race key with INCLUDE", () => {
  expect(NAR_RA_RACE_KEY_BUCKET_INDEX_SQL).toBe(
    `create index concurrently if not exists nvd_ra_race_key_bucket_idx
      on nvd_ra (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
      include (kyori, grade_cd, kyoso_shubetsu_code, kyoso_joken_code, kyoso_joken_meisho, track_code, kyosomei_hondai)`,
  );
});

test("buildConcurrentIndexSqls returns the three CREATE INDEX CONCURRENTLY statements", () => {
  expect(buildConcurrentIndexSqls()).toStrictEqual([
    `create index concurrently if not exists race_entry_corner_features_bucketing_idx
      on race_entry_corner_features (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
      include (ketto_toroku_bango, finish_position, finish_norm, shusso_tosu)`,
    `create index concurrently if not exists jvd_ra_race_key_bucket_idx
      on jvd_ra (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
      include (kyori, grade_cd, kyoso_shubetsu_code, kyoso_joken_code, kyoso_joken_meisho, track_code, kyosomei_hondai)`,
    `create index concurrently if not exists nvd_ra_race_key_bucket_idx
      on nvd_ra (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
      include (kyori, grade_cd, kyoso_shubetsu_code, kyoso_joken_code, kyoso_joken_meisho, track_code, kyosomei_hondai)`,
  ]);
});

test("buildAnalyzeSqls returns ANALYZE statements for 4 tables", () => {
  expect(buildAnalyzeSqls()).toStrictEqual([
    "analyze model_prediction_bucket_evaluations",
    "analyze race_entry_corner_features",
    "analyze jvd_ra",
    "analyze nvd_ra",
  ]);
});

test("buildBucketEvaluationsDdl exposes table DDL with all metric columns and indexes", () => {
  expect(buildBucketEvaluationsDdl()).toBe(`
    create table if not exists model_prediction_bucket_evaluations (
      model_version                 text not null,
      running_style_feature_version text not null,
      finish_position_version       text not null,
      category                      text not null,
      evaluation_window_from        text not null,
      evaluation_window_to          text not null,
      source                        text not null,
      keibajo_code                  text not null,
      kyori                         integer not null,
      kyoso_shubetsu_code           text not null,
      kyoso_joken_code              text,
      condition_key                 text,
      track_code                    text,
      grade_code                    text,
      race_name                     text,
      race_count                    integer not null,
      prediction_count              integer not null,
      top1_hit_sum                  numeric not null,
      place1_hit_sum                numeric not null,
      place2_hit_sum                numeric not null,
      place3_hit_sum                numeric not null,
      top3_box_hit_sum              numeric not null,
      top3_exact_hit_sum            numeric not null,
      top3_winner_capture_sum       numeric not null,
      top5_winner_capture_sum       numeric not null,
      top3_place_relation_sum       numeric not null,
      pair_score_sum                numeric not null,
      pair_score_pair_count         integer not null,
      ndcg_at_3_sum                 numeric not null,
      ndcg_at_3_race_count          integer not null,
      evaluated_at                  timestamptz not null default now()
    );
    create unique index if not exists model_prediction_bucket_evaluations_uq
      on model_prediction_bucket_evaluations (model_version, running_style_feature_version, finish_position_version, category, evaluation_window_from, evaluation_window_to, source, keibajo_code, kyori, kyoso_shubetsu_code, coalesce(kyoso_joken_code,''), coalesce(condition_key,''), coalesce(track_code,''), coalesce(grade_code,''), coalesce(race_name,''));
    create index if not exists model_prediction_bucket_evaluations_lookup
      on model_prediction_bucket_evaluations (model_version, running_style_feature_version, finish_position_version, category, source, keibajo_code, kyori, kyoso_shubetsu_code, kyoso_joken_code, condition_key, track_code, grade_code);
    create index if not exists model_prediction_bucket_evaluations_race_name
      on model_prediction_bucket_evaluations (category, source, race_name, keibajo_code, kyori)
      where race_name is not null;
  `);
});

test("buildBucketAggregateSql for jra emits full aggregate query", () => {
  expect(
    buildBucketAggregateSql({
      modelVersion: "vX",
      category: "jra",
      fromDate: "20240101",
      toDate: "20251231",
      runningStyleFeatureVersion: "v1",
      finishPositionVersion: "v1",
    }),
  ).toBe(`
    with predictions as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             ketto_toroku_bango, predicted_rank, predicted_score
      from bucket_predictions_loaded
      where model_version = 'vX'
        and running_style_feature_version = 'v1'
        and finish_position_version = 'v1'
    ),
    actuals as (
      select rec.source, rec.kaisai_nen, rec.kaisai_tsukihi, rec.keibajo_code, rec.race_bango,
             rec.ketto_toroku_bango, rec.finish_position
      from race_entry_corner_features rec
      where rec.race_date between '20240101' and '20251231'
        and rec.finish_position is not null
        and rec.source = 'jra'
    ),
    joined as (
      select p.*, a.finish_position
      from predictions p
      join actuals a using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    ),
    races as (
      select distinct source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango from joined
    ),
    race_dims as (
      select r.source, r.kaisai_nen, r.kaisai_tsukihi, r.keibajo_code, r.race_bango,
             ra.kyori, ra.kyoso_shubetsu_code,
             ra.kyoso_joken_code as kyoso_joken_code,
             null::text as condition_key,
             ra.track_code as track_code,
             nullif(trim(ra.grade_cd), '') as grade_code,
             case when ra.grade_cd in ('A','F') then trim(ra.kyosomei_hondai) else null end as race_name
      from races r
      join jvd_ra ra
        on ra.kaisai_nen = r.kaisai_nen
       and ra.kaisai_tsukihi = r.kaisai_tsukihi
       and ra.keibajo_code = r.keibajo_code
       and ra.race_bango = r.race_bango
      where true
    ),
    per_race as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             max(case when predicted_rank = 1 and finish_position = 1 then 1 else 0 end) top1_hit,
             (sum(case when predicted_rank <= 3 and finish_position <= 3 then 1 else 0 end) = 3)::int top3_box_hit,
             (
               max(case when predicted_rank = 1 and finish_position = 1 then 1 else 0 end) = 1
               and max(case when predicted_rank = 2 and finish_position = 2 then 1 else 0 end) = 1
               and max(case when predicted_rank = 3 and finish_position = 3 then 1 else 0 end) = 1
             )::int top3_exact_hit,
             max(case when predicted_rank = 1 and finish_position = 1 then 1 else 0 end) place1_hit,
             max(case when predicted_rank = 2 and finish_position = 2 then 1 else 0 end) place2_hit,
             max(case when predicted_rank = 3 and finish_position = 3 then 1 else 0 end) place3_hit,
             max(case when predicted_rank <= 3 and finish_position = 1 then 1 else 0 end) top3_winner_capture_hit,
             max(case when predicted_rank <= 5 and finish_position = 1 then 1 else 0 end) top5_winner_capture_hit,
             sum(case when predicted_rank <= 3 and finish_position <= 3 then 1.0 else 0.0 end) / 3.0 top3_place_relation_val,
             count(*) prediction_rows
      from joined
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
    ),
    pair_per_race as (
      select j1.source, j1.kaisai_nen, j1.kaisai_tsukihi, j1.keibajo_code, j1.race_bango,
             sum(
               case
                 when (j1.predicted_rank < j2.predicted_rank) = (j1.finish_position < j2.finish_position)
                 then 1.0 else 0.0
               end
             ) pair_correct_sum,
             count(*) pair_count
      from joined j1
      join joined j2
        on j1.source = j2.source
        and j1.kaisai_nen = j2.kaisai_nen
        and j1.kaisai_tsukihi = j2.kaisai_tsukihi
        and j1.keibajo_code = j2.keibajo_code
        and j1.race_bango = j2.race_bango
        and j1.ketto_toroku_bango < j2.ketto_toroku_bango
      group by j1.source, j1.kaisai_nen, j1.kaisai_tsukihi, j1.keibajo_code, j1.race_bango
    ),
    ndcg_per_race as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             sum(
               case
                 when predicted_rank <= 3
                 then (greatest(0, 4 - finish_position)) / ln(2 + predicted_rank)
                 else 0
               end
             ) dcg,
             (3 / ln(2 + 1) + 2 / ln(2 + 2) + 1 / ln(2 + 3)) ideal_dcg
      from joined
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
    )
    select
      d.source,
      d.keibajo_code,
      d.kyori,
      d.kyoso_shubetsu_code,
      d.kyoso_joken_code,
      d.condition_key,
      d.track_code,
      d.grade_code,
      d.race_name,
      count(*) race_count,
      coalesce(sum(pr.prediction_rows), 0) prediction_count,
      coalesce(sum(pr.top1_hit::numeric), 0) top1_hit_sum,
      coalesce(sum(pr.place1_hit::numeric), 0) place1_hit_sum,
      coalesce(sum(pr.place2_hit::numeric), 0) place2_hit_sum,
      coalesce(sum(pr.place3_hit::numeric), 0) place3_hit_sum,
      coalesce(sum(pr.top3_box_hit::numeric), 0) top3_box_hit_sum,
      coalesce(sum(pr.top3_exact_hit::numeric), 0) top3_exact_hit_sum,
      coalesce(sum(pr.top3_winner_capture_hit::numeric), 0) top3_winner_capture_sum,
      coalesce(sum(pr.top5_winner_capture_hit::numeric), 0) top5_winner_capture_sum,
      coalesce(sum(pr.top3_place_relation_val), 0) top3_place_relation_sum,
      coalesce(sum(pp.pair_correct_sum), 0) pair_score_sum,
      coalesce(sum(pp.pair_count), 0) pair_score_pair_count,
      coalesce(sum(case when nd.ideal_dcg > 0 then nd.dcg / nd.ideal_dcg else 0 end), 0) ndcg_at_3_sum,
      coalesce(sum(case when nd.ideal_dcg > 0 then 1 else 0 end), 0) ndcg_at_3_race_count
    from race_dims d
    join per_race pr using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    left join pair_per_race pp using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    left join ndcg_per_race nd using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    group by d.source, d.keibajo_code, d.kyori, d.kyoso_shubetsu_code,
             d.kyoso_joken_code, d.condition_key, d.track_code, d.grade_code, d.race_name
  `);
});

test("buildBucketAggregateSql for nar uses nvd_ra join and condition CASE expression", () => {
  expect(
    buildBucketAggregateSql({
      modelVersion: "vX",
      category: "nar",
      fromDate: "20240101",
      toDate: "20251231",
      runningStyleFeatureVersion: "v1",
      finishPositionVersion: "v1",
    }),
  ).toBe(`
    with predictions as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             ketto_toroku_bango, predicted_rank, predicted_score
      from bucket_predictions_loaded
      where model_version = 'vX'
        and running_style_feature_version = 'v1'
        and finish_position_version = 'v1'
    ),
    actuals as (
      select rec.source, rec.kaisai_nen, rec.kaisai_tsukihi, rec.keibajo_code, rec.race_bango,
             rec.ketto_toroku_bango, rec.finish_position
      from race_entry_corner_features rec
      where rec.race_date between '20240101' and '20251231'
        and rec.finish_position is not null
        and rec.source = 'nar' and rec.keibajo_code <> '83'
    ),
    joined as (
      select p.*, a.finish_position
      from predictions p
      join actuals a using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    ),
    races as (
      select distinct source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango from joined
    ),
    race_dims as (
      select r.source, r.kaisai_nen, r.kaisai_tsukihi, r.keibajo_code, r.race_bango,
             ra.kyori, ra.kyoso_shubetsu_code,
             ra.kyoso_joken_code as kyoso_joken_code,
             case
        when ra.kyoso_joken_code = '005' then '1勝クラス'
        when ra.kyoso_joken_code = '010' then '2勝クラス'
        when ra.kyoso_joken_code = '016' then '3勝クラス'
        when ra.kyoso_joken_code = '701' then '新馬'
        when ra.kyoso_joken_code = '702' then '未出走'
        when ra.kyoso_joken_code = '703' then '未勝利'
        when ra.kyoso_joken_code = '999' then 'オープン'
        else nullif(split_part(trim(ra.kyoso_joken_meisho), ' ', 1), '')
      end as condition_key,
             ra.track_code as track_code,
             nullif(trim(ra.grade_cd), '') as grade_code,
             case when ra.grade_cd in ('A','F') then trim(ra.kyosomei_hondai) else null end as race_name
      from races r
      join nvd_ra ra
        on ra.kaisai_nen = r.kaisai_nen
       and ra.kaisai_tsukihi = r.kaisai_tsukihi
       and ra.keibajo_code = r.keibajo_code
       and ra.race_bango = r.race_bango
      where ra.keibajo_code <> '83'
    ),
    per_race as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             max(case when predicted_rank = 1 and finish_position = 1 then 1 else 0 end) top1_hit,
             (sum(case when predicted_rank <= 3 and finish_position <= 3 then 1 else 0 end) = 3)::int top3_box_hit,
             (
               max(case when predicted_rank = 1 and finish_position = 1 then 1 else 0 end) = 1
               and max(case when predicted_rank = 2 and finish_position = 2 then 1 else 0 end) = 1
               and max(case when predicted_rank = 3 and finish_position = 3 then 1 else 0 end) = 1
             )::int top3_exact_hit,
             max(case when predicted_rank = 1 and finish_position = 1 then 1 else 0 end) place1_hit,
             max(case when predicted_rank = 2 and finish_position = 2 then 1 else 0 end) place2_hit,
             max(case when predicted_rank = 3 and finish_position = 3 then 1 else 0 end) place3_hit,
             max(case when predicted_rank <= 3 and finish_position = 1 then 1 else 0 end) top3_winner_capture_hit,
             max(case when predicted_rank <= 5 and finish_position = 1 then 1 else 0 end) top5_winner_capture_hit,
             sum(case when predicted_rank <= 3 and finish_position <= 3 then 1.0 else 0.0 end) / 3.0 top3_place_relation_val,
             count(*) prediction_rows
      from joined
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
    ),
    pair_per_race as (
      select j1.source, j1.kaisai_nen, j1.kaisai_tsukihi, j1.keibajo_code, j1.race_bango,
             sum(
               case
                 when (j1.predicted_rank < j2.predicted_rank) = (j1.finish_position < j2.finish_position)
                 then 1.0 else 0.0
               end
             ) pair_correct_sum,
             count(*) pair_count
      from joined j1
      join joined j2
        on j1.source = j2.source
        and j1.kaisai_nen = j2.kaisai_nen
        and j1.kaisai_tsukihi = j2.kaisai_tsukihi
        and j1.keibajo_code = j2.keibajo_code
        and j1.race_bango = j2.race_bango
        and j1.ketto_toroku_bango < j2.ketto_toroku_bango
      group by j1.source, j1.kaisai_nen, j1.kaisai_tsukihi, j1.keibajo_code, j1.race_bango
    ),
    ndcg_per_race as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             sum(
               case
                 when predicted_rank <= 3
                 then (greatest(0, 4 - finish_position)) / ln(2 + predicted_rank)
                 else 0
               end
             ) dcg,
             (3 / ln(2 + 1) + 2 / ln(2 + 2) + 1 / ln(2 + 3)) ideal_dcg
      from joined
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
    )
    select
      d.source,
      d.keibajo_code,
      d.kyori,
      d.kyoso_shubetsu_code,
      d.kyoso_joken_code,
      d.condition_key,
      d.track_code,
      d.grade_code,
      d.race_name,
      count(*) race_count,
      coalesce(sum(pr.prediction_rows), 0) prediction_count,
      coalesce(sum(pr.top1_hit::numeric), 0) top1_hit_sum,
      coalesce(sum(pr.place1_hit::numeric), 0) place1_hit_sum,
      coalesce(sum(pr.place2_hit::numeric), 0) place2_hit_sum,
      coalesce(sum(pr.place3_hit::numeric), 0) place3_hit_sum,
      coalesce(sum(pr.top3_box_hit::numeric), 0) top3_box_hit_sum,
      coalesce(sum(pr.top3_exact_hit::numeric), 0) top3_exact_hit_sum,
      coalesce(sum(pr.top3_winner_capture_hit::numeric), 0) top3_winner_capture_sum,
      coalesce(sum(pr.top5_winner_capture_hit::numeric), 0) top5_winner_capture_sum,
      coalesce(sum(pr.top3_place_relation_val), 0) top3_place_relation_sum,
      coalesce(sum(pp.pair_correct_sum), 0) pair_score_sum,
      coalesce(sum(pp.pair_count), 0) pair_score_pair_count,
      coalesce(sum(case when nd.ideal_dcg > 0 then nd.dcg / nd.ideal_dcg else 0 end), 0) ndcg_at_3_sum,
      coalesce(sum(case when nd.ideal_dcg > 0 then 1 else 0 end), 0) ndcg_at_3_race_count
    from race_dims d
    join per_race pr using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    left join pair_per_race pp using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    left join ndcg_per_race nd using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    group by d.source, d.keibajo_code, d.kyori, d.kyoso_shubetsu_code,
             d.kyoso_joken_code, d.condition_key, d.track_code, d.grade_code, d.race_name
  `);
});

test("buildBucketAggregateSql for ban-ei nulls track and joken and restricts to keibajo 83", () => {
  expect(
    buildBucketAggregateSql({
      modelVersion: "vX",
      category: "ban-ei",
      fromDate: "20240101",
      toDate: "20251231",
      runningStyleFeatureVersion: "v1",
      finishPositionVersion: "v1",
    }),
  ).toBe(`
    with predictions as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             ketto_toroku_bango, predicted_rank, predicted_score
      from bucket_predictions_loaded
      where model_version = 'vX'
        and running_style_feature_version = 'v1'
        and finish_position_version = 'v1'
    ),
    actuals as (
      select rec.source, rec.kaisai_nen, rec.kaisai_tsukihi, rec.keibajo_code, rec.race_bango,
             rec.ketto_toroku_bango, rec.finish_position
      from race_entry_corner_features rec
      where rec.race_date between '20240101' and '20251231'
        and rec.finish_position is not null
        and rec.source = 'nar' and rec.keibajo_code = '83'
    ),
    joined as (
      select p.*, a.finish_position
      from predictions p
      join actuals a using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    ),
    races as (
      select distinct source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango from joined
    ),
    race_dims as (
      select r.source, r.kaisai_nen, r.kaisai_tsukihi, r.keibajo_code, r.race_bango,
             ra.kyori, ra.kyoso_shubetsu_code,
             null::text as kyoso_joken_code,
             null::text as condition_key,
             null::text as track_code,
             nullif(trim(ra.grade_cd), '') as grade_code,
             case when ra.grade_cd in ('A','F') then trim(ra.kyosomei_hondai) else null end as race_name
      from races r
      join nvd_ra ra
        on ra.kaisai_nen = r.kaisai_nen
       and ra.kaisai_tsukihi = r.kaisai_tsukihi
       and ra.keibajo_code = r.keibajo_code
       and ra.race_bango = r.race_bango
      where ra.keibajo_code = '83'
    ),
    per_race as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             max(case when predicted_rank = 1 and finish_position = 1 then 1 else 0 end) top1_hit,
             (sum(case when predicted_rank <= 3 and finish_position <= 3 then 1 else 0 end) = 3)::int top3_box_hit,
             (
               max(case when predicted_rank = 1 and finish_position = 1 then 1 else 0 end) = 1
               and max(case when predicted_rank = 2 and finish_position = 2 then 1 else 0 end) = 1
               and max(case when predicted_rank = 3 and finish_position = 3 then 1 else 0 end) = 1
             )::int top3_exact_hit,
             max(case when predicted_rank = 1 and finish_position = 1 then 1 else 0 end) place1_hit,
             max(case when predicted_rank = 2 and finish_position = 2 then 1 else 0 end) place2_hit,
             max(case when predicted_rank = 3 and finish_position = 3 then 1 else 0 end) place3_hit,
             max(case when predicted_rank <= 3 and finish_position = 1 then 1 else 0 end) top3_winner_capture_hit,
             max(case when predicted_rank <= 5 and finish_position = 1 then 1 else 0 end) top5_winner_capture_hit,
             sum(case when predicted_rank <= 3 and finish_position <= 3 then 1.0 else 0.0 end) / 3.0 top3_place_relation_val,
             count(*) prediction_rows
      from joined
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
    ),
    pair_per_race as (
      select j1.source, j1.kaisai_nen, j1.kaisai_tsukihi, j1.keibajo_code, j1.race_bango,
             sum(
               case
                 when (j1.predicted_rank < j2.predicted_rank) = (j1.finish_position < j2.finish_position)
                 then 1.0 else 0.0
               end
             ) pair_correct_sum,
             count(*) pair_count
      from joined j1
      join joined j2
        on j1.source = j2.source
        and j1.kaisai_nen = j2.kaisai_nen
        and j1.kaisai_tsukihi = j2.kaisai_tsukihi
        and j1.keibajo_code = j2.keibajo_code
        and j1.race_bango = j2.race_bango
        and j1.ketto_toroku_bango < j2.ketto_toroku_bango
      group by j1.source, j1.kaisai_nen, j1.kaisai_tsukihi, j1.keibajo_code, j1.race_bango
    ),
    ndcg_per_race as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             sum(
               case
                 when predicted_rank <= 3
                 then (greatest(0, 4 - finish_position)) / ln(2 + predicted_rank)
                 else 0
               end
             ) dcg,
             (3 / ln(2 + 1) + 2 / ln(2 + 2) + 1 / ln(2 + 3)) ideal_dcg
      from joined
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
    )
    select
      d.source,
      d.keibajo_code,
      d.kyori,
      d.kyoso_shubetsu_code,
      d.kyoso_joken_code,
      d.condition_key,
      d.track_code,
      d.grade_code,
      d.race_name,
      count(*) race_count,
      coalesce(sum(pr.prediction_rows), 0) prediction_count,
      coalesce(sum(pr.top1_hit::numeric), 0) top1_hit_sum,
      coalesce(sum(pr.place1_hit::numeric), 0) place1_hit_sum,
      coalesce(sum(pr.place2_hit::numeric), 0) place2_hit_sum,
      coalesce(sum(pr.place3_hit::numeric), 0) place3_hit_sum,
      coalesce(sum(pr.top3_box_hit::numeric), 0) top3_box_hit_sum,
      coalesce(sum(pr.top3_exact_hit::numeric), 0) top3_exact_hit_sum,
      coalesce(sum(pr.top3_winner_capture_hit::numeric), 0) top3_winner_capture_sum,
      coalesce(sum(pr.top5_winner_capture_hit::numeric), 0) top5_winner_capture_sum,
      coalesce(sum(pr.top3_place_relation_val), 0) top3_place_relation_sum,
      coalesce(sum(pp.pair_correct_sum), 0) pair_score_sum,
      coalesce(sum(pp.pair_count), 0) pair_score_pair_count,
      coalesce(sum(case when nd.ideal_dcg > 0 then nd.dcg / nd.ideal_dcg else 0 end), 0) ndcg_at_3_sum,
      coalesce(sum(case when nd.ideal_dcg > 0 then 1 else 0 end), 0) ndcg_at_3_race_count
    from race_dims d
    join per_race pr using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    left join pair_per_race pp using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    left join ndcg_per_race nd using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    group by d.source, d.keibajo_code, d.kyori, d.kyoso_shubetsu_code,
             d.kyoso_joken_code, d.condition_key, d.track_code, d.grade_code, d.race_name
  `);
});

test("buildBucketAggregateSql escapes single quote in model version", () => {
  expect(
    buildBucketAggregateSql({
      modelVersion: "o'reilly",
      category: "jra",
      fromDate: "20240101",
      toDate: "20251231",
      runningStyleFeatureVersion: "rs1",
      finishPositionVersion: "fp1",
    }),
  ).toBe(`
    with predictions as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             ketto_toroku_bango, predicted_rank, predicted_score
      from bucket_predictions_loaded
      where model_version = 'o''reilly'
        and running_style_feature_version = 'rs1'
        and finish_position_version = 'fp1'
    ),
    actuals as (
      select rec.source, rec.kaisai_nen, rec.kaisai_tsukihi, rec.keibajo_code, rec.race_bango,
             rec.ketto_toroku_bango, rec.finish_position
      from race_entry_corner_features rec
      where rec.race_date between '20240101' and '20251231'
        and rec.finish_position is not null
        and rec.source = 'jra'
    ),
    joined as (
      select p.*, a.finish_position
      from predictions p
      join actuals a using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    ),
    races as (
      select distinct source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango from joined
    ),
    race_dims as (
      select r.source, r.kaisai_nen, r.kaisai_tsukihi, r.keibajo_code, r.race_bango,
             ra.kyori, ra.kyoso_shubetsu_code,
             ra.kyoso_joken_code as kyoso_joken_code,
             null::text as condition_key,
             ra.track_code as track_code,
             nullif(trim(ra.grade_cd), '') as grade_code,
             case when ra.grade_cd in ('A','F') then trim(ra.kyosomei_hondai) else null end as race_name
      from races r
      join jvd_ra ra
        on ra.kaisai_nen = r.kaisai_nen
       and ra.kaisai_tsukihi = r.kaisai_tsukihi
       and ra.keibajo_code = r.keibajo_code
       and ra.race_bango = r.race_bango
      where true
    ),
    per_race as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             max(case when predicted_rank = 1 and finish_position = 1 then 1 else 0 end) top1_hit,
             (sum(case when predicted_rank <= 3 and finish_position <= 3 then 1 else 0 end) = 3)::int top3_box_hit,
             (
               max(case when predicted_rank = 1 and finish_position = 1 then 1 else 0 end) = 1
               and max(case when predicted_rank = 2 and finish_position = 2 then 1 else 0 end) = 1
               and max(case when predicted_rank = 3 and finish_position = 3 then 1 else 0 end) = 1
             )::int top3_exact_hit,
             max(case when predicted_rank = 1 and finish_position = 1 then 1 else 0 end) place1_hit,
             max(case when predicted_rank = 2 and finish_position = 2 then 1 else 0 end) place2_hit,
             max(case when predicted_rank = 3 and finish_position = 3 then 1 else 0 end) place3_hit,
             max(case when predicted_rank <= 3 and finish_position = 1 then 1 else 0 end) top3_winner_capture_hit,
             max(case when predicted_rank <= 5 and finish_position = 1 then 1 else 0 end) top5_winner_capture_hit,
             sum(case when predicted_rank <= 3 and finish_position <= 3 then 1.0 else 0.0 end) / 3.0 top3_place_relation_val,
             count(*) prediction_rows
      from joined
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
    ),
    pair_per_race as (
      select j1.source, j1.kaisai_nen, j1.kaisai_tsukihi, j1.keibajo_code, j1.race_bango,
             sum(
               case
                 when (j1.predicted_rank < j2.predicted_rank) = (j1.finish_position < j2.finish_position)
                 then 1.0 else 0.0
               end
             ) pair_correct_sum,
             count(*) pair_count
      from joined j1
      join joined j2
        on j1.source = j2.source
        and j1.kaisai_nen = j2.kaisai_nen
        and j1.kaisai_tsukihi = j2.kaisai_tsukihi
        and j1.keibajo_code = j2.keibajo_code
        and j1.race_bango = j2.race_bango
        and j1.ketto_toroku_bango < j2.ketto_toroku_bango
      group by j1.source, j1.kaisai_nen, j1.kaisai_tsukihi, j1.keibajo_code, j1.race_bango
    ),
    ndcg_per_race as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             sum(
               case
                 when predicted_rank <= 3
                 then (greatest(0, 4 - finish_position)) / ln(2 + predicted_rank)
                 else 0
               end
             ) dcg,
             (3 / ln(2 + 1) + 2 / ln(2 + 2) + 1 / ln(2 + 3)) ideal_dcg
      from joined
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
    )
    select
      d.source,
      d.keibajo_code,
      d.kyori,
      d.kyoso_shubetsu_code,
      d.kyoso_joken_code,
      d.condition_key,
      d.track_code,
      d.grade_code,
      d.race_name,
      count(*) race_count,
      coalesce(sum(pr.prediction_rows), 0) prediction_count,
      coalesce(sum(pr.top1_hit::numeric), 0) top1_hit_sum,
      coalesce(sum(pr.place1_hit::numeric), 0) place1_hit_sum,
      coalesce(sum(pr.place2_hit::numeric), 0) place2_hit_sum,
      coalesce(sum(pr.place3_hit::numeric), 0) place3_hit_sum,
      coalesce(sum(pr.top3_box_hit::numeric), 0) top3_box_hit_sum,
      coalesce(sum(pr.top3_exact_hit::numeric), 0) top3_exact_hit_sum,
      coalesce(sum(pr.top3_winner_capture_hit::numeric), 0) top3_winner_capture_sum,
      coalesce(sum(pr.top5_winner_capture_hit::numeric), 0) top5_winner_capture_sum,
      coalesce(sum(pr.top3_place_relation_val), 0) top3_place_relation_sum,
      coalesce(sum(pp.pair_correct_sum), 0) pair_score_sum,
      coalesce(sum(pp.pair_count), 0) pair_score_pair_count,
      coalesce(sum(case when nd.ideal_dcg > 0 then nd.dcg / nd.ideal_dcg else 0 end), 0) ndcg_at_3_sum,
      coalesce(sum(case when nd.ideal_dcg > 0 then 1 else 0 end), 0) ndcg_at_3_race_count
    from race_dims d
    join per_race pr using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    left join pair_per_race pp using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    left join ndcg_per_race nd using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
    group by d.source, d.keibajo_code, d.kyori, d.kyoso_shubetsu_code,
             d.kyoso_joken_code, d.condition_key, d.track_code, d.grade_code, d.race_name
  `);
});

test("buildBucketUpsertSql exposes the full UPSERT statement", () => {
  expect(buildBucketUpsertSql()).toBe(`
    insert into model_prediction_bucket_evaluations (
      model_version, running_style_feature_version, finish_position_version,
      category, evaluation_window_from, evaluation_window_to,
      source, keibajo_code, kyori, kyoso_shubetsu_code,
      kyoso_joken_code, condition_key, track_code, grade_code, race_name,
      race_count, prediction_count,
      top1_hit_sum, place1_hit_sum, place2_hit_sum, place3_hit_sum,
      top3_box_hit_sum, top3_exact_hit_sum,
      top3_winner_capture_sum, top5_winner_capture_sum, top3_place_relation_sum,
      pair_score_sum, pair_score_pair_count,
      ndcg_at_3_sum, ndcg_at_3_race_count,
      evaluated_at
    )
    values (
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
      $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
      $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
      now()
    )
    on conflict (
      model_version, running_style_feature_version, finish_position_version,
      category, evaluation_window_from, evaluation_window_to,
      source, keibajo_code, kyori, kyoso_shubetsu_code,
      coalesce(kyoso_joken_code,''), coalesce(condition_key,''),
      coalesce(track_code,''), coalesce(grade_code,''), coalesce(race_name,'')
    )
    do update set
      race_count = excluded.race_count,
      prediction_count = excluded.prediction_count,
      top1_hit_sum = excluded.top1_hit_sum,
      place1_hit_sum = excluded.place1_hit_sum,
      place2_hit_sum = excluded.place2_hit_sum,
      place3_hit_sum = excluded.place3_hit_sum,
      top3_box_hit_sum = excluded.top3_box_hit_sum,
      top3_exact_hit_sum = excluded.top3_exact_hit_sum,
      top3_winner_capture_sum = excluded.top3_winner_capture_sum,
      top5_winner_capture_sum = excluded.top5_winner_capture_sum,
      top3_place_relation_sum = excluded.top3_place_relation_sum,
      pair_score_sum = excluded.pair_score_sum,
      pair_score_pair_count = excluded.pair_score_pair_count,
      ndcg_at_3_sum = excluded.ndcg_at_3_sum,
      ndcg_at_3_race_count = excluded.ndcg_at_3_race_count,
      evaluated_at = now()
  `);
});
