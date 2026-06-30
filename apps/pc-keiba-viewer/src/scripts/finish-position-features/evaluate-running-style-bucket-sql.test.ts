// Run with: bunx vitest run src/scripts/finish-position-features/evaluate-running-style-bucket-sql.test.ts
import { expect, test } from "vitest";

import {
  BUCKET_LOOKUP_INDEX_COLUMNS,
  BUCKET_RACE_NAME_INDEX_SQL,
  BUCKET_REPLICA_IDENTITY_SQL,
  BUCKET_TABLE,
  BUCKET_UNIQUE_INDEX_COLUMNS,
  CATEGORY_BAN_EI,
  CATEGORY_JRA,
  CATEGORY_NAR,
  CM_CLASS_PAIRS,
  JRA_RA_TABLE,
  LOG_EPSILON,
  LOG_LOSS_CLASSES,
  NAR_RA_TABLE,
  PREDICTIONS_TEMP_TABLE,
  buildRunningStyleAnalyzeSqls,
  buildRunningStyleBucketAggregateSql,
  buildRunningStyleBucketBatchUpsertSql,
  buildRunningStyleBucketEvaluationsDdl,
  buildRunningStyleBucketUpsertSql,
  buildRunningStyleCategoryRaceSourceFilter,
  buildRunningStyleConditionKeySql,
  buildRunningStyleRaceNameExpressionSql,
  escapeSqlLiteral,
  resolveRunningStyleClassIndex,
} from "./evaluate-running-style-bucket-sql";

test("BUCKET_TABLE points to running_style_model_bucket_evaluations", () => {
  expect(BUCKET_TABLE).toBe("running_style_model_bucket_evaluations");
});

test("PREDICTIONS_TEMP_TABLE points to bucket_running_style_predictions_loaded", () => {
  expect(PREDICTIONS_TEMP_TABLE).toBe("bucket_running_style_predictions_loaded");
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

test("LOG_EPSILON is 1e-15", () => {
  expect(LOG_EPSILON).toBe("1e-15");
});

test("CM_CLASS_PAIRS exposes 16 actual-predicted ordered pairs", () => {
  expect(CM_CLASS_PAIRS).toStrictEqual([
    ["nige", "nige"],
    ["nige", "senkou"],
    ["nige", "sashi"],
    ["nige", "oikomi"],
    ["senkou", "nige"],
    ["senkou", "senkou"],
    ["senkou", "sashi"],
    ["senkou", "oikomi"],
    ["sashi", "nige"],
    ["sashi", "senkou"],
    ["sashi", "sashi"],
    ["sashi", "oikomi"],
    ["oikomi", "nige"],
    ["oikomi", "senkou"],
    ["oikomi", "sashi"],
    ["oikomi", "oikomi"],
  ]);
});

test("LOG_LOSS_CLASSES lists 4 running-style classes in fixed order", () => {
  expect(LOG_LOSS_CLASSES).toStrictEqual(["nige", "senkou", "sashi", "oikomi"]);
});

test("resolveRunningStyleClassIndex maps nige to 0", () => {
  expect(resolveRunningStyleClassIndex("nige")).toBe(0);
});

test("resolveRunningStyleClassIndex maps senkou to 1", () => {
  expect(resolveRunningStyleClassIndex("senkou")).toBe(1);
});

test("resolveRunningStyleClassIndex maps sashi to 2", () => {
  expect(resolveRunningStyleClassIndex("sashi")).toBe(2);
});

test("resolveRunningStyleClassIndex maps oikomi to 3", () => {
  expect(resolveRunningStyleClassIndex("oikomi")).toBe(3);
});

test("resolveRunningStyleClassIndex throws on unknown label", () => {
  expect(() => resolveRunningStyleClassIndex("xxx")).toThrowError(
    "Unknown running-style class label: xxx",
  );
});

test("BUCKET_UNIQUE_INDEX_COLUMNS lists 14 unique columns including coalesce expressions", () => {
  expect(BUCKET_UNIQUE_INDEX_COLUMNS).toStrictEqual([
    "model_version",
    "running_style_feature_version",
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

test("BUCKET_LOOKUP_INDEX_COLUMNS lists 11 viewer lookup columns", () => {
  expect(BUCKET_LOOKUP_INDEX_COLUMNS).toStrictEqual([
    "model_version",
    "running_style_feature_version",
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

test("escapeSqlLiteral doubles single quotes", () => {
  expect(escapeSqlLiteral("it's")).toBe("it''s");
});

test("escapeSqlLiteral returns plain text unchanged", () => {
  expect(escapeSqlLiteral("plain")).toBe("plain");
});

test("buildRunningStyleRaceNameExpressionSql nullif-trims kyosomei_hondai for grade A/F", () => {
  expect(buildRunningStyleRaceNameExpressionSql("ra.grade_code", "ra.kyosomei_hondai")).toBe(
    "case when ra.grade_code in ('A','F') then nullif(trim(ra.kyosomei_hondai), '') else null end",
  );
});

test("buildRunningStyleConditionKeySql nullif-trims kyoso_joken_meisho", () => {
  expect(buildRunningStyleConditionKeySql("ra.kyoso_joken_meisho")).toBe(
    "nullif(trim(ra.kyoso_joken_meisho), '')",
  );
});

test("BUCKET_RACE_NAME_INDEX_SQL is a partial index where race_name is not null", () => {
  expect(BUCKET_RACE_NAME_INDEX_SQL).toBe(
    `create index if not exists running_style_model_bucket_evaluations_race_name
      on running_style_model_bucket_evaluations (category, source, race_name, keibajo_code, kyori)
      where race_name is not null`,
  );
});

test("buildRunningStyleCategoryRaceSourceFilter returns jvd_ra for jra without filter", () => {
  expect(buildRunningStyleCategoryRaceSourceFilter("jra")).toStrictEqual({
    table: "jvd_ra",
    filter: "true",
  });
});

test("buildRunningStyleCategoryRaceSourceFilter returns nvd_ra excluding keibajo 83 for nar", () => {
  expect(buildRunningStyleCategoryRaceSourceFilter("nar")).toStrictEqual({
    table: "nvd_ra",
    filter: "ra.keibajo_code <> '83'",
  });
});

test("buildRunningStyleCategoryRaceSourceFilter throws for ban-ei (unsupported)", () => {
  expect(() => buildRunningStyleCategoryRaceSourceFilter("ban-ei")).toThrowError(
    "Running-style bucket eval does not support category: ban-ei",
  );
});

test("buildRunningStyleCategoryRaceSourceFilter throws for unknown category", () => {
  expect(() => buildRunningStyleCategoryRaceSourceFilter("xxx")).toThrowError(
    "Running-style bucket eval does not support category: xxx",
  );
});

test("buildRunningStyleAnalyzeSqls returns 3 ANALYZE statements", () => {
  expect(buildRunningStyleAnalyzeSqls()).toStrictEqual([
    "analyze running_style_model_bucket_evaluations",
    "analyze jvd_ra",
    "analyze nvd_ra",
  ]);
});

test("buildRunningStyleBucketEvaluationsDdl emits 16 cm + 8 log_loss + top2 columns and 3 indexes", () => {
  expect(buildRunningStyleBucketEvaluationsDdl()).toBe(`
    create table if not exists running_style_model_bucket_evaluations (
      model_version                 text not null,
      running_style_feature_version text not null,
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
      cm_actual_nige_pred_nige_count       integer not null,
      cm_actual_nige_pred_senkou_count     integer not null,
      cm_actual_nige_pred_sashi_count      integer not null,
      cm_actual_nige_pred_oikomi_count     integer not null,
      cm_actual_senkou_pred_nige_count     integer not null,
      cm_actual_senkou_pred_senkou_count   integer not null,
      cm_actual_senkou_pred_sashi_count    integer not null,
      cm_actual_senkou_pred_oikomi_count   integer not null,
      cm_actual_sashi_pred_nige_count      integer not null,
      cm_actual_sashi_pred_senkou_count    integer not null,
      cm_actual_sashi_pred_sashi_count     integer not null,
      cm_actual_sashi_pred_oikomi_count    integer not null,
      cm_actual_oikomi_pred_nige_count     integer not null,
      cm_actual_oikomi_pred_senkou_count   integer not null,
      cm_actual_oikomi_pred_sashi_count    integer not null,
      cm_actual_oikomi_pred_oikomi_count   integer not null,
      log_loss_nige_sum    numeric not null,
      log_loss_nige_count  integer not null,
      log_loss_senkou_sum  numeric not null,
      log_loss_senkou_count integer not null,
      log_loss_sashi_sum   numeric not null,
      log_loss_sashi_count integer not null,
      log_loss_oikomi_sum  numeric not null,
      log_loss_oikomi_count integer not null,
      top2_hit_count       integer not null,
      evaluated_at         timestamptz not null default now()
    );
    create unique index if not exists running_style_model_bucket_evaluations_uq
      on running_style_model_bucket_evaluations (model_version, running_style_feature_version, category, evaluation_window_from, evaluation_window_to, source, keibajo_code, kyori, kyoso_shubetsu_code, coalesce(kyoso_joken_code,''), coalesce(condition_key,''), coalesce(track_code,''), coalesce(grade_code,''), coalesce(race_name,''));
    create index if not exists running_style_model_bucket_evaluations_lookup
      on running_style_model_bucket_evaluations (model_version, running_style_feature_version, category, source, keibajo_code, kyori, kyoso_shubetsu_code, kyoso_joken_code, condition_key, track_code, grade_code);
    create index if not exists running_style_model_bucket_evaluations_race_name
      on running_style_model_bucket_evaluations (category, source, race_name, keibajo_code, kyori)
      where race_name is not null;
    alter table running_style_model_bucket_evaluations
      replica identity full;
  `);
});

test("BUCKET_REPLICA_IDENTITY_SQL emits ALTER TABLE ... REPLICA IDENTITY FULL so logical replication UPDATE works", () => {
  expect(BUCKET_REPLICA_IDENTITY_SQL).toBe(
    `alter table running_style_model_bucket_evaluations
      replica identity full`,
  );
});

test("buildRunningStyleBucketEvaluationsDdl emits replica identity ALTER TABLE so logical replication UPDATE works after recreate", () => {
  const ddl = buildRunningStyleBucketEvaluationsDdl();
  expect(ddl.indexOf("alter table running_style_model_bucket_evaluations")).toBeGreaterThanOrEqual(
    0,
  );
  expect(ddl.indexOf("replica identity full")).toBeGreaterThanOrEqual(0);
});

test("buildRunningStyleBucketEvaluationsDdl places ALTER TABLE replica identity after the unique index creation", () => {
  const ddl = buildRunningStyleBucketEvaluationsDdl();
  const uniqueIndexOffset = ddl.indexOf(
    "create unique index if not exists running_style_model_bucket_evaluations_uq",
  );
  const replicaIdentityOffset = ddl.indexOf("replica identity full");
  expect(uniqueIndexOffset < replicaIdentityOffset).toBe(true);
});

test("buildRunningStyleBucketAggregateSql for jra emits full aggregate with 16 cm cells, 8 log_loss cells, top2_hit", () => {
  expect(
    buildRunningStyleBucketAggregateSql({
      modelVersion: "vX",
      category: "jra",
      fromDate: "20240101",
      toDate: "20241231",
      runningStyleFeatureVersion: "v1",
    }),
  ).toBe(`
    with predictions as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             ketto_toroku_bango, predicted_class, second_predicted_class,
             target_running_style_class,
             p_nige, p_senkou, p_sashi, p_oikomi
      from bucket_running_style_predictions_loaded
      where model_version = 'vX'
        and running_style_feature_version = 'v1'
        and race_date between '20240101' and '20241231'
    ),
    races as (
      select distinct source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango from predictions
    ),
    race_dims as (
      select r.source, r.kaisai_nen, r.kaisai_tsukihi, r.keibajo_code, r.race_bango,
             nullif(trim(ra.kyori), '')::integer as kyori,
             ra.kyoso_shubetsu_code,
             ra.kyoso_joken_code as kyoso_joken_code,
             null::text as condition_key,
             ra.track_code as track_code,
             nullif(trim(ra.grade_code), '') as grade_code,
             case when ra.grade_code in ('A','F') then nullif(trim(ra.kyosomei_hondai), '') else null end as race_name
      from races r
      join jvd_ra ra
        on ra.kaisai_nen = r.kaisai_nen
       and ra.kaisai_tsukihi = r.kaisai_tsukihi
       and ra.keibajo_code = r.keibajo_code
       and ra.race_bango = r.race_bango
      where true
        and ra.kyori is not null
        and length(trim(ra.kyori)) > 0
        and ra.kyoso_shubetsu_code is not null
        and length(trim(ra.kyoso_shubetsu_code)) > 0
    ),
    joined as (
      select d.source, d.keibajo_code, d.kyori, d.kyoso_shubetsu_code,
             d.kyoso_joken_code, d.condition_key, d.track_code, d.grade_code, d.race_name,
             d.kaisai_nen, d.kaisai_tsukihi, d.race_bango,
             p.predicted_class, p.second_predicted_class, p.target_running_style_class,
             p.p_nige, p.p_senkou, p.p_sashi, p.p_oikomi
      from race_dims d
      join predictions p
        on p.source = d.source
       and p.kaisai_nen = d.kaisai_nen
       and p.kaisai_tsukihi = d.kaisai_tsukihi
       and p.keibajo_code = d.keibajo_code
       and p.race_bango = d.race_bango
    )
    select
      source,
      keibajo_code,
      kyori,
      kyoso_shubetsu_code,
      kyoso_joken_code,
      condition_key,
      track_code,
      grade_code,
      race_name,
      count(distinct (kaisai_nen, kaisai_tsukihi, race_bango)) race_count,
      count(*) prediction_count,
      coalesce(sum(case when target_running_style_class = 0 and predicted_class = 0 then 1 else 0 end), 0) cm_actual_nige_pred_nige_count,
      coalesce(sum(case when target_running_style_class = 0 and predicted_class = 1 then 1 else 0 end), 0) cm_actual_nige_pred_senkou_count,
      coalesce(sum(case when target_running_style_class = 0 and predicted_class = 2 then 1 else 0 end), 0) cm_actual_nige_pred_sashi_count,
      coalesce(sum(case when target_running_style_class = 0 and predicted_class = 3 then 1 else 0 end), 0) cm_actual_nige_pred_oikomi_count,
      coalesce(sum(case when target_running_style_class = 1 and predicted_class = 0 then 1 else 0 end), 0) cm_actual_senkou_pred_nige_count,
      coalesce(sum(case when target_running_style_class = 1 and predicted_class = 1 then 1 else 0 end), 0) cm_actual_senkou_pred_senkou_count,
      coalesce(sum(case when target_running_style_class = 1 and predicted_class = 2 then 1 else 0 end), 0) cm_actual_senkou_pred_sashi_count,
      coalesce(sum(case when target_running_style_class = 1 and predicted_class = 3 then 1 else 0 end), 0) cm_actual_senkou_pred_oikomi_count,
      coalesce(sum(case when target_running_style_class = 2 and predicted_class = 0 then 1 else 0 end), 0) cm_actual_sashi_pred_nige_count,
      coalesce(sum(case when target_running_style_class = 2 and predicted_class = 1 then 1 else 0 end), 0) cm_actual_sashi_pred_senkou_count,
      coalesce(sum(case when target_running_style_class = 2 and predicted_class = 2 then 1 else 0 end), 0) cm_actual_sashi_pred_sashi_count,
      coalesce(sum(case when target_running_style_class = 2 and predicted_class = 3 then 1 else 0 end), 0) cm_actual_sashi_pred_oikomi_count,
      coalesce(sum(case when target_running_style_class = 3 and predicted_class = 0 then 1 else 0 end), 0) cm_actual_oikomi_pred_nige_count,
      coalesce(sum(case when target_running_style_class = 3 and predicted_class = 1 then 1 else 0 end), 0) cm_actual_oikomi_pred_senkou_count,
      coalesce(sum(case when target_running_style_class = 3 and predicted_class = 2 then 1 else 0 end), 0) cm_actual_oikomi_pred_sashi_count,
      coalesce(sum(case when target_running_style_class = 3 and predicted_class = 3 then 1 else 0 end), 0) cm_actual_oikomi_pred_oikomi_count,
      coalesce(sum(case when target_running_style_class = 0 then -ln(greatest(p_nige, 1e-15)) else 0 end), 0) log_loss_nige_sum,
      coalesce(sum(case when target_running_style_class = 0 then 1 else 0 end), 0) log_loss_nige_count,
      coalesce(sum(case when target_running_style_class = 1 then -ln(greatest(p_senkou, 1e-15)) else 0 end), 0) log_loss_senkou_sum,
      coalesce(sum(case when target_running_style_class = 1 then 1 else 0 end), 0) log_loss_senkou_count,
      coalesce(sum(case when target_running_style_class = 2 then -ln(greatest(p_sashi, 1e-15)) else 0 end), 0) log_loss_sashi_sum,
      coalesce(sum(case when target_running_style_class = 2 then 1 else 0 end), 0) log_loss_sashi_count,
      coalesce(sum(case when target_running_style_class = 3 then -ln(greatest(p_oikomi, 1e-15)) else 0 end), 0) log_loss_oikomi_sum,
      coalesce(sum(case when target_running_style_class = 3 then 1 else 0 end), 0) log_loss_oikomi_count,
      coalesce(sum(case when target_running_style_class in (predicted_class, second_predicted_class) then 1 else 0 end), 0) top2_hit_count
    from joined
    where target_running_style_class is not null
      and predicted_class is not null
    group by source, keibajo_code, kyori, kyoso_shubetsu_code,
             kyoso_joken_code, condition_key, track_code, grade_code, race_name
  `);
});

test("buildRunningStyleBucketAggregateSql for nar uses nvd_ra, excludes keibajo 83, and nullifs kyoso_joken_meisho", () => {
  expect(
    buildRunningStyleBucketAggregateSql({
      modelVersion: "vX",
      category: "nar",
      fromDate: "20240101",
      toDate: "20241231",
      runningStyleFeatureVersion: "v1",
    }),
  ).toBe(`
    with predictions as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             ketto_toroku_bango, predicted_class, second_predicted_class,
             target_running_style_class,
             p_nige, p_senkou, p_sashi, p_oikomi
      from bucket_running_style_predictions_loaded
      where model_version = 'vX'
        and running_style_feature_version = 'v1'
        and race_date between '20240101' and '20241231'
    ),
    races as (
      select distinct source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango from predictions
    ),
    race_dims as (
      select r.source, r.kaisai_nen, r.kaisai_tsukihi, r.keibajo_code, r.race_bango,
             nullif(trim(ra.kyori), '')::integer as kyori,
             ra.kyoso_shubetsu_code,
             ra.kyoso_joken_code as kyoso_joken_code,
             nullif(trim(ra.kyoso_joken_meisho), '') as condition_key,
             ra.track_code as track_code,
             nullif(trim(ra.grade_code), '') as grade_code,
             case when ra.grade_code in ('A','F') then nullif(trim(ra.kyosomei_hondai), '') else null end as race_name
      from races r
      join nvd_ra ra
        on ra.kaisai_nen = r.kaisai_nen
       and ra.kaisai_tsukihi = r.kaisai_tsukihi
       and ra.keibajo_code = r.keibajo_code
       and ra.race_bango = r.race_bango
      where ra.keibajo_code <> '83'
        and ra.kyori is not null
        and length(trim(ra.kyori)) > 0
        and ra.kyoso_shubetsu_code is not null
        and length(trim(ra.kyoso_shubetsu_code)) > 0
    ),
    joined as (
      select d.source, d.keibajo_code, d.kyori, d.kyoso_shubetsu_code,
             d.kyoso_joken_code, d.condition_key, d.track_code, d.grade_code, d.race_name,
             d.kaisai_nen, d.kaisai_tsukihi, d.race_bango,
             p.predicted_class, p.second_predicted_class, p.target_running_style_class,
             p.p_nige, p.p_senkou, p.p_sashi, p.p_oikomi
      from race_dims d
      join predictions p
        on p.source = d.source
       and p.kaisai_nen = d.kaisai_nen
       and p.kaisai_tsukihi = d.kaisai_tsukihi
       and p.keibajo_code = d.keibajo_code
       and p.race_bango = d.race_bango
    )
    select
      source,
      keibajo_code,
      kyori,
      kyoso_shubetsu_code,
      kyoso_joken_code,
      condition_key,
      track_code,
      grade_code,
      race_name,
      count(distinct (kaisai_nen, kaisai_tsukihi, race_bango)) race_count,
      count(*) prediction_count,
      coalesce(sum(case when target_running_style_class = 0 and predicted_class = 0 then 1 else 0 end), 0) cm_actual_nige_pred_nige_count,
      coalesce(sum(case when target_running_style_class = 0 and predicted_class = 1 then 1 else 0 end), 0) cm_actual_nige_pred_senkou_count,
      coalesce(sum(case when target_running_style_class = 0 and predicted_class = 2 then 1 else 0 end), 0) cm_actual_nige_pred_sashi_count,
      coalesce(sum(case when target_running_style_class = 0 and predicted_class = 3 then 1 else 0 end), 0) cm_actual_nige_pred_oikomi_count,
      coalesce(sum(case when target_running_style_class = 1 and predicted_class = 0 then 1 else 0 end), 0) cm_actual_senkou_pred_nige_count,
      coalesce(sum(case when target_running_style_class = 1 and predicted_class = 1 then 1 else 0 end), 0) cm_actual_senkou_pred_senkou_count,
      coalesce(sum(case when target_running_style_class = 1 and predicted_class = 2 then 1 else 0 end), 0) cm_actual_senkou_pred_sashi_count,
      coalesce(sum(case when target_running_style_class = 1 and predicted_class = 3 then 1 else 0 end), 0) cm_actual_senkou_pred_oikomi_count,
      coalesce(sum(case when target_running_style_class = 2 and predicted_class = 0 then 1 else 0 end), 0) cm_actual_sashi_pred_nige_count,
      coalesce(sum(case when target_running_style_class = 2 and predicted_class = 1 then 1 else 0 end), 0) cm_actual_sashi_pred_senkou_count,
      coalesce(sum(case when target_running_style_class = 2 and predicted_class = 2 then 1 else 0 end), 0) cm_actual_sashi_pred_sashi_count,
      coalesce(sum(case when target_running_style_class = 2 and predicted_class = 3 then 1 else 0 end), 0) cm_actual_sashi_pred_oikomi_count,
      coalesce(sum(case when target_running_style_class = 3 and predicted_class = 0 then 1 else 0 end), 0) cm_actual_oikomi_pred_nige_count,
      coalesce(sum(case when target_running_style_class = 3 and predicted_class = 1 then 1 else 0 end), 0) cm_actual_oikomi_pred_senkou_count,
      coalesce(sum(case when target_running_style_class = 3 and predicted_class = 2 then 1 else 0 end), 0) cm_actual_oikomi_pred_sashi_count,
      coalesce(sum(case when target_running_style_class = 3 and predicted_class = 3 then 1 else 0 end), 0) cm_actual_oikomi_pred_oikomi_count,
      coalesce(sum(case when target_running_style_class = 0 then -ln(greatest(p_nige, 1e-15)) else 0 end), 0) log_loss_nige_sum,
      coalesce(sum(case when target_running_style_class = 0 then 1 else 0 end), 0) log_loss_nige_count,
      coalesce(sum(case when target_running_style_class = 1 then -ln(greatest(p_senkou, 1e-15)) else 0 end), 0) log_loss_senkou_sum,
      coalesce(sum(case when target_running_style_class = 1 then 1 else 0 end), 0) log_loss_senkou_count,
      coalesce(sum(case when target_running_style_class = 2 then -ln(greatest(p_sashi, 1e-15)) else 0 end), 0) log_loss_sashi_sum,
      coalesce(sum(case when target_running_style_class = 2 then 1 else 0 end), 0) log_loss_sashi_count,
      coalesce(sum(case when target_running_style_class = 3 then -ln(greatest(p_oikomi, 1e-15)) else 0 end), 0) log_loss_oikomi_sum,
      coalesce(sum(case when target_running_style_class = 3 then 1 else 0 end), 0) log_loss_oikomi_count,
      coalesce(sum(case when target_running_style_class in (predicted_class, second_predicted_class) then 1 else 0 end), 0) top2_hit_count
    from joined
    where target_running_style_class is not null
      and predicted_class is not null
    group by source, keibajo_code, kyori, kyoso_shubetsu_code,
             kyoso_joken_code, condition_key, track_code, grade_code, race_name
  `);
});

test("buildRunningStyleBucketAggregateSql throws when category is ban-ei", () => {
  expect(() =>
    buildRunningStyleBucketAggregateSql({
      modelVersion: "vX",
      category: "ban-ei",
      fromDate: "20240101",
      toDate: "20241231",
      runningStyleFeatureVersion: "v1",
    }),
  ).toThrowError("Running-style bucket eval does not support category: ban-ei");
});

test("buildRunningStyleBucketAggregateSql escapes single quote in model version", () => {
  expect(
    buildRunningStyleBucketAggregateSql({
      modelVersion: "o'reilly",
      category: "jra",
      fromDate: "20240101",
      toDate: "20241231",
      runningStyleFeatureVersion: "rs1",
    }),
  ).toBe(`
    with predictions as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             ketto_toroku_bango, predicted_class, second_predicted_class,
             target_running_style_class,
             p_nige, p_senkou, p_sashi, p_oikomi
      from bucket_running_style_predictions_loaded
      where model_version = 'o''reilly'
        and running_style_feature_version = 'rs1'
        and race_date between '20240101' and '20241231'
    ),
    races as (
      select distinct source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango from predictions
    ),
    race_dims as (
      select r.source, r.kaisai_nen, r.kaisai_tsukihi, r.keibajo_code, r.race_bango,
             nullif(trim(ra.kyori), '')::integer as kyori,
             ra.kyoso_shubetsu_code,
             ra.kyoso_joken_code as kyoso_joken_code,
             null::text as condition_key,
             ra.track_code as track_code,
             nullif(trim(ra.grade_code), '') as grade_code,
             case when ra.grade_code in ('A','F') then nullif(trim(ra.kyosomei_hondai), '') else null end as race_name
      from races r
      join jvd_ra ra
        on ra.kaisai_nen = r.kaisai_nen
       and ra.kaisai_tsukihi = r.kaisai_tsukihi
       and ra.keibajo_code = r.keibajo_code
       and ra.race_bango = r.race_bango
      where true
        and ra.kyori is not null
        and length(trim(ra.kyori)) > 0
        and ra.kyoso_shubetsu_code is not null
        and length(trim(ra.kyoso_shubetsu_code)) > 0
    ),
    joined as (
      select d.source, d.keibajo_code, d.kyori, d.kyoso_shubetsu_code,
             d.kyoso_joken_code, d.condition_key, d.track_code, d.grade_code, d.race_name,
             d.kaisai_nen, d.kaisai_tsukihi, d.race_bango,
             p.predicted_class, p.second_predicted_class, p.target_running_style_class,
             p.p_nige, p.p_senkou, p.p_sashi, p.p_oikomi
      from race_dims d
      join predictions p
        on p.source = d.source
       and p.kaisai_nen = d.kaisai_nen
       and p.kaisai_tsukihi = d.kaisai_tsukihi
       and p.keibajo_code = d.keibajo_code
       and p.race_bango = d.race_bango
    )
    select
      source,
      keibajo_code,
      kyori,
      kyoso_shubetsu_code,
      kyoso_joken_code,
      condition_key,
      track_code,
      grade_code,
      race_name,
      count(distinct (kaisai_nen, kaisai_tsukihi, race_bango)) race_count,
      count(*) prediction_count,
      coalesce(sum(case when target_running_style_class = 0 and predicted_class = 0 then 1 else 0 end), 0) cm_actual_nige_pred_nige_count,
      coalesce(sum(case when target_running_style_class = 0 and predicted_class = 1 then 1 else 0 end), 0) cm_actual_nige_pred_senkou_count,
      coalesce(sum(case when target_running_style_class = 0 and predicted_class = 2 then 1 else 0 end), 0) cm_actual_nige_pred_sashi_count,
      coalesce(sum(case when target_running_style_class = 0 and predicted_class = 3 then 1 else 0 end), 0) cm_actual_nige_pred_oikomi_count,
      coalesce(sum(case when target_running_style_class = 1 and predicted_class = 0 then 1 else 0 end), 0) cm_actual_senkou_pred_nige_count,
      coalesce(sum(case when target_running_style_class = 1 and predicted_class = 1 then 1 else 0 end), 0) cm_actual_senkou_pred_senkou_count,
      coalesce(sum(case when target_running_style_class = 1 and predicted_class = 2 then 1 else 0 end), 0) cm_actual_senkou_pred_sashi_count,
      coalesce(sum(case when target_running_style_class = 1 and predicted_class = 3 then 1 else 0 end), 0) cm_actual_senkou_pred_oikomi_count,
      coalesce(sum(case when target_running_style_class = 2 and predicted_class = 0 then 1 else 0 end), 0) cm_actual_sashi_pred_nige_count,
      coalesce(sum(case when target_running_style_class = 2 and predicted_class = 1 then 1 else 0 end), 0) cm_actual_sashi_pred_senkou_count,
      coalesce(sum(case when target_running_style_class = 2 and predicted_class = 2 then 1 else 0 end), 0) cm_actual_sashi_pred_sashi_count,
      coalesce(sum(case when target_running_style_class = 2 and predicted_class = 3 then 1 else 0 end), 0) cm_actual_sashi_pred_oikomi_count,
      coalesce(sum(case when target_running_style_class = 3 and predicted_class = 0 then 1 else 0 end), 0) cm_actual_oikomi_pred_nige_count,
      coalesce(sum(case when target_running_style_class = 3 and predicted_class = 1 then 1 else 0 end), 0) cm_actual_oikomi_pred_senkou_count,
      coalesce(sum(case when target_running_style_class = 3 and predicted_class = 2 then 1 else 0 end), 0) cm_actual_oikomi_pred_sashi_count,
      coalesce(sum(case when target_running_style_class = 3 and predicted_class = 3 then 1 else 0 end), 0) cm_actual_oikomi_pred_oikomi_count,
      coalesce(sum(case when target_running_style_class = 0 then -ln(greatest(p_nige, 1e-15)) else 0 end), 0) log_loss_nige_sum,
      coalesce(sum(case when target_running_style_class = 0 then 1 else 0 end), 0) log_loss_nige_count,
      coalesce(sum(case when target_running_style_class = 1 then -ln(greatest(p_senkou, 1e-15)) else 0 end), 0) log_loss_senkou_sum,
      coalesce(sum(case when target_running_style_class = 1 then 1 else 0 end), 0) log_loss_senkou_count,
      coalesce(sum(case when target_running_style_class = 2 then -ln(greatest(p_sashi, 1e-15)) else 0 end), 0) log_loss_sashi_sum,
      coalesce(sum(case when target_running_style_class = 2 then 1 else 0 end), 0) log_loss_sashi_count,
      coalesce(sum(case when target_running_style_class = 3 then -ln(greatest(p_oikomi, 1e-15)) else 0 end), 0) log_loss_oikomi_sum,
      coalesce(sum(case when target_running_style_class = 3 then 1 else 0 end), 0) log_loss_oikomi_count,
      coalesce(sum(case when target_running_style_class in (predicted_class, second_predicted_class) then 1 else 0 end), 0) top2_hit_count
    from joined
    where target_running_style_class is not null
      and predicted_class is not null
    group by source, keibajo_code, kyori, kyoso_shubetsu_code,
             kyoso_joken_code, condition_key, track_code, grade_code, race_name
  `);
});

test("buildRunningStyleBucketUpsertSql emits idempotent ON CONFLICT replacement assignments", () => {
  const sql = buildRunningStyleBucketUpsertSql();
  expect(sql.indexOf("do update set")).toBeGreaterThanOrEqual(0);
  expect(sql.indexOf("race_count = excluded.race_count")).toBeGreaterThanOrEqual(0);
  expect(sql.indexOf("prediction_count = excluded.prediction_count")).toBeGreaterThanOrEqual(0);
  expect(
    sql.indexOf("cm_actual_nige_pred_nige_count = excluded.cm_actual_nige_pred_nige_count"),
  ).toBeGreaterThanOrEqual(0);
  expect(sql.indexOf("log_loss_nige_sum = excluded.log_loss_nige_sum")).toBeGreaterThanOrEqual(0);
  expect(sql.indexOf("top2_hit_count = excluded.top2_hit_count")).toBeGreaterThanOrEqual(0);
  expect(sql.indexOf(" + running_style_model_bucket_evaluations.")).toBe(-1);
});

test("buildRunningStyleBucketAggregateSql for jra exposes 4 NOT NULL guard clauses in race_dims CTE", () => {
  const sql = buildRunningStyleBucketAggregateSql({
    modelVersion: "vX",
    category: "jra",
    fromDate: "20240101",
    toDate: "20241231",
    runningStyleFeatureVersion: "v1",
  });
  const guards = [
    sql.indexOf("and ra.kyori is not null"),
    sql.indexOf("and length(trim(ra.kyori)) > 0"),
    sql.indexOf("and ra.kyoso_shubetsu_code is not null"),
    sql.indexOf("and length(trim(ra.kyoso_shubetsu_code)) > 0"),
  ].map((index) => index >= 0);
  expect(guards).toStrictEqual([true, true, true, true]);
});

test("buildRunningStyleBucketAggregateSql for nar exposes 4 NOT NULL guard clauses in race_dims CTE", () => {
  const sql = buildRunningStyleBucketAggregateSql({
    modelVersion: "vX",
    category: "nar",
    fromDate: "20240101",
    toDate: "20241231",
    runningStyleFeatureVersion: "v1",
  });
  const guards = [
    sql.indexOf("and ra.kyori is not null"),
    sql.indexOf("and length(trim(ra.kyori)) > 0"),
    sql.indexOf("and ra.kyoso_shubetsu_code is not null"),
    sql.indexOf("and length(trim(ra.kyoso_shubetsu_code)) > 0"),
  ].map((index) => index >= 0);
  expect(guards).toStrictEqual([true, true, true, true]);
});

test("buildRunningStyleBucketAggregateSql places NOT NULL guards before the race_dims CTE closing paren", () => {
  const sql = buildRunningStyleBucketAggregateSql({
    modelVersion: "vX",
    category: "jra",
    fromDate: "20240101",
    toDate: "20241231",
    runningStyleFeatureVersion: "v1",
  });
  const kyoriGuardOffset = sql.indexOf("and ra.kyori is not null");
  const joinedCteOffset = sql.indexOf("joined as (");
  expect(kyoriGuardOffset < joinedCteOffset).toBe(true);
});

test("buildRunningStyleBucketUpsertSql does not emit PG-native $1 placeholder (psycopg incompatible)", () => {
  const sql = buildRunningStyleBucketUpsertSql();
  expect(sql.indexOf("$1")).toBe(-1);
});

test("buildRunningStyleBucketUpsertSql does not emit PG-native $41 placeholder (psycopg incompatible)", () => {
  const sql = buildRunningStyleBucketUpsertSql();
  expect(sql.indexOf("$41")).toBe(-1);
});

test("buildRunningStyleBucketUpsertSql emits exactly 41 psycopg %s placeholders in values clause", () => {
  const sql = buildRunningStyleBucketUpsertSql();
  const placeholderMatches = sql.match(/%s/g);
  expect(placeholderMatches?.length).toBe(41);
});

test("buildRunningStyleBucketBatchUpsertSql with rowCount 1 emits exactly 41 psycopg %s placeholders (single-row path preserved)", () => {
  const sql = buildRunningStyleBucketBatchUpsertSql(1);
  const placeholderMatches = sql.match(/%s/g);
  expect(placeholderMatches?.length).toBe(41);
});

test("buildRunningStyleBucketBatchUpsertSql with rowCount 3 emits 123 psycopg %s placeholders (3 * 41)", () => {
  const sql = buildRunningStyleBucketBatchUpsertSql(3);
  const placeholderMatches = sql.match(/%s/g);
  expect(placeholderMatches?.length).toBe(123);
});

test("buildRunningStyleBucketBatchUpsertSql with rowCount 100 emits 4100 psycopg %s placeholders (100 * 41)", () => {
  const sql = buildRunningStyleBucketBatchUpsertSql(100);
  const placeholderMatches = sql.match(/%s/g);
  expect(placeholderMatches?.length).toBe(4100);
});

test("buildRunningStyleBucketBatchUpsertSql with rowCount 3 replaces race_count on conflict", () => {
  const sql = buildRunningStyleBucketBatchUpsertSql(3);
  expect(sql.indexOf("race_count = excluded.race_count")).toBeGreaterThanOrEqual(0);
  expect(
    sql.indexOf(
      "race_count = excluded.race_count + running_style_model_bucket_evaluations.race_count",
    ),
  ).toBe(-1);
});

test("buildRunningStyleBucketBatchUpsertSql with rowCount 3 replaces top2_hit_count on conflict", () => {
  const sql = buildRunningStyleBucketBatchUpsertSql(3);
  expect(sql.indexOf("top2_hit_count = excluded.top2_hit_count")).toBeGreaterThanOrEqual(0);
  expect(
    sql.indexOf(
      "top2_hit_count = excluded.top2_hit_count + running_style_model_bucket_evaluations.top2_hit_count",
    ),
  ).toBe(-1);
});

test("buildRunningStyleBucketBatchUpsertSql with rowCount 3 replaces log_loss_nige_sum on conflict", () => {
  const sql = buildRunningStyleBucketBatchUpsertSql(3);
  expect(sql.indexOf("log_loss_nige_sum = excluded.log_loss_nige_sum")).toBeGreaterThanOrEqual(0);
  expect(
    sql.indexOf(
      "log_loss_nige_sum = excluded.log_loss_nige_sum + running_style_model_bucket_evaluations.log_loss_nige_sum",
    ),
  ).toBe(-1);
});

test("buildRunningStyleBucketBatchUpsertSql with rowCount 3 keeps the evaluated_at = now() trailing SET", () => {
  const sql = buildRunningStyleBucketBatchUpsertSql(3);
  expect(sql.indexOf("evaluated_at = now()")).toBeGreaterThanOrEqual(0);
});

test("buildRunningStyleBucketBatchUpsertSql with rowCount 1 emits a single VALUES row tuple closed by now()", () => {
  const sql = buildRunningStyleBucketBatchUpsertSql(1);
  expect(
    sql.indexOf(
      "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())",
    ),
  ).toBeGreaterThanOrEqual(0);
  expect(sql.indexOf("do update set")).toBeGreaterThanOrEqual(0);
});

test("buildRunningStyleBucketBatchUpsertSql with rowCount 2 emits two comma-separated VALUES row tuples", () => {
  const sql = buildRunningStyleBucketBatchUpsertSql(2);
  const rowMatches = sql.match(
    /\(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now\(\)\)/g,
  );
  expect(rowMatches?.length).toBe(2);
});

test("buildRunningStyleBucketBatchUpsertSql with rowCount 3 places ON CONFLICT after the comma-separated VALUES tuples", () => {
  const sql = buildRunningStyleBucketBatchUpsertSql(3);
  const valuesOffset = sql.indexOf("values\n      (%s,");
  const conflictOffset = sql.indexOf("on conflict (");
  expect(valuesOffset >= 0 && valuesOffset < conflictOffset).toBe(true);
});

test("buildRunningStyleBucketBatchUpsertSql throws when rowCount is zero", () => {
  expect(() => buildRunningStyleBucketBatchUpsertSql(0)).toThrowError(
    "rowCount must be greater than zero.",
  );
});

test("buildRunningStyleBucketBatchUpsertSql throws when rowCount is negative", () => {
  expect(() => buildRunningStyleBucketBatchUpsertSql(-3)).toThrowError(
    "rowCount must be greater than zero.",
  );
});

test("buildRunningStyleBucketBatchUpsertSql does not emit PG-native $1 placeholder (psycopg incompatible)", () => {
  const sql = buildRunningStyleBucketBatchUpsertSql(5);
  expect(sql.indexOf("$1")).toBe(-1);
});
