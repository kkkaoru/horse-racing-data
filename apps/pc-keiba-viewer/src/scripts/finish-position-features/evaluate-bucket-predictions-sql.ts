// Run with: imported from evaluate-bucket-21y.ts (bun runtime)

const BUCKET_TABLE = "model_prediction_bucket_evaluations";
const PREDICTIONS_TEMP_TABLE = "bucket_predictions_loaded";
const SOURCE_TABLE = "race_entry_corner_features";
const JRA_RA_TABLE = "jvd_ra";
const NAR_RA_TABLE = "nvd_ra";
const CATEGORY_JRA = "jra";
const CATEGORY_NAR = "nar";
const CATEGORY_BAN_EI = "ban-ei";

const BUCKET_UNIQUE_INDEX_COLUMNS = [
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
];

const BUCKET_LOOKUP_INDEX_COLUMNS = [
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
];

const CONDITION_LABEL_PAIRS: ReadonlyArray<readonly [string, string]> = [
  ["005", "1勝クラス"],
  ["010", "2勝クラス"],
  ["016", "3勝クラス"],
  ["701", "新馬"],
  ["702", "未出走"],
  ["703", "未勝利"],
  ["999", "オープン"],
];

interface BuildBucketAggregateSqlArgs {
  modelVersion: string;
  category: string;
  fromDate: string;
  toDate: string;
  runningStyleFeatureVersion: string;
  finishPositionVersion: string;
}

export const escapeSqlLiteral = (value: string): string => value.replaceAll("'", "''");

export const buildConditionCaseSql = (kyosoJokenColumn: string, meishoColumn: string): string => {
  const whenClauses = CONDITION_LABEL_PAIRS.map(
    ([code, label]) =>
      `when ${kyosoJokenColumn} = '${escapeSqlLiteral(code)}' then '${escapeSqlLiteral(label)}'`,
  ).join("\n        ");
  return `case
        ${whenClauses}
        else nullif(split_part(trim(${meishoColumn}), ' ', 1), '')
      end`;
};

export const buildRaceNameExpressionSql = (gradeColumn: string, hondaiColumn: string): string =>
  `case when ${gradeColumn} in ('A','F') then trim(${hondaiColumn}) else null end`;

export const BUCKET_RACE_NAME_INDEX_SQL = `create index if not exists ${BUCKET_TABLE}_race_name
      on ${BUCKET_TABLE} (category, source, race_name, keibajo_code, kyori)
      where race_name is not null`;

export const RACE_ENTRY_CORNER_FEATURES_BUCKETING_INDEX_SQL = `create index concurrently if not exists race_entry_corner_features_bucketing_idx
      on race_entry_corner_features (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
      include (ketto_toroku_bango, finish_position, finish_norm, shusso_tosu)`;

export const JRA_RA_RACE_KEY_BUCKET_INDEX_SQL = `create index concurrently if not exists jvd_ra_race_key_bucket_idx
      on jvd_ra (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
      include (kyori, grade_cd, kyoso_shubetsu_code, kyoso_joken_code, kyoso_joken_meisho, track_code, kyosomei_hondai)`;

export const NAR_RA_RACE_KEY_BUCKET_INDEX_SQL = `create index concurrently if not exists nvd_ra_race_key_bucket_idx
      on nvd_ra (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
      include (kyori, grade_cd, kyoso_shubetsu_code, kyoso_joken_code, kyoso_joken_meisho, track_code, kyosomei_hondai)`;

export const buildBucketEvaluationsDdl = (): string => `
    create table if not exists ${BUCKET_TABLE} (
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
    create unique index if not exists ${BUCKET_TABLE}_uq
      on ${BUCKET_TABLE} (${BUCKET_UNIQUE_INDEX_COLUMNS.join(", ")});
    create index if not exists ${BUCKET_TABLE}_lookup
      on ${BUCKET_TABLE} (${BUCKET_LOOKUP_INDEX_COLUMNS.join(", ")});
    ${BUCKET_RACE_NAME_INDEX_SQL};
  `;

export const buildConcurrentIndexSqls = (): string[] => [
  RACE_ENTRY_CORNER_FEATURES_BUCKETING_INDEX_SQL,
  JRA_RA_RACE_KEY_BUCKET_INDEX_SQL,
  NAR_RA_RACE_KEY_BUCKET_INDEX_SQL,
];

export const buildCategoryRaceSourceFilter = (
  category: string,
): { table: string; filter: string } => {
  if (category === CATEGORY_JRA) {
    return { table: JRA_RA_TABLE, filter: "true" };
  }
  if (category === CATEGORY_NAR) {
    return { table: NAR_RA_TABLE, filter: "ra.keibajo_code <> '83'" };
  }
  if (category === CATEGORY_BAN_EI) {
    return { table: NAR_RA_TABLE, filter: "ra.keibajo_code = '83'" };
  }
  throw new Error(`Unknown category: ${category}`);
};

export const buildCategoryActualsFilter = (category: string): string => {
  if (category === CATEGORY_JRA) {
    return "rec.source = 'jra'";
  }
  if (category === CATEGORY_NAR) {
    return "rec.source = 'nar' and rec.keibajo_code <> '83'";
  }
  if (category === CATEGORY_BAN_EI) {
    return "rec.source = 'nar' and rec.keibajo_code = '83'";
  }
  throw new Error(`Unknown category: ${category}`);
};

export const buildBucketAggregateSql = (args: BuildBucketAggregateSqlArgs): string => {
  const {
    modelVersion,
    category,
    fromDate,
    toDate,
    runningStyleFeatureVersion,
    finishPositionVersion,
  } = args;
  const raMeta = buildCategoryRaceSourceFilter(category);
  const actualsFilter = buildCategoryActualsFilter(category);
  const trackExpr = category === CATEGORY_BAN_EI ? "null::text" : "ra.track_code";
  const jokenExpr = category === CATEGORY_BAN_EI ? "null::text" : "ra.kyoso_joken_code";
  const conditionKeyExpr =
    category === CATEGORY_NAR
      ? buildConditionCaseSql("ra.kyoso_joken_code", "ra.kyoso_joken_meisho")
      : "null::text";
  return `
    with predictions as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             ketto_toroku_bango, predicted_rank, predicted_score
      from ${PREDICTIONS_TEMP_TABLE}
      where model_version = '${escapeSqlLiteral(modelVersion)}'
        and running_style_feature_version = '${escapeSqlLiteral(runningStyleFeatureVersion)}'
        and finish_position_version = '${escapeSqlLiteral(finishPositionVersion)}'
    ),
    actuals as (
      select rec.source, rec.kaisai_nen, rec.kaisai_tsukihi, rec.keibajo_code, rec.race_bango,
             rec.ketto_toroku_bango, rec.finish_position
      from ${SOURCE_TABLE} rec
      where rec.race_date between '${escapeSqlLiteral(fromDate)}' and '${escapeSqlLiteral(toDate)}'
        and rec.finish_position is not null
        and ${actualsFilter}
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
             ${jokenExpr} as kyoso_joken_code,
             ${conditionKeyExpr} as condition_key,
             ${trackExpr} as track_code,
             nullif(trim(ra.grade_cd), '') as grade_code,
             ${buildRaceNameExpressionSql("ra.grade_cd", "ra.kyosomei_hondai")} as race_name
      from races r
      join ${raMeta.table} ra
        on ra.kaisai_nen = r.kaisai_nen
       and ra.kaisai_tsukihi = r.kaisai_tsukihi
       and ra.keibajo_code = r.keibajo_code
       and ra.race_bango = r.race_bango
      where ${raMeta.filter}
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
  `;
};

export const buildBucketUpsertSql = (): string => `
    insert into ${BUCKET_TABLE} (
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
  `;

export const buildAnalyzeSqls = (): string[] => [
  `analyze ${BUCKET_TABLE}`,
  "analyze race_entry_corner_features",
  "analyze jvd_ra",
  "analyze nvd_ra",
];

export {
  BUCKET_TABLE,
  BUCKET_LOOKUP_INDEX_COLUMNS,
  BUCKET_UNIQUE_INDEX_COLUMNS,
  CATEGORY_BAN_EI,
  CATEGORY_JRA,
  CATEGORY_NAR,
  CONDITION_LABEL_PAIRS,
  JRA_RA_TABLE,
  NAR_RA_TABLE,
  PREDICTIONS_TEMP_TABLE,
  SOURCE_TABLE,
};
