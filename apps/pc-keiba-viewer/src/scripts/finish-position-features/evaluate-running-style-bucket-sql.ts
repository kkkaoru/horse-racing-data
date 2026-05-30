// Run with: imported from evaluate-running-style-bucket-21y.ts (bun runtime)

const BUCKET_TABLE = "running_style_model_bucket_evaluations";
const PREDICTIONS_TEMP_TABLE = "bucket_running_style_predictions_loaded";
const JRA_RA_TABLE = "jvd_ra";
const NAR_RA_TABLE = "nvd_ra";
const CATEGORY_JRA = "jra";
const CATEGORY_NAR = "nar";
const CATEGORY_BAN_EI = "ban-ei";
const LOG_EPSILON = "1e-15";

const BUCKET_UNIQUE_INDEX_COLUMNS = [
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
];

const BUCKET_LOOKUP_INDEX_COLUMNS = [
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
];

const CM_CLASS_PAIRS: ReadonlyArray<readonly [string, string]> = [
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
];

const LOG_LOSS_CLASSES: ReadonlyArray<string> = ["nige", "senkou", "sashi", "oikomi"];

interface BuildRunningStyleBucketAggregateSqlArgs {
  modelVersion: string;
  category: string;
  fromDate: string;
  toDate: string;
  runningStyleFeatureVersion: string;
}

export const escapeSqlLiteral = (value: string): string => value.replaceAll("'", "''");

export const buildRunningStyleRaceNameExpressionSql = (
  gradeColumn: string,
  hondaiColumn: string,
): string =>
  `case when ${gradeColumn} in ('A','F') then nullif(trim(${hondaiColumn}), '') else null end`;

export const buildRunningStyleConditionKeySql = (meishoColumn: string): string =>
  `nullif(trim(${meishoColumn}), '')`;

const buildCmColumnName = (actual: string, predicted: string): string =>
  `cm_actual_${actual}_pred_${predicted}_count`;

const buildCmSumCaseSql = (actual: string, predicted: string): string =>
  `coalesce(sum(case when target_running_style_class = '${actual}' and predicted_class = '${predicted}' then 1 else 0 end), 0) ${buildCmColumnName(actual, predicted)}`;

const buildLogLossSumCaseSql = (className: string): string =>
  `coalesce(sum(case when target_running_style_class = '${className}' then -ln(greatest(p_${className}, ${LOG_EPSILON})) else 0 end), 0) log_loss_${className}_sum`;

const buildLogLossCountCaseSql = (className: string): string =>
  `coalesce(sum(case when target_running_style_class = '${className}' then 1 else 0 end), 0) log_loss_${className}_count`;

const buildTop2HitCountSql = (): string =>
  `coalesce(sum(case when target_running_style_class in (predicted_class, second_predicted_class) then 1 else 0 end), 0) top2_hit_count`;

export const BUCKET_RACE_NAME_INDEX_SQL = `create index if not exists ${BUCKET_TABLE}_race_name
      on ${BUCKET_TABLE} (category, source, race_name, keibajo_code, kyori)
      where race_name is not null`;

export const buildRunningStyleBucketEvaluationsDdl = (): string => `
    create table if not exists ${BUCKET_TABLE} (
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
    create unique index if not exists ${BUCKET_TABLE}_uq
      on ${BUCKET_TABLE} (${BUCKET_UNIQUE_INDEX_COLUMNS.join(", ")});
    create index if not exists ${BUCKET_TABLE}_lookup
      on ${BUCKET_TABLE} (${BUCKET_LOOKUP_INDEX_COLUMNS.join(", ")});
    ${BUCKET_RACE_NAME_INDEX_SQL};
  `;

export const buildRunningStyleCategoryRaceSourceFilter = (
  category: string,
): { table: string; filter: string } => {
  if (category === CATEGORY_JRA) {
    return { table: JRA_RA_TABLE, filter: "true" };
  }
  if (category === CATEGORY_NAR) {
    return { table: NAR_RA_TABLE, filter: "ra.keibajo_code <> '83'" };
  }
  throw new Error(`Running-style bucket eval does not support category: ${category}`);
};

const buildCmSelectClauses = (): string =>
  CM_CLASS_PAIRS.map(([actual, predicted]) => buildCmSumCaseSql(actual, predicted)).join(
    ",\n      ",
  );

const buildLogLossSelectClauses = (): string =>
  LOG_LOSS_CLASSES.flatMap((className) => [
    buildLogLossSumCaseSql(className),
    buildLogLossCountCaseSql(className),
  ]).join(",\n      ");

export const buildRunningStyleBucketAggregateSql = (
  args: BuildRunningStyleBucketAggregateSqlArgs,
): string => {
  const { modelVersion, category, fromDate, toDate, runningStyleFeatureVersion } = args;
  const raMeta = buildRunningStyleCategoryRaceSourceFilter(category);
  const trackExpr = "ra.track_code";
  const jokenExpr = "ra.kyoso_joken_code";
  const conditionKeyExpr =
    category === CATEGORY_NAR
      ? buildRunningStyleConditionKeySql("ra.kyoso_joken_meisho")
      : "null::text";
  return `
    with predictions as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             ketto_toroku_bango, predicted_class, second_predicted_class,
             target_running_style_class,
             p_nige, p_senkou, p_sashi, p_oikomi
      from ${PREDICTIONS_TEMP_TABLE}
      where model_version = '${escapeSqlLiteral(modelVersion)}'
        and running_style_feature_version = '${escapeSqlLiteral(runningStyleFeatureVersion)}'
        and race_date between '${escapeSqlLiteral(fromDate)}' and '${escapeSqlLiteral(toDate)}'
    ),
    races as (
      select distinct source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango from predictions
    ),
    race_dims as (
      select r.source, r.kaisai_nen, r.kaisai_tsukihi, r.keibajo_code, r.race_bango,
             ra.kyori, ra.kyoso_shubetsu_code,
             ${jokenExpr} as kyoso_joken_code,
             ${conditionKeyExpr} as condition_key,
             ${trackExpr} as track_code,
             nullif(trim(ra.grade_cd), '') as grade_code,
             ${buildRunningStyleRaceNameExpressionSql("ra.grade_cd", "ra.kyosomei_hondai")} as race_name
      from races r
      join ${raMeta.table} ra
        on ra.kaisai_nen = r.kaisai_nen
       and ra.kaisai_tsukihi = r.kaisai_tsukihi
       and ra.keibajo_code = r.keibajo_code
       and ra.race_bango = r.race_bango
      where ${raMeta.filter}
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
      ${buildCmSelectClauses()},
      ${buildLogLossSelectClauses()},
      ${buildTop2HitCountSql()}
    from joined
    where target_running_style_class is not null
      and predicted_class is not null
    group by source, keibajo_code, kyori, kyoso_shubetsu_code,
             kyoso_joken_code, condition_key, track_code, grade_code, race_name
  `;
};

const buildUpsertColumnList = (): string[] => [
  "model_version",
  "running_style_feature_version",
  "category",
  "evaluation_window_from",
  "evaluation_window_to",
  "source",
  "keibajo_code",
  "kyori",
  "kyoso_shubetsu_code",
  "kyoso_joken_code",
  "condition_key",
  "track_code",
  "grade_code",
  "race_name",
  "race_count",
  "prediction_count",
  ...CM_CLASS_PAIRS.map(([actual, predicted]) => buildCmColumnName(actual, predicted)),
  ...LOG_LOSS_CLASSES.flatMap((className) => [
    `log_loss_${className}_sum`,
    `log_loss_${className}_count`,
  ]),
  "top2_hit_count",
];

const buildUpsertPlaceholderList = (count: number): string =>
  Array.from({ length: count }, (_, index) => `$${index + 1}`).join(", ");

const buildAdditiveSetClause = (column: string): string =>
  `${column} = excluded.${column} + ${BUCKET_TABLE}.${column}`;

const buildAdditiveColumns = (): string[] => [
  "race_count",
  "prediction_count",
  ...CM_CLASS_PAIRS.map(([actual, predicted]) => buildCmColumnName(actual, predicted)),
  ...LOG_LOSS_CLASSES.flatMap((className) => [
    `log_loss_${className}_sum`,
    `log_loss_${className}_count`,
  ]),
  "top2_hit_count",
];

export const buildRunningStyleBucketUpsertSql = (): string => {
  const columns = buildUpsertColumnList();
  const placeholders = buildUpsertPlaceholderList(columns.length);
  const additiveColumns = buildAdditiveColumns();
  const setClauses = additiveColumns
    .map((column) => buildAdditiveSetClause(column))
    .concat(["evaluated_at = now()"])
    .join(",\n      ");
  return `
    insert into ${BUCKET_TABLE} (
      ${columns.join(", ")},
      evaluated_at
    )
    values (
      ${placeholders},
      now()
    )
    on conflict (
      model_version, running_style_feature_version, category,
      evaluation_window_from, evaluation_window_to,
      source, keibajo_code, kyori, kyoso_shubetsu_code,
      coalesce(kyoso_joken_code,''), coalesce(condition_key,''),
      coalesce(track_code,''), coalesce(grade_code,''), coalesce(race_name,'')
    )
    do update set
      ${setClauses}
  `;
};

export const buildRunningStyleAnalyzeSqls = (): string[] => [
  `analyze ${BUCKET_TABLE}`,
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
  CM_CLASS_PAIRS,
  JRA_RA_TABLE,
  LOG_LOSS_CLASSES,
  LOG_EPSILON,
  NAR_RA_TABLE,
  PREDICTIONS_TEMP_TABLE,
};
