// Run with: imported from evaluate-running-style-bucket-21y.ts (bun runtime)

const BUCKET_TABLE = "running_style_model_bucket_evaluations";
const PREDICTIONS_TEMP_TABLE = "bucket_running_style_predictions_loaded";
const JRA_RA_TABLE = "jvd_ra";
const NAR_RA_TABLE = "nvd_ra";
const CATEGORY_JRA = "jra";
const CATEGORY_NAR = "nar";
const CATEGORY_BAN_EI = "ban-ei";
const LOG_EPSILON = "1e-15";
const SOURCE_FEATURE_TABLE = "race_entry_corner_features";

const BUCKET_UNIQUE_INDEX_COLUMNS = [
  "model_version",
  "running_style_feature_version",
  "category",
  "coalesce(cell_model_key,'')",
  "coalesce(cell_variant_id,'')",
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
  "cell_model_key",
  "cell_variant_id",
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
const ORDER_PAIR_METRICS: ReadonlyArray<string> = ["corner1", "corner3", "corner4", "finish"];

// Predictions loader (load_running_style_predictions.py) writes
// target_running_style_class / predicted_class / second_predicted_class as
// INTEGER columns whose values come from apply-running-style-postproc.ts:
//   nige=0, senkou=1, sashi=2, oikomi=3 (CLASS_LABELS index order).
// SQL comparisons must use the integer encoding; comparing to the text label
// (e.g. target_running_style_class = 'nige') raises
// "invalid input syntax for type integer".
const RUNNING_STYLE_CLASS_INDEX_BY_LABEL: Readonly<Record<string, number>> = {
  nige: 0,
  senkou: 1,
  sashi: 2,
  oikomi: 3,
};

export const resolveRunningStyleClassIndex = (label: string): number => {
  const index = RUNNING_STYLE_CLASS_INDEX_BY_LABEL[label];
  if (index === undefined) throw new Error(`Unknown running-style class label: ${label}`);
  return index;
};

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
  `coalesce(sum(case when target_running_style_class = ${resolveRunningStyleClassIndex(actual)} and predicted_class = ${resolveRunningStyleClassIndex(predicted)} then 1 else 0 end), 0) ${buildCmColumnName(actual, predicted)}`;

const buildLogLossSumCaseSql = (className: string): string =>
  `coalesce(sum(case when target_running_style_class = ${resolveRunningStyleClassIndex(className)} then -ln(greatest(p_${className}, ${LOG_EPSILON})) else 0 end), 0) log_loss_${className}_sum`;

const buildLogLossCountCaseSql = (className: string): string =>
  `coalesce(sum(case when target_running_style_class = ${resolveRunningStyleClassIndex(className)} then 1 else 0 end), 0) log_loss_${className}_count`;

const buildTop2HitCountSql = (): string =>
  `coalesce(sum(case when target_running_style_class in (predicted_class, second_predicted_class) then 1 else 0 end), 0) top2_hit_count`;

const buildOrderPairScoreColumn = (metric: string): string => `${metric}_pair_score_sum`;

const buildOrderPairCountColumn = (metric: string): string => `${metric}_pair_score_count`;

const buildOrderPairSelectClauses = (): string =>
  ORDER_PAIR_METRICS.flatMap((metric) => [
    `coalesce(max(op.${buildOrderPairScoreColumn(metric)}), 0) ${buildOrderPairScoreColumn(metric)}`,
    `coalesce(max(op.${buildOrderPairCountColumn(metric)}), 0) ${buildOrderPairCountColumn(metric)}`,
  ]).join(",\n      ");

const buildOrderPairValueSql = (actualColumn: string, positiveOnly = false): string => `
             case
               when j1.${actualColumn} is null or j2.${actualColumn} is null${
                 positiveOnly ? ` or j1.${actualColumn} <= 0 or j2.${actualColumn} <= 0` : ""
               } then null
               when j1.predicted_front_score = j2.predicted_front_score
                 or j1.${actualColumn} = j2.${actualColumn}
               then 0.5
               when (j1.predicted_front_score < j2.predicted_front_score)
                 = (j1.${actualColumn} < j2.${actualColumn})
               then 1.0
               else 0.0
             end`;

const buildOrderPairAddColumnDdl = (): string =>
  ORDER_PAIR_METRICS.flatMap((metric) => [
    `alter table ${BUCKET_TABLE} add column if not exists ${buildOrderPairScoreColumn(metric)} numeric not null default 0;`,
    `alter table ${BUCKET_TABLE} add column if not exists ${buildOrderPairCountColumn(metric)} integer not null default 0;`,
  ]).join("\n    ");

export const BUCKET_RACE_NAME_INDEX_SQL = `create index if not exists ${BUCKET_TABLE}_race_name
      on ${BUCKET_TABLE} (category, source, race_name, keibajo_code, kyori)
      where race_name is not null`;

// Logical replication publishes UPDATEs on this table, so PG requires a
// REPLICA IDENTITY. USING INDEX cannot point at the _uq index because PG
// rejects expression indexes (the unique index wraps nullable columns with
// coalesce(...)) and also requires every covered column to be NOT NULL, so
// we fall back to FULL. ALTER TABLE ... REPLICA IDENTITY is idempotent and
// safe to re-run on every DDL bootstrap.
export const BUCKET_REPLICA_IDENTITY_SQL = `alter table ${BUCKET_TABLE}
      replica identity full`;

export const buildRunningStyleBucketEvaluationsDdl = (): string => `
    create table if not exists ${BUCKET_TABLE} (
      model_version                 text not null,
      running_style_feature_version text not null,
      category                      text not null,
      cell_model_key                text,
      cell_variant_id               text,
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
      corner1_pair_score_sum   numeric not null default 0,
      corner1_pair_score_count integer not null default 0,
      corner3_pair_score_sum   numeric not null default 0,
      corner3_pair_score_count integer not null default 0,
      corner4_pair_score_sum   numeric not null default 0,
      corner4_pair_score_count integer not null default 0,
      finish_pair_score_sum    numeric not null default 0,
      finish_pair_score_count  integer not null default 0,
      evaluated_at         timestamptz not null default now()
    );
    create unique index if not exists ${BUCKET_TABLE}_uq
      on ${BUCKET_TABLE} (${BUCKET_UNIQUE_INDEX_COLUMNS.join(", ")});
    create index if not exists ${BUCKET_TABLE}_lookup
      on ${BUCKET_TABLE} (${BUCKET_LOOKUP_INDEX_COLUMNS.join(", ")});
    ${BUCKET_RACE_NAME_INDEX_SQL};
    alter table ${BUCKET_TABLE} add column if not exists cell_model_key text;
    alter table ${BUCKET_TABLE} add column if not exists cell_variant_id text;
    ${buildOrderPairAddColumnDdl()}
    ${BUCKET_REPLICA_IDENTITY_SQL};
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
             p_nige, p_senkou, p_sashi, p_oikomi,
             cell_model_key, cell_variant_id
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
             nullif(trim(ra.kyori), '')::integer as kyori,
             ra.kyoso_shubetsu_code,
             ${jokenExpr} as kyoso_joken_code,
             ${conditionKeyExpr} as condition_key,
             ${trackExpr} as track_code,
             nullif(trim(ra.grade_code), '') as grade_code,
             ${buildRunningStyleRaceNameExpressionSql("ra.grade_code", "ra.kyosomei_hondai")} as race_name
      from races r
      join ${raMeta.table} ra
        on ra.kaisai_nen = r.kaisai_nen
       and ra.kaisai_tsukihi = r.kaisai_tsukihi
       and ra.keibajo_code = r.keibajo_code
       and ra.race_bango = r.race_bango
      where ${raMeta.filter}
        and ra.kyori is not null
        and length(trim(ra.kyori)) > 0
        and ra.kyoso_shubetsu_code is not null
        and length(trim(ra.kyoso_shubetsu_code)) > 0
    ),
    joined as (
      select d.source, d.keibajo_code, d.kyori, d.kyoso_shubetsu_code,
             d.kyoso_joken_code, d.condition_key, d.track_code, d.grade_code, d.race_name,
             p.cell_model_key, p.cell_variant_id,
             d.kaisai_nen, d.kaisai_tsukihi, d.race_bango,
             p.ketto_toroku_bango,
             p.predicted_class, p.second_predicted_class, p.target_running_style_class,
             p.p_nige, p.p_senkou, p.p_sashi, p.p_oikomi,
             (p.p_senkou + (2 * p.p_sashi) + (3 * p.p_oikomi)) predicted_front_score,
             rec.corner1_norm, rec.corner3_norm, rec.corner4_norm, rec.finish_position
      from race_dims d
      join predictions p
        on p.source = d.source
       and p.kaisai_nen = d.kaisai_nen
       and p.kaisai_tsukihi = d.kaisai_tsukihi
       and p.keibajo_code = d.keibajo_code
       and p.race_bango = d.race_bango
      left join ${SOURCE_FEATURE_TABLE} rec
        on rec.source = p.source
       and rec.kaisai_nen = p.kaisai_nen
       and rec.kaisai_tsukihi = p.kaisai_tsukihi
       and rec.keibajo_code = p.keibajo_code
       and rec.race_bango = p.race_bango
       and rec.ketto_toroku_bango = p.ketto_toroku_bango
    ),
    labeled as (
      select *
      from joined
      where target_running_style_class is not null
        and predicted_class is not null
    ),
    order_pairs as (
      select source, keibajo_code, kyori, kyoso_shubetsu_code,
             kyoso_joken_code, condition_key, track_code, grade_code, race_name,
             cell_model_key, cell_variant_id,
             coalesce(sum(corner1_pair_score), 0) corner1_pair_score_sum,
             count(corner1_pair_score) corner1_pair_score_count,
             coalesce(sum(corner3_pair_score), 0) corner3_pair_score_sum,
             count(corner3_pair_score) corner3_pair_score_count,
             coalesce(sum(corner4_pair_score), 0) corner4_pair_score_sum,
             count(corner4_pair_score) corner4_pair_score_count,
             coalesce(sum(finish_pair_score), 0) finish_pair_score_sum,
             count(finish_pair_score) finish_pair_score_count
      from (
        select j1.source, j1.keibajo_code, j1.kyori, j1.kyoso_shubetsu_code,
               j1.kyoso_joken_code, j1.condition_key, j1.track_code, j1.grade_code, j1.race_name,
               j1.cell_model_key, j1.cell_variant_id,
               ${buildOrderPairValueSql("corner1_norm")} corner1_pair_score,
               ${buildOrderPairValueSql("corner3_norm")} corner3_pair_score,
               ${buildOrderPairValueSql("corner4_norm")} corner4_pair_score,
               ${buildOrderPairValueSql("finish_position", true)} finish_pair_score
        from labeled j1
        join labeled j2
          on j1.source = j2.source
         and j1.kaisai_nen = j2.kaisai_nen
         and j1.kaisai_tsukihi = j2.kaisai_tsukihi
         and j1.keibajo_code = j2.keibajo_code
         and j1.race_bango = j2.race_bango
         and j1.ketto_toroku_bango < j2.ketto_toroku_bango
      ) pairs
      group by source, keibajo_code, kyori, kyoso_shubetsu_code,
               kyoso_joken_code, condition_key, track_code, grade_code, race_name,
               cell_model_key, cell_variant_id
    )
    select
      j.source,
      j.cell_model_key,
      j.cell_variant_id,
      j.keibajo_code,
      j.kyori,
      j.kyoso_shubetsu_code,
      j.kyoso_joken_code,
      j.condition_key,
      j.track_code,
      j.grade_code,
      j.race_name,
      count(distinct (j.kaisai_nen, j.kaisai_tsukihi, j.race_bango)) race_count,
      count(*) prediction_count,
      ${buildCmSelectClauses()},
      ${buildLogLossSelectClauses()},
      ${buildTop2HitCountSql()},
      ${buildOrderPairSelectClauses()}
    from labeled j
    left join order_pairs op
      on op.source = j.source
     and op.keibajo_code = j.keibajo_code
     and op.kyori = j.kyori
     and op.kyoso_shubetsu_code = j.kyoso_shubetsu_code
     and coalesce(op.kyoso_joken_code, '') = coalesce(j.kyoso_joken_code, '')
     and coalesce(op.condition_key, '') = coalesce(j.condition_key, '')
     and coalesce(op.track_code, '') = coalesce(j.track_code, '')
     and coalesce(op.grade_code, '') = coalesce(j.grade_code, '')
     and coalesce(op.race_name, '') = coalesce(j.race_name, '')
     and coalesce(op.cell_model_key, '') = coalesce(j.cell_model_key, '')
     and coalesce(op.cell_variant_id, '') = coalesce(j.cell_variant_id, '')
    group by j.source, j.keibajo_code, j.kyori, j.kyoso_shubetsu_code,
             j.kyoso_joken_code, j.condition_key, j.track_code, j.grade_code, j.race_name,
             j.cell_model_key, j.cell_variant_id
  `;
};

const buildUpsertColumnList = (): string[] => [
  "model_version",
  "running_style_feature_version",
  "category",
  "cell_model_key",
  "cell_variant_id",
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
  ...ORDER_PAIR_METRICS.flatMap((metric) => [
    buildOrderPairScoreColumn(metric),
    buildOrderPairCountColumn(metric),
  ]),
];

// psycopg cursor.execute(query, params) requires "%s" placeholders, not the
// PG-native "$N" form. The Python loader (load_running_style_predictions.py)
// runs each upsert through psycopg, so passing "$1, $2, ..." results in
// "the query has 0 placeholders but N parameters were passed".
const buildUpsertPlaceholderList = (count: number): string =>
  Array.from({ length: count }, () => "%s").join(", ");

const buildReplacementSetClause = (column: string): string => `${column} = excluded.${column}`;

const buildReplacementColumns = (): string[] => [
  "race_count",
  "prediction_count",
  ...CM_CLASS_PAIRS.map(([actual, predicted]) => buildCmColumnName(actual, predicted)),
  ...LOG_LOSS_CLASSES.flatMap((className) => [
    `log_loss_${className}_sum`,
    `log_loss_${className}_count`,
  ]),
  "top2_hit_count",
  ...ORDER_PAIR_METRICS.flatMap((metric) => [
    buildOrderPairScoreColumn(metric),
    buildOrderPairCountColumn(metric),
  ]),
];

const buildBucketUpsertOnConflictClause = (): string => {
  const replacementColumns = buildReplacementColumns();
  return replacementColumns
    .map((column) => buildReplacementSetClause(column))
    .concat(["evaluated_at = now()"])
    .join(",\n      ");
};

export const buildRunningStyleBucketUpsertSql = (): string => {
  const columns = buildUpsertColumnList();
  const placeholders = buildUpsertPlaceholderList(columns.length);
  const setClauses = buildBucketUpsertOnConflictClause();
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
      coalesce(cell_model_key,''), coalesce(cell_variant_id,''),
      evaluation_window_from, evaluation_window_to,
      source, keibajo_code, kyori, kyoso_shubetsu_code,
      coalesce(kyoso_joken_code,''), coalesce(condition_key,''),
      coalesce(track_code,''), coalesce(grade_code,''), coalesce(race_name,'')
    )
    do update set
      ${setClauses}
  `;
};

const buildBatchValuesRowSql = (columnCount: number): string =>
  `(${buildUpsertPlaceholderList(columnCount)}, now())`;

// Multi-row UPSERT path. PostgreSQL caps bind parameters at 65535, so callers
// must keep rowCount * column-count under that limit (51 columns * 100 rows =
// 5100 placeholders, well within budget). ON CONFLICT replaces aggregate
// columns with the new row values so re-running the same window is idempotent.
export const buildRunningStyleBucketBatchUpsertSql = (rowCount: number): string => {
  if (rowCount <= 0) throw new Error("rowCount must be greater than zero.");
  const columns = buildUpsertColumnList();
  const valueRows = Array.from({ length: rowCount }, () => buildBatchValuesRowSql(columns.length));
  const setClauses = buildBucketUpsertOnConflictClause();
  return `
    insert into ${BUCKET_TABLE} (
      ${columns.join(", ")},
      evaluated_at
    )
    values
      ${valueRows.join(",\n      ")}
    on conflict (
      model_version, running_style_feature_version, category,
      coalesce(cell_model_key,''), coalesce(cell_variant_id,''),
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
  `analyze ${SOURCE_FEATURE_TABLE}`,
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
