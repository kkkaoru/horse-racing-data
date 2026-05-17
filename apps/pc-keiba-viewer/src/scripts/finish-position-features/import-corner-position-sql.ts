// Run with: imported from import-corner-position-predictions.ts (bun runtime)

const PREDICTIONS_TABLE = "race_corner_position_model_predictions";
const ACTIVE_MODELS_TABLE = "corner_position_active_models";
const EVALUATIONS_TABLE = "corner_position_model_evaluations";

const PRIMARY_KEY_COLUMNS = [
  "model_version",
  "source",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
  "ketto_toroku_bango",
];

const PREDICTION_COLUMNS = ["corner_1_pred", "corner_3_pred", "corner_4_pred"];

const INSERT_COLUMNS = [...PRIMARY_KEY_COLUMNS, "umaban", ...PREDICTION_COLUMNS];

const UPDATABLE_COLUMNS = ["umaban", ...PREDICTION_COLUMNS];

const EVALUATION_PRIMARY_KEYS = [
  "model_version",
  "category",
  "evaluation_window_from",
  "evaluation_window_to",
];

const EVALUATION_METRIC_COLUMNS = [
  "corner_1_mae",
  "corner_3_mae",
  "corner_4_mae",
  "mean_mae",
  "corner_1_top3_agreement",
];

const buildPredictionsTableDdl = (): string => `
    create table if not exists ${PREDICTIONS_TABLE} (
      model_version text not null,
      source text not null,
      kaisai_nen text not null,
      kaisai_tsukihi text not null,
      keibajo_code text not null,
      race_bango text not null,
      ketto_toroku_bango text not null,
      umaban integer not null,
      corner_1_pred numeric,
      corner_3_pred numeric,
      corner_4_pred numeric,
      prediction_generated_at timestamptz not null default now(),
      primary key (${PRIMARY_KEY_COLUMNS.join(", ")})
    )
  `;

const buildActiveModelsTableDdl = (): string => `
    create table if not exists ${ACTIVE_MODELS_TABLE} (
      category text primary key,
      model_version text not null,
      activated_at timestamptz not null default now()
    )
  `;

const buildEvaluationsTableDdl = (): string => `
    create table if not exists ${EVALUATIONS_TABLE} (
      model_version text not null,
      category text not null,
      evaluation_window_from text not null,
      evaluation_window_to text not null,
      race_count integer not null,
      prediction_count integer not null,
      corner_1_mae numeric,
      corner_3_mae numeric,
      corner_4_mae numeric,
      mean_mae numeric,
      corner_1_top3_agreement numeric,
      evaluated_at timestamptz not null default now(),
      primary key (${EVALUATION_PRIMARY_KEYS.join(", ")})
    )
  `;

const buildPredictionsLookupIndexSql = (): string =>
  `create index if not exists ${PREDICTIONS_TABLE}_race_lookup_idx
     on ${PREDICTIONS_TABLE} (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)`;

const buildPlaceholderRow = (rowIndex: number): string => {
  const start = rowIndex * INSERT_COLUMNS.length + 1;
  const placeholders = INSERT_COLUMNS.map((_, columnOffset) => `$${start + columnOffset}`);
  return `(${placeholders.join(", ")})`;
};

const buildBatchInsertSql = (rowCount: number): string => {
  const valuesClause = Array.from({ length: rowCount }, (_, rowIndex) =>
    buildPlaceholderRow(rowIndex),
  ).join(",\n      ");
  const updateAssignments = UPDATABLE_COLUMNS.map((column) => `${column} = excluded.${column}`);
  return `
    insert into ${PREDICTIONS_TABLE} (${INSERT_COLUMNS.join(", ")})
    values
      ${valuesClause}
    on conflict (${PRIMARY_KEY_COLUMNS.join(", ")})
    do update set
      ${updateAssignments.join(",\n      ")},
      prediction_generated_at = now()
  `;
};

const buildActivateModelSql = (): string =>
  `insert into ${ACTIVE_MODELS_TABLE} (category, model_version)
     values ($1, $2)
     on conflict (category)
     do update set model_version = excluded.model_version, activated_at = now()`;

const buildEvaluationUpsertSql = (): string => {
  const allColumns = [
    ...EVALUATION_PRIMARY_KEYS,
    "race_count",
    "prediction_count",
    ...EVALUATION_METRIC_COLUMNS,
  ];
  const placeholders = allColumns.map((_, index) => `$${index + 1}`).join(", ");
  const updateAssignments = ["race_count", "prediction_count", ...EVALUATION_METRIC_COLUMNS]
    .map((column) => `${column} = excluded.${column}`)
    .join(",\n      ");
  return `
    insert into ${EVALUATIONS_TABLE} (${allColumns.join(", ")}, evaluated_at)
    values (${placeholders}, now())
    on conflict (${EVALUATION_PRIMARY_KEYS.join(", ")})
    do update set
      ${updateAssignments},
      evaluated_at = now()
  `;
};

export {
  ACTIVE_MODELS_TABLE,
  buildActivateModelSql,
  buildActiveModelsTableDdl,
  buildBatchInsertSql,
  buildEvaluationsTableDdl,
  buildEvaluationUpsertSql,
  buildPredictionsLookupIndexSql,
  buildPredictionsTableDdl,
  EVALUATION_METRIC_COLUMNS,
  EVALUATION_PRIMARY_KEYS,
  EVALUATIONS_TABLE,
  INSERT_COLUMNS,
  PREDICTION_COLUMNS,
  PREDICTIONS_TABLE,
  PRIMARY_KEY_COLUMNS,
  UPDATABLE_COLUMNS,
};
