// Run with: imported from import-running-style-predictions.ts (bun runtime)

const PREDICTIONS_TABLE = "race_running_style_model_predictions";
const ACTIVE_MODELS_TABLE = "running_style_active_models";
const EVALUATIONS_TABLE = "running_style_model_evaluations";

const PRIMARY_KEY_COLUMNS = [
  "model_version",
  "source",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
  "ketto_toroku_bango",
];

const PROBABILITY_COLUMNS = ["p_nige", "p_senkou", "p_sashi", "p_oikomi"];
const LABEL_COLUMNS = ["predicted_label", "predicted_class"];
const CELL_PROVENANCE_COLUMNS = ["cell_model_key", "cell_variant_id"];

const INSERT_COLUMNS = [
  ...PRIMARY_KEY_COLUMNS,
  "umaban",
  ...CELL_PROVENANCE_COLUMNS,
  ...PROBABILITY_COLUMNS,
  ...LABEL_COLUMNS,
];

const UPDATABLE_COLUMNS = [
  "umaban",
  ...CELL_PROVENANCE_COLUMNS,
  ...PROBABILITY_COLUMNS,
  ...LABEL_COLUMNS,
];

const EVALUATION_PRIMARY_KEYS = [
  "model_version",
  "category",
  "evaluation_window_from",
  "evaluation_window_to",
];

const EVALUATION_METRIC_COLUMNS = [
  "accuracy",
  "macro_f1",
  "precision_nige",
  "precision_senkou",
  "precision_sashi",
  "precision_oikomi",
  "recall_nige",
  "recall_senkou",
  "recall_sashi",
  "recall_oikomi",
  "support_nige",
  "support_senkou",
  "support_sashi",
  "support_oikomi",
  "kyakushitsuhantei_agreement",
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
      cell_model_key text,
      cell_variant_id text,
      p_nige numeric not null,
      p_senkou numeric not null,
      p_sashi numeric not null,
      p_oikomi numeric not null,
      predicted_label text not null,
      predicted_class integer not null,
      prediction_generated_at timestamptz not null default now(),
      primary key (${PRIMARY_KEY_COLUMNS.join(", ")})
    );
    alter table ${PREDICTIONS_TABLE} add column if not exists cell_model_key text;
    alter table ${PREDICTIONS_TABLE} add column if not exists cell_variant_id text;
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
      accuracy numeric,
      macro_f1 numeric,
      precision_nige numeric,
      precision_senkou numeric,
      precision_sashi numeric,
      precision_oikomi numeric,
      recall_nige numeric,
      recall_senkou numeric,
      recall_sashi numeric,
      recall_oikomi numeric,
      support_nige integer,
      support_senkou integer,
      support_sashi integer,
      support_oikomi integer,
      kyakushitsuhantei_agreement numeric,
      evaluated_at timestamptz not null default now(),
      primary key (${EVALUATION_PRIMARY_KEYS.join(", ")})
    )
  `;

const buildPredictionsLookupIndexSql = (): string =>
  `create index if not exists ${PREDICTIONS_TABLE}_race_lookup_idx
     on ${PREDICTIONS_TABLE} (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)`;

const buildHorseLookupIndexSql = (): string =>
  `create index if not exists ${PREDICTIONS_TABLE}_horse_lookup_idx
     on ${PREDICTIONS_TABLE} (ketto_toroku_bango, prediction_generated_at desc)`;

const buildPredictionsCellLookupIndexSql = (): string =>
  `create index if not exists ${PREDICTIONS_TABLE}_cell_lookup_idx
     on ${PREDICTIONS_TABLE} (source, cell_variant_id, cell_model_key, kaisai_nen, kaisai_tsukihi)`;

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
  buildPredictionsCellLookupIndexSql,
  buildEvaluationsTableDdl,
  buildEvaluationUpsertSql,
  buildHorseLookupIndexSql,
  buildPredictionsLookupIndexSql,
  buildPredictionsTableDdl,
  EVALUATION_METRIC_COLUMNS,
  EVALUATION_PRIMARY_KEYS,
  EVALUATIONS_TABLE,
  INSERT_COLUMNS,
  LABEL_COLUMNS,
  PREDICTIONS_TABLE,
  CELL_PROVENANCE_COLUMNS,
  PRIMARY_KEY_COLUMNS,
  PROBABILITY_COLUMNS,
  UPDATABLE_COLUMNS,
};
