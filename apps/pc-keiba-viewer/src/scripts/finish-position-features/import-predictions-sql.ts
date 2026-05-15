// Run with: imported from import-finish-position-predictions.ts (bun runtime)

const PREDICTIONS_TABLE = "race_finish_position_model_predictions";
const ACTIVE_MODELS_TABLE = "finish_position_active_models";

const PRIMARY_KEY_COLUMNS = [
  "model_version",
  "source",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
  "ketto_toroku_bango",
];

const INSERT_COLUMNS = [
  ...PRIMARY_KEY_COLUMNS,
  "umaban",
  "predicted_score",
  "predicted_rank",
  "predicted_top1_prob",
  "predicted_top3_prob",
  "predicted_finish_position",
];

const UPDATABLE_COLUMNS = [
  "umaban",
  "predicted_score",
  "predicted_rank",
  "predicted_top1_prob",
  "predicted_top3_prob",
  "predicted_finish_position",
];

export const buildPredictionsTableDdl = (): string => `
    create table if not exists ${PREDICTIONS_TABLE} (
      model_version text not null,
      source text not null,
      kaisai_nen text not null,
      kaisai_tsukihi text not null,
      keibajo_code text not null,
      race_bango text not null,
      ketto_toroku_bango text not null,
      umaban integer not null,
      predicted_score numeric not null,
      predicted_rank integer not null,
      predicted_top1_prob numeric,
      predicted_top3_prob numeric,
      predicted_finish_position numeric,
      prediction_generated_at timestamptz not null default now(),
      primary key (${PRIMARY_KEY_COLUMNS.join(", ")})
    )
  `;

export const buildActiveModelsTableDdl = (): string => `
    create table if not exists ${ACTIVE_MODELS_TABLE} (
      category text primary key,
      model_version text not null,
      activated_at timestamptz not null default now()
    )
  `;

export const buildPredictionsLookupIndexSql = (): string =>
  `create index if not exists ${PREDICTIONS_TABLE}_race_lookup_idx
     on ${PREDICTIONS_TABLE} (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)`;

const buildPlaceholderRow = (rowIndex: number): string => {
  const start = rowIndex * INSERT_COLUMNS.length + 1;
  const placeholders = INSERT_COLUMNS.map((_, columnOffset) => `$${start + columnOffset}`);
  return `(${placeholders.join(", ")})`;
};

export const buildBatchInsertSql = (rowCount: number): string => {
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

export const buildActivateModelSql = (): string =>
  `insert into ${ACTIVE_MODELS_TABLE} (category, model_version)
     values ($1, $2)
     on conflict (category)
     do update set model_version = excluded.model_version, activated_at = now()`;

export {
  ACTIVE_MODELS_TABLE,
  INSERT_COLUMNS,
  PREDICTIONS_TABLE,
  PRIMARY_KEY_COLUMNS,
  UPDATABLE_COLUMNS,
};
