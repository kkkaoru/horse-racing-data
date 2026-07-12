// Run with: imported from import-finish-position-predictions.ts (bun runtime)

const PREDICTIONS_TABLE = "race_finish_position_model_predictions";
const ACTIVE_MODELS_TABLE = "finish_position_active_models";
// ``subclass`` remains for historical rows, but current production activation
// only writes the NULL-subclass category default. Per-cell routing is resolved
// from cell_routing.json / prediction model_version rows, not this table.
const ACTIVE_MODELS_SUBCLASS_INDEX = `${ACTIVE_MODELS_TABLE}_category_subclass_idx`;

const PRIMARY_KEY_COLUMNS = [
  "model_version",
  "source",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
  "ketto_toroku_bango",
];

// Race-level subgroup metadata mirrored from the Python Container path
// (apps/finish-position-predict-container/src/predict_lib/subgroup.py). Nullable
// text labels classified from race metadata; they sit in INSERT + UPDATABLE only
// and are NOT part of the primary key.
const SUBGROUP_COLUMNS: string[] = [
  "distance_band",
  "field_size_band",
  "season_band",
  "class_code",
  "surface",
];

const INSERT_COLUMNS = [
  ...PRIMARY_KEY_COLUMNS,
  "umaban",
  "predicted_score",
  "predicted_rank",
  "predicted_top1_prob",
  "predicted_top3_prob",
  "predicted_finish_position",
  ...SUBGROUP_COLUMNS,
];

const UPDATABLE_COLUMNS = [
  "umaban",
  "predicted_score",
  "predicted_rank",
  "predicted_top1_prob",
  "predicted_top3_prob",
  "predicted_finish_position",
  ...SUBGROUP_COLUMNS,
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
      distance_band text,
      field_size_band text,
      season_band text,
      class_code text,
      surface text,
      prediction_generated_at timestamptz not null default now(),
      first_served_at timestamptz,
      primary key (${PRIMARY_KEY_COLUMNS.join(", ")})
    )
  `;

export const buildActiveModelsTableDdl = (): string => `
    create table if not exists ${ACTIVE_MODELS_TABLE} (
      category text not null,
      subclass text,
      model_version text not null,
      activated_at timestamptz not null default now()
    )
  `;

export const buildAddSubclassColumnSql = (): string =>
  `alter table ${ACTIVE_MODELS_TABLE} add column if not exists subclass text`;

// MASTER-INVENTORY finding #13: ON CONFLICT DO UPDATE unconditionally sets
// prediction_generated_at = now(), so a same-key re-score/backfill destroys
// the true first-serve timestamp. first_served_at is the fix: deliberately
// absent from both INSERT_COLUMNS and UPDATABLE_COLUMNS above, so once set it
// is never touched again by any later ON CONFLICT DO UPDATE. This ALTER
// (like buildAddSubclassColumnSql above) targets the table that already
// exists in production -- buildPredictionsTableDdl's CREATE TABLE IF NOT
// EXISTS only takes effect for a from-scratch table.
//
// Split into two statements deliberately: ADD COLUMN with a volatile DEFAULT
// (default now()) in one step forces Postgres to rewrite every existing row
// to materialise that default -- a long table-wide lock, dangerous against a
// live-serving table. Adding the column bare (no default) is metadata-only
// and instant; SET DEFAULT below only affects INSERTs from that point
// forward. Existing rows keep first_served_at = NULL, which is the honest
// answer ("first-serve time unknown for rows written before this fix"), not
// a value to backfill.
export const buildAddFirstServedAtColumnSql = (): string =>
  `alter table ${PREDICTIONS_TABLE} add column if not exists first_served_at timestamptz`;

export const buildSetFirstServedAtDefaultSql = (): string =>
  `alter table ${PREDICTIONS_TABLE} alter column first_served_at set default now()`;

export const buildDropLegacyPkSql = (): string =>
  `alter table ${ACTIVE_MODELS_TABLE} drop constraint if exists ${ACTIVE_MODELS_TABLE}_pkey`;

export const buildActiveModelsSubclassUniqueIndexSql = (): string =>
  `create unique index if not exists ${ACTIVE_MODELS_SUBCLASS_INDEX}
     on ${ACTIVE_MODELS_TABLE} (category, coalesce(subclass, ''))`;

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
  `insert into ${ACTIVE_MODELS_TABLE} (category, subclass, model_version)
     values ($1, null, $2)
     on conflict (category, coalesce(subclass, ''))
     do update set model_version = excluded.model_version, activated_at = now()`;

export {
  ACTIVE_MODELS_TABLE,
  INSERT_COLUMNS,
  PREDICTIONS_TABLE,
  PRIMARY_KEY_COLUMNS,
  UPDATABLE_COLUMNS,
};
