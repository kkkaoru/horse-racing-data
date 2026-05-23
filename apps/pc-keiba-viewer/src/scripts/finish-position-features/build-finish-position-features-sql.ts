// Run with: imported from build-finish-position-features.ts (bun runtime)

import type { FeatureCategory } from "./build-finish-position-features-types";

const PRIMARY_KEY_COLUMNS = [
  "source",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
  "ketto_toroku_bango",
];

const META_COLUMNS = [
  "source",
  "race_date",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
  "ketto_toroku_bango",
  "umaban",
  "category",
  "kyori",
  "track_code",
  "grade_code",
  "shusso_tosu",
];

const LABEL_COLUMNS = ["finish_position", "finish_norm"];

const HORSE_CAREER_COLUMNS = [
  "speed_index_avg_5",
  "speed_index_best_5",
  "kohan3f_avg_5",
  "corner_pass_avg_5",
  "career_win_rate",
  "career_place_rate",
  "career_top1_count",
  "same_keibajo_win_rate",
  "same_distance_win_rate",
  "same_track_win_rate",
  "same_grade_win_rate",
  "weight_avg_5",
  "weight_diff_from_avg",
  "days_since_last_race",
  "consecutive_race_count",
];

const JOCKEY_TRAINER_COLUMNS = [
  "jockey_career_win_rate",
  "jockey_recent_win_rate",
  "jockey_keibajo_win_rate",
  "jockey_distance_win_rate",
  "jockey_track_win_rate",
  "jockey_grade_win_rate",
  "jockey_horse_pair_count",
  "jockey_horse_pair_win_rate",
  "trainer_career_win_rate",
  "trainer_keibajo_win_rate",
  "trainer_distance_win_rate",
  "trainer_horse_win_rate",
];

const PEDIGREE_COLUMNS = [
  "sire_distance_win_rate",
  "sire_track_win_rate",
  "dam_sire_distance_win_rate",
  "sire_avg_finish_at_distance",
  "damsire_avg_finish_at_track",
  "pedigree_score_for_race",
];

const RACE_CONTEXT_COLUMNS = [
  "field_strength_avg_speed",
  "field_strength_top3_speed",
  "rival_count_at_distance",
  "track_bias_inside",
  "track_bias_front",
  "weather_normalized",
  "track_condition_normalized",
  "field_size_normalized",
  "is_grade_race",
];

const RECENT_FORM_COLUMNS = [
  "last_race_finish_norm",
  "last_race_margin_to_winner",
  "last_race_corner_pass_norm",
  "last_race_class_diff",
  "last_race_distance_diff",
  "finish_trend_5",
  "last_3_avg_finish_norm",
];

const LEGACY_FIVE_COLUMNS = [
  "avg_finish",
  "recent_finish",
  "popularity_score",
  "odds_score",
  "same_day_jockey_win_score",
];

const ALL_FEATURE_COLUMNS = [
  ...HORSE_CAREER_COLUMNS,
  ...JOCKEY_TRAINER_COLUMNS,
  ...PEDIGREE_COLUMNS,
  ...RACE_CONTEXT_COLUMNS,
  ...RECENT_FORM_COLUMNS,
  ...LEGACY_FIVE_COLUMNS,
];

const FEATURE_COLUMN_TYPES: Record<string, string> = {
  career_top1_count: "integer",
  consecutive_race_count: "integer",
  days_since_last_race: "integer",
  jockey_horse_pair_count: "integer",
  last_race_class_diff: "integer",
  last_race_distance_diff: "integer",
  rival_count_at_distance: "integer",
  is_grade_race: "smallint",
};

const featureColumnDdl = (column: string): string => {
  const type = FEATURE_COLUMN_TYPES[column] ?? "numeric";
  return `${column} ${type}`;
};

export const FEATURE_TABLE_NAME = "race_finish_position_features";

export const buildCreateTableSql = (): string => {
  const metaDdl = [
    "source text not null",
    "race_date text not null",
    "kaisai_nen text not null",
    "kaisai_tsukihi text not null",
    "keibajo_code text not null",
    "race_bango text not null",
    "ketto_toroku_bango text not null",
    "umaban integer",
    "category text not null",
    "kyori integer",
    "track_code text",
    "grade_code text",
    "shusso_tosu integer",
  ];
  const labelDdl = ["finish_position integer", "finish_norm numeric"];
  const featureDdl = ALL_FEATURE_COLUMNS.map(featureColumnDdl);
  const tailDdl = [
    "feature_schema_version text not null default 'v1'",
    "updated_at timestamptz not null default now()",
  ];
  return `
    create table if not exists ${FEATURE_TABLE_NAME} (
      ${[...metaDdl, ...labelDdl, ...featureDdl, ...tailDdl].join(",\n      ")},
      primary key (${PRIMARY_KEY_COLUMNS.join(", ")})
    )
  `;
};

export const buildIndexSqls = (): string[] => [
  `create index if not exists ${FEATURE_TABLE_NAME}_race_date_idx on ${FEATURE_TABLE_NAME} (race_date desc)`,
  `create index if not exists ${FEATURE_TABLE_NAME}_race_key_idx on ${FEATURE_TABLE_NAME} (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)`,
  `create index if not exists ${FEATURE_TABLE_NAME}_schema_date_idx on ${FEATURE_TABLE_NAME} (feature_schema_version, race_date)`,
  `create index if not exists ${FEATURE_TABLE_NAME}_category_date_idx on ${FEATURE_TABLE_NAME} (category, race_date desc)`,
];

const buildCategoryFilter = (category: FeatureCategory): string => {
  if (category === "jra") return "and source = 'jra'";
  if (category === "nar") return "and source = 'nar' and keibajo_code <> '83'";
  if (category === "ban-ei") return "and source = 'nar' and keibajo_code = '83'";
  return "";
};

const buildCategoryExpression = (category: FeatureCategory): string => {
  if (category === "jra") return "'jra'::text";
  if (category === "nar") return "'nar'::text";
  if (category === "ban-ei") return "'ban-ei'::text";
  return "case when source='jra' then 'jra' when keibajo_code='83' then 'ban-ei' else 'nar' end";
};

const conflictUpdateAssignment = (column: string): string => `${column} = excluded.${column}`;

const META_UPDATABLE_COLUMNS = [
  "race_date",
  "umaban",
  "category",
  "kyori",
  "track_code",
  "grade_code",
  "shusso_tosu",
  "finish_position",
  "finish_norm",
];

export const buildSkeletonUpsertSql = (
  category: FeatureCategory,
  schemaVersion: string,
): string => {
  const categoryExpression = buildCategoryExpression(category);
  const categoryFilter = buildCategoryFilter(category);
  const insertColumns = [...META_COLUMNS, ...LABEL_COLUMNS, "feature_schema_version", "updated_at"];
  const selectColumns = [
    "source",
    "race_date",
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "race_bango",
    "ketto_toroku_bango",
    "umaban",
    `${categoryExpression} as category`,
    "kyori",
    "track_code",
    "grade_code",
    `coalesce(
      nullif(shusso_tosu, 0),
      count(*) over (
        partition by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
      )::int
    ) as shusso_tosu`,
    "finish_position",
    "finish_norm",
    `'${schemaVersion}'::text as feature_schema_version`,
    "now() as updated_at",
  ];
  const updateAssignments = [
    ...META_UPDATABLE_COLUMNS.map(conflictUpdateAssignment),
    "feature_schema_version = excluded.feature_schema_version",
    "updated_at = now()",
  ];
  return `
    insert into ${FEATURE_TABLE_NAME} (${insertColumns.join(", ")})
    select ${selectColumns.join(",\n           ")}
    from race_entry_corner_features
    where race_date between $1 and $2
      and ketto_toroku_bango is not null
      ${categoryFilter}
    on conflict (${PRIMARY_KEY_COLUMNS.join(", ")})
    do update set
      ${updateAssignments.join(",\n      ")}
  `;
};

export {
  ALL_FEATURE_COLUMNS,
  HORSE_CAREER_COLUMNS,
  JOCKEY_TRAINER_COLUMNS,
  LEGACY_FIVE_COLUMNS,
  META_COLUMNS,
  PEDIGREE_COLUMNS,
  PRIMARY_KEY_COLUMNS,
  RACE_CONTEXT_COLUMNS,
  RECENT_FORM_COLUMNS,
};
