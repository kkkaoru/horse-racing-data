// Run with: imported from export-finish-position-dataset.ts (bun runtime)

import type { FeatureCategory } from "./build-finish-position-features-types";

const FEATURE_TABLE = "race_finish_position_features";

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

const FEATURE_COLUMNS = [
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
  "days_since_last_race",
  "consecutive_race_count",
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
  "sire_distance_win_rate",
  "sire_track_win_rate",
  "dam_sire_distance_win_rate",
  "sire_avg_finish_at_distance",
  "damsire_avg_finish_at_track",
  "pedigree_score_for_race",
  "field_strength_avg_speed",
  "field_strength_top3_speed",
  "rival_count_at_distance",
  "track_condition_normalized",
  "field_size_normalized",
  "is_grade_race",
  "last_race_finish_norm",
  "last_race_margin_to_winner",
  "last_race_corner_pass_norm",
  "last_race_class_diff",
  "last_race_distance_diff",
  "finish_trend_5",
  "last_3_avg_finish_norm",
  "avg_finish",
  "recent_finish",
  "popularity_score",
  "odds_score",
  "same_day_jockey_win_score",
];

const RACE_ID_EXPRESSION = `
    source || ':' || kaisai_nen || ':' || kaisai_tsukihi || ':' || keibajo_code || ':' || race_bango
  `;

const buildCategoryFilter = (category: FeatureCategory): string => {
  if (category === "jra") return "category = 'jra'";
  if (category === "nar") return "category = 'nar'";
  if (category === "ban-ei") return "category = 'ban-ei'";
  return "true";
};

const escapeLiteral = (value: string): string => `'${value.replaceAll("'", "''")}'`;

export const buildExportSelectSql = (
  category: FeatureCategory,
  featureSchemaVersion: string,
): string => {
  const columns = [
    ...META_COLUMNS,
    ...LABEL_COLUMNS,
    `(${RACE_ID_EXPRESSION.trim()}) as race_id`,
    ...FEATURE_COLUMNS,
  ];
  const schemaLiteral = escapeLiteral(featureSchemaVersion);
  return `
    select
      ${columns.join(",\n      ")}
    from ${FEATURE_TABLE}
    where race_date between $1 and $2
      and ${buildCategoryFilter(category)}
      and feature_schema_version = ${schemaLiteral}
    order by race_date, source, keibajo_code, race_bango, umaban
  `.trim();
};

export const EXPORT_COLUMN_ORDER: readonly string[] = [
  ...META_COLUMNS,
  ...LABEL_COLUMNS,
  "race_id",
  ...FEATURE_COLUMNS,
];

export { FEATURE_TABLE, FEATURE_COLUMNS, LABEL_COLUMNS, META_COLUMNS };
