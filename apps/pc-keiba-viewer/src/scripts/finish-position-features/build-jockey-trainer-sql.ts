// Run with: imported from build-finish-position-features.ts (bun runtime)

import type { FeatureCategory } from "./build-finish-position-features-types";

const SOURCE_FEATURE_TABLE = "race_entry_corner_features";
const TARGET_FEATURE_TABLE = "race_finish_position_features";
const HISTORY_LOOKBACK_DAYS_YYYYMMDD = 100000;
const RECENT_WINDOW_DAYS = 60;
const SAME_DISTANCE_TOLERANCE_METERS = 200;

interface CategoryFilterClauses {
  targetCategoryFilter: string;
  historySourceFilter: string;
}

const buildCategoryFilterClauses = (category: FeatureCategory): CategoryFilterClauses => {
  if (category === "jra") {
    return {
      historySourceFilter: "history.source = 'jra'",
      targetCategoryFilter: "target.category = 'jra'",
    };
  }
  if (category === "nar") {
    return {
      historySourceFilter: "history.source = 'nar' and history.keibajo_code <> '83'",
      targetCategoryFilter: "target.category = 'nar'",
    };
  }
  if (category === "ban-ei") {
    return {
      historySourceFilter: "history.source = 'nar' and history.keibajo_code = '83'",
      targetCategoryFilter: "target.category = 'ban-ei'",
    };
  }
  return {
    historySourceFilter: "true",
    targetCategoryFilter: "true",
  };
};

const winRateFilter = (whenClause: string | null): string => {
  const filterClause = whenClause === null ? "" : ` filter (where ${whenClause})`;
  return `avg(case when finish_position = 1 then 1 else 0 end)${filterClause}`;
};

const seasonBand = (dateColumn: string): string =>
  `(cast(month(to_date(${dateColumn}, 'YYYYMMDD')) as int) + 9) % 12 // 3`;

const SAME_SEASON_CLAUSE = `${seasonBand("history_race_date")} = ${seasonBand("target_race_date")}`;
const SAME_KEIBAJO_CLAUSE = "history_keibajo_code = target_keibajo_code";
const SAME_DISTANCE_CLAUSE = `abs(history_kyori - target_kyori) <= ${SAME_DISTANCE_TOLERANCE_METERS}`;
const SAME_SURFACE_CLAUSE =
  "left(coalesce(history_track_code, ''), 1) = left(coalesce(target_track_code, ''), 1)";
const SAME_GRADE_CLAUSE = "coalesce(history_grade_code, '') = coalesce(target_grade_code, '')";

const buildJockeyAggregationColumns = (): string[] => [
  `${winRateFilter(null)} as jockey_career_win_rate`,
  `${winRateFilter(`to_date(history_race_date, 'YYYYMMDD') >= to_date(target_race_date, 'YYYYMMDD') - ${RECENT_WINDOW_DAYS}`)} as jockey_recent_win_rate`,
  `${winRateFilter(SAME_KEIBAJO_CLAUSE)} as jockey_keibajo_win_rate`,
  `${winRateFilter(SAME_DISTANCE_CLAUSE)} as jockey_distance_win_rate`,
  `${winRateFilter(SAME_SURFACE_CLAUSE)} as jockey_track_win_rate`,
  `${winRateFilter(SAME_GRADE_CLAUSE)} as jockey_grade_win_rate`,
  `count(*) filter (where history_horse = target_horse) as jockey_horse_pair_count`,
  `${winRateFilter("history_horse = target_horse")} as jockey_horse_pair_win_rate`,
  `${winRateFilter(SAME_SEASON_CLAUSE)} as jockey_season_win_rate`,
  `${winRateFilter(`${SAME_SEASON_CLAUSE} and ${SAME_KEIBAJO_CLAUSE}`)} as jockey_season_keibajo_win_rate`,
  `${winRateFilter(`${SAME_KEIBAJO_CLAUSE} and ${SAME_DISTANCE_CLAUSE}`)} as jockey_keibajo_distance_win_rate`,
  `${winRateFilter(`${SAME_SEASON_CLAUSE} and ${SAME_KEIBAJO_CLAUSE} and ${SAME_DISTANCE_CLAUSE}`)} as jockey_season_keibajo_distance_win_rate`,
  `count(*) filter (where ${SAME_SEASON_CLAUSE} and ${SAME_KEIBAJO_CLAUSE} and ${SAME_DISTANCE_CLAUSE}) as jockey_season_keibajo_distance_count`,
];

const buildTrainerAggregationColumns = (): string[] => [
  `${winRateFilter(null)} as trainer_career_win_rate`,
  `${winRateFilter(SAME_KEIBAJO_CLAUSE)} as trainer_keibajo_win_rate`,
  `${winRateFilter(SAME_DISTANCE_CLAUSE)} as trainer_distance_win_rate`,
  `${winRateFilter("history_horse = target_horse")} as trainer_horse_win_rate`,
  `${winRateFilter(SAME_GRADE_CLAUSE)} as trainer_grade_win_rate`,
  `${winRateFilter(`${SAME_GRADE_CLAUSE} and ${SAME_SURFACE_CLAUSE} and ${SAME_SEASON_CLAUSE}`)} as trainer_class_surface_season_win_rate`,
  `count(*) filter (where ${SAME_GRADE_CLAUSE} and ${SAME_SURFACE_CLAUSE} and ${SAME_SEASON_CLAUSE}) as trainer_class_surface_season_count`,
];

const buildTargetCte = (
  category: FeatureCategory,
  partnerColumn: "kishumei_ryakusho" | "chokyoshimei_ryakusho",
): string => {
  const filters = buildCategoryFilterClauses(category);
  return `
    target as (
      select
        target.source,
        target.kaisai_nen,
        target.kaisai_tsukihi,
        target.keibajo_code,
        target.race_bango,
        target.ketto_toroku_bango,
        target.race_date,
        target.keibajo_code as target_keibajo_code,
        target.kyori as target_kyori,
        target.track_code as target_track_code,
        target.grade_code as target_grade_code,
        target.ketto_toroku_bango as target_horse,
        rec.${partnerColumn} as target_partner
      from ${TARGET_FEATURE_TABLE} target
      join ${SOURCE_FEATURE_TABLE} rec
        on rec.source = target.source
        and rec.kaisai_nen = target.kaisai_nen
        and rec.kaisai_tsukihi = target.kaisai_tsukihi
        and rec.keibajo_code = target.keibajo_code
        and rec.race_bango = target.race_bango
        and rec.ketto_toroku_bango = target.ketto_toroku_bango
      where target.race_date between $1 and $2
        and ${filters.targetCategoryFilter}
        and rec.${partnerColumn} is not null
        and btrim(rec.${partnerColumn}) <> ''
    )
  `;
};

const buildHistoryCte = (
  category: FeatureCategory,
  partnerColumn: "kishumei_ryakusho" | "chokyoshimei_ryakusho",
): string => {
  const filters = buildCategoryFilterClauses(category);
  return `
    history_raw as (
      select
        target.source,
        target.kaisai_nen,
        target.kaisai_tsukihi,
        target.keibajo_code,
        target.race_bango,
        target.ketto_toroku_bango,
        target.race_date as target_race_date,
        target.target_keibajo_code,
        target.target_kyori,
        target.target_track_code,
        target.target_grade_code,
        target.target_horse,
        history.race_date as history_race_date,
        history.finish_position,
        history.keibajo_code as history_keibajo_code,
        history.kyori as history_kyori,
        history.track_code as history_track_code,
        history.grade_code as history_grade_code,
        history.ketto_toroku_bango as history_horse
      from target
      join ${SOURCE_FEATURE_TABLE} history
        on history.source = target.source
        and history.${partnerColumn} = target.target_partner
        and history.race_date < target.race_date
        and history.race_date >= (target.race_date::integer - ${HISTORY_LOOKBACK_DAYS_YYYYMMDD})::text
      where ${filters.historySourceFilter}
        and history.finish_position is not null
    )
  `;
};

const buildAggregationCte = (alias: string, aggregationColumns: string[]): string => `
    ${alias} as (
      select
        source,
        kaisai_nen,
        kaisai_tsukihi,
        keibajo_code,
        race_bango,
        ketto_toroku_bango,
        ${aggregationColumns.join(",\n        ")}
      from history_raw
      group by
        source,
        kaisai_nen,
        kaisai_tsukihi,
        keibajo_code,
        race_bango,
        ketto_toroku_bango
    )
  `;

const buildJoinConditions = (): string =>
  [
    "target.source = source_agg.source",
    "target.kaisai_nen = source_agg.kaisai_nen",
    "target.kaisai_tsukihi = source_agg.kaisai_tsukihi",
    "target.keibajo_code = source_agg.keibajo_code",
    "target.race_bango = source_agg.race_bango",
    "target.ketto_toroku_bango = source_agg.ketto_toroku_bango",
  ].join("\n      and ");

export const buildJockeyUpdateSql = (category: FeatureCategory): string => {
  const targetCte = buildTargetCte(category, "kishumei_ryakusho");
  const historyCte = buildHistoryCte(category, "kishumei_ryakusho");
  const aggregationCte = buildAggregationCte("source_agg", buildJockeyAggregationColumns());
  const updateAssignments = [
    "jockey_career_win_rate = source_agg.jockey_career_win_rate",
    "jockey_recent_win_rate = source_agg.jockey_recent_win_rate",
    "jockey_keibajo_win_rate = source_agg.jockey_keibajo_win_rate",
    "jockey_distance_win_rate = source_agg.jockey_distance_win_rate",
    "jockey_track_win_rate = source_agg.jockey_track_win_rate",
    "jockey_grade_win_rate = source_agg.jockey_grade_win_rate",
    "jockey_horse_pair_count = source_agg.jockey_horse_pair_count",
    "jockey_horse_pair_win_rate = source_agg.jockey_horse_pair_win_rate",
    "jockey_season_win_rate = source_agg.jockey_season_win_rate",
    "jockey_season_keibajo_win_rate = source_agg.jockey_season_keibajo_win_rate",
    "jockey_keibajo_distance_win_rate = source_agg.jockey_keibajo_distance_win_rate",
    "jockey_season_keibajo_distance_win_rate = source_agg.jockey_season_keibajo_distance_win_rate",
    "jockey_season_keibajo_distance_count = source_agg.jockey_season_keibajo_distance_count",
    "updated_at = now()",
  ];
  return `
    with ${targetCte},
    ${historyCte},
    ${aggregationCte}
    update ${TARGET_FEATURE_TABLE} target
    set
      ${updateAssignments.join(",\n      ")}
    from source_agg
    where ${buildJoinConditions()}
  `;
};

export const buildTrainerUpdateSql = (category: FeatureCategory): string => {
  const targetCte = buildTargetCte(category, "chokyoshimei_ryakusho");
  const historyCte = buildHistoryCte(category, "chokyoshimei_ryakusho");
  const aggregationCte = buildAggregationCte("source_agg", buildTrainerAggregationColumns());
  const updateAssignments = [
    "trainer_career_win_rate = source_agg.trainer_career_win_rate",
    "trainer_keibajo_win_rate = source_agg.trainer_keibajo_win_rate",
    "trainer_distance_win_rate = source_agg.trainer_distance_win_rate",
    "trainer_horse_win_rate = source_agg.trainer_horse_win_rate",
    "trainer_grade_win_rate = source_agg.trainer_grade_win_rate",
    "trainer_class_surface_season_win_rate = source_agg.trainer_class_surface_season_win_rate",
    "trainer_class_surface_season_count = source_agg.trainer_class_surface_season_count",
    "updated_at = now()",
  ];
  return `
    with ${targetCte},
    ${historyCte},
    ${aggregationCte}
    update ${TARGET_FEATURE_TABLE} target
    set
      ${updateAssignments.join(",\n      ")}
    from source_agg
    where ${buildJoinConditions()}
  `;
};

export const buildSourceFeatureLookupIndexSqls = (): string[] => [
  `create index if not exists ${SOURCE_FEATURE_TABLE}_jockey_date_idx
     on ${SOURCE_FEATURE_TABLE} (source, kishumei_ryakusho, race_date)
     where kishumei_ryakusho is not null and finish_position is not null`,
  `create index if not exists ${SOURCE_FEATURE_TABLE}_trainer_date_idx
     on ${SOURCE_FEATURE_TABLE} (source, chokyoshimei_ryakusho, race_date)
     where chokyoshimei_ryakusho is not null and finish_position is not null`,
];

export {
  HISTORY_LOOKBACK_DAYS_YYYYMMDD,
  RECENT_WINDOW_DAYS,
  SAME_DISTANCE_TOLERANCE_METERS,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
};
