// Run with: imported from build-finish-position-features.ts (bun runtime)

import type { FeatureCategory } from "./build-finish-position-features-types";

const SOURCE_FEATURE_TABLE = "race_entry_corner_features";
const TARGET_FEATURE_TABLE = "race_finish_position_features";
const HISTORY_LOOKBACK_DAYS_YYYYMMDD = 100000;
const SAME_DISTANCE_TOLERANCE_METERS = 200;
const RECENT_HISTORY_WINDOW_SIZE = 5;
const CONSECUTIVE_RACE_WINDOW_DAYS = 30;

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

const winRateExpression = (whenClause: string | null): string => {
  const filterClause = whenClause === null ? "" : ` filter (where ${whenClause})`;
  return `avg(case when finish_position = 1 then 1 else 0 end)${filterClause}`;
};

const placeRateExpression = (whenClause: string | null): string => {
  const filterClause = whenClause === null ? "" : ` filter (where ${whenClause})`;
  return `avg(case when finish_position between 1 and 3 then 1 else 0 end)${filterClause}`;
};

const recentWindowFilter = (column: string): string =>
  `avg(${column}) filter (where recent_rank <= ${RECENT_HISTORY_WINDOW_SIZE})`;

const recentBestExpression = (column: string, aggregator: "min" | "max"): string =>
  `${aggregator}(${column}) filter (where recent_rank <= ${RECENT_HISTORY_WINDOW_SIZE})`;

export const buildHorseCareerUpdateSql = (category: FeatureCategory): string => {
  const filters = buildCategoryFilterClauses(category);
  const targetCte = `
    target as (
      select
        source,
        kaisai_nen,
        kaisai_tsukihi,
        keibajo_code,
        race_bango,
        ketto_toroku_bango,
        race_date,
        kyori,
        track_code,
        grade_code
      from ${TARGET_FEATURE_TABLE} target
      where race_date between $1 and $2
        and ${filters.targetCategoryFilter}
    )
  `;
  const historyCte = `
    history_raw as (
      select
        target.source,
        target.kaisai_nen,
        target.kaisai_tsukihi,
        target.keibajo_code,
        target.race_bango,
        target.ketto_toroku_bango,
        target.race_date as target_race_date,
        target.keibajo_code as target_keibajo_code,
        target.kyori as target_kyori,
        target.track_code as target_track_code,
        target.grade_code as target_grade_code,
        history.race_date as history_race_date,
        history.finish_position,
        history.time_sa::numeric as time_sa,
        history.kohan_3f::numeric as kohan_3f,
        history.corner4_norm::numeric as corner4_norm,
        history.keibajo_code as history_keibajo_code,
        history.kyori as history_kyori,
        history.track_code as history_track_code,
        history.grade_code as history_grade_code,
        row_number() over (
          partition by
            target.source,
            target.kaisai_nen,
            target.kaisai_tsukihi,
            target.keibajo_code,
            target.race_bango,
            target.ketto_toroku_bango
          order by history.race_date desc
        ) as recent_rank
      from target
      join ${SOURCE_FEATURE_TABLE} history
        on history.source = target.source
        and history.ketto_toroku_bango = target.ketto_toroku_bango
        and history.race_date < target.race_date
        and history.race_date >= (target.race_date::integer - ${HISTORY_LOOKBACK_DAYS_YYYYMMDD})::text
      where ${filters.historySourceFilter}
        and history.finish_position is not null
    )
  `;
  const aggregations = [
    `${recentWindowFilter("time_sa")} as speed_index_avg_5`,
    `${recentBestExpression("time_sa", "min")} as speed_index_best_5`,
    `${recentWindowFilter("kohan_3f")} as kohan3f_avg_5`,
    `${recentWindowFilter("corner4_norm")} as corner_pass_avg_5`,
    `${winRateExpression(null)} as career_win_rate`,
    `${placeRateExpression(null)} as career_place_rate`,
    `count(*) filter (where finish_position = 1) as career_top1_count`,
    `${winRateExpression("history_keibajo_code = target_keibajo_code")} as same_keibajo_win_rate`,
    `${winRateExpression(`abs(history_kyori - target_kyori) <= ${SAME_DISTANCE_TOLERANCE_METERS}`)} as same_distance_win_rate`,
    `${winRateExpression("left(coalesce(history_track_code, ''), 1) = left(coalesce(target_track_code, ''), 1)")} as same_track_win_rate`,
    `${winRateExpression("coalesce(history_grade_code, '') = coalesce(target_grade_code, '')")} as same_grade_win_rate`,
    `(to_date(max(target_race_date), 'YYYYMMDD') - to_date(max(history_race_date) filter (where recent_rank = 1), 'YYYYMMDD')) as days_since_last_race_raw`,
    `count(*) filter (where to_date(target_race_date, 'YYYYMMDD') - to_date(history_race_date, 'YYYYMMDD') <= ${CONSECUTIVE_RACE_WINDOW_DAYS}) as consecutive_race_count`,
  ];
  const aggregateCte = `
    history_agg as (
      select
        source,
        kaisai_nen,
        kaisai_tsukihi,
        keibajo_code,
        race_bango,
        ketto_toroku_bango,
        max(target_keibajo_code) as target_keibajo_code,
        max(target_kyori) as target_kyori,
        max(target_track_code) as target_track_code,
        max(target_grade_code) as target_grade_code,
        max(target_race_date) as target_race_date,
        ${aggregations.join(",\n        ")}
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
  const updateAssignments = [
    "speed_index_avg_5 = history_agg.speed_index_avg_5",
    "speed_index_best_5 = history_agg.speed_index_best_5",
    "kohan3f_avg_5 = history_agg.kohan3f_avg_5",
    "corner_pass_avg_5 = history_agg.corner_pass_avg_5",
    "career_win_rate = history_agg.career_win_rate",
    "career_place_rate = history_agg.career_place_rate",
    "career_top1_count = history_agg.career_top1_count",
    "same_keibajo_win_rate = history_agg.same_keibajo_win_rate",
    "same_distance_win_rate = history_agg.same_distance_win_rate",
    "same_track_win_rate = history_agg.same_track_win_rate",
    "same_grade_win_rate = history_agg.same_grade_win_rate",
    "days_since_last_race = history_agg.days_since_last_race_raw",
    "consecutive_race_count = history_agg.consecutive_race_count",
    "updated_at = now()",
  ];
  return `
    with ${targetCte},
    ${historyCte},
    ${aggregateCte}
    update ${TARGET_FEATURE_TABLE} target
    set
      ${updateAssignments.join(",\n      ")}
    from history_agg
    where target.source = history_agg.source
      and target.kaisai_nen = history_agg.kaisai_nen
      and target.kaisai_tsukihi = history_agg.kaisai_tsukihi
      and target.keibajo_code = history_agg.keibajo_code
      and target.race_bango = history_agg.race_bango
      and target.ketto_toroku_bango = history_agg.ketto_toroku_bango
  `;
};

export {
  CONSECUTIVE_RACE_WINDOW_DAYS,
  HISTORY_LOOKBACK_DAYS_YYYYMMDD,
  RECENT_HISTORY_WINDOW_SIZE,
  SAME_DISTANCE_TOLERANCE_METERS,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
};
