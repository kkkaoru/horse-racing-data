// Run with: imported from build-finish-position-features.ts (bun runtime)

import type { FeatureCategory } from "./build-finish-position-features-types";

const SOURCE_FEATURE_TABLE = "race_entry_corner_features";
const TARGET_FEATURE_TABLE = "race_finish_position_features";
const HISTORY_LOOKBACK_DAYS_YYYYMMDD = 100000;
const RECENT_TREND_WINDOW = 5;
const RECENT_THREE_WINDOW = 3;
const TREND_MIN_RACES = 3;

const JRA_CLASS_LEVELS: Record<string, number> = {
  "000": 0,
  "005": 1,
  "010": 2,
  "016": 3,
  "701": 4,
  "703": 5,
  "999": 6,
};

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

const classLevelCaseExpression = (codeExpression: string): string => {
  const branches = Object.entries(JRA_CLASS_LEVELS)
    .map(([code, level]) => `when '${code}' then ${level}`)
    .join(" ");
  return `case ${codeExpression} ${branches} else null end`;
};

const targetCte = (filters: CategoryFilterClauses): string => `
    target as (
      select
        target.source,
        target.kaisai_nen,
        target.kaisai_tsukihi,
        target.keibajo_code,
        target.race_bango,
        target.ketto_toroku_bango,
        target.race_date,
        target.kyori as target_kyori,
        ${classLevelCaseExpression("rec.kyoso_joken_code")} as target_class_level
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
    )
  `;

const historyCte = (filters: CategoryFilterClauses): string => `
    history_raw as (
      select
        target.source,
        target.kaisai_nen,
        target.kaisai_tsukihi,
        target.keibajo_code,
        target.race_bango,
        target.ketto_toroku_bango,
        target.target_kyori,
        target.target_class_level,
        history.finish_norm::numeric as finish_norm,
        history.time_sa::numeric as time_sa,
        history.corner3_norm::numeric as corner3_norm,
        history.kyori as history_kyori,
        ${classLevelCaseExpression("history.kyoso_joken_code")} as history_class_level,
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

const aggregationCte = (): string => `
    history_agg as (
      select
        source,
        kaisai_nen,
        kaisai_tsukihi,
        keibajo_code,
        race_bango,
        ketto_toroku_bango,
        max(finish_norm) filter (where recent_rank = 1) as last_race_finish_norm,
        max(time_sa) filter (where recent_rank = 1) as last_race_margin_to_winner,
        max(corner3_norm) filter (where recent_rank = 1) as last_race_corner_pass_norm,
        (
          max(target_class_level) filter (where recent_rank = 1)
            - max(history_class_level) filter (where recent_rank = 1)
        ) as last_race_class_diff,
        (
          max(history_kyori) filter (where recent_rank = 1)
            - max(target_kyori) filter (where recent_rank = 1)
        ) as last_race_distance_diff,
        case
          when count(*) filter (where recent_rank <= ${RECENT_TREND_WINDOW}) >= ${TREND_MIN_RACES}
          then regr_slope(finish_norm, recent_rank::numeric)
                 filter (where recent_rank <= ${RECENT_TREND_WINDOW})
          else null
        end as finish_trend_5,
        avg(finish_norm) filter (where recent_rank <= ${RECENT_THREE_WINDOW}) as last_3_avg_finish_norm
      from history_raw
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
    )
  `;

const updateAssignments = [
  "last_race_finish_norm = history_agg.last_race_finish_norm",
  "last_race_margin_to_winner = history_agg.last_race_margin_to_winner",
  "last_race_corner_pass_norm = history_agg.last_race_corner_pass_norm",
  "last_race_class_diff = history_agg.last_race_class_diff",
  "last_race_distance_diff = history_agg.last_race_distance_diff",
  "finish_trend_5 = history_agg.finish_trend_5",
  "last_3_avg_finish_norm = history_agg.last_3_avg_finish_norm",
  "updated_at = now()",
];

const joinConditions = [
  "target.source = history_agg.source",
  "target.kaisai_nen = history_agg.kaisai_nen",
  "target.kaisai_tsukihi = history_agg.kaisai_tsukihi",
  "target.keibajo_code = history_agg.keibajo_code",
  "target.race_bango = history_agg.race_bango",
  "target.ketto_toroku_bango = history_agg.ketto_toroku_bango",
];

export const buildRecentFormUpdateSql = (category: FeatureCategory): string => {
  const filters = buildCategoryFilterClauses(category);
  return `
    with ${targetCte(filters)},
    ${historyCte(filters)},
    ${aggregationCte()}
    update ${TARGET_FEATURE_TABLE} target
    set
      ${updateAssignments.join(",\n      ")}
    from history_agg
    where ${joinConditions.join("\n      and ")}
  `;
};

export {
  HISTORY_LOOKBACK_DAYS_YYYYMMDD,
  JRA_CLASS_LEVELS,
  RECENT_THREE_WINDOW,
  RECENT_TREND_WINDOW,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
  TREND_MIN_RACES,
};
