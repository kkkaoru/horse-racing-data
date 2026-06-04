// Run with: imported from build-finish-position-features.ts (bun runtime)

import type { FeatureCategory } from "./build-finish-position-features-types";

const SOURCE_FEATURE_TABLE = "race_entry_corner_features";
const TARGET_FEATURE_TABLE = "race_finish_position_features";
const HISTORY_LOOKBACK_DAYS_YYYYMMDD = 100000;
const CLASS_VARIANCE_WINDOW = 5;
const CLASS_VARIANCE_MIN_RACES = 2;

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
  historySourceFilter: string;
  targetCategoryFilter: string;
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
        target.race_date
      from ${TARGET_FEATURE_TABLE} target
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
        ) as mapped_rank
      from target
      join ${SOURCE_FEATURE_TABLE} history
        on history.source = target.source
        and history.ketto_toroku_bango = target.ketto_toroku_bango
        and history.race_date < target.race_date
        and history.race_date >= (target.race_date::integer - ${HISTORY_LOOKBACK_DAYS_YYYYMMDD})::text
      where ${filters.historySourceFilter}
        and ${classLevelCaseExpression("history.kyoso_joken_code")} is not null
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
        case
          when count(*) filter (where mapped_rank <= ${CLASS_VARIANCE_WINDOW}) >= ${CLASS_VARIANCE_MIN_RACES}
          then stddev_pop(history_class_level::numeric)
                 filter (where mapped_rank <= ${CLASS_VARIANCE_WINDOW})
          else null
        end as horse_recent_class_variance
      from history_raw
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
    )
  `;

const updateAssignments = [
  "horse_recent_class_variance = history_agg.horse_recent_class_variance",
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

export const buildHorseClassVarianceUpdateSql = (category: FeatureCategory): string => {
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
  CLASS_VARIANCE_MIN_RACES,
  CLASS_VARIANCE_WINDOW,
  HISTORY_LOOKBACK_DAYS_YYYYMMDD,
  JRA_CLASS_LEVELS,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
};
