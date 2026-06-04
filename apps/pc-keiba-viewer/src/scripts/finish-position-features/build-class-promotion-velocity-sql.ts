// Run with: imported from build-finish-position-features.ts (bun runtime)

import type { FeatureCategory } from "./build-finish-position-features-types";
import { JRA_CLASS_LEVELS } from "./build-recent-form-sql";

const SOURCE_FEATURE_TABLE = "race_entry_corner_features";
const TARGET_FEATURE_TABLE = "race_finish_position_features";
const HISTORY_LOOKBACK_DAYS_YYYYMMDD = 100000;
const PROMOTION_LEVEL_BUFFER = 1;
const WINNING_FINISH_POSITION = 1;

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
        target.race_date as target_race_date,
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
        target.target_race_date,
        target.target_class_level,
        history.race_date as history_race_date,
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
        ) as promotion_rank
      from target
      join ${SOURCE_FEATURE_TABLE} history
        on history.source = target.source
        and history.ketto_toroku_bango = target.ketto_toroku_bango
        and history.race_date < target.target_race_date
        and history.race_date >= (target.target_race_date::integer - ${HISTORY_LOOKBACK_DAYS_YYYYMMDD})::text
      where ${filters.historySourceFilter}
        and history.finish_position = ${WINNING_FINISH_POSITION}
        and target.target_class_level is not null
        and ${classLevelCaseExpression("history.kyoso_joken_code")}
              >= target.target_class_level - ${PROMOTION_LEVEL_BUFFER}
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
        (
          to_date(max(target_race_date) filter (where promotion_rank = 1), 'YYYYMMDD')
            - to_date(max(history_race_date) filter (where promotion_rank = 1), 'YYYYMMDD')
        ) as class_promotion_velocity
      from history_raw
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
    )
  `;

const joinConditions = [
  "target.source = history_agg.source",
  "target.kaisai_nen = history_agg.kaisai_nen",
  "target.kaisai_tsukihi = history_agg.kaisai_tsukihi",
  "target.keibajo_code = history_agg.keibajo_code",
  "target.race_bango = history_agg.race_bango",
  "target.ketto_toroku_bango = history_agg.ketto_toroku_bango",
];

const updateAssignments = [
  "class_promotion_velocity = history_agg.class_promotion_velocity",
  "updated_at = now()",
];

export const buildClassPromotionVelocityUpdateSql = (category: FeatureCategory): string => {
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
  PROMOTION_LEVEL_BUFFER,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
  WINNING_FINISH_POSITION,
};
