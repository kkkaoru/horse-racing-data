// Run with: imported from build-finish-position-features.ts (bun runtime)

import type { FeatureCategory } from "./build-finish-position-features-types";

const SOURCE_FEATURE_TABLE = "race_entry_corner_features";
const TARGET_FEATURE_TABLE = "race_finish_position_features";
const JRA_RUNNER_TABLE = "jvd_se";
const NAR_RUNNER_TABLE = "nvd_se";
const HISTORY_LOOKBACK_DAYS_YYYYMMDD = 100000;
const RECENT_HISTORY_WINDOW_SIZE = 5;

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

const bataijuJoinClause = (
  prefix: string,
  table: string,
  alias: string,
  source: "jra" | "nar",
): string => `
    left join ${table} ${alias}
      on ${prefix}.source = '${source}'
      and ${alias}.kaisai_nen = ${prefix}.kaisai_nen
      and ${alias}.kaisai_tsukihi = ${prefix}.kaisai_tsukihi
      and ${alias}.keibajo_code = ${prefix}.keibajo_code
      and ${alias}.race_bango = ${prefix}.race_bango
      and ${alias}.ketto_toroku_bango = ${prefix}.ketto_toroku_bango
  `;

const safeBataijuCast = (alias: string): string => `
  case
    when trim(coalesce(${alias}.bataiju::text, '')) ~ '^-?[0-9]+$'
      then trim(${alias}.bataiju::text)::integer
    else null
  end
`;

const bataijuExpression = (jraAlias: string, narAlias: string): string =>
  `coalesce(${safeBataijuCast(jraAlias)}, ${safeBataijuCast(narAlias)})`;

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
        history.kyori::numeric as hist_kyori,
        history.soha_time::numeric as hist_soha_time,
        history.barei::numeric as hist_barei,
        history.futan_juryo::numeric as hist_futan_juryo,
        history.finish_position::numeric as hist_finish_position,
        ${bataijuExpression("hj", "hn")}::numeric as hist_bataiju,
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
      ${bataijuJoinClause("history", JRA_RUNNER_TABLE, "hj", "jra")}
      ${bataijuJoinClause("history", NAR_RUNNER_TABLE, "hn", "nar")}
      where ${filters.historySourceFilter}
        and history.finish_position is not null
        and history.kyori is not null
        and history.kyori > 0
        and history.soha_time is not null
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
        avg(hist_soha_time / hist_kyori * hist_bataiju)
          filter (where recent_rank <= ${RECENT_HISTORY_WINDOW_SIZE} and hist_bataiju is not null)
          as past_speed_kg_normalized_avg5,
        avg(hist_soha_time / hist_kyori * hist_futan_juryo)
          filter (where recent_rank <= ${RECENT_HISTORY_WINDOW_SIZE} and hist_futan_juryo is not null)
          as past_speed_futan_normalized_avg5,
        avg((hist_soha_time / hist_kyori) / nullif(hist_barei, 0))
          filter (where recent_rank <= ${RECENT_HISTORY_WINDOW_SIZE} and hist_barei is not null)
          as past_speed_age_adjusted_avg5,
        stddev_pop(hist_soha_time / hist_kyori)
          filter (where recent_rank <= ${RECENT_HISTORY_WINDOW_SIZE})
          as past_speed_volatility_5,
        stddev_pop(hist_finish_position::double precision)
          filter (where recent_rank <= ${RECENT_HISTORY_WINDOW_SIZE})
          as past_finish_position_volatility_5
      from history_raw
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
    )
  `;

const updateAssignments = [
  "past_speed_kg_normalized_avg5 = history_agg.past_speed_kg_normalized_avg5",
  "past_speed_futan_normalized_avg5 = history_agg.past_speed_futan_normalized_avg5",
  "past_speed_age_adjusted_avg5 = history_agg.past_speed_age_adjusted_avg5",
  "past_speed_volatility_5 = history_agg.past_speed_volatility_5",
  "past_finish_position_volatility_5 = history_agg.past_finish_position_volatility_5",
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

export const buildRelationshipHistoryUpdateSql = (category: FeatureCategory): string => {
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
  JRA_RUNNER_TABLE,
  NAR_RUNNER_TABLE,
  RECENT_HISTORY_WINDOW_SIZE,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
};
