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

const bataijuExpression = (jraAlias: string, narAlias: string): string =>
  `coalesce(nullif(${jraAlias}.bataiju, '')::integer, nullif(${narAlias}.bataiju, '')::integer)`;

export const buildWeightUpdateSql = (category: FeatureCategory): string => {
  const filters = buildCategoryFilterClauses(category);
  const targetCte = `
    target_with_bataiju as (
      select
        target.source,
        target.kaisai_nen,
        target.kaisai_tsukihi,
        target.keibajo_code,
        target.race_bango,
        target.ketto_toroku_bango,
        target.race_date,
        ${bataijuExpression("tj", "tn")} as current_bataiju
      from ${TARGET_FEATURE_TABLE} target
      ${bataijuJoinClause("target", JRA_RUNNER_TABLE, "tj", "jra")}
      ${bataijuJoinClause("target", NAR_RUNNER_TABLE, "tn", "nar")}
      where target.race_date between $1 and $2
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
        target.current_bataiju,
        ${bataijuExpression("hj", "hn")} as history_bataiju,
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
      from target_with_bataiju target
      join ${SOURCE_FEATURE_TABLE} history
        on history.source = target.source
        and history.ketto_toroku_bango = target.ketto_toroku_bango
        and history.race_date < target.race_date
        and history.race_date >= (target.race_date::integer - ${HISTORY_LOOKBACK_DAYS_YYYYMMDD})::text
      ${bataijuJoinClause("history", JRA_RUNNER_TABLE, "hj", "jra")}
      ${bataijuJoinClause("history", NAR_RUNNER_TABLE, "hn", "nar")}
      where ${filters.historySourceFilter}
        and history.finish_position is not null
    )
  `;
  const aggregateCte = `
    history_agg as (
      select
        source,
        kaisai_nen,
        kaisai_tsukihi,
        keibajo_code,
        race_bango,
        ketto_toroku_bango,
        max(current_bataiju) as current_bataiju,
        avg(history_bataiju) filter (where recent_rank <= ${RECENT_HISTORY_WINDOW_SIZE}) as weight_avg_5
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
  return `
    with ${targetCte},
    ${historyCte},
    ${aggregateCte}
    update ${TARGET_FEATURE_TABLE} target
    set
      weight_avg_5 = history_agg.weight_avg_5,
      weight_diff_from_avg = history_agg.current_bataiju::numeric - history_agg.weight_avg_5,
      updated_at = now()
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
  HISTORY_LOOKBACK_DAYS_YYYYMMDD,
  JRA_RUNNER_TABLE,
  NAR_RUNNER_TABLE,
  RECENT_HISTORY_WINDOW_SIZE,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
};
