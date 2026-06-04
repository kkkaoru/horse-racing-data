// Run with: imported from build-finish-position-features.ts (bun runtime)

import type { FeatureCategory } from "./build-finish-position-features-types";

const SOURCE_FEATURE_TABLE = "race_entry_corner_features";
const TARGET_FEATURE_TABLE = "race_finish_position_features";
const HISTORY_LOOKBACK_DAYS_YYYYMMDD = 100000;
const HIRABA_KYOSO_JOKEN_CODES = ["000", "005", "010", "016"];
const PARTNER_COLUMN = "chokyoshimei_ryakusho";

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

const buildHirabaInClause = (): string =>
  HIRABA_KYOSO_JOKEN_CODES.map((code) => `'${code}'`).join(", ");

const buildTargetCte = (category: FeatureCategory): string => {
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
        rec.${PARTNER_COLUMN} as target_partner
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
        and rec.${PARTNER_COLUMN} is not null
        and btrim(rec.${PARTNER_COLUMN}) <> ''
    )
  `;
};

const buildHistoryCte = (category: FeatureCategory): string => {
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
        history.finish_position
      from target
      join ${SOURCE_FEATURE_TABLE} history
        on history.source = target.source
        and history.${PARTNER_COLUMN} = target.target_partner
        and history.race_date < target.race_date
        and history.race_date >= (target.race_date::integer - ${HISTORY_LOOKBACK_DAYS_YYYYMMDD})::text
      where ${filters.historySourceFilter}
        and history.finish_position is not null
        and history.kyoso_joken_code in (${buildHirabaInClause()})
    )
  `;
};

const buildAggregationCte = (): string => `
    source_agg as (
      select
        source,
        kaisai_nen,
        kaisai_tsukihi,
        keibajo_code,
        race_bango,
        ketto_toroku_bango,
        avg(case when finish_position = 1 then 1 else 0 end) as trainer_hiraba_win_rate
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

export const buildTrainerHirabaUpdateSql = (category: FeatureCategory): string => {
  const targetCte = buildTargetCte(category);
  const historyCte = buildHistoryCte(category);
  const aggregationCte = buildAggregationCte();
  const updateAssignments = [
    "trainer_hiraba_win_rate = source_agg.trainer_hiraba_win_rate",
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

export const buildTrainerHirabaIndexSqls = (): string[] => [
  `create index if not exists ${SOURCE_FEATURE_TABLE}_trainer_hiraba_date_idx
     on ${SOURCE_FEATURE_TABLE} (source, chokyoshimei_ryakusho, race_date)
     where chokyoshimei_ryakusho is not null
       and finish_position is not null
       and kyoso_joken_code in ('000', '005', '010', '016')`,
];

export {
  HIRABA_KYOSO_JOKEN_CODES,
  HISTORY_LOOKBACK_DAYS_YYYYMMDD,
  PARTNER_COLUMN,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
};
