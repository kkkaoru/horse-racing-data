// Run with: imported from build-finish-position-features.ts (bun runtime)

import type { FeatureCategory } from "./build-finish-position-features-types";

const SOURCE_FEATURE_TABLE = "race_entry_corner_features";
const TARGET_FEATURE_TABLE = "race_finish_position_features";
const TRACK_BIAS_WINDOW_DAYS = 5;
const FRONT_CORNER_THRESHOLD = "0.33";

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

export const buildTrackBiasUpdateSql = (category: FeatureCategory): string => {
  const filters = buildCategoryFilterClauses(category);
  return `
    with target as (
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
    ),
    bias as (
      select
        target.source,
        target.kaisai_nen,
        target.kaisai_tsukihi,
        target.keibajo_code,
        target.race_bango,
        target.ketto_toroku_bango,
        avg(
          case
            when history.finish_position = 1
              and history.umaban * 2 <= history.shusso_tosu + 1
            then 1 else 0
          end
        ) as inside_win_rate,
        avg(
          case
            when history.finish_position = 1
              and history.corner1_norm::numeric <= ${FRONT_CORNER_THRESHOLD}
            then 1 else 0
          end
        ) as front_win_rate
      from target
      left join ${SOURCE_FEATURE_TABLE} history
        on history.keibajo_code = target.keibajo_code
        and history.race_date < target.race_date
        and to_date(history.race_date, 'YYYYMMDD')
          >= to_date(target.race_date, 'YYYYMMDD') - ${TRACK_BIAS_WINDOW_DAYS}
        and history.finish_position is not null
        and ${filters.historySourceFilter}
      group by
        target.source,
        target.kaisai_nen,
        target.kaisai_tsukihi,
        target.keibajo_code,
        target.race_bango,
        target.ketto_toroku_bango
    )
    update ${TARGET_FEATURE_TABLE} target
    set
      track_bias_inside = bias.inside_win_rate,
      track_bias_front = bias.front_win_rate,
      updated_at = now()
    from bias
    where target.source = bias.source
      and target.kaisai_nen = bias.kaisai_nen
      and target.kaisai_tsukihi = bias.kaisai_tsukihi
      and target.keibajo_code = bias.keibajo_code
      and target.race_bango = bias.race_bango
      and target.ketto_toroku_bango = bias.ketto_toroku_bango
  `;
};

export const buildTrackBiasIndexSqls = (): string[] => [
  `create index if not exists ${SOURCE_FEATURE_TABLE}_keibajo_date_idx
     on ${SOURCE_FEATURE_TABLE} (source, keibajo_code, race_date)
     where finish_position is not null`,
];

export {
  FRONT_CORNER_THRESHOLD,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
  TRACK_BIAS_WINDOW_DAYS,
};
