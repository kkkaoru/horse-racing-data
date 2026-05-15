// Run with: imported from build-finish-position-features.ts (bun runtime)

import type { FeatureCategory } from "./build-finish-position-features-types";

const TARGET_FEATURE_TABLE = "race_finish_position_features";

const TENKO_CODE_TO_WEATHER_VALUE: Record<string, string> = {
  "1": "0",
  "2": "0.3",
  "3": "0.7",
  "4": "0.7",
  "5": "1.0",
  "6": "1.0",
};

interface WeatherBinding {
  raceMasterTable: string;
  targetFilter: string;
}

const buildWeatherBinding = (category: FeatureCategory): WeatherBinding | null => {
  if (category === "jra") {
    return {
      raceMasterTable: "jvd_ra",
      targetFilter: "target.category = 'jra'",
    };
  }
  if (category === "nar" || category === "ban-ei") {
    return {
      raceMasterTable: "nvd_ra",
      targetFilter: category === "nar" ? "target.category = 'nar'" : "target.category = 'ban-ei'",
    };
  }
  return null;
};

const tenkoCaseExpression = (codeColumn: string): string => {
  const branches = Object.entries(TENKO_CODE_TO_WEATHER_VALUE)
    .map(([code, value]) => `when '${code}' then ${value}::numeric`)
    .join(" ");
  return `case ${codeColumn} ${branches} else null end`;
};

const noopUpdateSql = (): string =>
  `update ${TARGET_FEATURE_TABLE} set updated_at = updated_at where false`;

export const buildWeatherUpdateSql = (category: FeatureCategory): string => {
  const binding = buildWeatherBinding(category);
  if (binding === null) return noopUpdateSql();
  return `
    with weather_lookup as (
      select
        target.source,
        target.kaisai_nen,
        target.kaisai_tsukihi,
        target.keibajo_code,
        target.race_bango,
        target.ketto_toroku_bango,
        ra.tenko_code
      from ${TARGET_FEATURE_TABLE} target
      join ${binding.raceMasterTable} ra
        on ra.kaisai_nen = target.kaisai_nen
        and ra.kaisai_tsukihi = target.kaisai_tsukihi
        and ra.keibajo_code = target.keibajo_code
        and ra.race_bango = target.race_bango
      where target.race_date between $1 and $2
        and ${binding.targetFilter}
    )
    update ${TARGET_FEATURE_TABLE} target
    set
      weather_normalized = ${tenkoCaseExpression("weather_lookup.tenko_code")},
      updated_at = now()
    from weather_lookup
    where target.source = weather_lookup.source
      and target.kaisai_nen = weather_lookup.kaisai_nen
      and target.kaisai_tsukihi = weather_lookup.kaisai_tsukihi
      and target.keibajo_code = weather_lookup.keibajo_code
      and target.race_bango = weather_lookup.race_bango
      and target.ketto_toroku_bango = weather_lookup.ketto_toroku_bango
  `;
};

export { TARGET_FEATURE_TABLE, TENKO_CODE_TO_WEATHER_VALUE, buildWeatherBinding };
