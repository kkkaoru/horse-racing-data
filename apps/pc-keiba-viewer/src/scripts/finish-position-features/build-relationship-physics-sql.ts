// Run with: imported from build-finish-position-features.ts (bun runtime)

import type { FeatureCategory } from "./build-finish-position-features-types";

const SOURCE_FEATURE_TABLE = "race_entry_corner_features";
const TARGET_FEATURE_TABLE = "race_finish_position_features";
const JRA_RUNNER_TABLE = "jvd_se";
const NAR_RUNNER_TABLE = "nvd_se";

interface CategoryFilterClauses {
  targetCategoryFilter: string;
}

const buildCategoryFilterClauses = (category: FeatureCategory): CategoryFilterClauses => {
  if (category === "jra") {
    return { targetCategoryFilter: "target.category = 'jra'" };
  }
  if (category === "nar") {
    return { targetCategoryFilter: "target.category = 'nar'" };
  }
  if (category === "ban-ei") {
    return { targetCategoryFilter: "target.category = 'ban-ei'" };
  }
  return { targetCategoryFilter: "true" };
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

const RACE_PARTITION_COLUMNS = [
  "source",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
];

const racePartitionClause = (): string =>
  RACE_PARTITION_COLUMNS.map((column) => `        ${column}`).join(",\n");

const inputsCte = (filters: CategoryFilterClauses): string => `
    inputs as (
      select
        target.source,
        target.kaisai_nen,
        target.kaisai_tsukihi,
        target.keibajo_code,
        target.race_bango,
        target.ketto_toroku_bango,
        ${bataijuExpression("tj", "tn")}::numeric as bataiju,
        rec.futan_juryo::numeric as futan_juryo,
        rec.barei::numeric as barei,
        rec.kyori::numeric as kyori
      from ${TARGET_FEATURE_TABLE} target
      join ${SOURCE_FEATURE_TABLE} rec
        on rec.source = target.source
        and rec.kaisai_nen = target.kaisai_nen
        and rec.kaisai_tsukihi = target.kaisai_tsukihi
        and rec.keibajo_code = target.keibajo_code
        and rec.race_bango = target.race_bango
        and rec.ketto_toroku_bango = target.ketto_toroku_bango
      ${bataijuJoinClause("target", JRA_RUNNER_TABLE, "tj", "jra")}
      ${bataijuJoinClause("target", NAR_RUNNER_TABLE, "tn", "nar")}
      where target.race_date between $1 and $2
        and ${filters.targetCategoryFilter}
    )
  `;

const windowCte = (): string => `
    windowed as (
      select
        source,
        kaisai_nen,
        kaisai_tsukihi,
        keibajo_code,
        race_bango,
        ketto_toroku_bango,
        bataiju,
        futan_juryo,
        barei,
        kyori,
        futan_juryo / nullif(bataiju, 0) as bataiju_futan_ratio,
        futan_juryo / nullif(barei, 0) as futan_per_barei,
        bataiju / ln(1 + kyori) as bataiju_per_kyori_log,
        bataiju - avg(bataiju) over race_window as bataiju_diff_from_race_mean,
        rank() over (
          partition by
${racePartitionClause()}
          order by bataiju desc nulls last
        ) as bataiju_rank_in_race,
        (futan_juryo / nullif(bataiju, 0)) as joint_ratio,
        avg(futan_juryo / nullif(bataiju, 0)) over race_window as joint_ratio_mean,
        stddev_pop(futan_juryo / nullif(bataiju, 0)) over race_window as joint_ratio_stddev,
        barei - avg(barei) over race_window as barei_diff_from_race_mean
      from inputs
      window race_window as (
        partition by
${racePartitionClause()}
      )
    )
  `;

const zscoreExpression = `
        case
          when windowed.joint_ratio_stddev is null
            or windowed.joint_ratio_stddev = 0
          then null
          else (windowed.joint_ratio - windowed.joint_ratio_mean) / windowed.joint_ratio_stddev
        end
      `;

const UPDATE_ASSIGNMENTS = [
  "bataiju_futan_ratio = windowed.bataiju_futan_ratio",
  "futan_per_barei = windowed.futan_per_barei",
  "bataiju_per_kyori_log = windowed.bataiju_per_kyori_log",
  "bataiju_diff_from_race_mean = windowed.bataiju_diff_from_race_mean",
  "bataiju_rank_in_race = windowed.bataiju_rank_in_race",
  `futan_minus_bataiju_zscore_in_race =${zscoreExpression}`,
  "barei_diff_from_race_mean = windowed.barei_diff_from_race_mean",
  "updated_at = now()",
];

const JOIN_CONDITIONS = [
  "target.source = windowed.source",
  "target.kaisai_nen = windowed.kaisai_nen",
  "target.kaisai_tsukihi = windowed.kaisai_tsukihi",
  "target.keibajo_code = windowed.keibajo_code",
  "target.race_bango = windowed.race_bango",
  "target.ketto_toroku_bango = windowed.ketto_toroku_bango",
];

export const buildRelationshipPhysicsUpdateSql = (category: FeatureCategory): string => {
  const filters = buildCategoryFilterClauses(category);
  return `
    with ${inputsCte(filters)},
    ${windowCte()}
    update ${TARGET_FEATURE_TABLE} target
    set
      ${UPDATE_ASSIGNMENTS.join(",\n      ")}
    from windowed
    where ${JOIN_CONDITIONS.join("\n      and ")}
  `;
};

export {
  JRA_RUNNER_TABLE,
  NAR_RUNNER_TABLE,
  RACE_PARTITION_COLUMNS,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
};
