// Run with: imported from build-finish-position-features.ts (bun runtime)

import type { FeatureCategory } from "./build-finish-position-features-types";

const SOURCE_FEATURE_TABLE = "race_entry_corner_features";
const TARGET_FEATURE_TABLE = "race_finish_position_features";
const DISTANCE_BAND_WIDTH_METERS = 400;
const MIN_RACES_FOR_RELIABLE_STAT = 5;
const PEDIGREE_COMPOSITE_DIVISOR = 3;

interface PedigreeCategoryBinding {
  horseMasterTable: string;
  sourceFilter: string;
  targetFilter: string;
}

const buildPedigreeBinding = (category: FeatureCategory): PedigreeCategoryBinding | null => {
  if (category === "jra") {
    return {
      horseMasterTable: "jvd_um",
      sourceFilter: "source = 'jra'",
      targetFilter: "target.category = 'jra'",
    };
  }
  return null;
};

const distanceBandExpression = (kyoriColumn: string): string =>
  `(coalesce(${kyoriColumn}, 0) / ${DISTANCE_BAND_WIDTH_METERS})::int`;

const surfaceCodeExpression = (trackCodeColumn: string): string =>
  `left(coalesce(${trackCodeColumn}, ''), 1)`;

const winRateExpression = (): string => `avg(case when finish_position = 1 then 1 else 0 end)`;

const noopUpdateSql = (): string =>
  `update ${TARGET_FEATURE_TABLE} set updated_at = updated_at where false`;

const buildSireDistanceStatsCte = (binding: PedigreeCategoryBinding): string => `
    sire_distance_stats as (
      select
        um.ketto_joho_01b as sire,
        ${distanceBandExpression("rec.kyori")} as kyori_band,
        ${winRateExpression()} as sire_distance_win_rate_val,
        avg(rec.finish_norm) as sire_avg_finish_at_distance_val,
        count(*) as race_count
      from ${SOURCE_FEATURE_TABLE} rec
      join ${binding.horseMasterTable} um
        on um.ketto_toroku_bango = rec.ketto_toroku_bango
      where rec.${binding.sourceFilter}
        and rec.finish_position is not null
        and um.ketto_joho_01b is not null
        and btrim(um.ketto_joho_01b) <> ''
      group by um.ketto_joho_01b, ${distanceBandExpression("rec.kyori")}
    )
  `;

const buildSireTrackStatsCte = (binding: PedigreeCategoryBinding): string => `
    sire_track_stats as (
      select
        um.ketto_joho_01b as sire,
        ${surfaceCodeExpression("rec.track_code")} as surface,
        ${winRateExpression()} as sire_track_win_rate_val,
        count(*) as race_count
      from ${SOURCE_FEATURE_TABLE} rec
      join ${binding.horseMasterTable} um
        on um.ketto_toroku_bango = rec.ketto_toroku_bango
      where rec.${binding.sourceFilter}
        and rec.finish_position is not null
        and um.ketto_joho_01b is not null
        and btrim(um.ketto_joho_01b) <> ''
      group by um.ketto_joho_01b, ${surfaceCodeExpression("rec.track_code")}
    )
  `;

const buildDamSireDistanceStatsCte = (binding: PedigreeCategoryBinding): string => `
    damsire_distance_stats as (
      select
        um.ketto_joho_05b as damsire,
        ${distanceBandExpression("rec.kyori")} as kyori_band,
        ${winRateExpression()} as dam_sire_distance_win_rate_val,
        count(*) as race_count
      from ${SOURCE_FEATURE_TABLE} rec
      join ${binding.horseMasterTable} um
        on um.ketto_toroku_bango = rec.ketto_toroku_bango
      where rec.${binding.sourceFilter}
        and rec.finish_position is not null
        and um.ketto_joho_05b is not null
        and btrim(um.ketto_joho_05b) <> ''
      group by um.ketto_joho_05b, ${distanceBandExpression("rec.kyori")}
    )
  `;

const buildDamSireTrackStatsCte = (binding: PedigreeCategoryBinding): string => `
    damsire_track_stats as (
      select
        um.ketto_joho_05b as damsire,
        ${surfaceCodeExpression("rec.track_code")} as surface,
        avg(rec.finish_norm) as damsire_avg_finish_at_track_val,
        count(*) as race_count
      from ${SOURCE_FEATURE_TABLE} rec
      join ${binding.horseMasterTable} um
        on um.ketto_toroku_bango = rec.ketto_toroku_bango
      where rec.${binding.sourceFilter}
        and rec.finish_position is not null
        and um.ketto_joho_05b is not null
        and btrim(um.ketto_joho_05b) <> ''
      group by um.ketto_joho_05b, ${surfaceCodeExpression("rec.track_code")}
    )
  `;

const buildTargetWithPedigreeCte = (binding: PedigreeCategoryBinding): string => `
    target_with_pedigree as (
      select
        target.source,
        target.kaisai_nen,
        target.kaisai_tsukihi,
        target.keibajo_code,
        target.race_bango,
        target.ketto_toroku_bango,
        ${distanceBandExpression("target.kyori")} as target_kyori_band,
        ${surfaceCodeExpression("target.track_code")} as target_surface,
        um.ketto_joho_01b as target_sire,
        um.ketto_joho_05b as target_damsire
      from ${TARGET_FEATURE_TABLE} target
      left join ${binding.horseMasterTable} um
        on um.ketto_toroku_bango = target.ketto_toroku_bango
      where target.race_date between $1 and $2
        and ${binding.targetFilter}
    )
  `;

const pedigreeScoreExpression = (): string =>
  `((coalesce(sds.sire_distance_win_rate_val, 0)
       + coalesce(dsd.dam_sire_distance_win_rate_val, 0)
       + coalesce(sts.sire_track_win_rate_val, 0))
      / ${PEDIGREE_COMPOSITE_DIVISOR}::numeric)`;

const reliableValue = (alias: string, column: string): string =>
  `case when ${alias}.race_count >= ${MIN_RACES_FOR_RELIABLE_STAT} then ${alias}.${column} else null end`;

export const buildPedigreeUpdateSql = (category: FeatureCategory): string => {
  const binding = buildPedigreeBinding(category);
  if (binding === null) return noopUpdateSql();
  const sireDistanceCte = buildSireDistanceStatsCte(binding);
  const sireTrackCte = buildSireTrackStatsCte(binding);
  const damsireDistanceCte = buildDamSireDistanceStatsCte(binding);
  const damsireTrackCte = buildDamSireTrackStatsCte(binding);
  const targetCte = buildTargetWithPedigreeCte(binding);
  const sireDistanceValue = reliableValue("sds", "sire_distance_win_rate_val");
  const sireAvgFinishValue = reliableValue("sds", "sire_avg_finish_at_distance_val");
  const sireTrackValue = reliableValue("sts", "sire_track_win_rate_val");
  const damsireDistanceValue = reliableValue("dsd", "dam_sire_distance_win_rate_val");
  const damsireAvgFinishValue = reliableValue("dst", "damsire_avg_finish_at_track_val");
  return `
    with ${sireDistanceCte},
    ${sireTrackCte},
    ${damsireDistanceCte},
    ${damsireTrackCte},
    ${targetCte}
    update ${TARGET_FEATURE_TABLE} target
    set
      sire_distance_win_rate = ${sireDistanceValue},
      sire_track_win_rate = ${sireTrackValue},
      dam_sire_distance_win_rate = ${damsireDistanceValue},
      sire_avg_finish_at_distance = ${sireAvgFinishValue},
      damsire_avg_finish_at_track = ${damsireAvgFinishValue},
      pedigree_score_for_race = ${pedigreeScoreExpression()},
      updated_at = now()
    from target_with_pedigree twp
    left join sire_distance_stats sds
      on sds.sire = twp.target_sire and sds.kyori_band = twp.target_kyori_band
    left join sire_track_stats sts
      on sts.sire = twp.target_sire and sts.surface = twp.target_surface
    left join damsire_distance_stats dsd
      on dsd.damsire = twp.target_damsire and dsd.kyori_band = twp.target_kyori_band
    left join damsire_track_stats dst
      on dst.damsire = twp.target_damsire and dst.surface = twp.target_surface
    where target.source = twp.source
      and target.kaisai_nen = twp.kaisai_nen
      and target.kaisai_tsukihi = twp.kaisai_tsukihi
      and target.keibajo_code = twp.keibajo_code
      and target.race_bango = twp.race_bango
      and target.ketto_toroku_bango = twp.ketto_toroku_bango
  `;
};

export {
  DISTANCE_BAND_WIDTH_METERS,
  MIN_RACES_FOR_RELIABLE_STAT,
  PEDIGREE_COMPOSITE_DIVISOR,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
  buildPedigreeBinding,
};
