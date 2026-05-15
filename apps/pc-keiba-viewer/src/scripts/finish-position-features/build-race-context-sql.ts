// Run with: imported from build-finish-position-features.ts (bun runtime)

import type { FeatureCategory } from "./build-finish-position-features-types";

const SOURCE_FEATURE_TABLE = "race_entry_corner_features";
const TARGET_FEATURE_TABLE = "race_finish_position_features";
const TOP_SPEED_HORSE_COUNT = 3;
const RIVAL_DISTANCE_THRESHOLD = 0.3;
const MAX_FIELD_SIZE = 18;

const BABAJOTAI_NORMALIZED_VALUES: Record<string, string> = {
  "1": "0",
  "2": "0.3",
  "3": "0.6",
  "4": "1.0",
};

interface CategoryFilterClause {
  targetFilter: string;
}

const buildCategoryFilterClause = (category: FeatureCategory): CategoryFilterClause => {
  if (category === "jra") return { targetFilter: "target.category = 'jra'" };
  if (category === "nar") return { targetFilter: "target.category = 'nar'" };
  if (category === "ban-ei") return { targetFilter: "target.category = 'ban-ei'" };
  return { targetFilter: "true" };
};

const buildRaceHorsesCte = (categoryFilter: string): string => `
    race_horses as (
      select
        target.source,
        target.kaisai_nen,
        target.kaisai_tsukihi,
        target.keibajo_code,
        target.race_bango,
        target.ketto_toroku_bango,
        target.speed_index_avg_5,
        target.speed_index_best_5,
        target.same_distance_win_rate
      from ${TARGET_FEATURE_TABLE} target
      where target.race_date between $1 and $2
        and ${categoryFilter}
    )
  `;

const buildRaceFieldAggregatesCte = (): string => `
    race_field_aggregates as (
      select
        source,
        kaisai_nen,
        kaisai_tsukihi,
        keibajo_code,
        race_bango,
        avg(speed_index_avg_5) as race_avg_speed,
        count(*) filter (where same_distance_win_rate > ${RIVAL_DISTANCE_THRESHOLD}) as race_strong_count
      from race_horses
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
    )
  `;

const buildRaceTop3SpeedCte = (): string => `
    race_top3_speed as (
      select
        source,
        kaisai_nen,
        kaisai_tsukihi,
        keibajo_code,
        race_bango,
        avg(speed_index_best_5) as race_top_speed
      from (
        select
          source,
          kaisai_nen,
          kaisai_tsukihi,
          keibajo_code,
          race_bango,
          speed_index_best_5,
          row_number() over (
            partition by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
            order by speed_index_best_5 asc nulls last
          ) as speed_rank
        from race_horses
        where speed_index_best_5 is not null
      ) ranked
      where speed_rank <= ${TOP_SPEED_HORSE_COUNT}
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
    )
  `;

const buildTargetContextCte = (categoryFilter: string): string => `
    target_context as (
      select
        target.source,
        target.kaisai_nen,
        target.kaisai_tsukihi,
        target.keibajo_code,
        target.race_bango,
        target.ketto_toroku_bango,
        target.shusso_tosu as target_shusso_tosu,
        target.grade_code as target_grade_code,
        target.track_code as target_track_code,
        target.same_distance_win_rate as target_same_distance_win_rate,
        rec.babajotai_code_shiba as target_babajotai_shiba,
        rec.babajotai_code_dirt as target_babajotai_dirt
      from ${TARGET_FEATURE_TABLE} target
      join ${SOURCE_FEATURE_TABLE} rec
        on rec.source = target.source
        and rec.kaisai_nen = target.kaisai_nen
        and rec.kaisai_tsukihi = target.kaisai_tsukihi
        and rec.keibajo_code = target.keibajo_code
        and rec.race_bango = target.race_bango
        and rec.ketto_toroku_bango = target.ketto_toroku_bango
      where target.race_date between $1 and $2
        and ${categoryFilter}
    )
  `;

const babajotaiCaseExpression = (codeColumn: string): string => {
  const branches = Object.entries(BABAJOTAI_NORMALIZED_VALUES)
    .map(([code, value]) => `when '${code}' then ${value}::numeric`)
    .join(" ");
  return `case ${codeColumn} ${branches} else null end`;
};

const trackConditionExpression = (): string => {
  const turfCase = babajotaiCaseExpression("tc.target_babajotai_shiba");
  const dirtCase = babajotaiCaseExpression("tc.target_babajotai_dirt");
  return `case
    when left(coalesce(tc.target_track_code, ''), 1) = '1' then ${turfCase}
    else ${dirtCase}
  end`;
};

const fieldSizeExpression = (): string =>
  `least(1, greatest(0, coalesce(tc.target_shusso_tosu, 0)::numeric / ${MAX_FIELD_SIZE}))`;

const isGradeRaceExpression = (): string =>
  `case
    when btrim(coalesce(tc.target_grade_code, '')) in ('A', 'B', 'C', 'D', 'G', 'H') then 1
    else 0
  end::smallint`;

const rivalCountExpression = (): string =>
  `greatest(0, rfa.race_strong_count - case when tc.target_same_distance_win_rate > ${RIVAL_DISTANCE_THRESHOLD} then 1 else 0 end)`;

export const buildRaceContextUpdateSql = (category: FeatureCategory): string => {
  const filter = buildCategoryFilterClause(category);
  const updateAssignments = [
    "field_strength_avg_speed = rfa.race_avg_speed",
    "field_strength_top3_speed = rts.race_top_speed",
    `rival_count_at_distance = ${rivalCountExpression()}`,
    `track_condition_normalized = ${trackConditionExpression()}`,
    `field_size_normalized = ${fieldSizeExpression()}`,
    `is_grade_race = ${isGradeRaceExpression()}`,
    "updated_at = now()",
  ];
  return `
    with ${buildRaceHorsesCte(filter.targetFilter)},
    ${buildRaceFieldAggregatesCte()},
    ${buildRaceTop3SpeedCte()},
    ${buildTargetContextCte(filter.targetFilter)}
    update ${TARGET_FEATURE_TABLE} target
    set
      ${updateAssignments.join(",\n      ")}
    from target_context tc
    join race_field_aggregates rfa
      on rfa.source = tc.source
      and rfa.kaisai_nen = tc.kaisai_nen
      and rfa.kaisai_tsukihi = tc.kaisai_tsukihi
      and rfa.keibajo_code = tc.keibajo_code
      and rfa.race_bango = tc.race_bango
    left join race_top3_speed rts
      on rts.source = tc.source
      and rts.kaisai_nen = tc.kaisai_nen
      and rts.kaisai_tsukihi = tc.kaisai_tsukihi
      and rts.keibajo_code = tc.keibajo_code
      and rts.race_bango = tc.race_bango
    where target.source = tc.source
      and target.kaisai_nen = tc.kaisai_nen
      and target.kaisai_tsukihi = tc.kaisai_tsukihi
      and target.keibajo_code = tc.keibajo_code
      and target.race_bango = tc.race_bango
      and target.ketto_toroku_bango = tc.ketto_toroku_bango
  `;
};

export {
  BABAJOTAI_NORMALIZED_VALUES,
  MAX_FIELD_SIZE,
  RIVAL_DISTANCE_THRESHOLD,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
  TOP_SPEED_HORSE_COUNT,
};
