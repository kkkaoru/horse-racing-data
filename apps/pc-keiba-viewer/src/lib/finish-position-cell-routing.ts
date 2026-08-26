// Run with bun (bunx vitest)
//
// TypeScript port of the finish-position-predict-container's cell-level model
// routing (apps/finish-position-predict-container/src/predict_lib/cell_router.py
// + cell_routing.json), plus the NAR Set-Transformer blend override (see
// NAR_TRANSFORMER_BLEND_MODEL_VERSION below), so the viewer's display-priority
// query can tell when a race was scored by a non-default model and must
// surface that prediction instead of a later/duplicate write of the plain
// category default. See docs/finish-position-prediction-system.md for the
// incidents this guards against (2026-07-11: a JRA cell-routed Hakodate
// prediction, and separately the NAR transformer blend, were both shadowed by
// fallback writes of the plain champion model).
//
// FINISH_POSITION_CELL_ROUTING_CONFIG below is a hand-kept mirror of the
// container's cell_routing.json contract, not a runtime import of it (the
// container package is a separate deploy target with its own bundling, and
// this viewer's tsconfig does not enable resolveJsonModule). Its
// finish-position-cell-routing.test.ts parity test reads the real container
// file at test time and fails the build if the two diverge, so this mirror
// can never silently drift the way the 2026-07-03 hardcoded single-rule
// mechanism did before it was deleted wholesale on 2026-07-07.
import type { RaceDetail } from "./race-types";

// Mirrors predict_lib.model_meta.NAR_TRANSFORMER_MODEL_VERSION exactly. Unlike
// the JRA cell-routing variants above, this is not a cell_routing.json rule:
// the container opportunistically blends every NAR race with the Set
// Transformer whenever NAR_TRANSFORMER_BLEND_ENABLED is on and the race has no
// missing transformer features, writing this model_version only for races
// where the blend actually ran (races that fall back keep the plain category
// default, iter12-nar-xgb-hpo-v8-clean188). So this is always attempted as a
// display-priority candidate for every NAR race; the priority-0 SQL branch's
// row-existence check naturally does nothing when no blended row was written,
// falling through correctly to the plain champion tier below.
export const NAR_TRANSFORMER_BLEND_MODEL_VERSION = "iter40-nar-settransformer-blend-v1";

export interface CellRoutingCondition {
  dimension: string;
  values: string[];
}

export interface CellRoutingRule {
  conditions: CellRoutingCondition[];
  variant: string;
  effective_after?: string;
}

export interface CellRoutingVariant {
  model_version: string;
  feature_count: number;
  architecture: string;
  feature_set_hash?: string;
  routing_mode?: string;
  base_variant?: string;
  maximum_candidate_v2_rank?: number;
  minimum_candidate_margin?: number;
  minimum_candidate_top_z?: number;
  consensus_variants?: string[];
  consensus_required_votes?: number;
}

export interface CellRoutingCategoryConfig {
  default_variant: string;
  variants: Record<string, CellRoutingVariant>;
  rules: CellRoutingRule[];
}

export type CellRoutingConfig = Record<string, CellRoutingCategoryConfig>;

interface ResolveDimensionParams {
  race: RaceDetail;
  dimension: string;
  category: string;
  cardMaxRaceBango?: number | null;
}

interface AllConditionsMatchParams {
  race: RaceDetail;
  conditions: CellRoutingCondition[];
  category: string;
  cardMaxRaceBango?: number | null;
}

export interface ResolveCellRoutingParams {
  race: RaceDetail;
  category: string;
  cardMaxRaceBango?: number | null;
}

const SPRINT_MAX_KYORI = 1200;
const MILE_MAX_KYORI = 1600;
const INTERMEDIATE_MAX_KYORI = 2000;
const LONG_MAX_KYORI = 2400;
const SMALL_FIELD_MAX_SHUSSO_TOSU = 10;
const MEDIUM_FIELD_MAX_SHUSSO_TOSU = 13;
const LARGE_FIELD_MAX_SHUSSO_TOSU = 15;
const CANONICAL_SPRINT_MAX_KYORI = 1400;
const CANONICAL_MILE_MAX_KYORI = 1800;
const CANONICAL_INTERMEDIATE_MAX_KYORI = 2200;
const CANONICAL_LONG_MAX_KYORI = 2800;
const CANONICAL_SMALL_FIELD_MAX = 8;
const CANONICAL_MEDIUM_FIELD_MAX = 14;
const MONTH_PREFIX_LENGTH = 2;
const MONTH_PATTERN = /^\d{2}$/u;
const SPRING_MONTHS = new Set([3, 4, 5]);
const SUMMER_MONTHS = new Set([6, 7, 8]);
const AUTUMN_MONTHS = new Set([9, 10, 11]);

// Mirrors cell_routing.json exactly (jra / ban-ei rules currently deployed).
// See the module comment above for why this is a hand-kept copy rather than
// a runtime import, and finish-position-cell-routing.test.ts for the parity
// guard that keeps it honest.
const FINISH_POSITION_CELL_ROUTING_CONFIG: CellRoutingConfig = {
  jra: {
    default_variant: "sim",
    variants: {
      sim: {
        model_version: "jra-cb-v9-sim-2013-clean",
        feature_count: 250,
        architecture: "catboost",
      },
      jockey_pedigree_703: {
        model_version: "jra-cb-v9-sim-2013-clean-jockey-pedigree269",
        feature_count: 269,
        architecture: "catboost",
        feature_set_hash: "1f70d678d48b485d4fcf593de786880c8fcf748e464174279f1dfe1251c9ef07",
      },
      prior_corner_dirt_smallfield_005: {
        model_version: "jra-cb-v10-prior-corner274-2013",
        feature_count: 274,
        architecture: "catboost",
        feature_set_hash: "0b90ab1c7e19ef8d61c2b5419bd034bf277600c73b3f4a05e3b1ff1d99bbbb22",
      },
      joken_005: {
        model_version: "jra-joken-005-pooled-yetirank-v2",
        feature_count: 113,
        architecture: "catboost",
      },
      joken_005_dirt_mile_autumn_yeti_gated_top1: {
        model_version: "jra-joken-005-dirt-mile-autumn-yeti-gated-v1",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_005",
        minimum_candidate_margin: 0.05,
        minimum_candidate_top_z: 1.5,
        maximum_candidate_v2_rank: 20,
      },
      joken_005_turf_mile_yeti_gated_top1: {
        model_version: "jra-joken-005-turf-mile-yeti-gated-v1",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_005",
        minimum_candidate_margin: 0.1,
        minimum_candidate_top_z: 1.25,
      },
      joken_005_turf_long_yeti_gated_top1: {
        model_version: "jra-joken-005-turf-long-hierarchical-qsm-gated-v2",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_005",
        minimum_candidate_margin: 0.3,
        minimum_candidate_top_z: 1.5,
        maximum_candidate_v2_rank: 20,
      },
      joken_010: {
        model_version: "jra-joken-010-pooled-yetirank-v2",
        feature_count: 113,
        architecture: "catboost",
      },
      joken_016: {
        model_version: "jra-joken-016-pooled-yetirank-v2",
        feature_count: 113,
        architecture: "catboost",
      },
      joken_701: {
        model_version: "jra-joken-701-pooled-yetirank-v2",
        feature_count: 113,
        architecture: "catboost",
      },
      joken_010_dirt_intermediate_yeti_gated_top1: {
        model_version: "jra-joken-010-dirt-intermediate-yeti-gated-v1",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_010",
        minimum_candidate_margin: 0.2,
        minimum_candidate_top_z: 1.5,
        maximum_candidate_v2_rank: 20,
      },
      joken_701_turf_mile_qsm_gated_top1: {
        model_version: "jra-joken-701-turf-mile-qsm-gated-v1",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_701",
        minimum_candidate_margin: 0.5,
        minimum_candidate_top_z: 1.5,
        maximum_candidate_v2_rank: 20,
      },
      joken_701_turf_intermediate_qsm_gated_top1: {
        model_version: "jra-joken-701-turf-intermediate-qsm-gated-v1",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_701",
        minimum_candidate_margin: 0.5,
        minimum_candidate_top_z: 1.5,
        maximum_candidate_v2_rank: 20,
      },
      joken_701_turf_long_qsm_gated_top1: {
        model_version: "jra-joken-701-turf-long-qsm-gated-v1",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_701",
        minimum_candidate_margin: 0.5,
        minimum_candidate_top_z: 1.5,
      },
      joken_703: {
        model_version: "jra-joken-703-pooled-yetirank-v2",
        feature_count: 113,
        architecture: "catboost",
      },
      joken_703_turf_long_spring_qsm_gated_top1: {
        model_version: "jra-joken-703-turf-long-spring-qsm-gated-v1",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_703",
        minimum_candidate_margin: 0.1,
        minimum_candidate_top_z: 1.5,
        maximum_candidate_v2_rank: 2,
      },
      joken_703_turf_intermediate_qsm_gated_top1: {
        model_version: "jra-joken-703-turf-intermediate-qsm-gated-v1",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_703",
        minimum_candidate_margin: 0.05,
        minimum_candidate_top_z: 1.25,
        maximum_candidate_v2_rank: 2,
      },
      joken_703_other_extended_qsm_gated_top1: {
        model_version: "jra-joken-703-other-extended-qsm-gated-v1",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_703",
        minimum_candidate_margin: 0.15,
        minimum_candidate_top_z: 1.25,
        maximum_candidate_v2_rank: 2,
      },
      joken_703_dirt_sprint_yeti_gated_top1: {
        model_version: "jra-joken-703-dirt-sprint-yeti-gated-v1",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_703",
        minimum_candidate_margin: 0.02,
        minimum_candidate_top_z: 1.5,
        maximum_candidate_v2_rank: 20,
      },
      joken_703_dirt_intermediate_qsm_gated_top1: {
        model_version: "jra-joken-703-dirt-intermediate-qsm-gated-v1",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_703",
        minimum_candidate_margin: 0.01,
        minimum_candidate_top_z: 1.5,
        maximum_candidate_v2_rank: 2,
      },
      joken_703_dirt_mile_summer_qsm_top1: {
        model_version: "jra-joken-703-querysoftmax-maxrange-v1",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_703",
      },
      joken_999: {
        model_version: "jra-joken-999-pooled-yetirank-v2",
        feature_count: 113,
        architecture: "catboost",
      },
      joken_005_turf_intermediate_spring_qsm_gated_top1: {
        model_version: "jra-joken-005-turf-intermediate-spring-qsm-gated-v1",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_005",
        minimum_candidate_margin: 0.0,
        minimum_candidate_top_z: 1.0,
        maximum_candidate_v2_rank: 20,
      },
      joken_005_dirt_mile_spring_qsm_gated_top1: {
        model_version: "jra-joken-005-dirt-mile-spring-qsm-gated-v1",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_005",
        minimum_candidate_margin: 0.02,
        minimum_candidate_top_z: 1.25,
        maximum_candidate_v2_rank: 20,
      },
      joken_005_dirt_intermediate_autumn_yeti_gated_top1: {
        model_version: "jra-joken-005-dirt-intermediate-autumn-yeti-gated-v1",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_005",
        minimum_candidate_margin: 0.2,
        minimum_candidate_top_z: 1.25,
        maximum_candidate_v2_rank: 20,
      },
      joken_703_turf_long_summer_yeti_gated_top1: {
        model_version: "jra-joken-703-turf-long-summer-yeti-gated-v1",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_703",
        minimum_candidate_margin: 0.2,
        minimum_candidate_top_z: 1.5,
        maximum_candidate_v2_rank: 20,
      },
      joken_005_dirt_1700_summer_qsm_gated_top1: {
        model_version: "jra-joken-005-dirt-1700-summer-qsm-gated-v1",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_005",
        minimum_candidate_margin: 0.2,
        minimum_candidate_top_z: 1.25,
        maximum_candidate_v2_rank: 20,
      },
      joken_005_dirt_1200_winter_summer_qsm_gated_top1: {
        model_version: "jra-joken-005-dirt-1200-winter-summer-qsm-gated-v1",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_005",
        minimum_candidate_margin: 0.15,
        minimum_candidate_top_z: 1.25,
        maximum_candidate_v2_rank: 20,
      },
      joken_703_turf_1400_qsm_gated_top1: {
        model_version: "jra-joken-703-turf-1400-qsm-gated-v1",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_703",
        minimum_candidate_margin: 0.1,
        minimum_candidate_top_z: 1.5,
        maximum_candidate_v2_rank: 20,
      },
      joken_005_dirt_1800_nonautumn_qsm_gated_top1: {
        model_version: "jra-joken-005-dirt-1800-nonautumn-qsm-gated-v1",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_005",
        minimum_candidate_margin: 0.2,
        minimum_candidate_top_z: 1.25,
        maximum_candidate_v2_rank: 2,
      },
      joken_703_turf_1200_largefield_yeti_gated_top1: {
        model_version: "jra-joken-703-turf-1200-largefield-yeti-gated-v1",
        feature_count: 113,
        architecture: "catboost",
        routing_mode: "jra_variant_top1_swap",
        base_variant: "joken_703",
        minimum_candidate_margin: 0.15,
        minimum_candidate_top_z: 1.5,
        maximum_candidate_v2_rank: 20,
      },
    },
    rules: [
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-005"],
          },
          {
            dimension: "surface",
            values: ["dirt"],
          },
          {
            dimension: "distance_band",
            values: ["mile"],
          },
          {
            dimension: "season",
            values: ["autumn"],
          },
        ],
        variant: "joken_005_dirt_mile_autumn_yeti_gated_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-005"],
          },
          {
            dimension: "surface",
            values: ["turf"],
          },
          {
            dimension: "distance_band",
            values: ["mile"],
          },
        ],
        variant: "joken_005_turf_mile_yeti_gated_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-005"],
          },
          {
            dimension: "surface",
            values: ["turf"],
          },
          {
            dimension: "distance_band",
            values: ["long"],
          },
        ],
        variant: "joken_005_turf_long_yeti_gated_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-010"],
          },
          {
            dimension: "surface",
            values: ["dirt"],
          },
          {
            dimension: "distance_band",
            values: ["intermediate"],
          },
        ],
        variant: "joken_010_dirt_intermediate_yeti_gated_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-701"],
          },
          {
            dimension: "surface",
            values: ["turf"],
          },
          {
            dimension: "distance_band",
            values: ["long"],
          },
        ],
        variant: "joken_701_turf_long_qsm_gated_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-701"],
          },
          {
            dimension: "surface",
            values: ["turf"],
          },
          {
            dimension: "distance_band",
            values: ["mile"],
          },
        ],
        variant: "joken_701_turf_mile_qsm_gated_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-701"],
          },
          {
            dimension: "surface",
            values: ["turf"],
          },
          {
            dimension: "distance_band",
            values: ["intermediate"],
          },
        ],
        variant: "joken_701_turf_intermediate_qsm_gated_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-703"],
          },
          {
            dimension: "surface",
            values: ["turf"],
          },
          {
            dimension: "distance_band",
            values: ["long"],
          },
          {
            dimension: "season",
            values: ["spring"],
          },
        ],
        variant: "joken_703_turf_long_spring_qsm_gated_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-703"],
          },
          {
            dimension: "surface",
            values: ["turf"],
          },
          {
            dimension: "distance_band",
            values: ["intermediate"],
          },
        ],
        variant: "joken_703_turf_intermediate_qsm_gated_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-703"],
          },
          {
            dimension: "surface",
            values: ["other"],
          },
          {
            dimension: "distance_band",
            values: ["extended"],
          },
        ],
        variant: "joken_703_other_extended_qsm_gated_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-703"],
          },
          {
            dimension: "surface",
            values: ["dirt"],
          },
          {
            dimension: "distance_band",
            values: ["sprint"],
          },
        ],
        variant: "joken_703_dirt_sprint_yeti_gated_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-703"],
          },
          {
            dimension: "surface",
            values: ["dirt"],
          },
          {
            dimension: "distance_band",
            values: ["intermediate"],
          },
        ],
        variant: "joken_703_dirt_intermediate_qsm_gated_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-703"],
          },
          {
            dimension: "surface",
            values: ["dirt"],
          },
          {
            dimension: "distance_band",
            values: ["mile"],
          },
          {
            dimension: "season",
            values: ["summer"],
          },
        ],
        variant: "joken_703_dirt_mile_summer_qsm_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-005"],
          },
          {
            dimension: "surface",
            values: ["turf"],
          },
          {
            dimension: "distance_band",
            values: ["intermediate"],
          },
          {
            dimension: "season",
            values: ["spring"],
          },
        ],
        variant: "joken_005_turf_intermediate_spring_qsm_gated_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-005"],
          },
          {
            dimension: "surface",
            values: ["dirt"],
          },
          {
            dimension: "distance_band",
            values: ["mile"],
          },
          {
            dimension: "season",
            values: ["spring"],
          },
        ],
        variant: "joken_005_dirt_mile_spring_qsm_gated_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-005"],
          },
          {
            dimension: "surface",
            values: ["dirt"],
          },
          {
            dimension: "distance_band",
            values: ["intermediate"],
          },
          {
            dimension: "season",
            values: ["autumn"],
          },
        ],
        variant: "joken_005_dirt_intermediate_autumn_yeti_gated_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-703"],
          },
          {
            dimension: "surface",
            values: ["turf"],
          },
          {
            dimension: "distance_band",
            values: ["long"],
          },
          {
            dimension: "season",
            values: ["summer"],
          },
        ],
        variant: "joken_703_turf_long_summer_yeti_gated_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-005"],
          },
          {
            dimension: "surface",
            values: ["dirt"],
          },
          {
            dimension: "kyori",
            values: ["1700"],
          },
          {
            dimension: "season",
            values: ["summer"],
          },
        ],
        variant: "joken_005_dirt_1700_summer_qsm_gated_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-005"],
          },
          {
            dimension: "surface",
            values: ["dirt"],
          },
          {
            dimension: "kyori",
            values: ["1200"],
          },
          {
            dimension: "season",
            values: ["winter", "summer"],
          },
        ],
        variant: "joken_005_dirt_1200_winter_summer_qsm_gated_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-703"],
          },
          {
            dimension: "surface",
            values: ["turf"],
          },
          {
            dimension: "kyori",
            values: ["1400"],
          },
        ],
        variant: "joken_703_turf_1400_qsm_gated_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-005"],
          },
          {
            dimension: "surface",
            values: ["dirt"],
          },
          {
            dimension: "kyori",
            values: ["1800"],
          },
          {
            dimension: "season",
            values: ["winter", "spring", "summer"],
          },
        ],
        variant: "joken_005_dirt_1800_nonautumn_qsm_gated_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-703"],
          },
          {
            dimension: "surface",
            values: ["turf"],
          },
          {
            dimension: "kyori",
            values: ["1200"],
          },
          {
            dimension: "field_band",
            values: ["f14_15", "f16p"],
          },
        ],
        variant: "joken_703_turf_1200_largefield_yeti_gated_top1",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-005"],
          },
        ],
        variant: "joken_005",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-010"],
          },
        ],
        variant: "joken_010",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-016"],
          },
        ],
        variant: "joken_016",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-701"],
          },
        ],
        variant: "joken_701",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-703"],
          },
        ],
        variant: "joken_703",
      },
      {
        conditions: [
          {
            dimension: "class",
            values: ["joken-999"],
          },
        ],
        variant: "joken_999",
      },
      {
        conditions: [
          {
            dimension: "kyoso_joken_code",
            values: ["703"],
          },
        ],
        variant: "jockey_pedigree_703",
      },
      {
        conditions: [
          {
            dimension: "surface",
            values: ["dirt"],
          },
          {
            dimension: "field_band",
            values: ["f_le10"],
          },
          {
            dimension: "kyoso_joken_code",
            values: ["005"],
          },
        ],
        variant: "prior_corner_dirt_smallfield_005",
      },
      {
        conditions: [
          {
            dimension: "venue",
            values: ["02"],
          },
        ],
        variant: "jockey_pedigree_703",
      },
    ],
  },
  "ban-ei": {
    default_variant: "sim",
    variants: {
      sim: {
        model_version: "banei-cb-v9-sim-2011",
        feature_count: 130,
        architecture: "catboost",
      },
      base: {
        model_version: "banei-cb-v8-window2011-wf-15y",
        feature_count: 111,
        architecture: "catboost",
      },
    },
    rules: [
      {
        conditions: [
          {
            dimension: "grade_code",
            values: ["E"],
          },
        ],
        variant: "base",
      },
    ],
  },
  nar: {
    default_variant: "sim",
    variants: {
      sim: {
        model_version: "iter12-nar-xgb-hpo-v8-clean188",
        feature_count: 188,
        architecture: "xgboost",
      },
      mukatsu30: {
        model_version: "nar-cell-top1-30-mukatsu-sprint-summer-tc1-v1",
        feature_count: 67,
        architecture: "xgboost",
        routing_mode: "nar_transformer_top1_swap",
      },
      mukatsu30_tc2_top2: {
        model_version: "nar-cell-top2-30-mukatsu-sprint-summer-tc2-v1",
        feature_count: 67,
        architecture: "xgboost",
        routing_mode: "nar_transformer_top2_swap",
        minimum_candidate_margin: 0.2,
      },
      c50_tc2_consensus_market: {
        model_version: "nar-cell-top2-50-c-sprint-summer-tc2-consensus-v1-market-rider",
        feature_count: 10,
        architecture: "xgboost",
      },
      c50_tc2_consensus_pedigree: {
        model_version: "nar-cell-top2-50-c-sprint-summer-tc2-consensus-v1-pedigree-surface",
        feature_count: 15,
        architecture: "xgboost",
      },
      c50_tc2_consensus: {
        model_version: "nar-cell-top2-50-c-sprint-summer-tc2-consensus-v1-physiology-form",
        feature_count: 12,
        architecture: "xgboost",
        routing_mode: "nar_transformer_top2_consensus_swap",
        consensus_variants: ["c50_tc2_consensus_market", "c50_tc2_consensus_pedigree"],
        consensus_required_votes: 2,
      },
      c42_tc1: {
        model_version: "nar-cell-top1-42-c-sprint-summer-tc1-v1",
        feature_count: 67,
        architecture: "xgboost",
        routing_mode: "nar_transformer_top1_swap",
      },
      c30_tc1: {
        model_version: "nar-cell-top1-30-c-sprint-summer-tc1-v1",
        feature_count: 35,
        architecture: "xgboost",
        routing_mode: "nar_transformer_top1_swap",
      },
      c30_tc2_adaptive: {
        model_version: "nar-cell-top1-30-c-sprint-summer-tc2-adaptive-v1",
        feature_count: 67,
        architecture: "xgboost",
        routing_mode: "nar_transformer_top1_swap",
      },
      c50_tc1_rolling: {
        model_version: "nar-cell-top1-50-c-sprint-summer-tc1-rolling-v1",
        feature_count: 16,
        architecture: "xgboost",
        routing_mode: "nar_transformer_top1_swap",
      },
      c43_tc1_rolling: {
        model_version: "nar-cell-top1-43-c-sprint-winter-tc1-rolling-v1",
        feature_count: 16,
        architecture: "xgboost",
        routing_mode: "nar_transformer_top1_swap",
      },
    },
    rules: [
      {
        conditions: [
          {
            dimension: "venue",
            values: ["30"],
          },
          {
            dimension: "nar_subclass",
            values: ["MUKATSU"],
          },
          {
            dimension: "canonical_distance_band",
            values: ["sprint"],
          },
          {
            dimension: "season",
            values: ["summer"],
          },
          {
            dimension: "surface",
            values: ["dirt"],
          },
          {
            dimension: "canonical_field_size_band",
            values: ["medium"],
          },
          {
            dimension: "current_baba_condition",
            values: ["1"],
          },
        ],
        variant: "mukatsu30",
      },
      {
        conditions: [
          {
            dimension: "venue",
            values: ["30"],
          },
          {
            dimension: "nar_subclass",
            values: ["MUKATSU"],
          },
          {
            dimension: "canonical_distance_band",
            values: ["sprint"],
          },
          {
            dimension: "season",
            values: ["summer"],
          },
          {
            dimension: "surface",
            values: ["dirt"],
          },
          {
            dimension: "canonical_field_size_band",
            values: ["medium"],
          },
          {
            dimension: "current_baba_condition",
            values: ["2"],
          },
        ],
        variant: "mukatsu30_tc2_top2",
      },
      {
        conditions: [
          {
            dimension: "venue",
            values: ["50"],
          },
          {
            dimension: "nar_subclass",
            values: ["C"],
          },
          {
            dimension: "canonical_distance_band",
            values: ["sprint"],
          },
          {
            dimension: "season",
            values: ["summer"],
          },
          {
            dimension: "surface",
            values: ["dirt"],
          },
          {
            dimension: "canonical_field_size_band",
            values: ["medium"],
          },
          {
            dimension: "current_baba_condition",
            values: ["2"],
          },
        ],
        variant: "c50_tc2_consensus",
      },
      {
        conditions: [
          {
            dimension: "venue",
            values: ["42"],
          },
          {
            dimension: "nar_subclass",
            values: ["C"],
          },
          {
            dimension: "canonical_distance_band",
            values: ["sprint"],
          },
          {
            dimension: "season",
            values: ["summer"],
          },
          {
            dimension: "surface",
            values: ["dirt"],
          },
          {
            dimension: "canonical_field_size_band",
            values: ["medium"],
          },
          {
            dimension: "current_baba_condition",
            values: ["1"],
          },
        ],
        variant: "c42_tc1",
      },
      {
        conditions: [
          {
            dimension: "venue",
            values: ["30"],
          },
          {
            dimension: "nar_subclass",
            values: ["C"],
          },
          {
            dimension: "canonical_distance_band",
            values: ["sprint"],
          },
          {
            dimension: "season",
            values: ["summer"],
          },
          {
            dimension: "surface",
            values: ["dirt"],
          },
          {
            dimension: "canonical_field_size_band",
            values: ["medium"],
          },
          {
            dimension: "current_baba_condition",
            values: ["1"],
          },
        ],
        variant: "c30_tc1",
      },
      {
        conditions: [
          {
            dimension: "venue",
            values: ["30"],
          },
          {
            dimension: "nar_subclass",
            values: ["C"],
          },
          {
            dimension: "canonical_distance_band",
            values: ["sprint"],
          },
          {
            dimension: "season",
            values: ["summer"],
          },
          {
            dimension: "surface",
            values: ["dirt"],
          },
          {
            dimension: "canonical_field_size_band",
            values: ["medium"],
          },
          {
            dimension: "current_baba_condition",
            values: ["2"],
          },
        ],
        variant: "c30_tc2_adaptive",
        effective_after: "2026-06-30",
      },
      {
        conditions: [
          {
            dimension: "venue",
            values: ["50"],
          },
          {
            dimension: "nar_subclass",
            values: ["C"],
          },
          {
            dimension: "canonical_distance_band",
            values: ["sprint"],
          },
          {
            dimension: "season",
            values: ["summer"],
          },
          {
            dimension: "surface",
            values: ["dirt"],
          },
          {
            dimension: "canonical_field_size_band",
            values: ["medium"],
          },
          {
            dimension: "current_baba_condition",
            values: ["1"],
          },
        ],
        variant: "c50_tc1_rolling",
        effective_after: "2026-08-23",
      },
      {
        conditions: [
          {
            dimension: "venue",
            values: ["43"],
          },
          {
            dimension: "nar_subclass",
            values: ["C"],
          },
          {
            dimension: "canonical_distance_band",
            values: ["sprint"],
          },
          {
            dimension: "season",
            values: ["winter"],
          },
          {
            dimension: "surface",
            values: ["dirt"],
          },
          {
            dimension: "canonical_field_size_band",
            values: ["medium"],
          },
          {
            dimension: "current_baba_condition",
            values: ["1"],
          },
        ],
        variant: "c43_tc1_rolling",
        effective_after: "2026-08-23",
      },
    ],
  },
};

const parseIntOrNull = (value: string | null): number | null => {
  if (value === null) return null;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : null;
};

const trimmedOrNull = (value: string | null): string | null => {
  if (value === null) return null;
  const trimmed = value.trim();
  return trimmed.length === 0 ? null : trimmed;
};

const resolveDistanceBand = (kyori: string | null): string | null => {
  const parsed = parseIntOrNull(kyori);
  if (parsed === null) return null;
  if (parsed < SPRINT_MAX_KYORI) return "sprint";
  if (parsed < MILE_MAX_KYORI) return "mile";
  if (parsed < INTERMEDIATE_MAX_KYORI) return "intermediate";
  if (parsed < LONG_MAX_KYORI) return "long";
  return "extended";
};

const resolveCanonicalDistanceBand = (kyori: string | null): string | null => {
  const parsed = parseIntOrNull(kyori);
  if (parsed === null) return null;
  if (parsed <= CANONICAL_SPRINT_MAX_KYORI) return "sprint";
  if (parsed <= CANONICAL_MILE_MAX_KYORI) return "mile";
  if (parsed <= CANONICAL_INTERMEDIATE_MAX_KYORI) return "intermediate";
  if (parsed <= CANONICAL_LONG_MAX_KYORI) return "long";
  return "extended";
};

const resolveCanonicalFieldSizeBand = (shussoTosu: string | null): string | null => {
  const parsed = parseIntOrNull(shussoTosu);
  if (parsed === null) return null;
  if (parsed <= CANONICAL_SMALL_FIELD_MAX) return "small";
  if (parsed <= CANONICAL_MEDIUM_FIELD_MAX) return "medium";
  return "large";
};

const resolveFieldBand = (shussoTosu: string | null): string | null => {
  const parsed = parseIntOrNull(shussoTosu);
  if (parsed === null) return null;
  if (parsed <= SMALL_FIELD_MAX_SHUSSO_TOSU) return "f_le10";
  if (parsed <= MEDIUM_FIELD_MAX_SHUSSO_TOSU) return "f11_13";
  if (parsed <= LARGE_FIELD_MAX_SHUSSO_TOSU) return "f14_15";
  return "f16p";
};

const resolveSurface = (race: RaceDetail, category: string): string | null => {
  if (category !== "jra") return "dirt";
  const trackCode = trimmedOrNull(race.trackCode);
  if (trackCode === null) return null;
  if (trackCode.startsWith("1")) return "turf";
  if (trackCode.startsWith("2")) return "dirt";
  return "other";
};

const deriveSeasonFromMonth = (month: number): string => {
  if (SPRING_MONTHS.has(month)) return "spring";
  if (SUMMER_MONTHS.has(month)) return "summer";
  if (AUTUMN_MONTHS.has(month)) return "autumn";
  return "winter";
};

const resolveSeason = (kaisaiTsukihi: string): string | null => {
  const monthText = kaisaiTsukihi.trim().slice(0, MONTH_PREFIX_LENGTH);
  return MONTH_PATTERN.test(monthText)
    ? deriveSeasonFromMonth(Number.parseInt(monthText, 10))
    : null;
};

const resolveClass = (race: RaceDetail): string | null => {
  const gradeCode = trimmedOrNull(race.gradeCode);
  if (gradeCode !== null) return gradeCode;
  const conditionCode = trimmedOrNull(race.kyosoJokenCode);
  return conditionCode === null ? "unknown" : `joken-${conditionCode}`;
};

const resolveNarSubclass = (race: RaceDetail): string | null => {
  if (race.source !== "nar" || race.keibajoCode === "83") return null;
  const name = race.kyosoJokenMeisho ?? "";
  if (/ＯＰ/u.test(name)) return "OP";
  if (/新馬/u.test(name)) return "NEW";
  if (/未勝利|未出走/u.test(name)) return "MUKATSU";
  if (/２歳|2歳/u.test(name)) return "2YO";
  if (/３歳|3歳/u.test(name)) return "3YO";
  if (/Ａ/u.test(name)) return "A";
  if (/Ｂ/u.test(name)) return "B";
  if (/Ｃ/u.test(name)) return "C";
  return "other";
};

// A single race can never answer "is this the day's last race" from its own
// columns -- that requires knowing every race_bango registered for the same
// card, which lives outside this race. cardMaxRaceBango is therefore a
// caller-supplied value (mirrors cell_router.py's resolve_dimension --
// card_max_race_bango param, see tmp/kochi-final/cell_design.md for the
// serve-time derivation this is designed for). No value supplied, or a
// non-numeric raceBango, both fail closed to null: the condition simply
// never matches rather than guessing.
const resolveIsFinalRace = (
  raceBango: string,
  cardMaxRaceBango: number | null | undefined,
): string | null => {
  if (cardMaxRaceBango === null || cardMaxRaceBango === undefined) return null;
  const parsed = parseIntOrNull(raceBango);
  if (parsed === null) return null;
  return parsed === cardMaxRaceBango ? "true" : "false";
};

const SPECIAL_DIMENSION_RESOLVERS = new Map<
  string,
  (race: RaceDetail, category: string, cardMaxRaceBango?: number | null) => string | null
>([
  ["venue", (race) => trimmedOrNull(race.keibajoCode)],
  ["surface", (race, category) => resolveSurface(race, category)],
  ["distance_band", (race) => resolveDistanceBand(race.kyori)],
  ["canonical_distance_band", (race) => resolveCanonicalDistanceBand(race.kyori)],
  ["field_band", (race) => resolveFieldBand(race.shussoTosu)],
  ["canonical_field_size_band", (race) => resolveCanonicalFieldSizeBand(race.shussoTosu)],
  ["season", (race) => resolveSeason(race.kaisaiTsukihi)],
  ["class", (race) => resolveClass(race)],
  ["nar_subclass", (race) => resolveNarSubclass(race)],
  [
    "is_final_race",
    (race, _category, cardMaxRaceBango) => resolveIsFinalRace(race.raceBango, cardMaxRaceBango),
  ],
]);

// Raw (non-derived) dimensions: any rule dimension not covered above reads
// this race column directly, mirroring cell_router.py's resolve_dimension
// fallback (`entry.get(dimension)`). A dimension name used by a future
// cell_routing.json rule that has no accessor here resolves to null (the
// condition simply never matches, degrading to "no routing" rather than
// throwing) -- the parity test flags this loudly by failing on divergence
// before it can reach production silently.
const RAW_DIMENSION_ACCESSORS = new Map<string, (race: RaceDetail) => string | null>([
  ["current_baba_condition", (race) => trimmedOrNull(race.babajotaiCodeDirt)],
  ["grade_code", (race) => trimmedOrNull(race.gradeCode)],
  ["kaisai_tsukihi", (race) => race.kaisaiTsukihi.trim()],
  ["keibajo_code", (race) => trimmedOrNull(race.keibajoCode)],
  ["kyori", (race) => trimmedOrNull(race.kyori)],
  ["kyoso_joken_code", (race) => trimmedOrNull(race.kyosoJokenCode)],
  ["shusso_tosu", (race) => trimmedOrNull(race.shussoTosu)],
  ["track_code", (race) => trimmedOrNull(race.trackCode)],
]);

// Exported so finish-position-cell-routing.test.ts can exercise every
// dimension branch directly, independent of which dimensions today's real
// cell_routing.json rules happen to reference (the parity test still keeps
// the config mirror itself honest).
export const resolveDimension = (params: ResolveDimensionParams): string | null => {
  const special = SPECIAL_DIMENSION_RESOLVERS.get(params.dimension);
  if (special) return special(params.race, params.category, params.cardMaxRaceBango);
  const raw = RAW_DIMENSION_ACCESSORS.get(params.dimension);
  return raw ? raw(params.race) : null;
};

const allConditionsMatch = (params: AllConditionsMatchParams): boolean =>
  params.conditions.every((condition) => {
    const value = resolveDimension({
      cardMaxRaceBango: params.cardMaxRaceBango,
      category: params.category,
      dimension: condition.dimension,
      race: params.race,
    });
    return value !== null && condition.values.includes(value);
  });

const isRuleEffective = (race: RaceDetail, rule: CellRoutingRule): boolean => {
  if (rule.effective_after === undefined) return true;
  const raceDate = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
  const threshold = rule.effective_after.replaceAll("-", "");
  return raceDate.length === 8 && threshold.length === 8 && raceDate > threshold;
};

const findMatchingRule = (
  params: ResolveCellRoutingParams,
  rules: CellRoutingRule[],
): CellRoutingRule | undefined =>
  rules.find(
    (rule) =>
      isRuleEffective(params.race, rule) &&
      allConditionsMatch({
        cardMaxRaceBango: params.cardMaxRaceBango,
        category: params.category,
        conditions: rule.conditions,
        race: params.race,
      }),
  );

export interface ResolveCellRoutingForConfigParams extends ResolveCellRoutingParams {
  config: CellRoutingConfig;
}

/**
 * Config-parameterized core of resolveFinishPositionCellRoutingModelVersion,
 * exported so finish-position-cell-routing.test.ts can exercise the
 * missing-variant guard directly: with the real, hand-verified
 * FINISH_POSITION_CELL_ROUTING_CONFIG every rule's `variant` always names a
 * key present in that category's `variants`, so a synthetic config is the
 * only way to legitimately reach that guard (required by
 * noUncheckedIndexedAccess) instead of leaving it as untested dead code.
 */
export const resolveCellRoutingModelVersionForConfig = (
  params: ResolveCellRoutingForConfigParams,
): string | null => {
  const categoryConfig = params.config[params.category];
  if (categoryConfig === undefined) return null;
  const matchedRule = findMatchingRule(params, categoryConfig.rules);
  const variantName = matchedRule?.variant ?? categoryConfig.default_variant;
  if (variantName === categoryConfig.default_variant) return null;
  return categoryConfig.variants[variantName]?.model_version ?? null;
};

/**
 * Return the cell-routing variant's model_version for `race`, or null when
 * the category has no routing config or the race resolves to the default
 * variant (i.e. the plain category-default model already covers it, so no
 * display-priority override is needed).
 */
export const resolveFinishPositionCellRoutingModelVersion = (
  params: ResolveCellRoutingParams,
): string | null =>
  resolveCellRoutingModelVersionForConfig({
    cardMaxRaceBango: params.cardMaxRaceBango,
    category: params.category,
    config: FINISH_POSITION_CELL_ROUTING_CONFIG,
    race: params.race,
  });

/**
 * Return the display-priority-0 candidate model_version for `race`: the NAR
 * transformer blend for category "nar" (always attempted, see
 * NAR_TRANSFORMER_BLEND_MODEL_VERSION above), otherwise the cell-routing
 * variant for jra / ban-ei. This is the single entry point queries.ts should
 * call — it composes both opportunistic-override mechanisms so callers never
 * need to know NAR uses a different one than JRA / ban-ei.
 */
export const resolveFinishPositionDisplayPriorityModelVersion = (
  params: ResolveCellRoutingParams,
): string | null =>
  resolveFinishPositionCellRoutingModelVersion(params) ??
  (params.category === "nar" ? NAR_TRANSFORMER_BLEND_MODEL_VERSION : null);

/**
 * Every distinct model_version referenced by any category's variants,
 * deduped -- each category's own default_variant included (e.g.
 * "jra-cb-v9-sim-2013-clean" / "banei-cb-v9-sim-2011" today, since
 * cell_routing.json lists the default alongside every rule-only variant in
 * the same `variants` map). That inclusiveness is intentional here: this
 * backs a broad ALLOW-list (FINISH_POSITION_LEAK_FREE_MODEL_VERSIONS in
 * queries.ts) where sweeping in the default too is harmless.
 *
 * Do NOT reuse this set as an EXCLUSION list for "off-label variant"
 * purposes -- it is not restricted to rule-only variants, so excluding it
 * wholesale would also exclude each category's plain default/champion model.
 * See getAllFinishPositionCellRoutingOffLabelVariantModelVersions below for
 * that narrower, exclusion-safe view.
 */
export const getAllFinishPositionCellRoutingModelVersions = (): string[] => {
  const versions = Object.values(FINISH_POSITION_CELL_ROUTING_CONFIG).flatMap((categoryConfig) =>
    Object.values(categoryConfig.variants).map((variant) => variant.model_version),
  );
  return Array.from(new Set(versions));
};

/**
 * Every cell-routing variant model_version EXCEPT each category's own
 * default_variant -- the true "off-label" set a race can reach only through
 * an explicit cell_routing.json rule match, mirroring how priority 0 in
 * queries.ts::getFinishPositionLambdarankPredictions resolves them
 * (resolveFinishPositionCellRoutingModelVersion /
 * resolveCellRoutingModelVersionForConfig above return null -- "no
 * override, use the plain default" -- exactly when the matched variant name
 * equals the category's default_variant).
 *
 * Deliberately narrower than getAllFinishPositionCellRoutingModelVersions()
 * above: reusing that broader set as an exclusion list would sweep in each
 * category's plain default (jra-cb-v9-sim-2013-clean / banei-cb-v9-sim-2011
 * today) and wrongly block queries.ts's priority-3 fallback from ever
 * selecting that same default/base model_version again once it stops being
 * the "active" row (priority 2) -- even though
 * FINISH_POSITION_LEAK_FREE_BASE_MODEL_VERSIONS in queries.ts explicitly
 * keeps it allowed for exactly that fallback purpose.
 */
export const getAllFinishPositionCellRoutingOffLabelVariantModelVersions = (): string[] => {
  const versions = Object.values(FINISH_POSITION_CELL_ROUTING_CONFIG).flatMap((categoryConfig) =>
    Object.entries(categoryConfig.variants)
      .filter(([variantName]) => variantName !== categoryConfig.default_variant)
      .map(([, variant]) => variant.model_version),
  );
  return Array.from(new Set(versions));
};

/**
 * Every model_version a priority-0 display override could ever select: every
 * cell-routing variant plus the NAR transformer blend. queries.ts merges this
 * into FINISH_POSITION_LEAK_FREE_MODEL_VERSIONS so a priority-0 selection is
 * never filtered out downstream by allowed_prediction_model_versions.
 */
export const getAllFinishPositionDisplayPriorityModelVersions = (): string[] =>
  Array.from(
    new Set([
      ...getAllFinishPositionCellRoutingModelVersions(),
      NAR_TRANSFORMER_BLEND_MODEL_VERSION,
    ]),
  );

export const FINISH_POSITION_CELL_ROUTING_CONFIG_FOR_TESTS: CellRoutingConfig =
  FINISH_POSITION_CELL_ROUTING_CONFIG;
