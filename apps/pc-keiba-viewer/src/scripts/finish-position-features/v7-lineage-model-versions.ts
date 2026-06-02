// Run with: imported from evaluate-bucket-21y-v7lineage.ts + Stage 4/5/6 (bun runtime)
//
// Single source of truth for the finish-position v7-lineage walk-forward 21y
// pipeline. Stage 3 (score_finish_position_walk_forward.py), Stage 4
// (evaluate-bucket-21y-v7lineage.ts), Stage 5 (deploy), and Stage 6 (activate)
// all read these constants so the per-category model_version, architecture,
// authoritative feature parquet path, feature-count parity guard, and inherited
// hyperparameters stay aligned across the whole pipeline (Review D DRY fix).

export type V7LineageCategory = "jra" | "nar" | "banei";
export type V7LineageArchitecture = "catboost" | "xgboost";

export interface V7LineageCatBoostHyperparams {
  architecture: "catboost";
  iterations: number;
  depth: number;
  l2LeafReg: number;
  learningRate: number;
  relevanceRank1: number;
  relevanceRank2: number;
  relevanceRank3: number;
  noCatFeatures: boolean;
}

export interface V7LineageXgboostHyperparams {
  architecture: "xgboost";
  numRounds: number;
  maxDepth: number;
  relevanceRank1: number;
  relevanceRank2: number;
  relevanceRank3: number;
  noCatFeatures: boolean;
}

export type V7LineageHyperparams = V7LineageCatBoostHyperparams | V7LineageXgboostHyperparams;

export interface V7LineageCategoryMetadata {
  modelVersion: string;
  architecture: V7LineageArchitecture;
  featuresParquet: string;
  featureCount: number;
  hyperparams: V7LineageHyperparams;
}

export const FINISH_POSITION_V7_LINEAGE_MODEL_VERSIONS = {
  jra: "jra-cb-v7-lineage-wf-21y",
  nar: "nar-xgb-v7-lineage-wf-21y",
  banei: "banei-cb-v7-lineage-wf-21y",
} satisfies Record<V7LineageCategory, string>;

export const FINISH_POSITION_V7_LINEAGE_FEATURES_PARQUET = {
  jra: "tmp/feat-jra-v7-final",
  nar: "tmp/feat-nar-v7-baba",
  banei: "tmp/feat-ban-ei-v7-grade",
} satisfies Record<V7LineageCategory, string>;

export const FINISH_POSITION_V7_LINEAGE_FEATURE_COUNT = {
  jra: 226,
  nar: 175,
  banei: 111,
} satisfies Record<V7LineageCategory, number>;

export const FINISH_POSITION_V7_LINEAGE_ARCHITECTURE = {
  jra: "catboost",
  nar: "xgboost",
  banei: "catboost",
} satisfies Record<V7LineageCategory, V7LineageArchitecture>;

const JRA_BANEI_HYPERPARAMS = {
  architecture: "catboost",
  iterations: 500,
  depth: 8,
  l2LeafReg: 3.0,
  learningRate: 0.05,
  relevanceRank1: 3,
  relevanceRank2: 2,
  relevanceRank3: 1,
  noCatFeatures: true,
} satisfies V7LineageCatBoostHyperparams;

const BANEI_HYPERPARAMS = {
  architecture: "catboost",
  iterations: 300,
  depth: 8,
  l2LeafReg: 3.0,
  learningRate: 0.05,
  relevanceRank1: 3,
  relevanceRank2: 2,
  relevanceRank3: 1,
  noCatFeatures: true,
} satisfies V7LineageCatBoostHyperparams;

const NAR_HYPERPARAMS = {
  architecture: "xgboost",
  numRounds: 450,
  maxDepth: 6,
  relevanceRank1: 3,
  relevanceRank2: 2,
  relevanceRank3: 2,
  noCatFeatures: false,
} satisfies V7LineageXgboostHyperparams;

export const FINISH_POSITION_V7_LINEAGE_HYPERPARAMS = {
  jra: JRA_BANEI_HYPERPARAMS,
  nar: NAR_HYPERPARAMS,
  banei: BANEI_HYPERPARAMS,
} satisfies Record<V7LineageCategory, V7LineageHyperparams>;

export const FINISH_POSITION_V7_LINEAGE_CATEGORY_METADATA = {
  jra: {
    modelVersion: FINISH_POSITION_V7_LINEAGE_MODEL_VERSIONS.jra,
    architecture: FINISH_POSITION_V7_LINEAGE_ARCHITECTURE.jra,
    featuresParquet: FINISH_POSITION_V7_LINEAGE_FEATURES_PARQUET.jra,
    featureCount: FINISH_POSITION_V7_LINEAGE_FEATURE_COUNT.jra,
    hyperparams: FINISH_POSITION_V7_LINEAGE_HYPERPARAMS.jra,
  },
  nar: {
    modelVersion: FINISH_POSITION_V7_LINEAGE_MODEL_VERSIONS.nar,
    architecture: FINISH_POSITION_V7_LINEAGE_ARCHITECTURE.nar,
    featuresParquet: FINISH_POSITION_V7_LINEAGE_FEATURES_PARQUET.nar,
    featureCount: FINISH_POSITION_V7_LINEAGE_FEATURE_COUNT.nar,
    hyperparams: FINISH_POSITION_V7_LINEAGE_HYPERPARAMS.nar,
  },
  banei: {
    modelVersion: FINISH_POSITION_V7_LINEAGE_MODEL_VERSIONS.banei,
    architecture: FINISH_POSITION_V7_LINEAGE_ARCHITECTURE.banei,
    featuresParquet: FINISH_POSITION_V7_LINEAGE_FEATURES_PARQUET.banei,
    featureCount: FINISH_POSITION_V7_LINEAGE_FEATURE_COUNT.banei,
    hyperparams: FINISH_POSITION_V7_LINEAGE_HYPERPARAMS.banei,
  },
} satisfies Record<V7LineageCategory, V7LineageCategoryMetadata>;
