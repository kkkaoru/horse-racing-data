// Run with: bunx vitest run src/scripts/finish-position-features/v7-lineage-model-versions.test.ts
import { expect, test } from "vitest";

import {
  FINISH_POSITION_V7_LINEAGE_ARCHITECTURE,
  FINISH_POSITION_V7_LINEAGE_CATEGORY_METADATA,
  FINISH_POSITION_V7_LINEAGE_FEATURE_COUNT,
  FINISH_POSITION_V7_LINEAGE_FEATURES_PARQUET,
  FINISH_POSITION_V7_LINEAGE_HYPERPARAMS,
  FINISH_POSITION_V7_LINEAGE_MODEL_VERSIONS,
} from "./v7-lineage-model-versions";

test("model versions expose the per-category WF 21y namespaces", () => {
  expect(FINISH_POSITION_V7_LINEAGE_MODEL_VERSIONS).toStrictEqual({
    jra: "jra-cb-v7-lineage-wf-21y",
    nar: "nar-xgb-v7-lineage-wf-21y",
    banei: "banei-cb-v7-lineage-wf-21y",
  });
});

test("features parquet paths match the authoritative Stage 0/1/2 dirs", () => {
  expect(FINISH_POSITION_V7_LINEAGE_FEATURES_PARQUET).toStrictEqual({
    jra: "tmp/feat-jra-v7-final",
    nar: "tmp/feat-nar-v7-baba",
    banei: "tmp/feat-ban-ei-v7-grade",
  });
});

test("feature counts match the deployed-model parity guards", () => {
  expect(FINISH_POSITION_V7_LINEAGE_FEATURE_COUNT).toStrictEqual({
    jra: 226,
    nar: 175,
    banei: 111,
  });
});

test("architecture dispatch maps jra/banei to catboost and nar to xgboost", () => {
  expect(FINISH_POSITION_V7_LINEAGE_ARCHITECTURE).toStrictEqual({
    jra: "catboost",
    nar: "xgboost",
    banei: "catboost",
  });
});

test("jra hyperparams inherit CatBoost YetiRank 500 iterations depth 8", () => {
  expect(FINISH_POSITION_V7_LINEAGE_HYPERPARAMS.jra).toStrictEqual({
    architecture: "catboost",
    iterations: 500,
    depth: 8,
    l2LeafReg: 3.0,
    learningRate: 0.05,
    relevanceRank1: 3,
    relevanceRank2: 2,
    relevanceRank3: 1,
    noCatFeatures: true,
  });
});

test("banei hyperparams inherit CatBoost YetiRank 300 iterations depth 8", () => {
  expect(FINISH_POSITION_V7_LINEAGE_HYPERPARAMS.banei).toStrictEqual({
    architecture: "catboost",
    iterations: 300,
    depth: 8,
    l2LeafReg: 3.0,
    learningRate: 0.05,
    relevanceRank1: 3,
    relevanceRank2: 2,
    relevanceRank3: 1,
    noCatFeatures: true,
  });
});

test("nar hyperparams inherit XGBoost rank:pairwise 450 rounds depth 6 relevance 3/2/2", () => {
  expect(FINISH_POSITION_V7_LINEAGE_HYPERPARAMS.nar).toStrictEqual({
    architecture: "xgboost",
    numRounds: 450,
    maxDepth: 6,
    relevanceRank1: 3,
    relevanceRank2: 2,
    relevanceRank3: 2,
    noCatFeatures: false,
  });
});

test("jra category metadata bundles version architecture parquet count and hyperparams", () => {
  expect(FINISH_POSITION_V7_LINEAGE_CATEGORY_METADATA.jra).toStrictEqual({
    modelVersion: "jra-cb-v7-lineage-wf-21y",
    architecture: "catboost",
    featuresParquet: "tmp/feat-jra-v7-final",
    featureCount: 226,
    hyperparams: {
      architecture: "catboost",
      iterations: 500,
      depth: 8,
      l2LeafReg: 3.0,
      learningRate: 0.05,
      relevanceRank1: 3,
      relevanceRank2: 2,
      relevanceRank3: 1,
      noCatFeatures: true,
    },
  });
});

test("nar category metadata bundles xgboost version parquet count and hyperparams", () => {
  expect(FINISH_POSITION_V7_LINEAGE_CATEGORY_METADATA.nar).toStrictEqual({
    modelVersion: "nar-xgb-v7-lineage-wf-21y",
    architecture: "xgboost",
    featuresParquet: "tmp/feat-nar-v7-baba",
    featureCount: 175,
    hyperparams: {
      architecture: "xgboost",
      numRounds: 450,
      maxDepth: 6,
      relevanceRank1: 3,
      relevanceRank2: 2,
      relevanceRank3: 2,
      noCatFeatures: false,
    },
  });
});

test("banei category metadata bundles catboost 300-iteration version and parquet", () => {
  expect(FINISH_POSITION_V7_LINEAGE_CATEGORY_METADATA.banei).toStrictEqual({
    modelVersion: "banei-cb-v7-lineage-wf-21y",
    architecture: "catboost",
    featuresParquet: "tmp/feat-ban-ei-v7-grade",
    featureCount: 111,
    hyperparams: {
      architecture: "catboost",
      iterations: 300,
      depth: 8,
      l2LeafReg: 3.0,
      learningRate: 0.05,
      relevanceRank1: 3,
      relevanceRank2: 2,
      relevanceRank3: 1,
      noCatFeatures: true,
    },
  });
});
