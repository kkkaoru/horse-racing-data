// Run with bun. LambdaRank single-score evaluator over the compact LightGBM
// JSON produced by export_lightgbm_to_json.py. Reuses walkTree from
// running-style-lightgbm-tree (the tree structure is identical) but treats
// the ensemble as a regression-style sum-of-leaves rather than a softmax,
// because finish-position LGBM has 1 tree per iteration.

import {
  buildFeatureVector,
  walkTree,
  type CompactLightGBMModel,
  type FeatureVector,
} from "./running-style-lightgbm-tree";

export type { CompactLightGBMModel, FeatureVector };
export { buildFeatureVector };

export const computeFinishPositionScore = (
  model: CompactLightGBMModel,
  vector: FeatureVector,
): number =>
  model.trees.reduce((acc, tree) => acc + walkTree(tree.tree_structure, vector), 0);

export interface RaceHorseScore {
  kettoTorokuBango: string;
  umaban: number;
  score: number;
}

export interface RaceHorseRanked extends RaceHorseScore {
  predictedRank: number;
}

const compareScoreDescThenUmaban = (left: RaceHorseScore, right: RaceHorseScore): number =>
  right.score - left.score || left.umaban - right.umaban;

export const assignRanksWithinRace = (
  scores: ReadonlyArray<RaceHorseScore>,
): ReadonlyArray<RaceHorseRanked> => {
  const sorted = [...scores].sort(compareScoreDescThenUmaban);
  return sorted.map((entry, index) => ({ ...entry, predictedRank: index + 1 }));
};
