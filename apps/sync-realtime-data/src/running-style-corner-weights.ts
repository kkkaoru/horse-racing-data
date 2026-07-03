// Run with bun. Single source of truth for the running-style "corner front
// score" weights used by the display/persistence inference path
// (running-style-inference.ts). The score feeds predicted_corner_rank, the
// race-internal ordering shown to users. The per-category JRA/NAR weights were
// learned and validated on the serve-path prod-v3 probabilities: NAR 1106
// races corner3+4 +0.096pp (LB95 > 0) and JRA 240 races +0.352pp (LB95 > 0),
// across blind-2025 plus the R2 serve sample from June 2026. ban-ei and
// unknown categories intentionally keep the historical fixed (0, 1, 2, 3)
// formula. NOTE: the finish-position feature pipeline (add-pacestyle-features.py)
// deliberately keeps the fixed (0, 1, 2, 3) formula because the FP models were
// trained on it; do not change that pipeline to match these weights.

export interface CornerFrontScoreWeights {
  nige: number;
  senkou: number;
  sashi: number;
  oikomi: number;
}

export interface CornerFrontScoreProbabilities {
  nige: number;
  senkou: number;
  sashi: number;
  oikomi: number;
}

export const FIXED_CORNER_FRONT_SCORE_WEIGHTS: CornerFrontScoreWeights = {
  nige: 0,
  oikomi: 3.0,
  sashi: 2,
  senkou: 1,
};

export const JRA_CORNER_FRONT_SCORE_WEIGHTS: CornerFrontScoreWeights = {
  nige: 0,
  oikomi: 3.0,
  sashi: 1.5,
  senkou: 0.49,
};

export const NAR_CORNER_FRONT_SCORE_WEIGHTS: CornerFrontScoreWeights = {
  nige: 0,
  oikomi: 3.0,
  sashi: 1.39,
  senkou: 0.68,
};

const CATEGORY_WEIGHTS: Map<string, CornerFrontScoreWeights> = new Map([
  ["jra", JRA_CORNER_FRONT_SCORE_WEIGHTS],
  ["nar", NAR_CORNER_FRONT_SCORE_WEIGHTS],
]);

// `category` is typed `string` (not a literal union) because RaceHorseFeatureRow
// carries it as `string`; "ban-ei" and any unrecognised category fall through
// the Map lookup to the fixed (0, 1, 2, 3) weights.
export const resolveCornerFrontScoreWeights = (category: string): CornerFrontScoreWeights =>
  CATEGORY_WEIGHTS.get(category) ?? FIXED_CORNER_FRONT_SCORE_WEIGHTS;

export const computeCornerFrontScore = (
  probabilities: CornerFrontScoreProbabilities,
  weights: CornerFrontScoreWeights,
): number =>
  weights.nige * probabilities.nige +
  weights.senkou * probabilities.senkou +
  weights.sashi * probabilities.sashi +
  weights.oikomi * probabilities.oikomi;
