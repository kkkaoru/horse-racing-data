// Run with: bun (this module has no standalone CLI; it is imported by
// apply-running-style-postproc.ts). Single source of truth for the running-style
// "corner front score" weights used by the display / persistence path that
// produces predicted_corner_rank (the race-internal ordering of runners).
//
// The JRA / NAR weights below were LEARNED and validated on the serve-path
// prod-v3 running-style probabilities (blind-2025 hold-out + R2 serve data from
// June 2026): NAR 1106 races evaluated on corner3+4 gave +0.096pp with LB95 > 0,
// and JRA 240 races gave +0.352pp with LB95 > 0. ban-ei and unknown sources keep
// the FIXED (0, 1, 2, 3) formula because the learned weights were not validated
// for them.
//
// NOTE: the finish-position feature pipeline (add-pacestyle-features.py)
// deliberately keeps the FIXED (0, 1, 2, 3) corner-front-score formula. The
// finish-position models were trained on that fixed formula, so it MUST NOT be
// changed there. These category-aware weights apply only to the display /
// persistence path (predicted_corner_rank), never to the FP training features.

export interface CornerFrontScoreWeights {
  nige: number;
  senkou: number;
  sashi: number;
  oikomi: number;
}

export interface CornerFrontScoreRaceContext {
  source: string;
  keibajoCode: string;
}

export const FIXED_CORNER_FRONT_SCORE_WEIGHTS: CornerFrontScoreWeights = {
  nige: 0,
  senkou: 1,
  sashi: 2,
  oikomi: 3,
};

export const JRA_CORNER_FRONT_SCORE_WEIGHTS: CornerFrontScoreWeights = {
  nige: 0,
  senkou: 0.49,
  sashi: 1.5,
  oikomi: 3,
};

export const NAR_CORNER_FRONT_SCORE_WEIGHTS: CornerFrontScoreWeights = {
  nige: 0,
  senkou: 0.68,
  sashi: 1.39,
  oikomi: 3,
};

const BANEI_KEIBAJO_CODE = "83";
const JRA_SOURCE = "jra";
const NAR_SOURCE = "nar";

const SOURCE_WEIGHTS: Map<string, CornerFrontScoreWeights> = new Map([
  [JRA_SOURCE, JRA_CORNER_FRONT_SCORE_WEIGHTS],
  [NAR_SOURCE, NAR_CORNER_FRONT_SCORE_WEIGHTS],
]);

export const resolveCornerFrontScoreWeightsForRace = (
  context: CornerFrontScoreRaceContext,
): CornerFrontScoreWeights => {
  if (context.keibajoCode === BANEI_KEIBAJO_CODE) return FIXED_CORNER_FRONT_SCORE_WEIGHTS;
  return SOURCE_WEIGHTS.get(context.source) ?? FIXED_CORNER_FRONT_SCORE_WEIGHTS;
};

export const computeCornerFrontScore = (
  probabilities: readonly number[],
  weights: CornerFrontScoreWeights,
): number =>
  weights.nige * (probabilities[0] ?? 0) +
  weights.senkou * (probabilities[1] ?? 0) +
  weights.sashi * (probabilities[2] ?? 0) +
  weights.oikomi * (probabilities[3] ?? 0);
