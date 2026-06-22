// Run with bun. E-top2 place-preserving XGBoost override for JRA, a faithful TS
// port of apps/finish-position-predict-container/src/predict_lib/etop2_override.py.
//
// Given CatBoost ranking CB#1, CB#2, CB#3, ... and XGBoost's rank-1 horse, the
// override fires when XGB#1 == CB#2 AND the race class is not 701 (新馬):
//   rank-1 = CB#2 (= XGB#1)   <- promoted from CB rank-2 (score max(cb)+1.0)
//   rank-2 = CB#1             <- demoted from CB rank-1 (score max(cb)+0.5)
//   rank-3 = CB#3             <- UNCHANGED (place3 preserved by construction)
// All other cases keep pure CatBoost scores:
//   XGB#1 == CB#1 (already agree), XGB#1 ∈ CB#3+ (preserves place3),
//   class == 701 (XGB winner less reliable in maiden races), n < 2.
//
// The returned values are scores for ranking only (higher = better); absolute
// magnitudes are not meaningful — they are injected so the downstream ranker
// (rank-by-descending-score, ties on ketto) reproduces the production order.

const ETOP2_EXCLUDED_CLASS = "701";
const MIN_HORSES_FOR_OVERRIDE = 2;
const CB_RANK1_SLOT = 0;
const CB_RANK2_SLOT = 1;
const PROMOTED_SCORE_OFFSET = 1;
const DEMOTED_SCORE_OFFSET = 0.5;

const indexByDescendingScore = (scores: ReadonlyArray<number>): number[] =>
  scores.map((_score, index) => index).sort((left, right) => scores[right]! - scores[left]!);

const argmax = (scores: ReadonlyArray<number>): number =>
  scores.reduce((best, score, index) => (score > scores[best]! ? index : best), CB_RANK1_SLOT);

export interface ApplyEtop2Input {
  cbScores: ReadonlyArray<number>;
  xgbScores: ReadonlyArray<number>;
  raceClass: string | null;
}

// Returns scores in the same entry order as the inputs. Equal to cbScores when
// the override is not eligible; otherwise the CB#1 / CB#2 pair is swapped via
// score injection. Mirrors apply_etop2_scores.
export const applyEtop2Scores = (input: ApplyEtop2Input): number[] => {
  const cbScores = input.cbScores;
  if (cbScores.length < MIN_HORSES_FOR_OVERRIDE) return [...cbScores];
  if (input.raceClass === ETOP2_EXCLUDED_CLASS) return [...cbScores];

  const sortedByCb = indexByDescendingScore(cbScores);
  const cbRank1Index = sortedByCb[CB_RANK1_SLOT]!;
  const cbRank2Index = sortedByCb[CB_RANK2_SLOT]!;
  const xgbRank1Index = argmax(input.xgbScores);

  if (xgbRank1Index !== cbRank2Index) return [...cbScores];

  const cbMax = Math.max(...cbScores);
  const result = [...cbScores];
  result[cbRank2Index] = cbMax + PROMOTED_SCORE_OFFSET;
  result[cbRank1Index] = cbMax + DEMOTED_SCORE_OFFSET;
  return result;
};

// Returns true when the override would change the rank-1 horse. Mirrors
// is_etop2_override_active — used for smoke logging that the override fires.
export const isEtop2OverrideActive = (input: ApplyEtop2Input): boolean => {
  if (input.cbScores.length < MIN_HORSES_FOR_OVERRIDE) return false;
  if (input.raceClass === ETOP2_EXCLUDED_CLASS) return false;
  const sortedByCb = indexByDescendingScore(input.cbScores);
  const cbRank2Index = sortedByCb[CB_RANK2_SLOT]!;
  return argmax(input.xgbScores) === cbRank2Index;
};
