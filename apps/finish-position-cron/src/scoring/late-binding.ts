// Run with bun. Recompute the 5 late-binding (target-race-only) feature columns
// from the latest bataiju + odds, matching the Python DuckDB feature builder
// (apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py) bit-for-
// bit. Stage 2 of the per-race rebuild reads the early-binding history columns
// from the R2 feature cache as-is and overwrites ONLY these 5 columns from the
// freshest odds/weight snapshot, so a re-score never re-runs the 21y Neon scan.
//
// Column provenance (builder source lines):
//   tansho_odds          rec COALESCE(rt.tansho_odds_realtime, se.tansho_odds/10)  L490-493
//   tansho_ninkijun      rec COALESCE(rt.ninkijun_realtime, se.tansho_ninkijun)    L486-489
//   odds_score           legacy_five_cte: clamp(ln(max(odds,1))/ln(300), 0, 1)     L1481-1486
//   popularity_score     legacy_five_cte: clamp((ninkijun-1)/(runner_count-1),0,1) L1475-1480
//   weight_diff_from_avg weight_cte: current_bataiju - weight_avg_5                L1589
//
// odds_score / popularity_score fall back to the category training median when
// the odds inputs are absent (UPCOMING races before odds publish). tansho_odds /
// tansho_ninkijun are NOT model features themselves (they do not appear in the
// 244-feature list) — they are the raw inputs odds_score / popularity_score are
// derived from — but they are recomputed here so the cached row carries the
// fresh raw odds too.

const ODDS_LOG_DENOMINATOR_INPUT = 300;
const ODDS_LOG_NUMERATOR_FLOOR = 1;
const CLAMP_MIN = 0;
const CLAMP_MAX = 1;
const POPULARITY_RUNNER_FLOOR = 1;

// Empirical training medians, per category, mirroring the builder constants
// (POPULARITY_SCORE_MEDIAN_* / ODDS_SCORE_MEDIAN_*). Ban-ei shares the NAR
// medians (both are NAR-feed races with similar odds distributions).
const POPULARITY_SCORE_MEDIAN_JRA = 0.5;
const POPULARITY_SCORE_MEDIAN_NAR = 0.5;
const ODDS_SCORE_MEDIAN_JRA = 0.5664;
const ODDS_SCORE_MEDIAN_NAR = 0.5048;

const ODDS_LOG_DENOMINATOR = Math.log(ODDS_LOG_DENOMINATOR_INPUT);

export type LateBindingCategory = "jra" | "nar" | "ban-ei";

export interface OddsSnapshot {
  // Latest single-win odds (already in absolute units, e.g. 3.5). null when not
  // yet published. The realtime fetcher already divides the raw se.tansho_odds
  // by 10, so callers pass the absolute odds here.
  tanshoOdds: number | null;
  // Latest single-win popularity rank (1-based). null when not yet published.
  tanshoNinkijun: number | null;
  // Runner count for the race (shusso_tosu) — denominator of popularity_score.
  runnerCount: number | null;
}

export interface WeightSnapshot {
  // Latest declared bataiju (kg) for the horse, T-30..50min before post. null
  // before the weight board is published.
  currentBataiju: number | null;
  // 5-race average bataiju from history — read from the R2 cache (early-binding).
  weightAvg5: number | null;
}

export interface LateBindingColumns {
  tanshoOdds: number | null;
  tanshoNinkijun: number | null;
  oddsScore: number;
  popularityScore: number;
  weightDiffFromAvg: number | null;
}

const POPULARITY_MEDIAN_BY_CATEGORY: Record<LateBindingCategory, number> = {
  "ban-ei": POPULARITY_SCORE_MEDIAN_NAR,
  jra: POPULARITY_SCORE_MEDIAN_JRA,
  nar: POPULARITY_SCORE_MEDIAN_NAR,
};

const ODDS_MEDIAN_BY_CATEGORY: Record<LateBindingCategory, number> = {
  "ban-ei": ODDS_SCORE_MEDIAN_NAR,
  jra: ODDS_SCORE_MEDIAN_JRA,
  nar: ODDS_SCORE_MEDIAN_NAR,
};

const clamp = (value: number): number => Math.min(CLAMP_MAX, Math.max(CLAMP_MIN, value));

// odds_score = clamp(ln(max(odds, 1)) / ln(300), 0, 1) when odds is present and
// strictly positive; otherwise the category median. Mirrors legacy_five_cte
// L1481-1486 (greatest(odds, 1) before ln, COALESCE to median).
const computeOddsScore = (odds: number | null, category: LateBindingCategory): number => {
  if (odds === null || odds <= 0) return ODDS_MEDIAN_BY_CATEGORY[category];
  const numerator = Math.log(Math.max(odds, ODDS_LOG_NUMERATOR_FLOOR));
  return clamp(numerator / ODDS_LOG_DENOMINATOR);
};

// popularity_score = clamp((ninkijun - 1) / (runner_count - 1), 0, 1) when
// runner_count > 1 and ninkijun present; otherwise the category median.
// Mirrors legacy_five_cte L1475-1480.
const computePopularityScore = (
  ninkijun: number | null,
  runnerCount: number | null,
  category: LateBindingCategory,
): number => {
  if (runnerCount === null || runnerCount <= POPULARITY_RUNNER_FLOOR || ninkijun === null) {
    return POPULARITY_MEDIAN_BY_CATEGORY[category];
  }
  return clamp((ninkijun - POPULARITY_RUNNER_FLOOR) / (runnerCount - POPULARITY_RUNNER_FLOOR));
};

// weight_diff_from_avg = current_bataiju - weight_avg_5 (builder L1589). Both
// sides null-propagate: the builder's CAST(...) - weight_avg_5 yields NULL when
// either operand is NULL, which the scorer treats as a missing feature cell.
const computeWeightDiff = (weight: WeightSnapshot): number | null => {
  if (weight.currentBataiju === null || weight.weightAvg5 === null) return null;
  return weight.currentBataiju - weight.weightAvg5;
};

export interface LateBindingInput {
  odds: OddsSnapshot;
  weight: WeightSnapshot;
  category: LateBindingCategory;
}

export const computeLateBindingColumns = (input: LateBindingInput): LateBindingColumns => ({
  oddsScore: computeOddsScore(input.odds.tanshoOdds, input.category),
  popularityScore: computePopularityScore(
    input.odds.tanshoNinkijun,
    input.odds.runnerCount,
    input.category,
  ),
  tanshoNinkijun: input.odds.tanshoNinkijun,
  tanshoOdds: input.odds.tanshoOdds,
  weightDiffFromAvg: computeWeightDiff(input.weight),
});

// The model feature-column names these late-binding values overwrite. tansho_*
// are intentionally absent: they are not in the 244-feature model list, so they
// are surfaced on LateBindingColumns for the cache row but never projected into
// the score vector.
export const LATE_BINDING_FEATURE_COLUMNS = {
  oddsScore: "odds_score",
  popularityScore: "popularity_score",
  weightDiffFromAvg: "weight_diff_from_avg",
} as const satisfies Record<string, string>;
