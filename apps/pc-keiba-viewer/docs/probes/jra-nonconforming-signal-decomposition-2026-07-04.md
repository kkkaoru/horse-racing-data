# Non-Conforming Race Signal-Family Decomposition (2026-07-04)

- **Date**: 2026-07-04
- **Category**: JRA finish-position feature-family diagnostic, feeding the
  odds-free ability model design (task #37 / dynamic odds-reliance via
  volatility). Task #38.
- **Trigger**: task #37 needs to know which pre-race signal families still
  carry predictive power when the betting market gets the winner wrong, so
  the odds-free "ability score" it builds can be weighted toward the
  families that actually survive market failure, not just the families that
  are strongest overall.
- **Different from prior upset work** (not duplicated): `jra-summer-upset-divergence-2026-07-04.md`
  (+2024 extension) profiled **who** wins upsets — descriptive traits
  (running style, layoff, class, jockey) of the actual upset winners versus
  control winners. This doc asks a different question: **conditioned on a
  non-conforming race, how well does each _feature family_, used alone, rank
  the field** — i.e. "when the market is wrong, what still works, and by how
  much." No claims here overlap or restate that doc's findings.

## Data and scope

Offline engineered-feature store (`tmp/candidate-eval-jra/augmented/`, 250+
columns, JRA only, `source='jra'`), **2015-2025** (store also has 2013-2014;
excluded per task scope to match the "2015+" instruction). 527,280 horse-rows
/ 38,002 races loaded; 526,474 rows / 37,943 races survive a clean-winner
filter (drops the rare race with zero or >1 `finish_position==1` row, so
severity is well-defined for every race kept).

- **Train population** (family model fitting + single-best-column selection):
  2015-2022, **JRA-wide, unconditional on severity** (385,170 rows) — this
  matters: families are built the way a real model would be built, without
  knowing in advance which races will turn out to be upsets.
- **Eval population**: 2023/2024/2025, held out entirely from family
  training (141,304 rows / 10,350 races).
- **Cuts** (evaluated separately, pooled across the 3 eval years for the
  venue/summer cuts, and per-year for the JRA-wide cut to check year
  stability):
  - `jra_wide` — all 10 JRA venues, all months.
  - `venue_021003` — 函館(02)/福島(03)/小倉(10) only, all months.
  - `summer` — June-September, all venues.
- **Severity** (race-level, defined by the actual winner's own market rank,
  `tansho_ninkijun`):
  - `conforming` — favorite (`ninkijun==1`) won.
  - `s1` — favorite did **not** win (`ninkijun!=1`); complement of conforming.
  - `s2` — the severe subset of s1: winner's own `ninkijun>=4`.
- Pooled eval-year n (races): `jra_wide` conforming=3,466 / s1=6,884 / s2=3,453;
  `venue_021003` conforming=605 / s1=1,333 / s2=693; `summer` conforming=1,163
  / s1=2,287 / s2=1,138. All cells have enough races for the headline claims;
  per-year × per-venue (crossed) would be underpowered and was **not**
  computed — year stability was checked at the `jra_wide` cut only, venue/
  summer stability was checked pooled-across-years only (documented gap, not
  a silent gap).

**Caveat up front (per task instruction, repeated at the end too)**: `s1`/`s2`
are defined from the race's actual outcome, so this whole analysis is
post-hoc/outcome-conditioned. It is valid for **informing model design**
(which families to weight, which to de-weight, when a volatility signal
predicts an upset is likely) — it is **not** evidence that a deployable model
built this way would actually improve serve accuracy. Only #37's genuine
blind walk-forward on pre-race volatility predictors can show that.

## Method

1. **8 signal families**, grouped from the store's engineered columns (full
   column lists in `tmp/candidate-nonconform-decomp/families.py`, with an
   explicit list of what was left out — leak columns, race-level physics/
   weather constants, workout laps — and why):

   | Family           | # cols | Contents                                                                                                    |
   | ---------------- | ------ | ----------------------------------------------------------------------------------------------------------- |
   | `market`         | 15     | odds, ninkijun, popularity/inverse-odds derivatives, favorite-disagreement                                  |
   | `recent_form`    | 18     | last-race finish/margin/corner, finish trend, layoff, class/dist diff                                       |
   | `career_ability` | 23     | career win/place/place2 rates by course-cut, grade-race counts, h2h, baba win-rate                          |
   | `speed_time`     | 16     | speed index, 上がり3F averages (firm/soft), field speed-strength aggregates                                 |
   | `style_pace`     | 49     | running-style probabilities/history (self + field pace-pressure context)                                    |
   | `physical`       | 18     | weight avg/trend/volatility, 斤量 (futan_juryo) level+rank, age, sex, field size                            |
   | `connections`    | 70     | jockey + trainer + **sire/damsire** win/place2 rates (brief's "jockey/trainer/sire")                        |
   | `similarity`     | 19     | `sim_*` — comparable-race pattern features (odds-pattern, jockey/trainer/sire/owner rates in similar races) |

   Two leak columns families were explicitly kept **out**:
   `target_corner_1_norm`/`target_corner_3_norm`/`target_corner_4_norm` are
   the within-race actual corner passage of the target race itself — the
   exact leak family fixed in `project_target_corner_leak_2026_07_04`. Never
   used here. `rs_p_*` (pre-race predicted running-style probabilities) were
   used instead for style/pace signal.

2. **Single-best-column ranking**: per family, pick the one column with
   largest `|Spearman(col, finish_position)|` on the train population,
   **restricted to columns that vary within a race** (a race-level constant
   like `field_size_normalized` scored artificially well in a first pass
   because it correlates with finish*position pooled across races of
   different sizes, but produces an all-tied, degenerate ranking \_within* any
   single race — excluded from candidacy, kept only in the LightGBM feature
   set where tree interactions can still use it meaningfully).
3. **Per-family LightGBM**: one small regressor per family (`num_leaves=15`,
   up to 300 trees with early stopping on a 2022 internal validation slice,
   `min_child_samples=100`), features = that family's own columns only,
   target = `finish_position`. Predicted score ranks the field within race.
4. **Combined non-market ensemble**: simple Borda-style average of the
   within-race percentile rank of all 7 non-market families' LightGBM scores
   (no tuning, no weighting — deliberately naive, to see whether even an
   unweighted combination beats the market or the single best family).
5. **Metrics**, computed per race then averaged over the slice: winner's
   assigned rank, winner-in-top-K rate for K=1/3/5/6 (per
   `feedback_eval_rank_1_to_6`), and mean per-race Spearman rho between the
   family's assigned rank and the actual finish order (closed-form, since
   both are 1..n permutations within a clean race).

Scripts: `tmp/candidate-nonconform-decomp/families.py` (grouping),
`run_decomposition.py` (load, fit, score, aggregate — ~25s end to end),
`analyze.py` / `analyze2.py` (headline tables). Outputs:
`race_scores.parquet` (per-race scores/ranks for all 16 family×method
combinations + ensemble), `agg_metrics.csv` / `agg_topk.csv` (full aggregate
tables), `family_info.json` (chosen single column + train correlation per
family).

## Headline result 1 — retention ratio (S2 / conforming), LightGBM method, JRA-wide, pooled 2023-2025

| Family                       | conforming top3 | s1 top3 | s2 top3    | retention (s2/conf) |
| ---------------------------- | --------------- | ------- | ---------- | ------------------- |
| **physical**                 | 38.46%          | 29.45%  | **24.73%** | **0.643**           |
| **style_pace**               | 38.92%          | 28.20%  | 23.26%     | **0.598**           |
| speed_time                   | 48.93%          | 30.23%  | 23.63%     | 0.483               |
| similarity                   | 54.70%          | 31.86%  | 22.94%     | 0.419               |
| connections                  | 71.93%          | 36.88%  | 23.28%     | 0.324               |
| recent_form                  | 73.98%          | 37.73%  | 21.14%     | 0.286               |
| career_ability               | 74.44%          | 37.04%  | 20.85%     | 0.280               |
| ensemble (naive, non-market) | 80.12%          | 40.21%  | 22.13%     | 0.276               |
| market                       | 99.16%          | 50.42%  | **8.14%**  | **0.082**           |

("top3" = actual winner lands in that family's own top-3 within-race ranking.)

Two things jump out. First, **market collapses far harder than any other
family** — its retention ratio (0.082) is roughly 3-8x worse than every
non-market family. Second, and less expected: **physical and style_pace are
the best-retaining families, not career_ability or recent_form** — the two
families closest to a naive notion of "true ability" actually retain the
_least_ (0.28-0.29) among the non-market families, tied with the naive
ensemble.

**Absolute S2 performance tells a similar but more compressed story**: the
top five non-market families (physical 24.7%, speed_time 23.6%, connections
23.3%, style_pace 23.3%, similarity 22.9%) cluster tightly within a 3.9pp
band; recent_form (21.1%) and career_ability (20.9%) trail by ~2-3pp; market
(8.1%) is in a different league entirely. **Every single non-market family
clears market by 2.5-3x on S2 winner-top-3 rate.**

Spearman (full-field rank correlation, not just the winner) tells a
genuinely different story that matters for interpretation — see Headline
result 2.

## Headline result 2 — market fails at the top, not everywhere (the brief's explicit "measure, don't assume" question)

| Metric (S2, LightGBM, JRA-wide)     | market     | physical | style_pace | speed_time | ensemble (non-mkt) |
| ----------------------------------- | ---------- | -------- | ---------- | ---------- | ------------------ |
| winner top1 rate                    | **0.35%**  | 8.98%    | 8.08%      | 7.82%      | 4.43%              |
| winner top3 rate                    | **8.14%**  | 24.73%   | 23.26%     | 23.63%     | 22.13%             |
| winner top5 rate                    | **49.23%** | 41.15%   | 39.99%     | 40.46%     | 43.35%             |
| winner top6 rate                    | **63.57%** | 49.26%   | 47.23%     | 48.65%     | 52.88%             |
| mean per-race Spearman (full field) | **0.456**  | 0.179    | 0.167      | 0.201      | 0.382              |

This is the key structural finding. On S2 races, market is catastrophic at
top1 (0.35%, essentially never — nearly tautological, since S2 requires the
winner's own ninkijun>=4) and still very weak at top3 (8.1%) — but it
**recovers to be competitive with, or better than, every single non-market
family by top5/top6**, and it has the **highest full-field Spearman of any
family, including the combined ensemble** (0.456 vs ensemble's 0.382).
Market is not "wrong about everything" in an upset race — bettors still
correctly identify roughly which 5-6 horses form the competitive tier; they
are specifically wrong about how to **order the top of that tier**, which is
exactly where top1/top3 metrics bite and exactly where the non-market
families — physical and style_pace above all — earn their keep. This pattern
replicates cleanly across all three eval years (market top1/top3/top5/top6
on S2: 2023 = 0.33/8.97/49.83/65.28%, 2024 = 0.63/8.97/50.67/64.84%, 2025 =
0.09/6.44/47.18/60.49%) and across both the venue and summer cuts (market
top5/top6 stays 44-49%/59-64% at every cut checked).

**Direct answer to the brief's question**: yes, every non-market family
clearly beats market on S2 for top1/top3; **no**, market is not beaten on
top5/top6 or full-field Spearman by any single non-market family, and the
naive combined ensemble also loses to market on top6 and full-field Spearman
(only wins on top1/top3/top5-adjacent). Market's S2 weakness is a **sharp,
narrow-band failure at the very top of the order**, not a broad information
collapse.

## Headline result 3 — why physical/style_pace/speed_time win: they are the least market-redundant families

Checked whether each family's information is already priced into the odds,
by correlating (Spearman, train population) each family's columns against
`tansho_ninkijun` itself:

| Family         | mean `    | `rho vs ninkijun`                     | ` across all cols                                                                                              | best single col | that col's rho vs ninkijun |
| -------------- | --------- | ------------------------------------- | -------------------------------------------------------------------------------------------------------------- | --------------- | -------------------------- |
| style_pace     | **0.065** | `past_corner_progression_avg_5`       | 0.21                                                                                                           |
| physical       | **0.089** | `futan_juryo_rank_in_race`            | 0.14                                                                                                           |
| similarity     | 0.096     | `sim_jockey_place_rate`               | -0.32 (mixed family: `sim_odds_rank_correlation`/`sim_fav_win_rate` are ~0.02, `sim_jockey_place_rate` is not) |
| speed_time     | 0.134     | `kohan3f_firm_avg5`                   | 0.19                                                                                                           |
| career_ability | 0.156     | `career_place_rate`                   | -0.44                                                                                                          |
| connections    | 0.188     | `jockey_recent_win_rate_rank_in_race` | 0.44                                                                                                           |
| recent_form    | **0.262** | `last_3_avg_finish_norm`              | **0.61**                                                                                                       |

This lines up with the retention ranking almost exactly: the Spearman rank
correlation between "family's mean market-redundancy" and "family's S2
retention ratio" across the 7 non-market families is **-0.82** (strong
inverse relationship: more market-redundant → less retention).
`style_pace`
and `physical` are the two _least_ market-redundant families and the two
_best_-retaining families on S2; `recent_form` is the _most_ market-redundant
non-market family (bettors clearly price recent form heavily — 0.61
correlation with ninkijun is close to `career_place_rate`'s -0.44) and one of
the two _worst_-retaining. The mechanism is intuitive once stated: when the
market's read of "who's good" is wrong, a family whose signal mostly
re-derives the market's own read (recent form, career win-rate, jockey
recent win-rate) is wrong in much the same direction — it isn't adding
independent information, so it degrades along with the market. A family
carrying information the market doesn't price as heavily (斤量/weight
carrying, running-style/pace fit, own speed figures) keeps working precisely
because it was never redundant with the odds in the first place.

## Combined non-market ensemble vs. market, and vs. the single best family

The naive (unweighted Borda-average) ensemble of all 7 non-market families:
S2 top1=4.43%, top3=22.13%, top5=43.35%, top6=52.88%, full-field Spearman
0.382.

- **Crushes market** on top1 (4.43% vs 0.35%) and top3 (22.13% vs 8.14%).
- **Loses to market** on top6 (52.88% vs 63.57%) and full-field Spearman
  (0.382 vs 0.456).
- **Underperforms the single best family (`physical`, top3=24.73%) on
  absolute S2 top3** — simple equal-weight averaging dilutes `physical`'s
  and `style_pace`'s edge with weaker, more market-redundant families
  (`recent_form`, `career_ability`). This is an actionable point for #37:
  a naive average of "everything non-market" is not the right target;
  a family-weighted combination (or a proper stacked meta-learner, following
  this campaign's established preference for GBDT stacking over hand-tuned
  weights) leaning on `physical`/`style_pace`/`speed_time` would very likely
  do better than either the naive ensemble or market alone on S2, while still
  needing market for what it's actually good at (the top5/6 contender set,
  full-field ordering).

## Stability checks

- **Year stability** (`jra_wide`, S2 top3 retention, LightGBM): family order
  is stable across 2023/2024/2025 — physical (0.627/0.656/0.650) leads all
  three years, market (0.091/0.091/0.065) trails all three, and the
  recent_form/career_ability/ensemble cluster (~0.27-0.29) stays bunched in
  every year.
- **Venue/summer cuts** (S2 top3 retention, pooled years): the same ordering
  holds directionally at both `venue_021003` (physical 0.727, style_pace
  0.542, ... market 0.073) and `summer` (physical 0.694, style_pace 0.645,
  ... market 0.075) — physical is if anything **stronger** at the 3 focus
  venues and in summer than JRA-wide, which is directly useful for #37 given
  the campaign's summer-venue focus. style_pace and speed_time swap 2nd/3rd
  place depending on the cut but stay in the top tier throughout; market
  stays last by a wide margin in every cut.
- **Single-best-column method** (weaker overall than LightGBM, as expected —
  one column vs 15-70): the same qualitative pattern holds. `style_pace`'s
  single column (`past_corner_progression_avg_5`) shows the _highest_
  retention of any single-column family (0.827), consistent with
  style*pace's strength in the LightGBM version; `market`'s single column
  (`tansho_ninkijun` itself) again collapses hardest (retention ~0.005, by
  near-tautological construction). Absolute single-column S2 top3 rates are
  10-20% lower than the corresponding LightGBM numbers across the board —
  confirms the families' value comes substantially from combining several
  correlated-but-distinct columns, not from one dominant column (most
  visible for `physical`, whose best single column has the \_lowest* train
  correlation of any family, 0.119, yet whose LightGBM model is the
  strongest S2 performer of all eight).

## Caveats (read before using this for #37)

1. **Outcome-dependent conditioning is post-hoc.** `s1`/`s2` are defined from
   the actual winner's market rank — a race's severity is only knowable after
   it's run. This analysis says which families _would have_ worked if you
   somehow knew in advance a race would be non-conforming. It does **not**
   show that a deployable model — which must decide _before_ the race, using
   only a volatility/upset-likelihood signal built from pre-race data — will
   capture the same gain. That is exactly #37's job (genuine blind
   walk-forward on a pre-race volatility predictor), and this doc is
   explicitly not a substitute for it.
2. **Family sizes vary 15x** (market/recent_form/speed_time/physical ~15-18
   columns vs connections 70, style_pace 49) — cross-family LightGBM
   comparisons partly reflect available degrees of freedom, not purely
   "information content." `connections`' large column count didn't translate
   into a correspondingly large S2 edge (middling retention despite 70
   columns), which is reassuring, but the asymmetry should be kept in mind
   before over-interpreting any single family's exact rank.
3. **One frozen, unconditional model per family.** Each LightGBM was trained
   once on the natural (severity-unconditional) 2015-2022 population, not
   tuned per severity or per cut — deliberately, to mirror what a real model
   would see, but it means a severity-aware or cut-aware refit could show
   different numbers (probably better for the small cuts, at the cost of
   losing the "would a normal model already have this" framing).
4. **Winner-centric metrics only.** This scored how well each family ranks
   the _actual winner_ (top-K rate, full-field Spearman) — it did not run
   the full multi-metric evaluation gate (`place2`/`place3`/`top3_box`
   composite per `feedback_multi_metric_accept_gate`) that a real ADOPT/
   REJECT decision would require. That gate applies to #37's eventual model,
   not to this exploratory decomposition.
5. **Leak discipline verified**: `target_corner_*` (the leak family fixed
   2026-07-04) was explicitly excluded from every family; `rs_p_*` (pre-race
   predicted style, not the leaked in-race actual) was used for style/pace
   signal instead.

## Ranked handoff for #37

1. **Weight the odds-free ability score toward `physical`, `style_pace`, and
   `speed_time`** — they are simultaneously the best S2-retaining families
   _and_ the least redundant with the market's own pricing (0.82 rank
   correlation between market-redundancy and S2 retention across families),
   which is a mechanistically coherent reason to trust the pattern, not just
   a coincidence of this sample.
2. **De-weight `recent_form` and `career_ability` specifically when the
   volatility model suspects an upset** — they are the strongest families in
   normal (conforming) races but the _weakest_ non-market families on S2,
   because they mostly re-derive information the market has already priced.
   They should stay heavily weighted for _normal_ races (that's most of the
   77-80% of JRA races); the case for down-weighting is specifically
   upset-conditional.
3. **Don't discard the market signal on suspected-upset races** — it remains
   the single best full-field ranker even on S2 (highest Spearman of any
   family) and recovers to lead or tie at top5/top6. Its failure is narrow:
   picking the top1-3 order. A sensible design keeps market for defining the
   contender set / place4-6 tail and leans on the non-market families
   specifically to re-order who's actually 1st-3rd within that set.
4. **A naive equal-weight non-market ensemble is not the answer** — it
   underperforms the single best family (`physical`) on S2 top3 and loses to
   market outright on top6/Spearman. A weighted or stacked combination is
   needed, consistent with this campaign's established GBDT-stacking-over-
   hand-tuned-weights preference.
5. **This is a design input, not a validation result** (see caveat 1) — #37
   must still run genuine blind walk-forward before any ADOPT decision.

## 日本語まとめ

市場(オッズ・人気)通りに決まらなかったレース(S1=1番人気が勝てず、S2=勝ち馬の
人気が4番人気以下という厳しい方の非該当条件)に絞り、armBの250特徴量を8つの
シグナル・ファミリー(market/recent_form/career_ability/speed_time/style_pace/
physical/connections/similarity)に分解し、各ファミリー単独で(a)最良1列 (b)
小型LightGBM の2手法で当該レースの馬をランキングした場合、勝ち馬をどれだけ
上位に置けるかを2015-2022学習・2023-2025評価(学習年と評価年は完全分離)で
検証した。

1. **市場の崩壊が突出**: S2でのwinner-top3率は市場ファミリー単独で8.1%(通常
   レースの99.2%から劇的に低下、retention比0.082)——他の全非市場ファミリー
   (20.9-24.7%、retention比0.28-0.64)より2.5-3倍以上悪い。
2. **市場が弱いのはtop1-3だけ**: しかしtop5(49.2%)・top6(63.6%)まで見ると
   市場は逆にどの非市場ファミリーよりも高い、あるいは同等——全馬順位との
   Spearman相関も市場が最高(0.456、アンサンブルより上)。市場は「大外れ」
   ではなく、「上位候補集団の特定はできているが、その中の1-3着順を外す」
   という狭く鋭い失敗パターンであることが明確になった。
3. **retentionが最も高いのは physical(斤量/馬体重/性齢) と style_pace(脚質
   ・展開適性)**——career_ability や recent_form ではない。理由を市場との
   相関で確認したところ、physical/style_pace/speed_time は市場(人気順)との
   相関が最も低い(平均|rho|=0.065-0.134)一方、recent_form は最も高い
   (0.262、単独最良列は人気順と rho=0.61)。ファミリーの「市場との非重複度」
   と「S2でのretention」の順位相関は0.82——市場が既に織り込んでいる情報を
   なぞるファミリーほど、市場が外れたときに一緒に外れる、という一貫した
   メカニズムが確認できた(「市場との非重複度」と「S2でのretention」の
   順位相関は-0.82の負の相関、絶対値としては強い逆相関)。
4. **単純平均アンサンブル(非市場7ファミリー)は最良単体(physical)に劣る**
   ——重み付けなしの平均は signal を希釈するため、#37では重み付き/スタック
   型の統合が必要。
5. **重要な留保**: S1/S2の切り分けは結果を知った後の事後条件付け——このドキュ
   メントは#37のモデル設計指針であり、それ自体は本番改善の証拠ではない。
   実際の採否判断は#37の厳密なblind walk-forwardでのみ可能。

## Artifacts

- Grouping: `tmp/candidate-nonconform-decomp/families.py` (8-family column
  lists + explicit exclusion rationale — leak columns, race-level constants,
  workout/course/weather out of scope)
- Pipeline: `tmp/candidate-nonconform-decomp/run_decomposition.py` (load →
  clean-winner filter → train/eval split → per-family single-column pick +
  LightGBM fit → within-race ranking → race-level aggregation, ~25s)
- Analysis: `analyze.py` (retention-ratio tables, single-vs-LGBM comparison,
  year/venue stability), `analyze2.py` (top1/3/5/6 expanded breakdown, the
  market-recovers-by-top5/6 headline check)
- Data: `race_scores.parquet` (10,350 races x per-family/method scores+ranks),
  `agg_metrics.csv` (612-row full aggregate table), `agg_topk.csv` (top1/3/5/6
  expanded aggregate), `family_info.json` (chosen column + train rho per
  family)
- Related: `jra-summer-upset-divergence-2026-07-04.md` (descriptive upset-
  winner profile, different question), `project_target_corner_leak_2026_07_04`
  (the leak family this doc explicitly excludes), `feedback_multi_metric_accept_gate`
  / `feedback_eval_rank_1_to_6` (evaluation conventions this doc follows for
  top-K but does not replace for an eventual ADOPT decision)
