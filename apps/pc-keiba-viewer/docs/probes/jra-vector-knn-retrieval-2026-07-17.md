---
probe: jra-vector-knn-retrieval
date: 2026-07-17
category: jra
method: per-horse continuous L2 kNN retrieval in a market-orthogonal (physical/style_pace/speed_time) embedding, pool-scoped by surface x distance-band, additive GBDT feature
status: REJECT — probe passed for one novel target (market-beat residual), WF gate clean fail (0/3 primaries, place4 no-reg breach, summer-4-venue negative, cell scan pattern-free)
mlflow_run_id: 00a02dd756ee47749ed806a5fc7b2288
mlflow_experiment: finish-position/wf-eval
---

# JRA Vector-Based kNN Retrieval Feature (2026-07-17)

## 0. Task

USER-specified lever: "ベクトル化とベクトル検索" (vectorization and vector search) — test
whether a continuous k-nearest-neighbor retrieval feature, built by embedding each
horse-entry into a market-orthogonal standardized feature vector and retrieving the
k most similar historical horse-entries (strictly prior in time), generalizes the
kind of context that exact-cell conditioning (venue x class x distance x season x
meet-day x waku x running-style x jockey x pedigree) has repeatedly failed to
capture profitably in this codebase's REJECT history — the hypothesis being that a
smooth neighborhood in continuous feature space avoids the "cell too narrow, n too
small, or the market already prices it" failure mode that killed the EB-shrunk
conditional-rate family (see `index_closed_probes.md` memory, and
`apps/pc-keiba-viewer/tmp/frontier-scout/lever_bank.md`'s Confirmed-dead table).

## 1. Prior art and dedup — read this before the design section

Two dedup passes were required: (a) the already-deployed `sim_*` similar-race
features, and (b) an entire closed campaign of kNN/vector-search experiments in
`docs/finish-position-accuracy/per-class/jra/` that the standard lever-scouting
pass (`lever_bank.md`, 2026-07-11) did not cover, because its methodology scanned
`docs/probes/*2026-07*.md` and system-doc §11 but not the
`docs/finish-position-accuracy/` tree. This section documents both, since the
second one materially changes how this lever must be scoped.

### 1.1 Not a duplicate of `sim_*` (already deployed, v9-sim)

`apps/pc-keiba-viewer/src/scripts/finish-position-features/add-similar-race-features.py`
matches a TARGET RACE (not an individual horse) to similar past RACES via a
**categorical cascade**: exact match on
`(source, keibajo_code, surface, kyori_band, season_band, class_group)`, falling
back through 4 coarsening levels until >=30 similar races are found (`MIN_SIMILAR`).
It then computes two kinds of feature: (Phase 1, race-level, constant across all
horses in a race) odds-calibration diagnostics for that race-bucket
(`sim_odds_rank_correlation`, `sim_fav_win_rate`, `sim_odds_correlation_variance`);
(Phase 2, per-horse) **entity-identity** win/place rates — this specific jockey's,
trainer's, sire's, damsire's, owner's, or draw-tercile's win rate _within the
matched race-bucket_.

This lever's mechanism is different in three respects: (1) similarity metric —
continuous L2 distance in a standardized numeric embedding vs. `sim_*`'s discrete
categorical-equality cascade; (2) unit and target — a per-horse ability/style/speed
profile matched against historical per-horse profiles vs. `sim_*`'s race-level
odds-calibration + named-entity conditional rate (the entity-rate half is
structurally the same family as the already-REJECTED EB-shrunk jockey/pedigree
conditional-rate levers, just wrapped in a race-bucket instead of a fixed cell);
(3) feature space — market-orthogonal physical/style_pace/speed_time vs.
odds-pattern + identity win-rate. No overlap in mechanism; not a re-implementation.

### 1.2 A closed campaign this lever must not blindly re-run

`docs/finish-position-accuracy/per-class/jra/` (all dated 2026-06-17/18, i.e. one
month before this task and **not found by the 2026-07-11 lever-scout**) contains
five documents that tested kNN/vector-search on JRA, all scoped to **class 703
(未勝利) only**, against the **old iter19/iter20 feature store (~236-244 cols,
pre-2026-07-04 leak-fix, no sim_v9)**:

| Doc                                   | Design tested                                                                                                                                                         | Verdict                                                                                                                                                                                                                                                            |
| ------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `pgvector-knn-703-005.md`             | Partial-ρ orthogonality probe, `horse_ability` embedding (15 feat, **odds INCLUDED**), k=5..100, controls = log(odds) + iter19 GBDT score                             | 703 PROCEED (partial-ρ -0.08 to -0.10), 005 ABORT; `race_condition` embedding (race-level constants only) ABORT both classes                                                                                                                                       |
| `clean-vectorization-ablation.md`     | 5 value-prep variants (V0..V4) isolating which fix mattered                                                                                                           | Odds-EXCLUDED alone FAILS the gate (V1, ρ=-0.062); **within-race z-score RESCUES it** without odds (V2, ρ=-0.0895) — this is the decisive fix, confirmed and reused below                                                                                          |
| **`knn-feature-cheapfilter.md`**      | The exact recommended next step: V4 score (odds-free, within-race z, curated 18-feat, k=50) added as a **single additive CatBoost feature**, real retrain, blind 2025 | **ABORT** — top1 **-0.72pp** (LB95 -1.76pp), all 5 metrics LB95<0. "ρ≈0.11 does NOT convert to a model gain... GBDT already captures the within-race-relative structure through its tree splits." Explicitly closes further kNN work on that feature space for 703 |
| `odds-independent-position-vector.md` | Same V4 vector, decomposed into per-position affinity [P1..P4+], used as a **post-hoc score correction** (not a feature) across all 6 JRA classes                     | ABORT — affinities fail the ρ>=0.08 orthogonality gate in every class; global blind-2025 correction: place2/place3 LB95 both negative                                                                                                                              |
| `ensemble-rl-vec-search.md`           | Learned 32d embedding (MLX MLP, pairwise ranking loss) + kNN retrieval in the learned space + REINFORCE RL policy, v1 (odds-in) and v2 (odds-free, within-race z)     | **ABORT both**, badly (top1 -22.3pp / -16.7pp vs GBDT). DO-NOT-RETEST for this architecture                                                                                                                                                                        |

`docs/finish-position-accuracy/per-class/ROADMAP.md` §3 additionally lists
`iter32-jra-vec-knn-{class}-v8` (kNN as an inference-time **ensemble member**) as
DO-NOT-RETEST for all 5 JRA classes, and §6 sets a standing design constraint for
any future pgvector work: different embedding space, additive-feature (not
ensemble-member) purpose, probe-first, per-class scope, and an explicit
differentiation writeup — this doc satisfies that constraint's spirit, extended to
pooled scope given the baseline has since changed.

**What this means for scope, and why this is still a legitimate probe rather than
a re-test:**

1. The tested `V4` vector was dominated by `recent_form`/`career_ability` columns
   (`recent_finish`, `avg_finish`, `last_race_finish_norm`, `career_win_rate`,
   `career_place_rate`, `speed_index_*`, `jockey_recent_win_rate`,
   `trainer_career_win_rate`, ...) — only 3/18 features touched `physical`/
   `speed_time`, and **zero** touched `style_pace`. `docs/probes/jra-nonconforming-signal-decomposition-2026-07-04.md`
   (published _after_ the kNN campaign) found the opposite ranking:
   `recent_form`/`career_ability` are the **worst**-retaining, most
   market-redundant families when the market misprices the winner (retention
   0.28-0.29, market-redundancy rho 0.26/0.16), while `physical`/`style_pace`/
   `speed_time` are the **best**-retaining, least market-redundant (retention
   0.60-0.64, market-redundancy rho 0.065-0.13). The closed campaign essentially
   tested a kNN-smoothed version of the two families most redundant with what
   GBDT already extracts from odds+recent-form splits — which is exactly
   consistent with its own diagnosis ("GBDT already captures it"). It never
   tested a vector built from the families the market itself prices _least_.
2. All five closed docs used the **stale iter19/iter20 store** (~236-244 cols),
   which pre-dates the 2026-07-04 within-race leak fix and the v9-sim deployment.
   The current champion (`jra-cb-v9-sim-2013-clean`, armB-250) is a materially
   different, newer, and cleaner baseline.
3. All five were **class-703-scoped only** (single per-class routing candidate),
   never evaluated pooled across JRA under this repo's standard §7.2 gate.
4. The neighbor-outcome target in every closed doc was raw neighbor `finish_norm`
   (or a P1..P4+ position-affinity decomposition of it). None tested a
   **market-relative residual** (did the neighbor beat _its own_ ninkijun-implied
   expectation) — which is the more direct operationalization of this task's
   brief ("similar horses that outperformed market expectation").

Given this, the honest prior here is **low, not zero** — this is a genuine but
narrow gap, and the closed campaign's consistent mechanism ("partial-ρ
orthogonality is necessary but not sufficient; GBDT already captures
within-race-relative structure via tree splits") is not obviously
feature-composition-specific, so it may well recur. This doc treats a probe pass
as much weaker evidence than it would be in isolation, and this framing (not just
the bare gate numbers) drives the final verdict below.

## 2. Design

### 2.1 Embedding vector — 31 armB-250 columns, physical/style_pace/speed_time only

All 31 are already-vetted, pre-race-safe columns live in the current champion's
own 250-feature set (`tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json`
key `armB`) — this lever only recombines them into a similarity vector, it does not
introduce new raw signals.

- **Physical (7)**: `bataiju_avg5`, `weight_diff_from_avg`, `weight_trend_5`,
  `weight_volatility_5`, `futan_juryo_diff_from_race_avg`,
  `futan_juryo_rank_in_race`, `umaban_norm`
- **Style_pace (16)**: `rs_p_nige`, `rs_p_senkou`, `rs_p_sashi`, `rs_p_oikomi`,
  `rs_confidence_entropy`, `rs_p_nige_x_field_pace`, `past_nige_rate_self`,
  `past_senkou_rate_self`, `past_sashi_rate_self`, `past_oikomi_rate_self`,
  `self_nige_rate_minus_field_avg`, `self_style_dominant_rate`,
  `past_style_x_field_pace_match`, `past_corner_progression_avg_5`,
  `past_corner_1_norm_avg_5`, `past_dominant_label_consistency_5`
- **Speed_time (8)**: `speed_index_avg_5_diff_from_race_avg`,
  `speed_index_avg_5_rank_in_race`, `speed_index_best_5_rank_in_race`,
  `kohan3f_avg_5`, `kohan3f_going_diff`, `recent_soha_time_per_meter_avg5`,
  `same_distance_soha_time_per_meter_avg5`, `last_3_avg_kohan_3f`

Deliberately excluded: all odds/market columns, all `recent_form`/`career_ability`
columns (the closed campaign's composition), all connections/pedigree/`sim_*`
columns (different, already-covered families).

### 2.2 Value prep

Within-race z-score per column (`(value - race_mean) / race_std`, grouped by
`race_id`; 0 if `race_std==0` or all-null in-race) — per
`clean-vectorization-ablation.md`'s finding, this is the fix that recovers
orthogonal signal once odds are excluded from the vector, and is applied here from
the start. Median-impute remaining nulls and fit a global `StandardScaler`, both
fit on train-fold rows only.

### 2.3 Pool scoping (context as a filter, not as vector dimensions)

Hard-filtered to matching `surface` (`left(coalesce(track_code,''),1)`) and
`kyori_band` (`<=1300` sprint / `<=1700` mile / `<=2200` intermediate / else long —
reusing `add-similar-race-features.py`'s exact boundary constants). Context is
used to scope _which pool_ is searched, not blended into the L2 distance itself —
this is a deliberate correction of the closed campaign's `race_condition`
embedding design, which concatenated race-level constants directly into the kNN
vector and was confirmed genuinely null (partial-rho ~0, even with correct
one-hot encoding of `track_code`).

### 2.4 Leak safety

Neighbor pool for a query row in calendar year Y is built exclusively from rows
with `race_year < Y` (index frozen at the start of year Y); the
`E[finish_norm | ninkijun]` expectation table used by the market-residual
candidate (2.5.4) is fit on that same `race_year < Y` pool, so both the pool and
the expectation baseline share one leak-safety argument. Confirmed by execution:
626,798 total rows processed in 55.6s (12 per-year iterations); the 49,639 rows
in `race_year == 2013` are null on all 8 candidate columns as expected (no prior
year exists within the 2013+ window). This is coarser than the per-row-strict
date cutoff `knn-feature-cheapfilter.md` used, but still strictly leak-free (no
same-year-or-later data ever enters a pool), and matches this repo's own WF
fold-boundary granularity (`docs/finish-position-prediction-system.md` §8.9:
train `<= (valid_year-1)/12/31`, valid = full calendar year).

### 2.5 Candidate aggregation columns

For k in {50, 200}, over the k nearest leak-free neighbors:

1. `knn_win_rate_k{k}` — mean(neighbor finished 1st)
2. `knn_top3_rate_k{k}` — mean(neighbor finished <=3rd)
3. `knn_finish_norm_mean_k{k}` — mean(neighbor `finish_norm`); replicates the
   closed campaign's target for direct comparability
4. `knn_mkt_residual_mean_k{k}` — mean over neighbors of
   `(neighbor finish_norm - E[finish_norm | neighbor's own tansho_ninkijun])`,
   the expectation table fit train-only per fold. This is the "did similar
   horses beat their own market expectation" signal — not tested by any of the
   five closed docs, all of which used raw `finish_norm` only.

## 3. Probe results

odds + champion-base-score-controlled partial Spearman vs `finish_position`,
pooled JRA, 2023/2024/2025. Controls: `tansho_ninkijun` + `champion_base_score`
(the 3-seed-averaged predicted score from the CURRENT clean armB-250 champion,
read from the cached models at `tmp/candidate-masked-lever-retest/models/base/`
— a strictly more rigorous control than a `sim_*` proxy, and the same style of
control (GBDT score + odds) the five closed docs used, making the partial-rho
figures below directly comparable to their 0.08-0.11 historical figures). Gate:
`|partial_rho| >= 0.02` (repo-standard, `tmp/venue-jockey-probe/probe_partial_rho.py`
convention) AND sign-stable 3/3 years across 2023/2024/2025.

Full results: `apps/pc-keiba-viewer/tmp/vector-knn-retrieval/probe_result.json`.

| Candidate                        |  2023 ρ |  2024 ρ |  2025 ρ | max &#124;ρ&#124; | sign-stable 3/3 | PASS 0.02 |       PASS 0.08        |
| -------------------------------- | ------: | ------: | ------: | ----------------: | :-------------: | :-------: | :--------------------: |
| `knn_win_rate_k50`               | -0.0213 | -0.0117 | +0.0350 |            0.0350 |       NO        |   FAIL    |          FAIL          |
| `knn_win_rate_k200`              | -0.0204 | -0.0114 | +0.0485 |            0.0485 |       NO        |   FAIL    |          FAIL          |
| `knn_top3_rate_k50`              | -0.0435 | -0.0337 | +0.0333 |            0.0435 |       NO        |   FAIL    |          FAIL          |
| `knn_top3_rate_k200`             | -0.0391 | -0.0291 | +0.0437 |            0.0437 |       NO        |   FAIL    |          FAIL          |
| `knn_finish_norm_mean_k50`       | +0.0604 | +0.0516 | -0.0148 |            0.0604 |       NO        |   FAIL    |          FAIL          |
| `knn_finish_norm_mean_k200`      | +0.0588 | +0.0480 | -0.0275 |            0.0588 |       NO        |   FAIL    |          FAIL          |
| **`knn_mkt_residual_mean_k50`**  | +0.0646 | +0.0611 | +0.0463 |            0.0646 |     **YES**     | **PASS**  |          FAIL          |
| **`knn_mkt_residual_mean_k200`** | +0.0782 | +0.0644 | +0.0548 |            0.0782 |     **YES**     | **PASS**  | FAIL (just under 0.08) |

**Interpretation**: this is a clean, interpretable split, not a marginal noisy
result. The three candidates that replicate the closed campaign's target design
(raw neighbor `finish_norm`, win-rate, top3-rate) fail on sign-instability across
years — consistent with the closed campaign's own finding that this signal shape
is redundant with what the champion GBDT already extracts, now confirmed to hold
even under the physical/style*pace/speed_time vector composition, i.e. the
redundancy is about the \_target shape* more than the _feature family_. The one
target genuinely new to this codebase — "did similar horses beat their own
market expectation" — is the only one that shows a real, sign-stable signal.
That said, both passing values (0.065, 0.078) sit **below** the 0.08 bar the
closed campaign used, and are **smaller** than the 0.11 signal
`knn-feature-cheapfilter.md` measured for its (also-passing) V4 score — which
still failed to convert into a model gain (top1 -0.72pp). This probe result is
necessary-condition evidence at best, and weaker necessary-condition evidence
than a case that already failed the sufficiency test.

## 4. Decision gate

> = 1 candidate passed → proceed to WF (§5), restricted to the single strongest
> surviving column per the near-duplicate-collapse rule (k=50 and k=200 are the
> same aggregation type; k=200 has the larger |partial ρ|): **`knn_mkt_residual_mean_k200`**
> only.

## 5. Walk-forward A/B

Harness: `retest_wf_vecknn.py`, a copy of the validated
`tmp/candidate-masked-lever-retest/retest_wf.py` pattern (control arm reuses the
cached `tmp/candidate-masked-lever-retest/models/base` predictions — verified
byte-exact row-count match against that cache for all 3 folds: 2023
tr=485,275/va=47,274; 2024 tr=532,549/va=46,752; 2025 tr=579,301/va=47,497 —
before trusting it, so only the candidate arm's 9 models were trained fresh;
`paired()`/`gate()`/cell sort-before-mask copied verbatim, not re-derived).
Candidate arm = armB-250 + `knn_mkt_residual_mean_k200` (251 features). CatBoost
YetiRank, iterations=300, depth=8, lr=0.05, l2=3.0, no early-stop, all-numeric,
seeds {42,101,2026}, 3 folds (blind 2023/2024/2025). Elapsed: 255.8s for the
resumed run (4 remaining models + full aggregation; see execution note below).

**Pooled seed-averaged** (base_pct / cand_pct / delta_pp / lb95_pp, n=10,365
races) — independently re-derived from `wf_result.json`, matches the executing
agent's report exactly:

| Metric     |   base |   cand | delta (pp) | LB95 (pp) |
| ---------- | -----: | -----: | ---------: | --------: |
| top1       | 33.796 | 33.915 |     +0.119 |    -0.058 |
| place2     | 18.119 | 18.296 |     +0.177 |    -0.029 |
| place3     | 14.163 | 14.221 |     +0.058 |    -0.174 |
| place4     | 12.166 | 12.063 | **-0.103** |    -0.325 |
| place5     | 11.076 | 11.040 |     -0.035 |    -0.244 |
| place6     | 10.416 | 10.629 |     +0.212 |    +0.013 |
| top3_box   |  9.410 |  9.464 |     +0.055 |    -0.045 |
| fukusho_2p | 74.912 | 74.880 |     -0.032 |    -0.199 |

**Gate** (`pooled_seedavg_gate`, verbatim, independently re-read from
`wf_result.json`):

```json
{
  "primaries_passed": { "top1": false, "place2": false, "place3": false },
  "n_primaries_passed": 0,
  "primaries_lb95_positive": { "top1": false, "place2": false, "place3": false },
  "n_lb95_positive": 0,
  "place2_or_place3": false,
  "worst_delta_pp": -0.1029,
  "ACCEPT_strict_gate": false
}
```

`worst_delta_pp` (-0.1029, `place4`) **breaches** the -0.05pp no-regression
floor — this is not merely "no gain," it is a small confirmed regression on a
secondary metric. Pooled top1 delta (+0.119pp) does **not** clear this repo's
established +0.4pp single-arm retrain noise floor (well under a third of it).

**Per-fold** (delta_pp [lb95_pp], seed-averaged, ~3,455-3,741 races/fold):

| Fold | top1            | place2              | place3          |
| ---- | --------------- | ------------------- | --------------- |
| 2023 | -0.058 [-0.376] | -0.203 [-0.588]     | +0.125 [-0.309] |
| 2024 | +0.203 [-0.106] | **+0.463 [+0.058]** | +0.183 [-0.183] |
| 2025 | +0.212 [-0.116] | +0.270 [-0.097]     | -0.135 [-0.492] |

top1's sign flips between 2023 (negative) and 2024/2025 (positive) — not a
consistent directional effect across blind years. Only one cell in this whole
9-cell fold x primary-metric grid (2024 place2) individually clears LB95>0.

**Summer-4-venue restricted** (`keibajo_code` in 01/02/03/10, n=2,448): top1
delta = **-0.150pp** [LB95 -0.545] — negative, i.e. this is not a hidden
summer-venue rescue case; place2 +0.150 [-0.314], place3 +0.381 [-0.082],
top3_box +0.163 [-0.027], fukusho_2p -0.041 [-0.368]. No metric clears LB95>0
on this cut.

**Cell scan** (`keibajo_code` / `kyori_band` / `season_band` /
`current_baba_condition`, n>=200, sort-before-mask): 8 of 22 scanned
cell x metric combinations individually clear LB95>0 (`keibajo_code=10`
top3_box +0.63[+0.25] n=792; `keibajo_code=08` top1 +0.50[+0.07] n=1,535;
`keibajo_code=05` top1 +0.56[+0.08] n=1,607; `keibajo_code=04` place2
+1.14[+0.43] n=935; `kyori_band=1` top1 +0.38[+0.02] n=3,276; `season_band=3`
top3_box +0.25[+0.04] n=2,508; `season_band=2` top1 +0.47[+0.11] n=2,495;
`current_baba_condition=3` place3 +0.89[+0.12] n=859). Read as **unadjusted
multiple-comparisons noise, not a cell-conditional adoption candidate**: no
repeated-measurement confirmation, no coherent story linking the hits (three
different venues hit on three different metrics; the "wins" don't cluster
around the summer-4-venue set this campaign is targeting, and the
summer-4-venue restricted cut above is directly negative on top1). Not pursued
further given the pooled gate's clean fail and the place4 regression — chasing
8/22 nominal hits with no adjustment for 22 comparisons would be exactly the
kind of unprincipled cell-mining this repo's §7.2 cell-conditional-adoption
rule is not meant to license.

**Execution note (for the record)**: the first WF attempt stalled — the
backgrounded training process died silently after completing 5 of 9 candidate
models (all of seed42 + seed101 fold-2023/2024), with no error logged, while
the executing agent incorrectly believed it was waiting on a valid background
completion notification. Caught by the orchestrator via direct process
inspection (`ps aux` showed no live training process; the stdout log had
stopped mid-stream ~59 minutes earlier) after a status check-in. Resumed as a
single foreground blocking call, which completed cleanly in 255.8s reusing the
5 already-trained models (`train_fold()`'s existing-file check) and training
only the remaining 4. No numeric results were affected — this only cost wall
clock, not correctness.

## 6. MLflow record

Logged via `apps/mlflow`'s `log-training-run` CLI
(`hr-mlflow-training-run/v1` manifest, schema read directly from
`training_run.py` before construction) to the real Neon-backed MLflow tracking
store (not a smoke-test destination). Independently verified (not just taken on
the executing agent's word) via `MlflowClient.get_run` against the actual
backend:

- **run_id**: `00a02dd756ee47749ed806a5fc7b2288`
- **experiment**: `finish-position/wf-eval` (confirmed via `get_experiment` —
  correct 0-1-fraction-scale offline-WF experiment, not `serve-accuracy`)
- **status**: `FINISHED`, 16 metrics logged (`top1_delta_pp`, `top1_lb95_pp`,
  `top1_base_pct`, `top1_cand_pct`, and the equivalent quartet for the other 4
  gated metrics)
- **tags** (confirmed present, values match): `eval_regime=oos`,
  `task=finish-position`, `category=jra`,
  `model_version=probe-vector-knn-retrieval-2026-07-17`,
  `lever=vector_knn_retrieval`, `verdict=REJECT`,
  `gate_ACCEPT_strict_gate=False`, `top1_delta_pp=+0.1190`,
  `top1_clears_0p4pp_noise_floor=False`, `noise_floor_pp=0.4`,
  `probe_max_abs_partial_rho=0.0782`, `probe_PASS_0p02_gate=True`,
  `probe_PASS_0p08_strict_gate=False`, `embedding_family=physical+style_pace+speed_time (odds-excluded, recent_form/career_ability-excluded)`,
  and a `prior_art_precedent` tag quoting the `knn-feature-cheapfilter.md`
  finding directly.
- No registry/champion side effects (`register=false`, `champion=false`) —
  this is a research-result log only, appropriate for a REJECT.

## 7. Verdict

**REJECT.** Pooled 0/3 primaries, a confirmed (if small) `place4` regression
breaching the no-regression floor, a negative summer-4-venue top1 delta, and a
cell scan whose 8/22 nominal hits show no coherent pattern and are read as
multiple-comparisons noise rather than a routing candidate. Pooled top1 delta
does not clear this repo's noise floor.

The result is more informative than a bare REJECT, though, and is worth
recording precisely for future lever-scouting:

1. Of 8 candidate aggregations, only the one genuinely novel to this codebase —
   `knn_mkt_residual_mean` ("did similar horses beat their own market
   expectation," not raw neighbor finish/win/top3 rate) — passed the
   sign-stability probe gate, and it is the _first_ kNN-retrieval variant
   across this codebase's whole vector-search history (the 5 docs in
   §1.2 plus this one) to clear that bar with 3/3-year sign stability. All
   three deliberate differentiators from the closed campaign (physical/
   style_pace/speed_time embedding instead of recent_form/career_ability;
   pooled JRA on the current clean armB-250 champion instead of stale
   per-class iter19/iter20; this novel residual target) were real and
   individually justified — and still converged to the same WF outcome as
   every prior kNN design in this codebase.
2. This reinforces, rather than weakens, the closed campaign's standing
   diagnosis: **partial-ρ orthogonality against the champion's own score is a
   necessary but not sufficient condition for a model gain, because CatBoost's
   own splits already capture equivalent within-race-relative structure** —
   now confirmed to hold across a materially different feature family, a
   materially different (and cleaner, newer) baseline, pooled instead of
   per-class scope, and a genuinely new neighbor-outcome target. The
   mechanism does not appear to be feature-composition-specific; it looks
   like a structural property of how boosted-tree ranking models absorb
   smoothed within-race-relative signal, additive-feature or otherwise.
3. **Recommendation**: add this doc to the DO-NOT-RETEST index alongside the 5
   closed `docs/finish-position-accuracy/per-class/jra/` kNN docs. Any future
   kNN/vector-retrieval proposal for JRA finish-position should be required to
   either (a) supply a genuinely new _input signal_ not already reachable from
   the champion's 250 features (this probe, like all its predecessors, only
   recombined already-in-champion columns), or (b) use a fundamentally
   non-additive-feature architecture (the closed campaign's learned-embedding
   - RL attempt is the only architecturally distinct thing tried, and it
     failed far worse) — a same-inputs, additive-feature kNN design is now
     closed across four independent target-shape variants (raw finish_norm,
     win-rate, top3-rate, market-beat residual) and two independent embedding
     compositions (recent_form/career_ability, physical/style_pace/speed_time).

No deploy. This closes the "ベクトル化とベクトル検索" lever as specified in
today's brief; reported back to the orchestrator for the campaign log.

## Artifacts

- `apps/pc-keiba-viewer/tmp/vector-knn-retrieval/knn_features.parquet` — 8
  candidate columns, 626,798 rows, keyed by (`race_id`, `ketto_toroku_bango`)
- `apps/pc-keiba-viewer/tmp/vector-knn-retrieval/probe_result.json`
- `apps/pc-keiba-viewer/tmp/vector-knn-retrieval/wf_result.json`
- `apps/pc-keiba-viewer/tmp/vector-knn-retrieval/{build_knn_features.py, probe_vecknn.py, retest_wf_vecknn.py, build_mlflow_manifest.py}`
- MLflow run `00a02dd756ee47749ed806a5fc7b2288` (`finish-position/wf-eval`)
- (not committed — `tmp/` per repo convention)
