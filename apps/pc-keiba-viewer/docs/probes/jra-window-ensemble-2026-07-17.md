# JRA window-offset ensemble probe — fixed 0.5/0.5 z-average of adjacent training-window CatBoost models (2026-07-17)

- **Date**: 2026-07-17
- **Category**: JRA finish-position ensembling lever probe. Cheap, cached-model-only —
  no training performed in this task, only `.predict()` calls on 9 already-cached
  CatBoost models plus a small amount of DuckDB/Polars aggregation.

## Background / motivation

Does a fixed 0.5/0.5 within-race-z-score-averaged ensemble of two CatBoost models
trained on **adjacent, overlapping training-window cutoffs** (e.g. "trained through
2023" + "trained through 2024") beat the single model trained through the later
cutoff alone, when both are evaluated on the same blind year? This is a test of
"bagging over time" — using the champion architecture/feature-spec/hyperparameters
unchanged, but drawing diversity purely from shifting the training window's end date
by one year, rather than from a different seed, a different architecture, or a
different (narrower) feature/row scope.

All 9 model artifacts already existed before this probe started
(`tmp/candidate-masked-lever-retest/models/base/seed{42,101,2026}/fold-{2023,2024,2025}/model.json`,
clean armB-250 spec, `iterations=300, depth=8, lr=0.05, l2_leaf_reg=3.0`,
`random_seed=seed_base+fold_year`) — trained and verified correct by multiple prior
probes this same day. `fold-2023` = trained on data from 2013-01-01 through
end-2022, `fold-2024` = trained through end-2023, `fold-2025` = trained through
end-2024. Nothing was retrained; this probe only runs new **inference** passes
(some of them genuinely new — see Design below) and computes paired comparisons.

## Dedup — why this is not a re-test of either superficially-similar closed lever

Two prior results in this campaign look superficially similar to "ensemble two
CatBoost models." Both are genuinely different designs, for different, specific
mechanistic reasons — not just relabeled to dodge a do-not-retest rule.

### (1) Not a re-test of `jra-cb-v9sim-seedensemble-mean-5fold` (2026-07-11 REJECT)

That MLflow-recorded run (`verdict=reject-do-not-retest-same-arch-seed`) ensembled
**3 seeds trained on the IDENTICAL training window** — 5-fold pooled 2021–2025
(n=17,277), top1 +0.156pp but LB95 crossed 0 (not significant), worst primary
place2 −0.017pp, 0/64 cells adopted. In that design the _only_ source of
diversity between ensemble members is CatBoost's internal `random_seed` — same
rows, same date cutoff, same everything else. Averaging 3 draws from what is
statistically closer to "the same fit with different random initialization" than
to genuinely different models.

This probe's diversity axis is categorically different: the two ensemble members
are trained on **different data** — one actually includes 2023's races in its
fit and the other doesn't. That is a real, structural difference in what
information entered the model, not a resampling of the same information. Whether
that structural difference is _large enough_ to produce useful decorrelation is
exactly the empirical question this probe tests — but it is not the same
experiment re-run under a new file name.

### (2) Not governed by `feedback_blend_precedent_needs_complementary_model`

That memory note captures a real, previously-tested failure mode: the
`iter40-nar-settransformer-blend-v1` precedent (a standalone-weak Set Transformer
blended 0.5/0.5 with the champion XGBoost, still nets a real gain) does **not**
generalize to "blend the champion with any weak model." Tested and REJECTED
2026-07-12 (Kochi round-2 lever 3): blending the champion with round-1's
`arm_a_final_only` specialist — same architecture, but trained on a
**data-starved narrow subset** (17K–20K rows vs. the champion's 2.2–2.5M) —
monotonically got _worse_ as the specialist's weight increased. The documented
mechanism: iter40's transformer helps because it is (a) a genuinely different
architecture whose errors are decorrelated from the champion's by construction,
**and** (b) trained on comparably full data breadth. `arm_a_final_only` fails
both conditions — same architecture (no decorrelation from architecture) _and_
a starved subset (its "weakness" is just noise from insufficient data, which
dilutes rather than complements the champion).

This probe's design satisfies **neither** failure-mode diagnostic cleanly, and
does not claim to satisfy the iter40 success condition either:

- It is **not** architecturally decorrelated like iter40 (both members are the
  identical CatBoost/armB-250/YetiRank spec — same architecture, same features,
  same hyperparameters).
- It is **not** the Kochi data-starvation shape either: `fold-2023` trains on
  2013–2022 (≈10 years) and `fold-2024` trains on 2013–2023 (≈11 years) — both
  are full-breadth, multi-million-row fits over the entire historical window,
  differing by **one year out of ten-to-thirteen**, not by three orders of
  magnitude in row count. Neither member is a narrow/starved subset.

In other words, this is a **third category**, distinct from both precedents: same
architecture (unlike iter40), same data breadth (unlike Kochi/`arm_a_final_only`),
diversity purely from a **shifted, not shrunk,** training window — closer to
bagging-over-time than to either architectural complementarity or a
data-starved-specialist blend. Because it doesn't cleanly match the mechanism
of either precedent, it also cannot borrow either precedent's verdict — its
justification has to stand or fall on its own evidence, which is what the rest
of this document reports.

## Design & harness

Two independent blind-fold tests, both reusing the same 9 cached models, no
training:

- **Primary** (headline verdict target): blind year **2025**. Standalone arm =
  `fold-2025` model (trained through 2024) scored on the 2025 validation split —
  the standard "champion(≤2024)" control every other probe today uses for a 2025
  blind fold. Ensemble arm = `0.5·z(fold-2024 model on 2025)` +
  `0.5·z(fold-2025 model on 2025)`. Scoring `fold-2024` against 2025 is a new
  inference pass — that model was previously only ever evaluated on its own
  designated blind year (2024).
- **Consistency** (supporting/contextual evidence, not a second adoption gate):
  blind year **2024**. Standalone arm = `fold-2024` model (trained through 2023)
  on the 2024 validation split. Ensemble arm =
  `0.5·z(fold-2023 model on 2024)` + `0.5·z(fold-2024 model on 2024)`.

Both tests: **no weight sweep, 0.5/0.5 fixed** throughout (avoids reintroducing a
selection-bias vector, matches the iter40 precedent's use of a predetermined,
not-searched blend weight). 3 seeds (42/101/2026) per arm, seed-averaged for the
headline numbers — scores are never mixed across different `seed_base` values;
each seed's own `fold-{2023,2024,2025}` models are used together throughout that
seed's own fit/apply chain.

Harness: `tmp/ms-window-ensemble/window_ensemble_wf.py`. Helper functions
(`load_store`, `predict_raw`, `zscore_within_race`, `rank_from_score`,
`per_race_hits`, `paired`, `gate`, `avg_hits`) copied byte-identical in logic
from `tmp/ms-contender-meta/contender_meta_wf.py` (today's most-recently-audited
version), with two narrow, explicitly-flagged deviations: (a) `load_store()`'s
needed-columns list drops the `BOOST_FEATS` term since this design has only one
feature spec (armB-250, used by every model, no sub-model); (b) `predict_raw()`
passes `thread_count=6` explicitly to `CatBoost.predict()` per this task's
inference-thread-cap constraint (the precedent script left it at the CatBoost
library default of `-1` because nothing in its own instructions capped it).
Same constants throughout: `METRICS` = 8-metric rank1–6 + top3_box + fukusho_2p,
`PRIMARIES = [top1, place2, place3]`, `GATE_MIN_DELTA=0.08`, `GATE_NO_REG=-0.05`,
`N_BOOT=2000`, `BOOT_SEED=20260519`, `CELL_DIMS` = 5-dim (`keibajo_code`,
`kyori_band`, `season_band`, `current_baba_condition`, `grade_code`),
`CELL_MIN=200`, `SUMMER_VENUE_CODES=["01","02","03","10"]`, `NOISE_FLOOR_TOP1=0.4`.

Sort-before-mask discipline (the historical `retest_wf.py` bug this campaign
fixed once already) is followed for every cell-scan mask: `standalone_hits`/
`ensemble_hits` are joined to the cell-dimension frame and `.sort("race_id")`ed
_before_ any boolean mask array is built from them, because `paired()`
internally re-sorts both of its inputs by `race_id` before joining — a mask
built against an unsorted frame would silently apply to the wrong races once
`paired()` re-sorts.

Each distinct `(seed_base, fold_year)` model is scored via `.predict()` exactly
once per test and the raw score reused for both its "standalone" role (when
that fold IS the test's `standalone_fold`) and its "ensemble constituent" role
— no redundant re-scoring. Total run: DuckDB store load (626,798 rows,
2013–2025, `memory_limit='6GB'`, `threads=4`) + 12 CatBoost `.predict()` calls
(2 distinct fold-year models × 3 seeds × 2 tests) + cell scans, **4.3 seconds
wall-clock** (warm OS page cache from an earlier probe run today against the
same store).

## Results — Primary test (blind year 2025)

Standalone = `fold-2025` (trained through 2024), n=47,497 rows / 3,455-ish
races (seed-averaged hit tables). Ensemble = `0.5·z(fold-2024) + 0.5·z(fold-2025)`
on the same 2025 validation split.

| metric     | standalone | ensemble | Δpp    | LB95pp |
| ---------- | ---------- | -------- | ------ | ------ |
| top1       | 33.140%    | 33.333%  | +0.193 | -0.058 |
| place2     | 18.360%    | 18.505%  | +0.145 | -0.145 |
| place3     | 13.768%    | 13.806%  | +0.039 | -0.241 |
| place4     | 12.233%    | 12.089%  | -0.145 | -0.425 |
| place5     | 11.394%    | 11.298%  | -0.097 | -0.376 |
| place6     | 10.005%    | 10.246%  | +0.241 | +0.000 |
| top3_box   | 9.426%     | 9.397%   | -0.029 | -0.184 |
| fukusho_2p | 74.771%    | 74.867%  | +0.097 | -0.125 |

**Gate**: `n_primaries_passed=0/3`, `primaries_lb95_positive` all `false`,
`place2_or_place3=false`, `worst_delta_pp=-0.145` (place4, below the
`-0.05pp` no-regression floor), **`ACCEPT_strict_gate=false`**.

All three primaries are directionally positive but every one of them has a
negative LB95 — none is statistically distinguishable from zero, and the point
estimates themselves (+0.19/+0.15/+0.04pp) sit inside the same noise band this
campaign has repeatedly measured for pure architectural variants.

## Results — Consistency test (blind year 2024)

Standalone = `fold-2024` (trained through 2023), n=46,752 rows. Ensemble =
`0.5·z(fold-2023) + 0.5·z(fold-2024)` on the same 2024 validation split.

| metric     | standalone | ensemble | Δpp    | LB95pp |
| ---------- | ---------- | -------- | ------ | ------ |
| top1       | 34.491%    | 34.424%  | -0.068 | -0.309 |
| place2     | 17.979%    | 17.960%  | -0.019 | -0.309 |
| place3     | 14.225%    | 14.380%  | +0.154 | -0.116 |
| place4     | 11.426%    | 11.359%  | -0.068 | -0.338 |
| place5     | 10.683%    | 10.645%  | -0.039 | -0.290 |
| place6     | 10.664%    | 10.606%  | -0.058 | -0.299 |
| top3_box   | 9.738%     | 9.747%   | +0.010 | -0.116 |
| fukusho_2p | 76.047%    | 76.037%  | -0.010 | -0.212 |

**Gate**: `n_primaries_passed=0/3`, `primaries_lb95_positive` all `false`,
`place2_or_place3=false`, `worst_delta_pp=-0.068` (top1/place4 tie, below the
`-0.05pp` floor), **`ACCEPT_strict_gate=false`**. (This test is contextual
evidence only, per the task's scoping — not a second independent adoption
gate.)

## Cell scan (5 dims, n≥200, sort-before-mask)

**Primary (2025)**: 21 cells cleared the `n≥200` size floor across the 5 dims
(8 `keibajo_code`, 4 `kyori_band`, 4 `season_band`, 3 `current_baba_condition`,
2 `grade_code`). 4 cleared the §7.2 gate (delta≥0.08pp AND LB95>0 on ≥1
primary):

| dim          | value | top1 Δpp   | place2 Δpp | place3 Δpp | n   | clears via           |
| ------------ | ----- | ---------- | ---------- | ---------- | --- | -------------------- |
| keibajo_code | 04    | +0.347     | +0.579     | **+1.389** | 288 | place3 (LB95 +0.463) |
| kyori_band   | 0     | **+0.717** | +0.134     | +0.224     | 744 | top1 (LB95 +0.134)   |
| season_band  | 0     | +0.256     | **+0.585** | -0.037     | 911 | place2 (LB95 +0.037) |
| grade_code   | E     | **+0.551** | +0.276     | +0.000     | 726 | top1 (LB95 +0.046)   |

**Consistency (2024)**: 20 cells cleared the size floor, 1 cleared gate:

| dim                    | value | top1 Δpp   | place2 Δpp | place3 Δpp | n   | clears via         |
| ---------------------- | ----- | ---------- | ---------- | ---------- | --- | ------------------ |
| current_baba_condition | 3     | **+1.115** | -0.248     | +0.620     | 269 | top1 (LB95 +0.372) |

### Why these are not treated as adopt-worthy — direct cross-test replication check

The task's headline ADOPT-worthy determination is scoped to the primary
(2025) test's pooled gate (which fails cleanly, above), and these cell hits are
reported per the evaluation spec rather than as an independent adoption path.
But it's worth being concrete about _why_ they don't move the needle even as a
"conditional adopt-candidate" idea: with 21+20=41 cells scanned across the two
tests at a roughly one-sided ~2.5% nominal false-positive rate per cell (LB95
is the 2.5th-percentile bootstrap bound), 4–5 gate-clears is elevated in the
primary test specifically but not wildly outside what multiple comparisons
alone would produce, especially given the dims are not independent (e.g.
`grade_code=E` races and `kyori_band=0` races overlap heavily).

The decisive check: every one of the primary test's 4 "clearing" cells was
**directly cross-referenced against the same cell in the independent
consistency-2024 test** (same `keibajo_code`/`kyori_band`/`season_band`/
`grade_code` value, different blind year, different model pair):

| primary hit cell | primary Δpp (top1/place2/place3) | same cell, consistency-2024 Δpp |
| ---------------- | -------------------------------- | ------------------------------- |
| keibajo_code=04  | +0.347 / +0.579 / +1.389         | **-0.697 / -0.697 / -0.199**    |
| kyori_band=0     | +0.717 / +0.134 / +0.224         | **-0.471 / -0.300 / -0.086**    |
| season_band=0    | +0.256 / +0.585 / -0.037         | **-0.300 / -0.188 / +0.413**    |
| grade_code=E     | +0.551 / +0.276 / +0.000         | **-0.320 / +0.046 / +0.274**    |

Every single one flips to negative on top1 and/or place2 in the other blind
year (season_band=0's place3 is the sole metric that stays same-signed, and it
wasn't the metric that cleared gate in the primary test anyway). None of the 4
primary-test cell hits replicates directionally in the independent
consistency-2024 fold — this is exactly the signature multiple-comparisons
noise produces, not a real conditional effect. (The single consistency-test
hit, `current_baba_condition=3`, echoes only partially — top1 stays positive
in primary (+0.43pp) but place2/place3 flip — and it's irrelevant to the
headline verdict regardless, since consistency-2024 is contextual-only.)

## Summer-4-venue pooled (keibajo_code ∈ {01,02,03,10})

| test               | n   | top1 Δpp             | place2 Δpp           | place3 Δpp           | clears gate |
| ------------------ | --- | -------------------- | -------------------- | -------------------- | ----------- |
| Primary (2025)     | 792 | +0.295 (LB95 -0.210) | +0.295 (LB95 -0.337) | +0.084 (LB95 -0.463) | **no**      |
| Consistency (2024) | 840 | +0.040 (LB95 -0.516) | +0.119 (LB95 -0.516) | +0.000 (LB95 -0.516) | **no**      |

Small, non-significant positive point estimates in both tests, no clear.

## Cross-test sign consistency

| primary | primary_2025 Δpp | consistency_2024 Δpp | sign-consistent? |
| ------- | ---------------- | -------------------- | ---------------- |
| top1    | +0.193           | -0.068               | **NO**           |
| place2  | +0.145           | -0.019               | **NO**           |
| place3  | +0.039           | +0.154               | yes              |

Both arms are genuinely blind for their own test year — the ensemble's
constituent models never saw their own test year during training in either
test — so this is legitimate fold-consistency evidence, not a
selection-bias-prone re-use of the same data. 2 of 3 primaries (top1, place2)
flip sign between the two independent blind years. This is the same signature
seen in the cell cross-check above: whatever small positive tilt the ensemble
shows on 2025, it does not reproduce on 2024 with the adjacent model pair —
consistent with noise rather than a real, generalizable "bagging over time"
edge.

## Noise-floor check

| test               | pooled top1 Δpp | threshold | noise-suspect? |
| ------------------ | --------------- | --------- | -------------- |
| Primary (2025)     | +0.193          | 0.4pp     | **yes**        |
| Consistency (2024) | -0.068          | 0.4pp     | **yes**        |

Both tests sit below the campaign's established 0.4pp pure-retraining-noise
floor on the headline top1 metric (the primary test's positive point estimate
is well under half the floor; the consistency test isn't even positive).

## Verdict: REJECT

Every level of evidence agrees. The primary (2025) test's headline pooled gate
fails cleanly (0/3 primaries pass, all 3 LB95s negative, no-regression floor
breached by place4 at -0.145pp). The consistency (2024) test independently
fails the same gate with a different sign pattern (top1/place2 negative,
place3 positive). The two blind years disagree in sign on 2 of the 3 primary
metrics — the opposite of what a genuine, generalizable effect should produce
given how much training data the constituent models share (adjacent folds
differ by exactly one year out of ten-to-thirteen). Both tests' pooled top1
deltas sit below the 0.4pp noise floor. The cell scan's few gate-clears in the
primary test do not replicate directionally in the same cells' independent
consistency-2024 evaluation — textbook multiple-comparisons noise, not a
conditional effect worth carving out a routing rule for. Summer-4-venue
pooled is flat in both tests.

This result is a genuine, structurally distinct test of the "bagging over
time" idea (see Dedup above) — it is not redundant with the seed-ensemble
REJECT or governed by the iter40-blend-precedent caution — and it comes back
negative on its own terms: the one-year training-window shift between
adjacent folds is evidently not enough of a diversity signal for CatBoost/
armB-250/YetiRank to produce a decorrelated, complementary error pattern. The
champion's own year-over-year fold-to-fold variation (visible in how much the
_standalone_ baseline itself moves between the 2024 and 2025 tests — e.g.
top1 34.491% vs 33.140% — driven by which races fall in which blind year, not
by the ensembling question) dwarfs the tiny, sign-inconsistent ensembling
effect this probe was built to isolate.

**DO-NOT-RETEST this exact design**: a fixed 0.5/0.5 within-race-z-score-
averaged ensemble of two CatBoost models trained on adjacent (one-year-offset)
training-window cutoffs, on this armB-250/YetiRank/`iterations=300,depth=8,
lr=0.05,l2_leaf_reg=3.0` spec. Untested variants that remain open if a future
session wants to revisit the underlying "bagging over time" idea: (a) a
larger window offset (2+ years apart instead of adjacent, which might produce
more genuine decorrelation than a 1-year shift buried in an
otherwise-10-to-13-year-shared window); (b) a genuinely trained (not
hand-fixed) blend weight, selected on a held-out fold disjoint from both test
years (this probe deliberately used a fixed 0.5/0.5 per the task's explicit
anti-selection-bias instruction, so a weight sweep was never run); (c) more
than 2 constituent windows (e.g. 3-way ensemble across fold-2023/2024/2025,
which starts to resemble a rolling/expanding-window bagging ensemble rather
than a pairwise blend, at the cost of needing a 3-way weight scheme).

## 日本語まとめ

「隣接する学習ウィンドウ (例: 2023年まで学習 vs 2024年まで学習) で訓練した2つの
CatBoostモデルを、レース内z-score平均で0.5/0.5固定ブレンドすると、後年cutoffの
単体モデルより勝るか」を検証した。既存のキャッシュ済9モデル (3seed×3fold、
armB-250特徴量、再学習なし) のみを使い、新規推論のみで完結する安価なprobe。

**dedup**: 07-11 REJECT済の`jra-cb-v9sim-seedensemble-mean-5fold`(同一学習窓・
seedのみ違う3体平均)とは多様性の軸が根本的に異なる——今回は学習データ自体が
1年分違う。また`feedback_blend_precedent_needs_complementary_model`
(iter40 Set-Transformerブレンドの教訓は「アーキテクチャが違う」+「データ規模が
十分」の2条件が必要、Kochi `arm_a_final_only`はどちらも満たさず失敗)にも該当し
ない——今回は同一アーキテクチャだがKochiのようなデータ枯渇サブセットでもない
(fold-2023は2013-2022の約10年、fold-2024は2013-2023の約11年、どちらもフル
ブレッドスの学習)。両先例のどちらの機序にも当てはまらない第3のカテゴリであり、
本probe自身の結果でのみ是非が決まる。

**結果**: primary (2025年blind)は3主要指標とも方向性は正 (top1+0.193pp、
place2+0.145pp、place3+0.039pp)だが全てLB95が負で有意でなく、0.4ppのnoise
floorも下回る。consistency (2024年blind)はtop1-0.068pp、place2-0.019pp、
place3+0.154ppとsignが割れる。2独立blind年でtop1/place2の符号が反転してお
り、真の効果があれば再現するはずの一貫性がない。cell scanでprimary側4セルが
gate通過したが、同じセルをconsistency側で直接照合すると全て符号反転——多重
比較のノイズと判断するのが妥当。夏季4場pooledも両testで不合格。

**結論: REJECT**。同一設計 (隣接1年ずれ学習窓・CatBoost・0.5/0.5固定z平均
ブレンド、armB-250スペック) の再テストは禁止。今後の余地: (a) 2年以上離れた
窓オフセット、(b) held-outフォールドで学習した重み(今回は選択バイアス回避の
ため0.5/0.5固定)、(c) 3-way以上のrolling window ensemble。

## Artifacts

- Harness: `tmp/ms-window-ensemble/window_ensemble_wf.py`
- Full report (both tests, all 8 metrics, per-cell breakdown): `tmp/ms-window-ensemble/window_ensemble_report.json`
- Run log: `tmp/ms-window-ensemble/run.log`
- Reused (not modified, not retrained): `tmp/candidate-masked-lever-retest/models/base/` (9 cached CatBoost models, 3 seeds × 3 folds), `tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json` (armB-250 feature list)
- MLflow: `finish-position/wf-eval`, run id `410e318c24e24811b60efe0f69ced017`, `model_version=window-offset-ensemble-2025-2026-07-17`, `campaign=2026-07-17-window-ensemble`, `verdict=reject-do-not-retest-same-design`. Read back independently against the live Neon-backed store (`scheme=postgresql`, `status=FINISHED`) with all 79 metrics / 15 params / tags intact.
