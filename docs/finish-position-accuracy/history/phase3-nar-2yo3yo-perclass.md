---
iteration: 37
date: 2026-06-11T00:00:00+09:00
phase: phase3-4a-nar-2yo3yo-perclass
lever: NAR 2YO / 3YO age-class per-class ensembles (Phase 3 #4a)
status: REJECT both 2YO and 3YO — no robust gain detected; no deployment
quality_gate: n/a — experiment scripts live under tmp/dq/nar_age/ (not an enforced package)
model_version_nar: per-class production config UNCHANGED (all iter30/iter36 ensembles live)
---

## Context

Commit `aa2afd8` split 2YO and 3YO out of the diluted `"other"` bucket in the NAR
`nar_subclass` regex (adding `'２歳|2歳' THEN '2YO'` and `'３歳|3歳' THEN '3YO'` arms
BEFORE the `Ａ`/`Ｂ`/`Ｃ` arms). This correctly classifies 48,506 races (19.8% of
NAR) that were previously mis-routed: 2YO=14,636, 3YO=33,870. However, those races
still route to the global iter12 fallback at inference time because no per-class ensemble
existed for them. Phase 3 #4a was the attempt to train dedicated per-class ensembles and
capture the potential gain.

### Re-label path used

The feature parquets (`feat-nar-v8-iter26-relationships`) do NOT contain `nar_subclass`
— the label is derived from PG (`nvd_ra.kyoso_joken_meisho`) at training time. The
cheapest path was therefore to reuse the existing feature parquets and apply the new
`2YO`/`3YO` SQL arms during training (no full rebuild needed). This was confirmed by
checking that `pg_class_map_df` with the new SQL correctly classifies 2YO=15,674 and
3YO=57,770 total NAR races.

## Training recipe

Iter 37 residual (same recipe as iter 30, adapted for 2YO/3YO):

- **Base model**: `iter12-nar-xgb-hpo-v8` (NAR global baseline)
- **Residual model**: CatBoost YetiRank, NDCG@3 eval metric
- **Features**: `feat-nar-v8-iter26-relationships` (173 cols) + `iter12_score` (174 total)
- **Class filter**: target class ONLY (no chain inclusion — 2YO/3YO are age-class
  buckets, not part of the A/B/C/OP tier hierarchy)
- **Walk-forward folds**: 2007–2026 (20 folds), train_start=2006
- **CB params**: depth=8, lr=0.05, l2=3.0, iter=1000, early_stopping=30
- **Time decay**: 0.5–1.0 linear weight on training rows
- **Nested split**: inner 2018–2020 / tuning (Optuna) 2021–2022 / holdout 2023–2026

Scripts:

- `tmp/dq/nar_age/train_nar_age_residual.py`
- `tmp/dq/nar_age/judge_nar_age.py`
- Summary JSONs: `tmp/dq/nar_age/train_2YO_summary.json`, `train_3YO_summary.json`
- Judge output: `tmp/dq/nar_age_judge.json`

## Per-class results (holdout 2023–2026)

### 2YO

n=2,176 holdout races; baseline top1=60.85%;
MDE (80% power, α=0.05, one-sided) ≈ 2.602pp

| axis       | baseline | candidate | Δpp    | delta LB95 (bs×10k) |
| ---------- | -------- | --------- | ------ | ------------------- |
| top1       | 60.85%   | 60.85%    | 0.000  | 0.000               |
| place2     | 37.27%   | 37.27%    | 0.000  | 0.000               |
| place3     | 31.30%   | 31.34%    | +0.046 | n/a                 |
| top3_box   | 40.35%   | 40.40%    | +0.046 | n/a                 |
| fukusho_2p | 90.44%   | 90.44%    | 0.000  | 0.000               |

Best blend weights: iter12 @ 0.690 + iter37-2YO-residual @ 0.310 (tuning top1 = 62.31%)

**REJECT** — no primary LB95 > 0 (fukusho_2p LB95 = 0.0000, top1 LB95 = 0.0000).
The ensemble is essentially degenerate: on the top1/fukusho_2p axes the blend converges
to the iter12 baseline (the residual adds no signal detectable above bootstrap noise).
Note: 2YO holdout is borderline powered (n=2,176; MDE≈2.6pp — the detector needs a
+2.6pp effect to reliably fire; any real gain smaller than that is invisible).

### 3YO

n=8,578 holdout races; baseline top1=59.63%;
MDE (80% power, α=0.05, one-sided) ≈ 1.317pp

| axis       | baseline | candidate | Δpp    | delta LB95 (bs×10k) |
| ---------- | -------- | --------- | ------ | ------------------- |
| top1       | 59.63%   | 59.64%    | +0.012 | -0.035              |
| place2     | 36.21%   | 36.16%    | -0.047 | n/a                 |
| place3     | 27.93%   | 28.01%    | +0.082 | n/a                 |
| top3_box   | 35.45%   | 35.47%    | +0.023 | n/a                 |
| fukusho_2p | 88.88%   | 88.93%    | +0.047 | -0.012              |

Best blend weights: iter12 @ 0.500 + iter37-3YO-residual @ 0.500 (tuning top1 = 59.13%)

**REJECT** — no primary LB95 > 0 (fukusho_2p LB95 = −0.012, top1 LB95 = −0.035).
The point-estimate deltas are positive on top1 (+0.012pp), place3 (+0.082pp), and
fukusho_2p (+0.047pp), but none clears bootstrap LB95 > 0. The gains are inside the
bootstrap noise band; 3YO is better powered than 2YO (n=8,578) but still below the
MDE≈1.317pp needed to reliably detect a real gain at this effect size.

## Root cause diagnosis

Both 2YO and 3YO fail for the same structural reason: **the residual adds no signal
beyond the iter12 baseline on the age-class races**. The tuning-window top1 for the
50/50 blend (3YO: 59.13%) equals the iter12 baseline top1 — Optuna finds equal weights
are locally optimal, which means the residual model's per-race ranking is not
systematically better than the baseline on this age-class slice.

This likely reflects that the feature set (`feat-nar-v8-iter26-relationships`) does not
contain age-specific signals that would help differentiate horses within a 2YO or 3YO
race above the global model's performance. The global iter12 model already sees all
NAR races (including 2YO/3YO) and is calibrated on the full distribution; the per-class
residual needs to find _incremental_ signal within the slice, but the slice's feature
distribution is not distinct enough from the full set to yield a detectable boost with
the current features.

## Power analysis

| class | n (holdout) | MDE @ 80% power | interpretation                            |
| ----- | ----------- | --------------- | ----------------------------------------- |
| 2YO   | 2,176       | ≈ 2.602 pp      | underpowered; gains < 2.6pp undetectable  |
| 3YO   | 8,578       | ≈ 1.317 pp      | borderline; observed gain +0.012pp << MDE |

The 3YO case is interesting: n=8,578 is above the iter30 minimum (200 races), but the
observed Δpp (+0.012pp on top1) is ~100× smaller than the MDE. The lack of a signal is
not purely a power problem — the effect size itself is negligible.

## Decision

**REJECT both 2YO and 3YO.** No per-class ensemble registered. Production config
unchanged: 2YO and 3YO races continue to route to the `iter12-nar-xgb-hpo-v8` global
fallback (via `normalize_class_code` in `per_class.py` folding unregistered codes to
`"other"` and the `"other"` ensemble being served by `iter30-nar-cb-ensemble-other-v8`).

Wait — correction: 2YO and 3YO are not in `NAMED_PER_CLASS_CODES_BY_CATEGORY["nar"]`
(which only lists `{"NEW", "MUKATSU", "C", "B", "A", "OP"}`), so they collapse via
`normalize_class_code` to `"other"` and hit `iter30-nar-cb-ensemble-other-v8`. The
commit `aa2afd8` correctly carved them out of `"other"` in the feature-build label;
however in `per_class.py` they still normalize to `"other"` — which is the correct
production behavior until/unless a per-class ensemble is registered for them.

No code change, no commit to `per_class.py`.

## Artifacts

| file                                                             | description                       |
| ---------------------------------------------------------------- | --------------------------------- |
| `tmp/dq/nar_age_judge.json`                                      | combined judge output (2YO + 3YO) |
| `tmp/dq/nar_age/judge_2YO.json`                                  | individual 2YO result             |
| `tmp/dq/nar_age/judge_3YO.json`                                  | individual 3YO result             |
| `tmp/dq/nar_age/train_2YO_summary.json`                          | 2YO WF training summary           |
| `tmp/dq/nar_age/train_3YO_summary.json`                          | 3YO WF training summary           |
| `tmp/dq/nar_age/train_nar_age_residual.py`                       | training script                   |
| `tmp/dq/nar_age/judge_nar_age.py`                                | judge script                      |
| `tmp/bucket-eval/finish-position/iter37-nar-cb-residual-2YO-v8/` | 2YO WF predictions                |
| `tmp/bucket-eval/finish-position/iter37-nar-cb-residual-3YO-v8/` | 3YO WF predictions                |

All artifacts are under `tmp/` (git-excluded).

## Next iteration recommendation

The per-class CatBoost residual approach does not capture useful signal for 2YO/3YO
races. Possible next levers if this class is prioritized:

1. **Age-specific features**: Add features that are distinctive for 2YO/3YO races —
   e.g., debut indicator, career_race_count, days_since_first_race, foal_month.
2. **Objective alignment**: Try a place-weighted LambdaRank (like the H4 iter36 approach
   for class C) to optimize fukusho_2p directly instead of top1.
3. **More data**: Wait for the 2YO/3YO pool to grow; holdout n=2,176 for 2YO is
   genuinely underpowered.
4. **Global model retrain with correctly-labeled data**: A full retrain of iter12
   with the corrected 2YO/3YO labels (so the model itself learns on clean labels) may
   produce better per-class predictions than a residual on top of a model trained on
   mislabeled data.

## Quality Gate Results

- tsc: n/a — no TypeScript changed
- lint: n/a — experiment scripts under tmp/ (not enforced package)
- format:check: n/a
- test:coverage: n/a — no enforced-package file modified
- python:check: n/a — experiment scripts under tmp/ (not enforced package)
- per_class.py: UNCHANGED (REJECT → no registration)
