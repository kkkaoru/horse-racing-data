---
iteration: 34
date: 2026-06-10T02:10:00+09:00
based_on_iteration: 33
follows: history/oi-2026-06-10-rounds-r2-r4-pgvector.md
lever: L-nar-similarity-member (R5 — NAR kNN "pgvector-style" member, judged on the FULL-NAR holdout)
status: REJECT all 7 NAR classes — adopted_classes=[]; the NAR similarity signal is orthogonal in feature space but carries no incremental finish-position signal
quality_gate: n/a — no code change adopted (R5 is evaluation-only; the two delivered wins are P0 routing already committed at iter32 + the eval-pipeline numpy-rescore speedup reused here)
model_version_jra: iter14-jra-cb-pacestyle-course-v8 + per-class ensembles (UNCHANGED — no JRA card on 2026-06-10)
model_version_nar: per-class production config (UNCHANGED — iter30 C/other ensembles + iter12 baseline fallback)
scope:
  venue: 大井 Ōi (keibajo_code=44) — target card
  judging_holdout: FULL NAR (all keibajo), per-class, 2023-26 — powered, unlike the underpowered Ōi-only slice from iter32/33
  target_card: 2026-06-10
  routing: C×5 / other×4 / B×3 races
  goal: source a GENUINELY-NEW signal (NAR similarity / kNN member) that is BOTH orthogonal AND finish-predictive, judged where n is large enough to clear LB95
probe:
  source: tmp/nar-perclass/nar_vec/probe_verdict.json
  year: 2022
  decorr_max: 0.95
  rule: "PROCEED iff >=half of powered(>=200 races) classes are decorrelated(<0.95) AND have winner signal AND null collapses"
  verdict: PROCEED (5/5 powered classes decorrelated + winner signal + null collapses)
  per_class: # spearman(vec,prod) / -rho(vec,finish) / -rho(prod,finish ref) / null-perm -rho / null_collapses / proceed
    C:
      {
        n_races: 7602,
        powered: true,
        spearman_vec_vs_prod: 0.8256,
        neg_rho_vec: 0.6448,
        neg_rho_prod_ref: 0.7860,
        null: 0.0023,
        null_collapses: true,
        proceed: true,
      }
    B:
      {
        n_races: 2155,
        powered: true,
        spearman_vec_vs_prod: 0.8157,
        neg_rho_vec: 0.6320,
        neg_rho_prod_ref: 0.7691,
        null: -0.0035,
        null_collapses: true,
        proceed: true,
      }
    other:
      {
        n_races: 2112,
        powered: true,
        spearman_vec_vs_prod: 0.8568,
        neg_rho_vec: 0.6487,
        neg_rho_prod_ref: 0.7758,
        null: -0.0024,
        null_collapses: true,
        proceed: true,
      }
    A:
      {
        n_races: 805,
        powered: true,
        spearman_vec_vs_prod: 0.8186,
        neg_rho_vec: 0.6113,
        neg_rho_prod_ref: 0.7625,
        null: -0.0072,
        null_collapses: true,
        proceed: true,
      }
    OP:
      {
        n_races: 363,
        powered: true,
        spearman_vec_vs_prod: 0.8386,
        neg_rho_vec: 0.6229,
        neg_rho_prod_ref: 0.7630,
        null: 0.0051,
        null_collapses: true,
        proceed: true,
      }
    MUKATSU:
      {
        n_races: 196,
        powered: false,
        spearman_vec_vs_prod: 0.8768,
        neg_rho_vec: 0.6609,
        neg_rho_prod_ref: 0.7394,
        null: 0.0338,
        null_collapses: true,
        proceed: true,
      }
    NEW:
      {
        n_races: 175,
        powered: false,
        spearman_vec_vs_prod: 0.8759,
        neg_rho_vec: 0.6561,
        neg_rho_prod_ref: 0.7124,
        null: 0.0521,
        null_collapses: false,
        proceed: false,
      }
strengthened_gate:
  split: nested — inner (fit blend weights) / tuning (pick rung) / holdout 2023-26 (touched once)
  multi_metric: ">=2 of {top1,place2,place3,top3_box} positive AND >=1 of {place2,place3} positive AND no axis < -0.05pp"
  significance: top1 paired-bootstrap (x10000, seed=42, race-resample) LB95 > 0
  family_wise: Holm across the 7 NAR classes (alpha=0.05)
judge:
  source: tmp/nar-perclass/nar_vec/r3-judge/{C,B,MUKATSU,A,OP,other,NEW,summary}.json
  vector_member: iter32-nar-vec-knn-{class}-v8 (NAR kNN similarity, reused the 665x numpy engine)
  adopted_classes: []
  holm_all_nonsignificant: true # every class adjusted p=1.0
key_insight: |
  The NAR similarity signal is genuinely ORTHOGONAL in FEATURE space — the probe confirmed it: within-race Spearman vs the production ranking is only ~0.82-0.88 (<0.95 = decorrelated), it has a real winner signal (vec -rho vs finish ~0.61-0.66, against production ref ~0.71-0.79), and its null-permutation -rho collapses to ~0 for every powered class. But it carries NO incremental FINISH-POSITION signal: the GBDT ensemble already captures the predictive content, so the orthogonal variance is target-noise rather than target-signal. The optimizer assigning the vector member ~0 weight in EVERY class (and the resulting holdout deltas ~0) is the cleanest possible saturation confirmation — a member with real, independent feature variance that the gate still cannot place because none of that variance helps predict the finish.
methodology_note: |
  R5 escalated the iter33 plan: instead of the underpowered Ōi-only slice (best top1 LB95 = -0.2593pp), the NAR similarity member was judged on the FULL-NAR per-class holdout (n = 26060 C / 7217 other / 7124 B etc.) where the same effect, if real, would clear LB95. A two-stage protocol was used: (1) a probe (decorrelation + winner-signal + null-permutation collapse) to confirm the member is not a degenerate copy and not pure noise BEFORE spending the judge budget, then (2) the strengthened judge (nested split + multi-metric + top1 paired-bootstrap LB95>0 + Holm across 7 classes). The probe PROCEEDED (real, decorrelated signal) and the judge still REJECTED every class — separating "orthogonal in feature space" from "incremental in target space" exactly as designed.
decision: |
  No model change adopted for the Ōi card or globally. The NAR similarity member is rejected in all 7 NAR classes; adopted_classes=[]. Tomorrow's Ōi prediction stands on the two genuine wins already delivered (P0 per-class routing fix + eval-pipeline numpy-rescore speedup), unchanged from iter32/33.
artifacts:
  probe: tmp/nar-perclass/nar_vec/probe_verdict.json
  probe_score: tmp/nar-perclass/nar_vec/probe_score_summary.json
  judge: tmp/nar-perclass/nar_vec/r3-judge/{C,B,MUKATSU,A,OP,other,NEW,summary}.json
  full_score: tmp/nar-perclass/nar_vec/full_score_summary.json
  oi_crosscheck: tmp/nar-perclass/nar_vec/oi_crosscheck.json
  prediction_log: ~/Library/Logs/finish-position-predict/20260610.log
---

## What was tried

This is the R5 follow-on to [`oi-2026-06-10-rounds-r2-r4-pgvector.md`](oi-2026-06-10-rounds-r2-r4-pgvector.md) (iter 33), which closed with the recommendation that the only credible path for tomorrow's Ōi card is a **global-NAR-powered judging of a genuinely-new signal** — a NAR similarity / pgvector member, mirroring the JRA pgvector probe, evaluated on the full-NAR holdout where n is ~10–25× the Ōi slice. R5 builds exactly that member and judges it.

**Lever R5 — NAR kNN similarity ("pgvector-style") member.** A within-race kNN similarity ranking member (`iter32-nar-vec-knn-{class}-v8`) was constructed for each NAR class and blended into the production per-class ensemble, then judged on the **FULL NAR holdout** (all keibajo, not the Ōi-only slice). The full-NAR holdout is the point: the Ōi-only gate is statistically underpowered (iter33 best top1 LB95 = −0.2593pp), but the full-NAR per-class holdout (n = 26060 C / 7217 other / 7124 B / 2812 A / 1231 OP / 573 NEW / 556 MUKATSU) can resolve a sub-1pp effect under paired bootstrap. The build reused the **~665× numpy rescore engine** from iter33, so the probe + 7-class Holm judge ran in one night.

A two-stage protocol guarded the budget: first a **probe** (per-class decorrelation + winner-signal + null-permutation collapse) to confirm the member is neither a degenerate copy of production nor pure noise, then the **strengthened judge** (nested split + multi-metric gate + top1 paired-bootstrap LB95>0 + Holm).

## Implementation summary

No production code changed this round. The R5 driver (`build_nar_vector_weights.py`, `nar_vector_engine.py`, `probe.py`, `judge_nar_vec.py`, `oi_crosscheck.py`) lives under `tmp/nar-perclass/nar_vec/` (git-excluded) and is intentionally NOT committed. The two load-bearing wins remain external to this evaluation: the P0 routing fix (already committed at iter32) and the eval-pipeline numpy-rescore speedup (reused here). The only artifact committed for this round is this docs record.

## Results

### Probe — PROCEED (real, decorrelated signal)

The probe ran on 2022 with `decorr_max=0.95`. Rule: `PROCEED iff >=half of powered(>=200 races) classes are decorrelated(<0.95) AND have winner signal AND null collapses`. **Verdict: PROCEED** (5/5 powered classes proceed).

| class   | n_races | powered | Spearman(vec,prod) | −ρ(vec,finish) | −ρ(prod,finish) ref | null-perm −ρ | null collapses | proceed |
| ------- | ------- | ------- | ------------------ | -------------- | ------------------- | ------------ | -------------- | ------- |
| C       | 7602    | yes     | 0.8256             | 0.6448         | 0.7860              | 0.0023       | yes            | yes     |
| B       | 2155    | yes     | 0.8157             | 0.6320         | 0.7691              | −0.0035      | yes            | yes     |
| other   | 2112    | yes     | 0.8568             | 0.6487         | 0.7758              | −0.0024      | yes            | yes     |
| A       | 805     | yes     | 0.8186             | 0.6113         | 0.7625              | −0.0072      | yes            | yes     |
| OP      | 363     | yes     | 0.8386             | 0.6229         | 0.7630              | 0.0051       | yes            | yes     |
| MUKATSU | 196     | no      | 0.8768             | 0.6609         | 0.7394              | 0.0338       | yes            | yes     |
| NEW     | 175     | no      | 0.8759             | 0.6561         | 0.7124              | **0.0521**   | **no**         | no      |

Reading: within-race Spearman vs production is **~0.82–0.88 (<0.95)** everywhere → the member is genuinely **decorrelated** from the production ranking (not a degenerate copy). It carries a **real winner signal** — vec −ρ vs finish ~0.61–0.66 (against the production reference ~0.71–0.79). And the **null-permutation −ρ collapses to ~0** for every powered class plus MUKATSU. The single exception is **NEW** (n=175, underpowered): its null did NOT collapse (0.0521), so the probe correctly excludes it from the proceed count. The two powered classes that matter for tomorrow's Ōi card — **C (n=7602)** and **B (n=2155)** — both proceed.

### Judge — REJECT all 7 NAR classes (`adopted_classes: []`)

Under the strengthened gate, the optimizer assigned the vector member **≈0 weight in every class**, so the holdout deltas collapse to ≈0. Holm across the 7 classes: every class non-significant (adjusted p = 1.0).

| class   | vec member weight | top1 Δ  | place2 Δ | place3 Δ | box Δ   | n_pos | top1 boot LB95 | boot p | holdout n | verdict |
| ------- | ----------------- | ------- | -------- | -------- | ------- | ----- | -------------- | ------ | --------- | ------- |
| C       | 0.0104            | 0.0     | −0.0192  | −0.0077  | 0.0     | 0     | −0.000115      | 0.648  | 26060     | REJECT  |
| other   | 0.0778            | 0.0     | −0.0416  | −0.0139  | +0.0139 | 1     | 0.000000       | 1.0    | 7217      | REJECT  |
| B       | 0.0236            | 0.0     | 0.0      | 0.0      | 0.0     | 0     | 0.000000       | 1.0    | 7124      | REJECT  |
| A       | 0.05              | 0.0     | −0.0356  | +0.0711  | 0.0     | 1     | −0.001067      | 0.659  | 2812      | REJECT  |
| OP      | 0.05              | 0.0     | 0.0      | 0.0      | 0.0     | 0     | 0.000000       | 1.0    | 1231      | REJECT  |
| NEW     | 0.05              | 0.0     | 0.0      | +0.3497  | +0.1748 | 2     | −0.005236      | 0.651  | 573       | REJECT  |
| MUKATSU | 0.05              | +0.1799 | +0.5396  | 0.0      | +0.3597 | 3     | 0.000000       | 0.358  | 556       | REJECT  |

The two **powered, tomorrow-relevant** classes are flat: **C** top1 0.0 / place2 −0.019 / place3 −0.008 (0 axes positive, LB95 −0.000115), and **B** all four axes exactly 0.0 (the optimizer placed only 0.024 on the vector member and the ensemble output was unchanged at the resolution measured). **A** and **OP** are likewise ≈0. The only directionally interesting class is **MUKATSU** (top1 +0.18 / place2 +0.54 / box +0.36, 3 axes positive) — but on its tiny n=556 holdout the top1 paired-bootstrap LB95 is exactly 0.000000 (p=0.358), so it fails the significance arm and Holm leaves it non-significant. **NEW** (top1 0.0 / place3 +0.35 / box +0.17, LB95 −0.005236) was already flagged by the probe (null did not collapse) and is rejected. `summary.json` → `adopted_classes: []`.

## Per-bucket findings

The decisive observation is the gap between the probe and the judge. The probe shows the NAR similarity member is **orthogonal in feature space and individually winner-predictive** (decorrelated <0.95, vec −ρ ~0.63, null collapses to ~0). The judge shows that, blended into the saturated production ensemble, the optimizer hands it **≈0 weight in every class** and the holdout deltas vanish. So the orthogonal variance the member adds is **target-noise, not target-signal**: the GBDT ensemble has already absorbed all of the finish-predictive content, and what is left over — the part the kNN member contributes independently — does not help predict the finish. The optimizer zeroing the weight is the cleanest possible saturation confirmation: not a member that is too correlated to add, but a member with real independent variance that the gate still cannot use.

## Decision

**No model change adopted — for the Ōi card or globally.** The NAR similarity / pgvector-style member is rejected in all 7 NAR classes; `adopted_classes: []`. Tomorrow's 2026-06-10 Ōi prediction stands unchanged on the two genuine wins already delivered (P0 per-class routing fix + eval-pipeline numpy-rescore speedup); zero noise was deployed.

## Overall conclusion — 7-lever saturation (2026-06-09/10)

Across the night of 2026-06-09/10, **seven** rigorous levers were tried for tomorrow's Ōi NAR card:

1. **member reweight** (R2) — REJECT (no orthogonal signal in the existing member pool)
2. **venue features** — REJECT (7 Ōi-specific features carry ≈0% importance; every axis regresses)
3. **standalone Ōi-specialist** (R3) — REJECT_as_noise (top1 directional but no place axis clears LB95>0)
4. **blended Ōi-specialist** (R4) — REJECT (optimizer loads heavy specialist weight → re-imports place damage; all top1 LB95<0)
5. **pgvector-JRA member** — REJECT (all 5 JRA classes; 005 near-miss 4/4 axes positive but top1 LB95 −0.000953)
6. **training-logic audit** — NO BUG (time-decay correctly applied; nothing to exploit)
7. **NAR similarity member** (R5, this round) — REJECT (all 7 NAR classes; optimizer assigns ≈0 weight, deltas ≈0)

**Every one was rejected under the strengthened gate** (nested split + multi-metric + top1 paired-bootstrap LB95 + Holm). The per-class ensembles are **saturated**: no same-night lever — reweight, hand-engineered feature, specialist, or orthogonal-but-noisy similarity member — yields a robust improvement, and **zero noise was deployed**.

**Genuine wins delivered:**

- the committed **P0 per-class routing fix** (live on the HEAD `finish-position-predict-local:split2` image, verified on the 06-10 Ōi card — C×5 → `iter30-nar-cb-ensemble-C-v8`, other×4 → `iter30-nar-cb-ensemble-other-v8`, B×3 → `iter12-nar-xgb-hpo-v8`, zero fallback errors);
- the **~665–878× eval-pipeline speedup** (vectorized numpy rescore, bit-equivalent to PG), which is what made both the iter33 R2–R4 + pgvector sweep and this R5 probe + 7-class Holm judge tractable in a single night.

**Path to real future gains:** a genuinely-new external/horse-level signal that is **BOTH orthogonal AND finish-predictive** (multi-day work — R5 proved that orthogonal-in-feature-space is necessary but not sufficient), or **global-NAR-powered judging of new features** — not a same-night ensemble lever. The R5 result tightens the iter33 conclusion: the issue was never holdout power (R5 judged on the full, powered NAR holdout and still found nothing), it is that the existing feature set is saturated. New signal must clear the bar that R5's similarity member could not: incremental in **target** space, not merely independent in feature space.

## Next iteration recommendation

Stop pulling same-night ensemble levers — seven consecutive rejections (four Ōi-specific, the JRA pgvector probe, the audit, and now the NAR similarity member judged on the full powered holdout) confirm the per-class ensembles are saturated over the current feature set. The next genuine improvement requires sourcing a **new horse-level / external signal that is finish-predictive** (validated under the same nested-split + multi-metric + paired-bootstrap-LB + Holm gate, on the full-NAR holdout for power), trained via a multi-hour full retrain or HPO/L4 — not a blend, feature bolt-on, or similarity member over the existing signal.

## Quality Gate Results

- tsc: n/a — no code change adopted this round
- lint: n/a — no code change adopted this round
- format:check: n/a — no code change adopted this round
- test:coverage: n/a — no enforced-package file modified
- python:check: n/a — R5 drivers live under tmp/ (not an enforced package)
