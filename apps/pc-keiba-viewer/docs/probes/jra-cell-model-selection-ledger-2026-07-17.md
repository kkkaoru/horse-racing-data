# JRA per-cell model-selection ledger v1 (2026-07-17)

- **Date**: 2026-07-17
- **Category**: JRA finish-position — cell-level model-selection infrastructure,
  per USER instruction: **"評価は常に cell 単位 × 着順 1-5 の個別精度で。要約精度
  での評価禁止。cell 単位でモデルを選択できるようにする"** (always evaluate at
  cell × rank1-5 individual-metric granularity, never a summarized number;
  make it possible to select models per cell).
- **Training performed**: none. Every number below is a predict-only
  re-score of already-generated walk-forward (WF) blind predictions (9
  cached champion CatBoost artifacts + two already-existing candidate gate
  outputs). `cell_training_evaluations` (the Neon table) is **never**
  queried — its finish_position rows are documented broken (doc
  `docs/finish-position-prediction-system.md` §6.4: eval-driver bug,
  top1 deflated ~5-16x, unfixed as of this repo state). Every ledger number
  here is an in-memory re-score per §6.3/§6.4's mandated `score_cells.py`
  convention.

---

## 0. Headline result

**Re-verification of the 3 live routing rules**: 703 rule re-confirms
robust ✅; venue02 rule confirms its known place3-only character (no new
top1 evidence, none contradicting either) ➖; the 005 rule's **exact**
literal `cell_routing.json` condition shows a robust top1 gain **paired
with a robust place5 regression** at n=210 — a genuine multi-metric
trade-off that fails the strict no-regression gate. **Orchestrator decision
(same day): KEEP the route as a deliberate, explicit trade-off acceptance**
(top1's value outweighs place5's cost at this scale) — see §3.3 for the
full rationale and the adoption-process gap this surfaced (the original
adoption evidence never reported the place5 cost at all).

**v2 addendum (same day, team-lead-directed follow-up)**: the one new lead
this ledger's sweep found (§4's venue=01×intermediate top1 gain for both
candidates, single-seed, gate-failed on a place2 regression) was re-tested
at 3-seed average before any ADOPT consideration — see §7 for the result.

**Systematic new-cell sweep** (7 single dimensions + 7 two-way crosses +
pooled, ×2 candidates = 290 cells, plus 43 champion-vs-market reference
cells): **zero new actionable ADD/CHANGE proposals**. Two champion-vs-market
cells nominally clear the loose gate (turf, spring) but neither is
model-selection-actionable (no turf/spring-specific candidate model exists
in the current roster) and both show the signature of a multiple-comparison
false-positive rather than a robust effect (detail in §4).

**No routing.json change is proposed by this ledger.** Per task instructions,
gate-passing proposals are reported, not deployed.

---

## 1. Candidate inventory

Every leak-free JRA finish-position candidate with either a live production
artifact or a saved 2023-2025 WF blind-prediction dataset (full sweep of
`tmp/candidate-*`, cross-checked against `model_meta.py`'s
`WITHIN_RACE_LEAK_COLUMNS` = `{target_corner_1_norm, target_corner_2_norm,
target_corner_3_norm, target_corner_4_norm, target_running_style_class}` —
5 columns, not 4; grepped every candidate's feature list against the full
set):

| Candidate          | model_version                                                                                                                                                                                                                | Routing (live, verbatim from `cell_routing.json`)                               | WF preds used here                                                                            | Seed/fold coverage                      |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- | --------------------------------------- |
| Champion clean250  | `jra-cb-v9-sim-2013-clean`                                                                                                                                                                                                   | `default_variant` (fallback for everything unmatched)                           | `tmp/candidate-masked-lever-retest/models/base/` (predict-only, this ledger's own run)        | 3 seed × 3 fold (42/101/2026 × 2023-25) |
| jockey-pedigree269 | `jra-cb-v9-sim-2013-clean-jockey-pedigree269` (269 feat = 250 clean base + 19 jockey×venue×day-count / grand-sire×dist×surface empirical-Bayes columns)                                                                      | `kyoso_joken_code=703` **OR** `venue=02` (two separate rules, same target)      | `tmp/candidate-jra-jockey-pedigree-cell/gate-v9sim-exact/{base_preds,cand_all_preds}.parquet` | single-seed `42+fold_year`, 2023-25     |
| prior-corner274    | `jra-cb-v10-prior-corner274-2013` (274 feat = 250 clean base + 24 prior-race corner-2/3/4 history aggregates — `past_corner_{2,3,4}_norm_*`/`last_race_corner_{2,3,4}_norm`, never the banned `target_corner_2_norm` itself) | `surface=dirt AND field_band=f_le10 AND kyoso_joken_code=005` (single AND rule) | `tmp/candidate-prior-corner/gate-v9sim-exact/{base_preds,cand_preds}.parquet`                 | single-seed `42+fold_year`, 2023-25     |

**Excluded from this v1 ledger, with reasons** (full sweep detail in the
build artifacts, not reproduced here):

- **5 masked-lever arms** (`draw_ablation`, `draw_affinity`, `pace_reversal`,
  `sameday_bias`, `tokubetsu_mkt` — all in `tmp/candidate-masked-lever-retest/`,
  leak-free armB-250 base, full 3-seed×3-fold `model.json` grids) — all
  already **REJECTED** at the standard (non-cell) gate
  (`ACCEPT_strict_gate: false` in each `reports/*.json`), and none has a
  saved WF predictions parquet (only model weights — `retest_wf.py` never
  writes per-race predictions), so including them would require a fresh
  predict-only pass. Out of scope for v1 given the ~2-3h budget; a
  reasonable v2 candidate pool if a future session wants to mine already-
  globally-rejected arms for cell-conditional wins (with the appropriate
  multiple-comparison discipline that implies).
- **6 pre-clean-retrain (leaky-baseline) candidates** with saved WF
  prediction parquets (`tmp/candidate-jra-largescale/stream-d-gate*`,
  `tmp/candidate-jra-speedfig/gate`, `tmp/candidate-jra-summer-signals/gate-out`,
  `tmp/candidate-jra-summer4-signals/gate-*`, `tmp/candidate-fp-cells/preds`)
  — all trained against a 254-feature baseline confirmed (by direct file
  read of `arm0_baseline.log`) to include all 4 of the `target_corner_*`
  leak columns present in that era's feature set. **Never used.**
- Everything else found in the `tmp/candidate-*` sweep is either a
  different prediction target (running-style, race-volatility), a
  feature-engineering-only build with no saved predictions, or a diagnostic
  re-scoring of the existing champion (this session's own earlier Sapporo
  work) rather than a new candidate model.

---

## 2. Method

### 2.1 Cell dimensions — production-exact, not the eval-store convention

Every cell dimension in this ledger is derived using
`apps/finish-position-predict-container/src/predict_lib/cell_router.py`'s
**exact** `derive_surface`/`derive_distance_band`/`derive_field_band`/
`derive_season`/`derive_class` functions (verbatim thresholds below),
reimplemented in polars — **not**
`learning/subgroup_diagnostics.py`'s eval-store convention used elsewhere in
this campaign's cell claims (champion_ledger.py, the summer4 baseline, this
session's own Sapporo diagnosis). This is a deliberate choice, confirmed by
direct code comparison: the two conventions **disagree on `surface` for JRA
track_code 20-22** — `cell_router.py`'s `derive_surface` classifies by
leading digit (`"1*"`→turf, `"2*"`→dirt, i.e. 20-22 are **dirt** in
production routing), while `subgroup_diagnostics.py`/`subgroup.py`'s
set-based convention (`{10..22}`→turf) treats 20-22 as **turf**. Since this
ledger's entire purpose is proposing routing rules that must correspond 1:1
with what `cell_routing.json` would actually match at serve, using anything
other than `cell_router.py`'s own logic would risk proposing a rule that
doesn't route the races it claims to.

| Dimension          | Production derivation (verbatim from `cell_router.py`)                               | Values                                          |
| ------------------ | ------------------------------------------------------------------------------------ | ----------------------------------------------- |
| `venue`            | `keibajo_code` (raw)                                                                 | `01`..`10`                                      |
| `surface`          | `track_code` starts with `"1"`→turf, `"2"`→dirt, else `other` (JRA only)             | turf / dirt / other                             |
| `distance_band`    | `kyori<1200`→sprint, `<1600`→mile, `<2000`→intermediate, `<2400`→long, else extended | 5 bands                                         |
| `field_band`       | `shusso_tosu<=10`→`f_le10`, `<=13`→`f11_13`, `<=15`→`f14_15`, else `f16p`            | 4 bands (**not** cells.py's small/medium/large) |
| `season`           | month 3-5→spring, 6-8→summer, 9-11→autumn, else winter                               | 4 bands                                         |
| `class`            | `grade_code` raw, or `unknown` if blank                                              | raw grade_code values                           |
| `kyoso_joken_code` | raw (this is the dimension the live 703/005 rules actually key on, not `class`)      | raw condition codes                             |

**`field_band` caveat, inherited from `cell_router.py`'s own documented,
accepted reality** (its code comment, read directly): live serving derives
`field_band` from the **declared entry count** at predict time (raw
`shusso_tosu` is unconditionally NULL at serve), whereas this ledger — like
the WF analysis that originally validated the live `f_le10` rule — uses
**actual post-race** `shusso_tosu`. Near a boundary, a late scratch can make
live routing disagree with this offline analysis. Not a defect introduced
here; the same accepted gap the live rule was validated under.

### 2.2 Cell taxonomy

Per candidate: `pooled` (1) + 7 single-dimension cuts (venue / surface /
distance_band / field_band / season / class / kyoso_joken_code) + 7 two-way
crosses (venue×surface, venue×distance_band, venue×field_band, venue×class,
surface×distance_band, surface×field_band, distance_band×field_band) + 3
explicit live-rule replicas (exact `cell_routing.json` AND-conditions,
verbatim). `n>=200` races enforced on every reported cell. A
champion-vs-market reference section (pooled + the 7 single dimensions) is
also computed, using the champion's genuine 3-seed×3-fold predict-only
ensemble — this is context for interpreting the other two sections'
absolute scale, not itself a model-selection decision (market is not a
routable candidate).

### 2.3 Metrics and gate — top1..place6 individually, per §7.2/§8.12

Every cell reports top1/place2/place3/place4/place5/place6/top3_box
**individually** (never averaged/summarized), delta = candidate − matched
base, paired bootstrap (n_boot=2000, seed=20260717) LB95/UB95. Gate (§7.2,
verbatim): **≥2 of {top1, place2, place3} positive**, **≥1 of {place2,
place3} positive**, **no metric regresses beyond −0.05pp**, delta ≥
**+0.08pp**, and (**top1's own LB95 > 0** OR **every one of the 7 gated
metrics individually clears +0.08pp**) — the LB95 disjunct is anchored to
top1 specifically, matching the precedent that originally adopted the 703
rule (`gate_report.json`'s own basis: "top1 +0.782pp [LB95 +0.270] — 頑健,
this is why the rule exists").

### 2.4 Matched-seed pairing (why candidate-vs-champion, not candidate-vs-3seed-champion)

jockey-pedigree269 and prior-corner274 only have single-seed cached WF
predictions (`random_seed = 42 + fold_year`, a seed value that doesn't
match any of the champion's own cached 3-seed set `{42, 101, 2026}`). Each
gate script generated its **own matched-seed champion-equivalent base**
alongside its candidate (same seed, same fold, armB-250) specifically for a
valid paired comparison — this ledger uses **that** base (`base_preds.parquet`
from within each candidate's own gate directory), not this campaign's
separately-cached 3-seed champion, for every candidate-vs-base delta. This
is a real, disclosed asymmetry versus the champion-vs-market reference
column (which does use the genuine 3-seed ensemble) — noted so it isn't
mistaken for an oversight.

### 2.5 Sort-before-mask discipline

Every cell slice is `race_dims.filter(pl.col(dim)==v)` → `race_id` list →
`.filter(pl.col("race_id").is_in(race_ids))` on the independently-computed
hit tables — never a positional mask against a separately-sorted frame (the
2026-07-04 incident's failure mode).

---

## 3. Re-verification of the 3 live routing rules

### 3.1 `kyoso_joken_code=703` → jockey-pedigree269 — RECONFIRMED, robust

| Metric | Base    | Cand    | Delta   | LB95    | UB95    |
| ------ | ------- | ------- | ------- | ------- | ------- |
| top1   | 34.9326 | 35.7143 | +0.7817 | +0.2695 | +1.2938 |
| place2 | 18.6253 | 19.0836 | +0.4582 | −0.1617 | +1.0782 |
| place3 | 15.3369 | 15.7682 | +0.4313 | −0.2426 | +1.0788 |

n=3710. **Reproduces the original adoption evidence almost exactly**
(`gate_report.json`'s own figure: +0.782pp [LB95+0.270]) — this ledger's
independent from-scratch re-score landing within 0.0003pp of the original
is a strong internal-consistency check on this ledger's own methodology,
not just on the rule. Fold-consistency (computed for every cell this
ledger's gate flags ADOPT): top1 delta +0.57/+0.57/+1.20pp across
2023/2024/2025 (n=1228/1230/1252) — same sign, similar magnitude, all 3
years. **Gate: ADOPT. No change proposed.**

### 3.2 `venue=02` (Hakodate) → jockey-pedigree269 — place3-specific character reconfirmed, no new top1 evidence

| Metric | Base    | Cand    | Delta   | LB95    | UB95    |
| ------ | ------- | ------- | ------- | ------- | ------- |
| top1   | 35.4167 | 36.3426 | +0.9259 | −0.4630 | +2.5463 |
| place2 | 17.1296 | 17.8241 | +0.6944 | −1.3889 | +3.0093 |
| place3 | 10.8796 | 13.8889 | +3.0093 | +1.1574 | +4.8611 |

n=432. **This independently reproduces the earlier summer4-baseline
finding almost exactly** (that doc's own venue=02-alone number: place3
+3.009pp [LB95+1.157]). Gate mechanics: only place3 is individually
LB95-robust; top1's own LB95 crosses zero, so this cell does **not** clear
this ledger's (top1-LB95-anchored) gate — consistent with the existing
documented history that venue=02's routing rides on the broader 703-cell
evidence, not an independent top1-level re-gate at n=432. **This
re-verification changes nothing**: no new evidence either strengthens or
undermines the existing rule. **No change proposed.**

**2026 reference point** (supplementary, not decision-basis — team-lead
shared the summer-baseline agent's high-fidelity 264-race 2026 local replay,
`tmp/candidate-jra-summer3-local-replay-2026-07-17/`, ρ=0.93/top1-match
76.2% vs the healthy Mac-batch cluster): venue=02 routed-vs-champion 2026
top1 delta +0.76pp [LB95 −0.76] — directionally positive, not
significant, n too small (264 races pooled across venues) to move the WF
verdict either way. Consistent with, not contradicting, the WF
characterization above.

### 3.3 `surface=dirt AND field_band=f_le10 AND kyoso_joken_code=005` → prior-corner274 — REJECT, a genuine multi-metric trade-off

| Metric   | Base    | Cand    | Delta   | LB95    | UB95    |
| -------- | ------- | ------- | ------- | ------- | ------- |
| top1     | 40.9524 | 42.8571 | +1.9048 | +0.4762 | +4.29   |
| place2   | 20.9524 | 21.4286 | +0.4762 | −1.9048 | +2.86   |
| place3   | 17.1429 | 17.1429 | 0.0000  | −3.3333 | +3.33   |
| place4   | 17.1429 | 17.1429 | 0.0000  | −1.9048 | +1.90   |
| place5   | 15.2381 | 13.3333 | −1.9048 | −3.8095 | −0.4762 |
| place6   | 19.0476 | 19.5238 | +0.4762 | −1.9048 | +2.86   |
| top3_box | 15.7143 | 16.6667 | +0.9524 | −0.9524 | +2.86   |

n=210. **This is a more precise and more interesting finding than "weak
evidence"**: top1's gain is itself individually robust (LB95 +0.48, clears
zero) — but **place5 shows an individually robust regression** (delta
−1.90pp, UB95 −0.48, i.e. the _entire_ 95% interval sits below zero). The
gate's `no_regression` check (worst delta across all 7 metrics must not
exceed −0.05pp) correctly fires on this place5 regression, overriding the
otherwise-clean top1 win. `primaries_positive=2` (top1, place2 — place3's
exact 0.0 doesn't count as positive), `place23_positive=1` (place2 only).
**Gate: REJECT**, specifically for regressing place5, not for lacking a
top1 effect.

**A discrepancy worth flagging, not resolving here**: the original
adoption evidence for this rule (per this session's own reconnaissance of
`tmp/candidate-prior-corner/gate-v9sim-exact/gate_report.json`) cited a
**different** cell — `keibajo_code=03 (Fukushima) × kyori_band=0`, n=276,
top1 +2.54pp [LB95+0.36] — as its basis, not this ledger's literal
reproduction of the exact `cell_routing.json` AND-condition (n=210). These
are related but **not the same population** (a venue+distance cross is
neither a subset nor a superset of a surface+field+condition-code cross in
general). This ledger did not attempt to reconcile which cell definition
the original decision actually validated, or re-derive the "03|0" cell's
own full multi-metric table — that is a well-scoped, cheap follow-up
(predict-only, same cached artifacts) a future session or this ledger's v2
should do before treating either finding as more authoritative than the
other.

**Recommendation at the time this ledger was first written**: flag for
orchestrator review, not an automatic removal. Reasons for restraint: n=210
is at the gate's own floor (structurally wide CIs expected); this is
single-seed data; the place5 cost may be an acceptable, already-implicit
trade-off if top1 was the metric the original decision optimized for (a
policy question, not one this ledger should decide unilaterally); and the
unresolved cell-definition discrepancy above means this finding and the
original adoption evidence have not been shown to actually conflict on the
_same_ population.

**Orchestrator decision (2026-07-17, same day): KEEP the route.** Rationale
(verbatim from the decision): top1 +1.90pp [LB95+0.48] is a robust real
gain, and the place5 −1.90pp cost is judged practically acceptable at this
scale — the value of an exact rank-5 hit is far smaller than the value of a
top1 hit, so a robust top1 win purchased at the cost of a robust place5
loss is, on balance, a reasonable trade to keep. **This is recorded here as
an explicit, deliberate trade-off acceptance, not as "no regression was
found."** The regression is real and was measured; the decision is to keep
the rule anyway because the asymmetry in what top1 vs place5 are worth
outweighs it.

**Adoption-process gap, recorded per orchestrator instruction**: the
original adoption evidence for this rule (§ the discrepancy above) did not
surface a place5 regression at all — it reported only the top1 win on a
different cell definition ("03|0"). Whether that's because the "03|0" cell
genuinely doesn't show the same place5 cost, or because the original
gate-check simply didn't report place4-6/top3_box for that specific
candidate at the time, was not established. **Either way, a routing
decision was made without the full rank1-6 picture that this ledger's
methodology (mandated by the same-day USER instruction to always evaluate
cell × rank1-5 individually) would have surfaced up front.** This is logged
as a process gap for future cell-route adoptions: the multi-metric table
(not just the headline top1 delta) should be checked before, not after,
a rule goes live. The "03|0"-vs-literal-AND cell-definition question
remains open and is **not** resolved by this decision — a future session
should still reconcile which population the original decision validated
before this route's next review, per §3.3's earlier note.

**No routing.json change is made as a result of this ledger** — the
existing rule stays exactly as configured.

---

## 4. Systematic new-cell sweep — no new actionable proposals

290 candidate-vs-base cells (145 per candidate: pooled + 7 dims + 7 crosses

- 3 live-rule replicas, all at n≥200) plus 36 champion-vs-market reference
  cells were swept. **Only 2 cells beyond the 3 known routes clear the gate**,
  both in the champion-vs-market reference section (not a model-selection
  question — there is no turf-specific or spring-specific candidate model in
  the current roster to route to):

| Cell            | n    | top1 Δ  | top1 LB95             | Why it clears / why it's not actionable                                                                                                                                                                                                                                                                                                                                 |
| --------------- | ---- | ------- | --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `surface=turf`  | 5012 | +0.13pp | −0.44 (crosses zero)  | Clears **only** via the "all 7 gated metrics ≥+0.08pp" exception — every single metric individually fails LB95>0 despite n=5012. With this much power, a genuine broad effect should clear at least one metric's own CI; that none do is more consistent with correlated small-positive noise across metrics than a real effect. **Not proposed.**                      |
| `season=spring` | 2663 | +0.85pp | +0.03 (barely clears) | A materially stronger signal (place3 LB95+0.91, top3_box LB95+0.24, genuinely robust on 2 metrics) — but this is champion-vs-**market**, not a candidate-model finding, and no season-conditional model exists to route to. Interesting enough to log for a future campaign (a spring-specific feature/lever), **not actionable in this ledger's v1 candidate roster.** |

**Multiple-comparison context**: 326 total cells were tested across all 3
sections. 4 cleared the gate — 1 is the already-known, independently-
reconfirmed 703 rule; 2 are the non-actionable champion-vs-market cells
above; 0 are new candidate-vs-base findings. This rate (4/326, and the only
new-territory hits being un-actionable) is consistent with — not evidence
against — the campaign's repeated finding that this candidate/cell space is
largely exhausted (`project_accuracy_stagnation_root_cause_2026_07_11`).

**Notable non-clearing trade-off patterns** (REJECT, but worth recording
for anyone extending this ledger): both candidates show a top1 gain at
`venue=01 (Sapporo) × distance_band=intermediate` (jockey-pedigree269:
+3.72pp [LB95+1.40, individually robust!], n=215; prior-corner274: +2.33pp
[LB95 0.00], n=215) that fails the overall gate because **place2 regresses**
at the same cell (269: −1.40pp; 274: −0.47pp) — the identical
robust-tradeoff shape as §3.3's place5 finding, just at a different cell.
This directly touches Sapporo, but is a **different question** from this
session's own now-closed Sapporo top1-vs-**market** diagnosis
(`jra-sapporo-top1-deficit-diagnosis-2026-07-17.md`) — this is "does a
different **feature set** (jockey-pedigree269/prior-corner274) beat
**champion** at Sapporo×intermediate," not "does champion beat market."
Not covered by that doc's DO-NOT-RETEST (which is specifically about a
connections-family blend hypothesis). At n=215 (barely above floor,
single-seed), this is not strong enough to act on, but is flagged as a
concrete, reproducible lead for a v2 ledger with more power (e.g. multi-seed
retraining of jockey-pedigree269, or pooling additional years).

---

## 5. Proposals (per task instructions: reported, not deployed)

- **ADD**: none. No new cell clears the gate with an actionable
  candidate-model routing implication.
- **CHANGE**: none. Both existing conditional rules (703, venue02) are
  re-confirmed at the same evidentiary strength as before — no new
  evidence to expand or narrow either.
- **REMOVE**: none. The literal `dirt×f_le10×005` condition's robust place5
  regression alongside its robust top1 gain (§3.3) was reviewed by the
  orchestrator same-day and the route is **kept**, as an explicit trade-off
  acceptance (top1's value judged to outweigh place5's cost) — see §3.3 for
  the full decision record and the adoption-process gap it surfaced. The
  "03|0"-vs-literal-AND cell-definition question remains open for a future
  review.
- **v2 addendum**: see §7 for the 3-seed re-test of the
  venue=01×intermediate lead and its resulting verdict.

---

## 6. Caveats

- Single-seed limitation for both candidates (§2.4) — CIs on
  candidate-vs-base cells are somewhat wider than a 3-seed-pooled
  equivalent would give; a genuine effect sitting just below this ledger's
  significance bar could still be real.
- `field_band` computed from actual post-race `shusso_tosu` (§2.1) — an
  accepted, precedented gap versus live serving's declared-entry-count
  basis, not new to this ledger.
- The 5 already-REJECTED masked-lever arms and 6 leaky-baseline candidates
  found during inventory (§1) were excluded from this v1 — a deliberate
  scope choice given budget, not evidence they'd be uninteresting at cell
  granularity.
- §3.3's cell-definition discrepancy (literal rule vs. "03|0" proxy) is
  flagged, not resolved — reconciling it was out of this ledger's budget.
- No claim in this doc has been checked against `cell_training_evaluations`
  — by design, per §6.4.

---

## 7. v2 addendum: 3-seed re-test of the venue=01×intermediate lead — CLOSED, noise

Team-lead pre-registered the decision criterion (verbatim) before this ran:
apply the standard §7.2/§8.12 gate, including the −0.05pp no-regression
floor, to the **3-seed average**; ADOPT only if it clears, otherwise close
as noise/trade-off.

**Training**: 2 additional seeds (101, 2026) of the jockey-pedigree269
candidate were trained, matching the existing single run's own seed-formula
shape (`seed_base + fold_year`) extended to champion's other 2 seed
identities, 3 folds each (6 models, ~44s/fold, ~266s total). Each new
seed's candidate was paired against its own matched-seed champion-equivalent
base (masked-lever-retest's cached `seed101`/`seed2026` armB models — a
valid seed-matched pair, same design principle as §2.4). `memory_pressure -Q`
confirmed 49-55% free immediately before and during training (other agents
training concurrently at the time).

### Result: still REJECT on 3-seed average

| Cell                                      | n    | top1 Δ  | top1 LB95 | place2 Δ | Gate   |
| ----------------------------------------- | ---- | ------- | --------- | -------- | ------ |
| `venue=01 × distance_band=intermediate`   | 215  | +2.1705 | +0.3101   | −0.7752  | REJECT |
| `venue=01` (all distance bands)           | 504  | +1.0582 | −0.0000   | −0.2646  | REJECT |
| `distance_band=intermediate` (all venues) | 4443 | +0.0450 | −0.2551   | −0.0150  | REJECT |

The 3-seed-averaged top1 delta (+2.17pp, LB95+0.31 — itself still
individually robust) is smaller than the single-seed figure (+3.72pp) but
**place2's regression persists** (−0.78pp, still the `worst_delta_pp`
driving `no_regression=False`) — the gate fails for the same structural
reason as the single-seed result, just at reduced magnitude. Neither
`venue=01` alone nor `distance_band=intermediate` alone shows a
comparable effect (the latter is flat at +0.045pp, LB95−0.26 — confirming
this was never a generic "intermediate distance" effect, only a
venue×distance **interaction**, if it's anything at all).

### Per-seed breakdown — this is genuine noise, not a diluted-but-real effect

| Seed           | top1 Δ  | place2 Δ    | place3 Δ |
| -------------- | ------- | ----------- | -------- |
| seed42 (equiv) | +3.7209 | −1.3953     | +0.4651  |
| seed101        | +0.4651 | −2.3256     | 0.0000   |
| seed2026       | +2.3256 | **+1.3953** | −0.4651  |

At n=215, **top1's own delta ranges 8x across the 3 seeds** (+0.47pp to
+3.72pp) and **place2's sign literally flips** (negative at 2 of 3 seeds,
positive at the third) — well outside the ±0.4pp single-arm noise floor
this campaign established elsewhere (`project_training_noise_floor_2026_07_11`)
for populations at this scale, and a textbook illustration of why a
single-seed, n=215 cell hit cannot be trusted without exactly this kind of
multi-seed confirmation.

**Verdict: CLOSED as noise, not ADOPTed.** This is a **different question**
from this session's own now-closed Sapporo top1-vs-**market** diagnosis
(`jra-sapporo-top1-deficit-diagnosis-2026-07-17.md`, closed same day under
its own separate criteria) — that doc asked whether champion beats market
at Sapporo (concluded: market has a durable informational edge, not
fillable); this addendum asked whether a different **feature set**
(jockey-pedigree269) beats **champion** at Sapporo×intermediate specifically
(concluded: no, the apparent single-seed edge was seed noise). Both are now
closed, for different reasons, and neither re-opens the other.

**DO-NOT-RETEST**: the jockey-pedigree269 (or prior-corner274)
venue=01×distance_band=intermediate routing hypothesis, at this population
and seed set — tested at 3-seed average with the pre-registered gate and
closed. Re-opening requires either a materially larger population (more
years, or pooling additional similar cells) or new external evidence, not
a re-slice of the same 2023-2025 single/3-seed WF population.

Artifacts: `apps/pc-keiba-viewer/tmp/candidate-jra-cell-model-ledger-2026-07-17/v2-multiseed/`
(`train_269_seeds.py`, `train.log`, `base_preds_seed{101,2026}.parquet`,
`cand_preds_seed{101,2026}.parquet`, `eval_3seed.py`, `eval.log`,
`eval_3seed_result.json`).

---

## 8. 日本語まとめ

USER 指示(「cell 単位 × rank1-5 個別精度で常に評価、要約精度禁止、cell 単位で
モデル選択可能に」)に基づき、JRA の leak-free 候補モデル 3 種(champion
clean250 / jockey-pedigree269 / prior-corner274)を対象に、cell(venue/
surface/distance_band/field_band/season/class/kyoso_joken_code の単軸 7
種+2軸交差 7 種+既存 3 route の厳密再現)× rank1-6(+top3_box)個別精度の
台帳を構築した。学習は一切行わず、既存の WF blind 予測(9 cached champion
artifact + 2 候補の既存 gate 出力)を in-memory 再スコアのみ(`score_cells.py`
方式)。`cell_training_evaluations`(壊れていることが確定済みの Neon テーブル、
§6.4)は一切参照していない。

**cell 次元の導出は `cell_router.py` の本番ルーティングロジックを直接再実装**
(subgroup_diagnostics.py の eval-store convention とは track_code 20-22 の
surface 判定で相違することを直接コード比較で確認——本番配線と 1:1 対応する
ことを最優先したため)。

**既存 3 route 再検証(headline)**: 703→jockey-pedigree269 は再確認・頑健
(元の採用根拠とほぼ完全一致、3 年連続同方向)。venue02→jockey-pedigree269 は
既知の「place3 のみ頑健」性格を再確認(新たな top1 根拠なし、既存根拠を
否定する材料もなし)。dirt×f_le10×005→prior-corner274 の**文字通りの**
条件(n=210)は、top1 の頑健な改善(+1.90pp、LB95+0.48)と**place5 の頑健な
悪化**(−1.90pp、UB95 も負)という真のトレードオフを示し、no-regression gate
(−0.05pp floor)により REJECT——これは「証拠不足」ではなく「明確な多指標
トレードオフ」という、より精密な性格づけ。元の採用根拠は異なるセル定義
(「03|0」= 福島×距離帯、n=276)を使っていたことも判明し、両者の整合性検証は
今回のスコープ外として明記した。

**新規セルの系統的走査**: 290 候補セル+参考 36 セル(計 326)のうち gate 通過
は 4 件のみ——既知 703 の再確認 1 件、champion vs market の非アクション
可能な参考セル 2 件(turf は全指標非有意で緩い例外条項のみで通過=多重比較
ノイズの兆候、spring はより強いが候補モデルが存在せずアクション不可)、
新規の candidate-vs-champion 提案は **0 件**。**追加/変更のレバー提案なし
——構造的に択一の余地が出尽くしている状態を裏付ける結果として報告する。**
撤去は提案せず、005 route の place5 トレードオフのみ review 用にフラグ。

札幌×中距離(venue=01×intermediate)で両候補とも top1 頑健改善+place2 悪化
という同型トレードオフを示した点は、本セッションで別途 CLOSED にした
「札幌 top1 vs 市場」診断とは異なる論点(モデル間比較であり市場比較ではない)
であり DO-NOT-RETEST の対象外だが、n=215・単一 seed のため v2 向けの
リードとして記録するに留めた。

**同日 追記2件**: (1) orchestrator 判断により 005 route は**維持**——top1
+1.90pp の実利が place5 −1.90pp の対価を上回るという明示的トレードオフ受容
として記録し、当初採用が place5 悪化を把握せずに決定された点を
adoption-process の欠陥として明記した(§3.3)。(2) 札幌×中距離リードは
team-lead 事前登録基準(3-seed 平均で標準 gate 通過時のみ ADOPT)に基づき
269 の seed101/2026 を追加学習(6 model、約266秒)して再検証——3-seed 平均
でも top1 +2.17pp[LB95+0.31]は頑健だが place2 悪化(−0.78pp)が消えず gate
不合格。seed 別内訳は top1 が+0.47〜+3.72pp の8倍レンジ、place2 の符号が
反転(2/3 seed 負、1/3 seed 正)——ノイズ床(±0.4pp)を大きく超える典型的な
single-seed noise と確定し、**CLOSED(ADOPT せず、DO-NOT-RETEST)**とした
(§7)。

---

## Artifacts

- `apps/pc-keiba-viewer/tmp/candidate-jra-cell-model-ledger-2026-07-17/build_ledger.py`
  — full pipeline (predict-only champion 3-seed + candidate re-score, production-exact cell derivation, gate)
- `.../run.log`, `.../ledger_full.json` (full nested ledger, all 326 cells,
  all 7 metrics, gate breakdown), `.../ledger_flat.parquet` /
  `.../ledger_flat.json` (flat table for quick scanning / MLflow)
- Reused unchanged: `tmp/candidate-masked-lever-retest/models/base/**` (9
  champion artifacts), `tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json`,
  `tmp/candidate-eval-jra/augmented/**`,
  `tmp/candidate-jra-jockey-pedigree-cell/gate-v9sim-exact/{base_preds,cand_all_preds}.parquet`,
  `tmp/candidate-prior-corner/gate-v9sim-exact/{base_preds,cand_preds}.parquet`
- Ground truth read directly (not assumed):
  `apps/finish-position-predict-container/src/predict_lib/cell_router.py`,
  `apps/finish-position-predict-container/src/predict_lib/cell_routing.json`,
  `apps/finish-position-predict-container/src/predict_lib/model_meta.py`,
  `docs/finish-position-prediction-system.md` §6-§8
- 2026 supplementary reference: `apps/pc-keiba-viewer/tmp/candidate-jra-summer3-local-replay-2026-07-17/`
  (team-lead-shared, high-fidelity 264-race local replay)
- v2 addendum (§7): `apps/pc-keiba-viewer/tmp/candidate-jra-cell-model-ledger-2026-07-17/v2-multiseed/`
  — `train_269_seeds.py` + `train.log` (2 new seeds, 6 models),
  `base_preds_seed{101,2026}.parquet` / `cand_preds_seed{101,2026}.parquet`,
  `eval_3seed.py` + `eval.log` + `eval_3seed_result.json` (3-seed gate re-test)
