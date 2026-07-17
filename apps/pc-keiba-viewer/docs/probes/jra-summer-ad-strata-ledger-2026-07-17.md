# JRA Summer-4-Venue Condition A-D Stratified Accuracy Ledger (2026-07-17)

- **Date**: 2026-07-17
- **Category**: JRA finish-position champion serving-accuracy diagnostic (descriptive, NOT a lever-adoption test)
- **Campaign**: `2026-07-17-summer4` (夏4場 cell 精度向上 + MLflow 記録キャンペーン)
- **Task**: wave5-1 — implement the original USER goal's conditions A-D
  (開催日目/枠/騎手勝率/血統系統, i.e. day-of-meeting / draw / jockey win-rate /
  bloodline-line) as **evaluation strata over the already-deployed champion
  model**, not as new trained features. This is a purely descriptive ledger:
  does the champion's already-achieved accuracy differ systematically when the
  2024/2025/2026 summer-4-venue serving population is sliced along these four
  axes? No training, no feature changes, no adoption decision.

## 0. Scope note (read first)

This ledger is orthogonal to two other closed threads on the exact same
condition letters — stated up front to avoid confusion:

- **Condition D (血統) as a trained FEATURE was already REJECTed twice on
  2026-07-04** (`docs/probes/jra-pedigree-winrate-clean-2026-07-04.md`,
  `docs/probes/jra-sire-line-2026-07-04.md`; memory
  `project_jra_pedigree_condition_d_closed_2026_07_04`): individual-sire EB
  rates and the FF/MFF 2-generation line-pooling proxy, both fed to CatBoost
  as new columns and retrained, added no signal armB-250 doesn't already
  capture via tree interactions with `keibajo_code`/`track_code`. **This
  ledger does not re-test that** — it adds no feature and trains nothing; it
  asks whether the model's _existing, unchanged_ predictions happen to be
  weaker or stronger for horses of a given bloodline class. Different
  question, no conflict. See section 5 for how this ledger's one notable
  finding cross-validates rather than contradicts that REJECT.
- **Conditions A-D as a reinforcement-learning formulation** were separately
  assessed and closed 2026-07-17 (memory `index_closed_probes.md`, RL
  formulation entry): single-race pick RL is mathematically non-distinct from
  the existing supervised ranking objective; this ledger is the plain
  descriptive-statistics answer the RL closure deferred to.

## 1. Data and population

| Era       | Source                                                                                                                                                                                       | Venues                                                                              | Races | Model score                                                                                                                                                                                                              |
| --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 2024/2025 | Cached 3-seed champion base arm (`tmp/candidate-masked-lever-retest/models/base/seed{42,101,2026}/fold-{2024,2025}/model.json`) scored against `tmp/candidate-eval-jra/augmented` STORE_GLOB | 01/02/03/10 (all 4 summer venues)                                                   | 1,632 | 3-seed averaged `predicted_score` (averaged **before** ranking; one canonical ordinal rank per race — see section 2 for why)                                                                                             |
| 2026      | Already-scored high-fidelity local replay (`tmp/candidate-jra-summer3-local-replay-2026-07-17/scored.parquet`, `docs/probes/jra-summer3-local-replay-2026-07-17.md`)                         | 02/03/10 only (Sapporo was out of scope for that replay's original 6/1-7/12 window) | 264   | `rank_champion_only` (champion arm alone, NOT `rank_routed` — this ledger measures the champion model consistently across all 3 years; routing effects were already evaluated separately in the wave1/wave3 replay docs) |

Total: **25,211 horse-runs across 1,896 races**. All predict-only reuse of
existing artifacts; no training occurred. **2026's population is 3-venue
while 2024/2025 is 4-venue** — a real, stated coverage gap, not an oversight.

Market baseline throughout: `tansho_ninkijun` used as a full predicted rank
(not just favorite=1), same convention as every other ledger in this
campaign.

## 2. Metric definition (sort-before-mask-safe, key-based joins only)

For rank _N_ in 1-5 and predictor _P_ in {model, market}: the "slot-_N_" row
of a race is the row whose _P_-predicted rank equals _N_. `hit = 1` if that
row's actual `finish_position == N`. This is **identical** to
`subgroup_diagnostics._placeN_per_race`'s per-race definition (verified by
reading its source, not assumed) when only race-level dims are used for
grouping, and extends naturally to horse-level stratification dims
(waku/rs-class/jockey-tier/bloodline-line) by carrying that specific slot-_N_
row's own horse attributes — i.e. "when the champion's rank-_N_ pick happens
to be an inside-draw / low-jockey-tier / Sunday-Silence-line horse, how often
is that pick right". Market comparison for horse-level-stratified cells uses
the **same races** (membership decided by the model's rank-_N_ pick) but the
market's own rank-_N_ hit in those races — race_id stays the shared pairing
key for bootstrap, exactly analogous to every other `paired()` in this
campaign. No positional numpy mask is used anywhere; every cell membership
test is a `.filter()` against a race_id-keyed frame (doc section 7.3 /
anti-pattern #11 compliance).

`top3_box` (predicted top-3 SET == actual top-3 SET) has no per-horse-slot
decomposition, so it is only reported on race-level-dims-only crosses (one
level coarser than each series' full stratification).

3-seed ensembling for 2024/2025 averages `predicted_score` **before** ranking
(not seed-averaged hit booleans, unlike `champion_ledger.py`'s pooled-metric
convention) — this ledger needs one well-defined "who occupies rank-slot-_N_"
horse identity per race for the horse-level strata, which seed-averaged-hit
cannot provide. Stated explicitly as a deliberate, documented departure.

Bootstrap: race-paired, 2000 resamples, percentile 2.5/97.5 for LB95/UB95,
seed 20260717. Delta convention: **model − market** throughout.

## 3. Dimension derivations

| Dimension      | Column               | Construction                                                                                                                                                                                                   | Note                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| -------------- | -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| class          | `class_code`         | raw `kyoso_joken_code` (6 values in this population: 703/005/010/016/701/999, each n≥850 before crossing)                                                                                                      | **Deliberate departure** from `class_label` (grade_code-derived, the convention `champion_ledger.py`/local-replay used elsewhere this campaign): grade_code only flags GRADED stakes, so 74% of this population collapsed into one undifferentiated "unknown" bucket under that definition — useless for a "class" stratum whose point is separating the competitive ladder (maiden/1-win/2-win/3-win/open). `class_label` remains correct for graded-stakes-focused ledgers; just not this one. |
| distance       | `distance_band`      | `subgroup_diagnostics._distance_band_expr` (canonical, unmodified)                                                                                                                                             | sprint/mile/intermediate/long/extended                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| day-of-meeting | `day_band`           | JRA's own `kaisai_nichime` (joined from `jvd_ra`, NOT a date-gap heuristic — see below), coarsened to early (day≤4) / late (day≥5)                                                                             | See failure note below                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| draw           | `waku_band`          | `umaban_norm` proxy (raw `wakuban` absent from STORE_GLOB): inside <0.33, mid <0.67, else outside                                                                                                              | Same proxy convention as `fetch_score_eval.py` elsewhere in this codebase                                                                                                                                                                                                                                                                                                                                                                                                                        |
| running style  | `rs_class`           | `rs_predicted_class` direct (nige/senkou/sashi/oikomi)                                                                                                                                                         | 88.4% non-null in this population (99.2% was the pooled 2024/2025-only figure checked pre-build; 2026's replay lowers the blended rate slightly) — no fallback needed                                                                                                                                                                                                                                                                                                                            |
| jockey tier    | `jockey_tier`        | Tercile of `jockey_career_win_rate`, **global cutpoints** pooled across all 2024+2025+2026 summer rows (0.0481 / 0.0678)                                                                                       | Descriptive ledger, not a trained feature — pooled-era cutpoints are the simplest defensible choice, stated explicitly                                                                                                                                                                                                                                                                                                                                                                           |
| sire line      | `sire_line_class`    | `jvd_um.ketto_joho_03b` (paternal grandsire, FF — 2-generation proxy per the pre-verified `tmp/candidate-jra-sire-line/build_sire_line.py` convention), classified "SS系" if == サンデーサイレンス else "他系" | 405/10,801 unique horses (3.7%) SS系 by this axis                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| damsire line   | `damsire_line_class` | `jvd_um.ketto_joho_11b` (damsire's sire, MFF), same SS-line-vs-other split                                                                                                                                     | 3,003/10,801 unique horses (27.8%) SS系 by this axis — much more balanced than the paternal axis, a real and well-known asymmetry (Sunday Silence's influence in current JRA breeding runs overwhelmingly through the broodmare-sire path)                                                                                                                                                                                                                                                       |

**Day-of-meeting: two false starts before the working construction, recorded
for anyone touching this again.** First attempt inferred meeting boundaries
from date gaps (>3 days = new meeting); this collapsed to just 2 values
because same-meeting weekend-to-weekend gaps (~5-6 days, Sunday to the
following Saturday) also exceeded the 3-day threshold, incorrectly starting a
"new meeting" every week. Fixed by joining JRA's own official
`kaisai_kai`/`kaisai_nichime` fields directly from `jvd_ra` instead of
re-deriving them — these are on the source data already and should have been
used from the start. Second issue: even with the correct `kaisai_nichime`
values (which range 1-12 for this population), crossing raw day-of-meeting
into any 3+-dimension stratum left **zero** cells above n≥100 (checked: even
the coarsest 3-way race-level venue×distance_band×day-of-meeting table topped
out at n=33 per combo). Coarsened to an early(≤4)/late(≥5) band — a ~50/50
split (12,838/12,373 rows) that also better matches what this condition is
actually meant to probe (within-meeting drift/track-bias-accumulation, not a
specific day number).

**Sire-line name matching bug, caught before use**: `jvd_um`'s
`ketto_joho_03b`/`_11b` name columns are fixed-width, padded with **U+3000
ideographic space**, not ASCII space. PostgreSQL's `trim()` only strips ASCII
space by default, so a first-pass exact-match against `"サンデーサイレンス"`
silently matched **zero** horses (all 10,801 fell into "他系"). Fixed with
`trim(both '　' from trim(col))` (explicit U+3000 strip); re-verified against
a known-good count (24,413 unique-horse matches for the Sunday Silence FF
group, matching the number independently reported in
`build_sire_line.py`'s pre-verification docstring).

## 4. Reporting rule

Only cells with **n≥100 races** enter the main tables (else multiple-
comparison noise on this population size). Among those, only cells with
**UB95pp<0** ("robust-deficient") or **LB95pp>0** ("robust-superior") are
individually highlighted; everything else is summarized as a compact
distribution (combos evaluated, combos ≥100, delta_pp range/mean among
survivors).

## 5. Results

### 5.1 Pooled reference (context only — NOT the primary evaluation)

Per USER instruction "評価は常にcell単位で...要約(pooled)した精度で評価しない",
this table is reference-only context, not a claim:

| Rank     | Model % | Market % | Delta (pp) | LB95  | UB95  | n     |
| -------- | ------- | -------- | ---------- | ----- | ----- | ----- |
| 1        | 33.17   | 33.23    | −0.05      | −1.01 | +1.01 | 1,890 |
| 2        | 16.98   | 17.08    | −0.11      | −1.22 | +1.06 | 1,891 |
| 3        | 13.61   | 13.24    | +0.37      | −0.79 | +1.59 | 1,888 |
| 4        | 10.64   | 11.12    | −0.48      | −1.60 | +0.64 | 1,880 |
| 5        | 11.29   | 11.13    | +0.16      | −0.95 | +1.27 | 1,887 |
| top3_box | 9.02    | 8.81     | +0.21      | −0.32 | +0.74 | 1,896 |

Flat at pooled level, as expected — this is the whole reason the campaign
mandates cell-level evaluation.

### 5.2 A series (venue × class_code × distance_band × day_band × waku_band)

**2,481 combos evaluated across 5 ranks; zero reached n≥100.** This is
itself the finding for A: at this population size (1,896 races across 5
dimensions, at least 3-4 way multiplicative), condition A is **structurally
incapable of supporting an individual-rank-level statistical claim**. Even
the race-level-only 4-way cross one level coarser than A (venue×class_code×
distance_band×day_band, no waku_band) topped out at n=49 for its single
largest combo (venue03 × class 703 × intermediate × early) — below the
n≥100 bar before any horse-level dimension or rank-slicing is even applied.
`A_top3box` (that same 4-way race-level cross): 0/combos ≥100.

### 5.3 B series (A + rs_class)

**4,216 combos evaluated across 5 ranks; zero reached n≥100.** A strict
superset of A's fragmentation (6-way instead of 5-way) — expected to fail
even more completely, and does.

### 5.4 C series (venue × distance_band × day_band × jockey_tier)

**589 combos evaluated across 5 ranks; zero reached n≥100** at the
individual-rank level (jockey_tier's ~1/3 split reliably pushes every
otherwise-viable 3-way race-level cell below 100). The race-level-only 3-way
cross one level coarser (venue×distance_band×day_band, `C_top3box`) **does**
clear the bar in 4 cells:

| Venue        | Distance     | Day          | Model top3_box % | Market % | Delta (pp) | LB95  | UB95  | n   |
| ------------ | ------------ | ------------ | ---------------- | -------- | ---------- | ----- | ----- | --- |
| 02 Hakodate  | intermediate | late (d5+)   | 14.95            | 14.02    | +0.93      | −1.87 | +3.74 | 107 |
| 03 Fukushima | intermediate | early (d1-4) | 7.14             | 6.35     | +0.79      | −1.59 | +3.17 | 126 |
| 10 Kokura    | intermediate | early (d1-4) | 10.58            | 9.62     | +0.96      | 0.00  | +2.88 | 104 |
| 10 Kokura    | intermediate | late (d5+)   | 5.04             | 6.72     | −1.68      | −4.20 | 0.00  | 119 |

None robust (all CIs straddle or touch zero). No individual-rank claim is
possible for C at this population size.

### 5.5 D series (venue × distance_band × sire_line_class / damsire_line_class)

The only series coarse enough (3-way, or 2-way at race-level) to reliably
clear n≥100. D_sire: 179 combos evaluated, **35 ≥100** (7 venue×distance
combos × 5 ranks); D_damsire: 200 evaluated, **25 ≥100**. `D_top3box`
(2-way race-level, shared by both axes): **7/8 possible venue×distance
combos ≥100**.

**Important caveat found while building this table**: every single
surviving D_sire/D_damsire cell at n≥100, at every rank, is "他系" (non-
Sunday-Silence). The "SS系" stratum (3.7% of horses on the paternal axis,
27.8% on the damsire axis) **never once reaches n≥100** in any
venue×distance×rank combination on either axis, on this population size.
The 2-class design is only empirically testable on its majority side here —
stated plainly rather than silently reporting only the side that happens to
clear the bar.

D_top3box (venue × distance_band, race-level, both axes share this table):

| Venue        | Distance     | Model % | Market % | Delta (pp) | LB95  | UB95  | n   |
| ------------ | ------------ | ------- | -------- | ---------- | ----- | ----- | --- |
| 01 Sapporo   | intermediate | 10.42   | 9.72     | +0.69      | 0.00  | +2.08 | 144 |
| 02 Hakodate  | intermediate | 11.18   | 11.18    | 0.00       | −2.48 | +2.48 | 161 |
| 02 Hakodate  | mile         | 8.13    | 8.13     | 0.00       | 0.00  | 0.00  | 123 |
| 03 Fukushima | intermediate | 6.73    | 6.25     | +0.48      | −0.96 | +2.40 | 208 |
| 03 Fukushima | mile         | 14.17   | 13.39    | +0.79      | 0.00  | +2.36 | 127 |
| 10 Kokura    | intermediate | 7.62    | 8.07     | −0.45      | −1.79 | +0.90 | 223 |
| 10 Kokura    | mile         | 6.45    | 7.10     | −0.65      | −2.60 | +1.29 | 155 |

None robust.

D_sire rank1 (7 venue×distance cells, all "他系"; delta_pp distribution
across all 35 rank×cell combos: min −3.90 / median 0.00 / mean +0.19 /
max +5.79):

| Venue            | Distance     | Model %   | Market %  | Delta (pp) | LB95      | UB95      | n       | Robust?                   |
| ---------------- | ------------ | --------- | --------- | ---------- | --------- | --------- | ------- | ------------------------- |
| 10 Kokura        | intermediate | 33.18     | 34.10     | −0.92      | −3.69     | +1.84     | 217     | no                        |
| 03 Fukushima     | intermediate | 30.73     | 31.22     | −0.49      | −3.90     | +2.93     | 205     | no                        |
| 02 Hakodate      | intermediate | 41.25     | 41.25     | 0.00       | −3.13     | +3.13     | 160     | no                        |
| 10 Kokura        | mile         | 30.61     | 29.93     | +0.68      | −2.04     | +3.40     | 147     | no                        |
| 01 Sapporo       | intermediate | 35.66     | 37.06     | −1.40      | −5.59     | +2.80     | 143     | no                        |
| **03 Fukushima** | **mile**     | **38.02** | **32.23** | **+5.79**  | **+1.65** | **+9.92** | **121** | **YES — robust-superior** |
| 02 Hakodate      | mile         | 29.66     | 29.66     | 0.00       | −4.24     | +4.24     | 118     | no                        |

D_damsire rank1 (5 cells, all "他系"; delta_pp distribution across all 25
combos: min −3.97 / median −0.86 / mean −0.52 / max +3.96) — no robust
cells; all CIs straddle zero. Ranks 2-5 for both axes (32 more cells,
detail in `ad_strata_ledger.json`): same pattern, all straddle zero except
the one D_sire rank1 cell above.

## 6. The one robust finding: D_sire, venue03(Fukushima)×mile×他系, rank1

Champion beats market by **+5.79pp** [LB95 +1.65, UB95 +9.92] on rank1
predictions for non-Sunday-Silence-paternal-line horses in Fukushima mile
races, n=121 races.

**Multiple-comparison read (primary explanation)**: 60 D-series rank-level
cells were tested at nominal 95% two-sided CIs. Under a true-null everywhere,
the expected false-positive count is ~3; observing exactly 1 is unremarkable
and well within chance. The adjacent cells for the identical stratum
(Fukushima × intermediate × 他系: −0.49pp; Hakodate × mile × 他系: 0.00pp;
Kokura × mile × 他系: +0.68pp) show no supporting gradient — an isolated
single-cell hit with no neighboring corroboration is the classic multiple-
comparison signature, not a mechanism signature.

**Speculative mechanism (secondary, unconfirmed)**: if real, a Fukushima-
mile-specific market inefficiency for non-flagship-bloodline runners would
need a story about connections/targeting patterns specific to that course-
distance combination — no such story is substantiated here and none is
claimed as established.

**Dedup against the closed condition-D feature-lever tests** (section 0):
`project_jra_pedigree_condition_d_closed_2026_07_04` REJECTed adding
individual-sire and FF/MFF line-pooled win/top3-rate as **trained features**,
concluding armB-250's existing tree interactions with `keibajo_code`/
`track_code` already capture whatever bloodline×venue×surface signal exists.
This ledger's near-total flatness (59/60 D-series cells non-robust, pooled
flat, A/B/C structurally untestable) is **consistent with, and mildly
reinforces**, that root-cause takeaway: if the model already captures
bloodline-venue-distance interactions adequately, stratifying its _existing_
predictions by bloodline should show — and does show — no systematic
pattern. Not a re-test; a different, corroborating angle on the same
underlying question.

## 7. Verdict

**No robust-deficient stratum was found anywhere in the A-D ledger.**
Across all four conditions:

- **A and B are not evaluable at individual-rank granularity on this
  population size** — 2,481 and 4,216 combos evaluated respectively, zero
  survivors at n≥100. This is a real, reportable methodological finding in
  its own right: meaningful rank-level A/B monitoring would need either many
  more seasons of summer-4-venue data or a coarser stratification design
  than the one specified.
- **C is evaluable only at the race-level top3_box granularity** (one level
  coarser than specified), where it produces 4 cells, none robust.
- **D is the only condition that supports individual-rank-level testing** as
  specified, producing 60 rank-level cells (35 sire + 25 damsire) plus 7
  race-level top3_box cells. Exactly 1 of 67 total cells is robust, and it
  is best explained by multiple-comparison noise rather than a real effect
  (section 6).

Per the pre-registered fallback for a flat result: **stratifying the
champion's 2024-2026 summer-4-venue predictions by conditions A-D reveals no
systematic deficiency**. This is itself the final answer to the original
USER goal's A-D-stratification ask. Combined with the 2026-07-04
feature-lever closure of condition D and the RL-formulation closure of all
four conditions (section 0), all three angles on conditions A-D — as
features, as an RL objective, and now as evaluation strata — converge on the
same conclusion: no exploitable structure remains along these four axes at
the champion's current serving population size.

## 8. Reproducibility

- `tmp/candidate-jra-ad-strata-ledger-2026-07-17/build_universe.py` — builds
  `universe.parquet` (25,211 rows: 2024/2025 WF cached-model scoring +
  2026 replay reuse, all A-D dimension derivations).
- `tmp/candidate-jra-ad-strata-ledger-2026-07-17/compute_ad_series.py` —
  computes A/B/C/D series + pooled reference, writes `ad_strata_ledger.json`
  (full nested detail: every combo evaluated, every ≥100 survivor, robust
  flags).
- Both predict-only; no training; DuckDB `memory_limit=6GB`/`threads=4`
  throughout (well under the 12GB ceiling, not needed for this task's data
  volume).
