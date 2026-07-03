# JRA Class-Ordinal Encoding Fix — WF Test (2026-07-04)

- **Date**: 2026-07-04
- **Category**: JRA finish-position, feature-correctness fix + WF validation
- **Trigger**: discovered while profiling class-drop patterns for the summer
  upset-divergence analysis (`jra-summer-upset-divergence-2026-07-04.md`,
  Part 1 §5) — the existing `last_race_class_diff` feature (armB, all JRA)
  is built from a non-monotonic class ordinal.
- **Owner**: this doc (WF verification of the fix, task #9).

## The bug

`src/scripts/finish_position_features_duckdb.py`, `HORSE_HISTORY_BASE_SELECT`,
maps `kyoso_joken_code` to an ordinal `class_level` via:

```sql
case kyoso_joken_code
  when '000' then 0 when '005' then 1 when '010' then 2 when '016' then 3
  when '701' then 4 when '703' then 5 when '999' then 6
  else null end
```

`target_class_level - history_class_level` (current minus previous race) is
then exposed as the single armB feature `last_race_class_diff` — this is the
**only** armB feature derived from this ordinal (confirmed by grep: no other
feature references `target_class_level`/`history_class_level`, and armB does
not carry the raw class levels themselves as separate columns — see
"redundancy check" below).

For JRA, `701` = 新馬 (2yo newcomer, first-ever start) and `703` = 未勝利
(maiden / non-winners) — the two **lowest** competitive tiers. `005`/`010`/
`016` are the 1-win/2-win/3-win allowance tiers, and `999` is Open. The
existing ordinal ranks `701`(→4)/`703`(→5) **above** `005`(→1)/`010`(→2)/
`016`(→3) — i.e. it treats graduating from 未勝利 into 1-win allowance
(`703`→`005`) as a **class level DROP** (5→1, diff −4), when it is actually a
**promotion**. This is not a GBDT-invariant relabeling: `last_race_class_diff`
is a _difference_ of the encoded levels, so a non-monotonic encoding conflates
semantically different transitions with the wrong sign/magnitude.

### Verifying `000`'s meaning and scope (before touching anything)

Queried local Postgres (`jvd_ra`, read-only): for the 2013+ training window
the deployed model actually uses (`TRAIN_START=20130101`), only **6**
`kyoso_joken_code` values appear in JRA at all:

| code | n (2013-2026) | meaning             |
| ---- | ------------- | ------------------- |
| 703  | 16,786        | 未勝利 (maiden)     |
| 005  | 13,483        | 1勝クラス (1-win)   |
| 010  | 6,097         | 2勝クラス (2-win)   |
| 999  | 4,004         | オープン (open)     |
| 701  | 3,926         | 新馬 (2yo newcomer) |
| 016  | 2,656         | 3勝クラス (3-win)   |

`000` appears **exactly once** in the entire JRA history back to 1954 (a
1954 special race, `三歳馬優勝競走`) — a data artifact, not a real class tier
in the modern conditions system. `001`/`004`/`006`-`009`/`014`/`015`/`702`
are legacy pre-2013 codes (superseded prize-money-bracket renamings) that
also don't appear in the 2013+ training window. **This fix only needs to
correct the 6 codes actually in play**; `000` and the legacy codes are left
mapping to the same values as before (functionally irrelevant either way).

### Corrected ordinal

```
701 -> 0 (新馬)      703 -> 1 (未勝利)      005 -> 2 (1-win)
010 -> 3 (2-win)     016 -> 4 (3-win)       999 -> 5 (open)
```

Matches the team lead's stated JRA semantics exactly (新馬 lowest, then
未勝利, then 1/2/3-win, then Open).

### Redundancy check (done BEFORE training, per instructions)

Grepped `tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json`
(armB, 250 feat) for anything else derived from `kyoso_joken_code` /
`class_level` / raw class codes: **nothing else found**. armB does not carry
`target_class_level` or `history_class_level` as standalone features — those
are intermediate CTE columns in the production builder, only ever exposed
downstream as the single (buggy) difference feature. This means a depth-8
tree **cannot** reconstruct the correct transition semantics from raw levels
elsewhere in the feature set — the fix is not obviously redundant, unlike
scenarios where a model has independent access to both un-differenced
quantities.

## Build + validation

Corrected `last_race_class_diff_fixed` computed via a `LAG()` window function
directly on the offline store's own `kyoso_joken_code` column (one row per
horse per race, ordered by `race_date` per `ketto_toroku_bango`) — no
self-join needed, strictly-prior by construction. Script:
`tmp/candidate-jra-class-ordinal-fix/build_class_fix.py`.

**Validation**: recomputed the _original_ ordinal the same way and compared
against the store's existing `last_race_class_diff` on non-null overlap:
**97.7% row-level match** (566,521 compared, 12,821 mismatches, evenly
distributed across 2013-2025, ~900-1,200/year — not concentrated at the 2013
window boundary). Most likely explanation: the production history join's
source table includes rows (e.g. scratched/DNF starts) that this offline
store's own finished-race projection doesn't retain, so a small fraction of
"previous race" lookups skip an intermediate race that production saw. This
residual mismatch is small and consistent across the whole window — treated
as an acceptable, disclosed approximation for this WF probe, not a blocking
issue.

**Value-changed rate**: 7.9% of all rows (50,179/635,453) get a different
`last_race_class_diff` value under the corrected ordinal — this is the
mechanism's blast radius (transitions crossing the newcomer/maiden ↔
1/2/3-win boundary; transitions entirely within 005/010/016 or entirely
within 701/703 are unaffected since relative order is preserved there).

## WF test

Control = armB 250-feat as-is (control models for seed42/seed101 reused
byte-for-byte from sibling workstreams — `tmp/candidate-jra-meetingday-waku-clean/models/base/`
and `tmp/candidate-jra-jockey-winrate-clean/models/base/`, which independently
trained the identical armB-as-is control spec and produced matching file
sizes; seed2026 control trained fresh here). Treatment = armB with
`last_race_class_diff` replaced by `last_race_class_diff_fixed` (same
feature count, 250). CatBoost YetiRank, it=300, depth=8, lr=0.05, l2=3.0, 3
folds (2023/2024/2025, train-from-2013) × 3 seeds (42/101/2026), paired
race-level bootstrap (2000 iters). Total wall time 839.3s.

### Global, pooled (all folds × seeds, n=93,285 race-fold-seed rows)

| metric     | base % | cand % | Δ (pp)     | LB95 (pp) |
| ---------- | ------ | ------ | ---------- | --------- |
| **top1**   | 33.816 | 33.761 | **−0.055** | −0.159    |
| **place2** | 18.151 | 18.122 | **−0.029** | −0.159    |
| **place3** | 14.195 | 14.141 | **−0.055** | −0.183    |
| place4     | 12.208 | 12.044 | −0.164     | −0.295    |
| place5     | 11.044 | 10.860 | −0.183     | −0.299    |
| place6     | 10.333 | 10.497 | +0.164     | +0.058    |
| top3_box   | 9.397  | 9.420  | +0.023     | −0.039    |

**All 3 primaries are negative.** None reach the +0.08pp adopt threshold in
either direction; place3 and top1 sit right at (top1 exactly, place3 just
under) the −0.05pp no-regression floor. place4/place5 show a somewhat larger,
LB95-negative decline. Only the loosest metrics (place6, top3_box) tick up
marginally, and top3_box's LB95 still crosses zero.

### Stability across seeds and folds (top1 / place2 / place3, Δpp)

|        | seed 42 | seed 101   | seed 2026 |
| ------ | ------- | ---------- | --------- |
| top1   | +0.068  | **−0.125** | −0.106    |
| place2 | −0.068  | −0.058     | +0.039    |
| place3 | −0.029  | −0.077     | −0.058    |

|        | 2023   | 2024       | 2025       |
| ------ | ------ | ---------- | ---------- |
| top1   | −0.048 | −0.126     | +0.010     |
| place2 | −0.212 | **+0.309** | −0.183     |
| place3 | +0.193 | −0.087     | **−0.270** |

top1 and place2 **flip sign** across both seeds and folds — this is not a
stable, direction-consistent effect at any granularity. place3 is negative
in 2 of 3 seeds and 2 of 3 folds.

### Summer-restricted (01/02/03/10, n=22,032)

| metric   | Δ (pp)     | LB95 (pp) |
| -------- | ---------- | --------- |
| top1     | **−0.191** | −0.409    |
| place2   | −0.027     | −0.286    |
| place3   | −0.068     | −0.318    |
| top3_box | +0.014     | −0.114    |

Net negative across all 3 primaries — the summer venues (where the earlier
upset-profile doc's class-drop investigation originated) show **no** special
benefit from the fix; if anything, top1 degrades more there (−0.19pp) than
in the global pool (−0.055pp).

### Class-band cells (kyoso_joken_code) — the mechanism's home turf

| class        | n races | top1 Δ (LB95)   | place2 Δ (LB95) | place3 Δ (LB95)     |
| ------------ | ------- | --------------- | --------------- | ------------------- |
| 701 (新馬)   | 8,172   | −0.367 (−0.673) | +0.220 (−0.159) | −0.477 (−0.869)     |
| 703 (未勝利) | 33,390  | −0.045 (−0.225) | −0.072 (−0.282) | −0.117 (−0.321)     |
| 005 (1-win)  | 24,984  | −0.060 (−0.272) | +0.048 (−0.200) | −0.012 (−0.264)     |
| 010 (2-win)  | 12,600  | −0.048 (−0.302) | −0.214 (−0.540) | −0.119 (−0.476)     |
| 016 (3-win)  | 5,760   | +0.156 (−0.260) | −0.104 (−0.625) | **+0.573 (+0.035)** |
| 999 (open)   | 8,379   | +0.072 (−0.335) | 0.000 (−0.418)  | +0.143 (−0.346)     |

The 701 cell (newcomer races) is by construction **unaffected by this fix**
(these horses have no prior race, so `last_race_class_diff` is null in both
arms regardless of ordinal) — its negative delta here is pure training noise
across arms, not a fix effect, and is a useful internal check on the overall
noise floor of this harness (±0.4-0.9pp swings even where the two arms are
feature-identical for that subpopulation).

703 (maiden — where the reordering should matter most, since these horses'
prior race is very often a 701 debut, and their _next_ race after a win is 005) shows **negative** deltas on all 3 primaries. The single positive-LB95
result in this table (016 place3, +0.573 [+0.035], n=5,760) is 1 of 18
class×metric comparisons in this table alone (60 across all three cell
dimensions) — at a nominal 95% CI, ~3 false positives are expected from 60
comparisons by chance, and this same cell's top1/place2 are flat-to-negative,
which is itself evidence against a genuine localized effect (a real
improvement should move primaries in the same direction, not 1-of-3).
keibajo_code=08 (Kyoto) top1 (+0.434 [+0.159], n=13,815) is the only other
positive-LB95 hit among the 30 venue×metric comparisons — same caveat.

## Verdict: **REJECT**

Does not clear the gate (`>=2/3 primaries delta>=+0.08pp AND LB95>0, >=1 of
place2/place3, no metric<-0.05pp`) at any level checked — global, per-seed,
per-fold, summer-restricted, or class-band cells. All 3 primaries are
negative pooled; sign is unstable across seeds/folds; the two positive-LB95
cell hits are consistent with multiple-comparison noise, not a real,
direction-consistent effect. This matches the NAR precedent the team lead
flagged (`project_nar_g1f1_combined_adopt_2026_06_12`): a well-reasoned,
technically-correct fix of a real encoding quirk does not translate into a
serve-relevant accuracy gain, and shows mild (sub-threshold, mostly
noise-band) net-negative drift instead.

**Why the redundancy check didn't predict this**: armB has no _other_
feature carrying the raw class levels, so the fix wasn't obviously
redundant on paper. In practice it likely fails for a different reason: (a)
`last_race_class_diff` is one feature among 250 in a model whose primary
signal is the betting market / recent-form composites (per this campaign's
repeated finding that odds-adjacent signal dominates), so even a real
encoding improvement in one minor feature has limited room to move top1; (b)
several _other_ armB features already correlate with "is this horse in a low
condition tier" (`is_grade_race`, `target_grade_trial_count`,
`same_grade_win_rate`, etc.), providing an indirect back-channel that
dilutes this fix's marginal information gain; (c) only 7.9% of rows change
value at all, and retraining noise at this feature-perturbation scale (a few
percent of rows shifting one feature's value) is plausibly on the same order
as the effect itself, which is exactly what the per-seed/per-fold sign
instability shows.

## Source-correctness recommendation (separate from the accuracy verdict)

The encoding bug in `finish_position_features_duckdb.py`
(`HORSE_HISTORY_BASE_SELECT`'s `case kyoso_joken_code ... end`) is **still a
genuine correctness issue independent of this WF result** — it does not
represent JRA's actual class hierarchy (ranks 新馬/未勝利 above 1/2/3-win
allowance), and the current `last_race_class_diff` values are
mis-signed/mis-scaled for any transition crossing that boundary (7.9% of
rows). That is a real, documentable bug.

**Recommendation: do NOT patch the production source file on the strength of
this task alone.** The accuracy case for touching it is now negative (this
WF), and per this repo's own coverage/change-control rules, editing
`finish_position_features_duckdb.py` would require its own test updates,
would force a **full retrain of the live model** (last_race_class_diff
values would shift for every future training run), and per this campaign's
established discipline (`feedback_hpo_selection_bias_blind_holdout`,
NAR-fix precedent) any correctness-motivated retrain still needs its own
fresh WF gate before deploy — it cannot be assumed safe just because the
old encoding was "wrong." Given this WF already tested exactly that retrain
and found no benefit (mild net-negative), a source patch today would trade a
real (if currently low-impact) code-correctness improvement for a
measured, if small, accuracy cost and non-trivial retrain/redeploy
overhead, for zero demonstrated upside.

If this is ever revisited (e.g. as part of an unrelated broader feature-store
refactor where a retrain is happening anyway), the fix is cheap to fold in at
that time — but should not be treated as pre-validated by this doc; the
correctness argument and the accuracy argument are genuinely separate, and
only the correctness one currently favors a change. Flagging this doc as the
record of "known encoding quirk, deliberately left as-is, WF-tested and
found non-beneficial to fix in isolation" for whoever next touches this CASE
statement.

## Artifacts

- Build + validation: `tmp/candidate-jra-class-ordinal-fix/build_class_fix.py`,
  `build_report.json`, `class_fix_features.parquet`
- WF harness: `tmp/candidate-jra-class-ordinal-fix/wf.py`, `wf_report.json`,
  `wf.log`
- Control models reused (byte-identical spec) from sibling workstreams:
  `tmp/candidate-jra-meetingday-waku-clean/models/base/seed{42,101}/fold-*/model.json`
  (copied into this dir's `models/base/`, seed2026 control trained fresh here)
- Related: `jra-summer-upset-divergence-2026-07-04.md` (where the bug was
  first noticed)
