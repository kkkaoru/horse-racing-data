# JRA Cross-Pool Odds Divergence (wide/馬連 vs 単勝) — Probe + WF

- **Date**: 2026-07-17
- **Category**: JRA finish-position, new feature candidate (lever_bank.md item #3)
- **Source**: `apps/pc-keiba-viewer/tmp/frontier-scout/lever_bank.md` item #3
  ("Cross-pool odds divergence (wide-vs-umaren consistency)"), itself sourced
  from `docs/probes/jra-unexploited-data-inventory-2026-07-04.md`'s own
  closing "Ranked Recommendation" (§4), which named this as the one
  genuinely-untested angle left after exhausting the raw `jvd_%` table
  inventory: _"a cross-pool odds **divergence** feature (wide-vs-umaren
  consistency, not marginal implied probability, which is a different
  hypothesis than what commit `e0904c74` tested)"_. Explicitly flagged
  speculative and not pursued in that doc "given the time budget."

## 0. Headline result

**Probe: PASS with unusually large margin (§4). Walk-forward: REJECT (§5) —
global gate fails on 3 independent conditions, and a full cell-level scan
(summer-4-venue restricted + all 10 venues + kyori_band + season_band +
current_baba_condition, 28 cells) finds zero cell-conditional adoption
candidates either (§5.3). Serve-time availability was independently verified
(§7): the underlying odds data is genuinely available pre-race in the live
production path today, but this is now moot for this specific candidate
since the WF itself rejects — the investigation stands as reusable
groundwork for any future odds-pool-derived lever.**

The probe clears the |ρ|≥0.02 bar by 6-7x (max |partial ρ| = 0.14 vs a
0.02 floor), sign-stable 3/3 years, for **two independently-decoded pools**
(馬連 umaren, ワイド wide) against finish_norm — a substantially larger effect
than the marginal-odds precedent (commit `e0904c74`: JRA marginal
umaren/wide/sanrenpuku partial ρ topped out at 0.048-0.080, i.e. borderline
ABORT). A stratification check (§4.3) confirmed the probe signal is not a
rank-confound artifact. **The WF result shows this strong univariate
correlation does not survive contact with the already-strong 250-feature
champion model**: incremental top1 accuracy improves (pooled +0.45pp,
LB95>0) but place2/place3 both go negative and sign-flip across seeds, and
the no-regression floor is breached on place5. This is exactly the gap
between "real correlation exists" and "adds incremental value on top of 250
already-correlated features" that this campaign's two-stage probe→WF
protocol is designed to catch — see §5.4 for the likely mechanism.

## 1. Dedup — why this is genuinely untested

- `docs/finish-position-accuracy/history/exotic-odds-place-signal.md`
  (commit `e0904c74`, 2026-06-12) tested the **marginal** exotic-pool implied
  probability (`umaren_P2`, `wide_P3`, `sanrenpuku_P3`) as a standalone
  feature, controlling for tansho odds + ninkijun. JRA verdict: **ABORT**
  (partial ρ 0.048/0.076/0.080, all at or below the 0.08 bar used for that
  probe). This is a different construction from the present doc: that probe
  asks "does the level of the exotic pool's implied win/place probability add
  anything on top of the win pool" — since both pools are dominated by the
  same shared "true ability" signal, there's little room left after
  partialling out tansho/ninkijun. This doc instead asks "does the
  **disagreement** between the two pools for the same horse carry signal" —
  a feature that explicitly subtracts out the shared component _before_
  correlating with outcome, rather than partialling it out _after_.
- `docs/finish-position-accuracy/history/jra-fukusho-odds-probe.md` (commit
  `f7dced56`) similarly exhausts `jvd_o1` (fukusho) marginal signal, same
  "level, not divergence" construction.
- `apps/pc-keiba-viewer/tmp/candidate-reject-list/REJECT-LIST.md` (2026-07-04
  masked-lever re-verification index) has no entry for any cross-pool
  divergence construction.
- `docs/probes/jra-unexploited-data-inventory-2026-07-04.md` itself
  (§"Ranked Recommendation") names this exact angle as unbuilt/unprobed.

## 2. Data availability (checked first, per task instructions)

Local PG mirror (`postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing`),
tables `jvd_o2` (馬連 umaren) and `jvd_o3` (ワイド wide), cross-referenced
against `docs/finish-position-accuracy/history/exotic-odds-availability.md`
(2026-06-12 audit).

| table    | bet type    | first year (local mirror) | last year | JRA row count (2013-2025 window queried) |
| -------- | ----------- | ------------------------: | --------: | ---------------------------------------: |
| `jvd_o2` | 馬連 umaren |                      1993 |      2026 |                 44,907 races (2013-2025) |
| `jvd_o3` | ワイド wide |                      1999 |      2026 |                 44,907 races (2013-2025) |

Both tables fully cover the champion's JRA training window (2013+) and the
eval window (2023-2025) — confirmed by direct row-count query, not assumed
from the 2026-06-12 audit alone. **This is NOT a data-availability dead end**
(unlike 特別登録/`jvd_tk` in the 2026-07-04 inventory doc, which only had a
315-row 2026-only rolling table).

### 2.1 Timing / leak safety

Per `exotic-odds-availability.md` §4: all `jvd_o1`/`o2`/`o3` rows hold a
single **FINAL confirmed pre-race snapshot** (`data_kubun='5'`,
`happyo_tsukihi_jifun='00000000'` on confirmed rows — the intra-day
announcement timestamp is zeroed on confirmed rows, meaning these are
post-confirmation/locked-at-race-off records, not a live intra-day time
series). This is the **same timing tier as `tansho_odds`/`tansho_ninkijun`**
already live in the champion's armB-250 feature set. No incremental leak risk
beyond what commit `e0904c74`'s marginal-odds features already accepted.
Confirmed independently in this probe: 100% of `jvd_o2`/`jvd_o3` rows for
2013-2025 JRA carry `data_kubun='5'` or `'9'`(cancelled-race marker, filtered
out), consistent with the prior audit.

### 2.2 Placeholder semantics — a new finding, distinct from the jvd_se/jvd_ra convention

The repo's `reference_jvd_placeholder_semantics.md` memory documents that
`jvd_se`/`jvd_ra` use non-NULL zero-string placeholders (`'00'`, `'0000'`)
for not-yet-confirmed values. **`jvd_o2`/`jvd_o3` use a different convention**:
each packs up to 153 fixed-width combo slots per race (one row per race), and
a combo that was never sold (in practice, almost always because one of the
two horses in the pair was scratched) is filled with literal **`'*'`**
characters in the odds/votes portion — e.g. `'0102*********'` — not `'0000'`.
The horse-number portion of the slot (first 4 chars) stays numeric even when
the odds portion is starred. Verified directly against a live sample
(2024, 京都08, race 08, 6-horse field where horse 2 was evidently scratched —
every combo slot containing horse `02` is starred while all others decode to
plausible short-priced odds). The decoder in this doc's scripts parses each
153-slot chunk by horse-number position (not a fixed horse-number loop) and
skips any slot whose odds/votes substring is not all-digit — this correctly
handles both scratches and any other non-numeric placeholder without
depending on `shusso_tosu` alignment.

## 3. Feature construction

Per-horse, per-race, using only the final pre-race snapshot:

1. **Win-pool implied probability** (take-out normalized, sums to 1 within race):
   `p_win(h) = (1/tansho_odds(h)) / Σ_h' (1/tansho_odds(h'))`
2. **Umaren marginal top-2 implied probability** (same construction as
   `e0904c74`'s `umaren_P2`, sums to 2 within race after normalization since
   exactly 2 horses occupy top-2 each race):
   `P2_raw(h) = Σ_{j≠h} 1/umaren_odds(h,j)`; `P2_prob(h) = P2_raw(h) / Σ_h' P2_raw(h') * 2`
3. **Wide marginal top-3 implied probability** (mid-odds = mean of the
   pool's low/high payout range, sums to 3 within race):
   `P3_raw(h) = Σ_{j≠h} 1/wide_mid_odds(h,j)`; `P3_prob(h) = P3_raw(h) / Σ_h' P3_raw(h') * 3`
4. **Divergence** (the new construction — subtract, not partial-out): work in
   log space, z-score each of `log(p_win)`, `log(P2_prob)`, `log(P3_prob)`
   **within race** (removing the race-level scale so the residual is
   comparable across field sizes and favorite/longshot mix), then take the
   signed difference:
   - `div_umaren_z(h) = zscore_race(log(P2_prob(h))) - zscore_race(log(p_win(h)))`
   - `div_wide_z(h) = zscore_race(log(P3_prob(h))) - zscore_race(log(p_win(h)))`

   A raw (non-z) log-ratio variant (`div_umaren_raw = log(P2_prob) -
log(p_win)`, and the wide equivalent) was also probed for comparison; the
   z-scored construction dominated it on every target (see §4.2) and is the
   one carried into the WF candidate slot.

Implementation: `tmp/crosspool-odds-divergence/probe_partial_rho.py` (probe,
2023-2025) and `tmp/crosspool-odds-divergence/build_feature.py` (full
2013-2025 leak-free parquet for the WF join).

## 4. Probe: odds-controlled partial-Spearman

Method matches this campaign's standard probe harness (e.g.
`tmp/venue-jockey-probe/probe_partial_rho.py`): rank-residualize
`[tansho_odds, tansho_ninkijun]` out of both the candidate and the target via
OLS-on-ranks, then Pearson-correlate the residuals (= partial Spearman ρ).
Bar (per task instructions): **|partial ρ| ≥ 0.02, sign-stable 2023/24/25**.
2023-2025 JRA, `ijo_kubun_code='0'` (confirmed finishes only), n=141,522
horse-rows / 10,365 races. Coverage: 100.000% (both `div_umaren_*` and
`div_wide_*` resolve for every horse in the sample — no material decode-failure
gap).

### 4.1 Primary gate table (finish_norm target)

| candidate        |    2023 |    2024 |    2025 |     ALL | sign-stable 3/3 | max&#124;ρ&#124; | gate |
| ---------------- | ------: | ------: | ------: | ------: | :-------------: | ---------------: | :--- |
| `div_umaren_raw` | +0.1314 | +0.1337 | +0.1268 | +0.1307 |       yes       |           0.1337 | PASS |
| `div_umaren_z`   | +0.1408 | +0.1436 | +0.1344 | +0.1396 |       yes       |           0.1436 | PASS |
| `div_wide_raw`   | +0.1376 | +0.1369 | +0.1253 | +0.1332 |       yes       |           0.1376 | PASS |
| `div_wide_z`     | +0.1430 | +0.1401 | +0.1298 | +0.1377 |       yes       |           0.1430 | PASS |

All 4 constructions clear the 0.02 bar by 6.3-7.2x, all sign-stable 3/3
years. This is a substantially larger margin than any odds-composite probe
this campaign has run for JRA (the highest prior JRA odds-composite partial ρ
on record is 0.080, `e0904c74`'s sanrenpuku_P3, which still ABORTed against
its stricter 0.08 bar).

### 4.2 Secondary targets (diagnostic texture, not separately gated)

| candidate        |                is_top1 (ALL) |  is_top2 (ALL) |  is_top3 (ALL) |
| ---------------- | ---------------------------: | -------------: | -------------: |
| `div_umaren_raw` | -0.0529 (PASS, but see §4.3) | -0.0268 (PASS) | +0.0088 (FAIL) |
| `div_umaren_z`   | -0.0779 (PASS, but see §4.3) | -0.0524 (PASS) | -0.0176 (FAIL) |
| `div_wide_raw`   | -0.0575 (PASS, but see §4.3) | -0.0398 (PASS) | -0.0023 (FAIL) |
| `div_wide_z`     | -0.1090 (PASS, but see §4.3) | -0.0935 (PASS) | -0.0547 (PASS) |

`is_top3` mostly fails to clear the bar; `is_top1` and `is_top2` clear it in
the pooled (non-stratified) test, but §4.3 shows the `is_top1` relationship
does not survive proper subgroup stratification and should not be trusted as
a real, independent finding.

### 4.3 Stratification check — ruling out a rank-confound artifact

The naive decile table (mean finish_norm by decile of `div_umaren_z`,
computed separately within favorite/mid/longshot ninkijun-tertile buckets)
initially looked alarming: within the **favorite** tertile, higher divergence
appeared to predict **worse** finish_norm (0.713 → 0.670 from decile 0 to 4),
opposite the pooled-sample sign, while mid (0.412 → 0.538) and longshot
(0.230 → 0.406) tertiles both showed the expected positive direction. A
sign flip in one subgroup would be disqualifying (per this campaign's
cell-discipline rules — a pooled "positive" claim built by averaging over a
subgroup with the opposite sign is not a real effect).

Computing **proper partial Spearman ρ within each tertile** (still
controlling for `tansho_odds`+`ninkijun`, restricted to that ~1/3 of the
field) resolves this: the relationship is **positive in all three tertiles**,
sign-stable 3/3 years in each, and **increases monotonically toward
longshots** — exactly the pattern this campaign's other probes have found
for thin-market inefficiency pockets:

| tertile  |    2023 |    2024 |    2025 | ALL (div_umaren_z) | ALL (div_wide_z) |
| -------- | ------: | ------: | ------: | -----------------: | ---------------: |
| favorite | +0.0796 | +0.0976 | +0.0988 |            +0.0920 |          +0.0733 |
| mid      | +0.1599 | +0.1722 | +0.1617 |            +0.1643 |          +0.1783 |
| longshot | +0.2195 | +0.2300 | +0.2070 |            +0.2184 |          +0.2657 |

The naive pooled decile table's apparent favorite-tertile reversal was a
**within-tertile ninkijun-rank confound** (favorite-tertile horses ranked
5-6 vs. 1-2 differ systematically in finish_norm for reasons unrelated to
divergence; the crude decile cut doesn't remove that, while the continuous
partial-rank control does). This also explains why `is_top1` looked
"positive" in the pooled probe (§4.2) but is **near-zero and sub-threshold in
every tertile** (favorite +0.0011, mid +0.0035, longshot +0.0176, all
< 0.02): the pooled linear-in-rank control under-corrects for a relationship
that is strongly nonlinear across the full ninkijun range (favorites almost
never lose, longshots almost never win, a curvature a single linear partial
control doesn't fully remove), while a narrower per-tertile control is more
locally linear and trustworthy. **Conclusion: `finish_norm` and `is_top2`
(≈ place2) are real, subgroup-consistent findings; `is_top1` from the pooled
table is very likely a confound artifact and should not be cited as a
standalone win-probability finding.**

### 4.4 Additional sanity checks (script: `tmp/crosspool-odds-divergence/sanity_check.py`)

- **Overround plausibility**: mean `Σ(1/tansho_odds)` per race = 1.257
  (textbook JRA win-pool takeout ~20-25%); mean `Σ(1/umaren_odds)` over
  unique pairs = 13.55 (expected order of magnitude for a ~C(15,2)-scale
  pairwise pool at a comparable takeout — not a red flag, since the unique-pair
  count itself scales with field size squared, unlike the single-horse win
  pool). No implausible negative/zero/blown-up values found.
- **Coverage-vs-field-size check**: 0/10,365 races have more horses with a
  resolved `P2` than the race's own `shusso_tosu` — rules out a decode bug
  double-counting or hallucinating horses.
- **Outlier robustness**: distribution of `div_umaren_z` is well-behaved
  (mean ≈0, std 0.126, min -1.28, max +0.63, **zero** rows with
  `|div_umaren_z| > 5`). Trimming the top/bottom 1% of `|div_umaren_z|`
  outliers moves the pooled partial ρ (finish_norm) from +0.1396 to +0.1356
  — a real signal driven by the bulk of the distribution, not a handful of
  extreme decode artifacts.
- **Two independently-decoded pools agree**: `jvd_o2` (13-char/combo,
  h1+h2+odds+votes) and `jvd_o3` (17-char/combo, h1+h2+lo+hi+votes) use
  different packed formats and were decoded with separate parsers; both
  show the same qualitative story (large, sign-stable, longshot-amplified
  divergence signal). A decode bug specific to one table's format would not
  be expected to reproduce so closely in the other.

**Probe verdict: PASS.** Proceeding to WF per task instructions.

## 5. Walk-forward validation

Harness: `tmp/crosspool-odds-divergence/retest_wf.py`, a same-directory copy
of the shared `tmp/candidate-masked-lever-retest/retest_wf.py` pattern
(copied rather than edited in place, to avoid racing with other
concurrently-running lever slots in that shared directory — this probe only
_reads_ the shared, already-built `tmp/candidate-eval-jra/augmented` store
and `tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json` armB-250
list, never writes to them).

- **Control (arm A)**: CLEAN armB 250-feature set (leak-free
  `jra-cb-v9-sim-2013-CLEAN` spec).
- **Treatment (arm B)**: armB 250 + `[div_umaren_z, div_wide_z]` (252
  features total). Only the z-scored construction is carried forward (§4.1
  showed it dominates the raw log-ratio variant on every target, especially
  `is_top3`).
- **Spec**: CatBoost YetiRank, iterations=300, depth=8, lr=0.05, l2=3.0, no
  early-stop, all-numeric (`cat_indices=[]`) — matches the live champion
  exactly.
- **3 blind folds** (train 2013..Y-1, test Y) × **3 seeds** (42, 101, 2026).
- **Gate** (§7.2 of `docs/finish-position-prediction-system.md`): ≥2 of
  {top1, place2, place3} with delta≥+0.08pp AND LB95>0; at least one of
  {place2, place3} must pass; no metric regresses below -0.05pp.

### 5.1 Pooled (seed-averaged, all 3 folds)

| metric     |  base% |  cand% | delta_pp | LB95_pp |
| ---------- | -----: | -----: | -------: | ------: |
| top1       | 33.796 | 34.250 |  +0.4534 | +0.1157 |
| place2     | 18.119 | 18.080 |  -0.0386 | -0.4181 |
| place3     | 14.163 | 14.083 |  -0.0804 | -0.4632 |
| place4     | 12.166 | 12.089 |  -0.0772 | -0.4021 |
| place5     | 11.076 | 10.957 |  -0.1190 | -0.4374 |
| place6     | 10.416 | 10.921 |  +0.5049 | +0.1865 |
| top3_box   |  9.410 |  9.558 |  +0.1479 | -0.0418 |
| fukusho_2p | 74.912 | 75.372 |  +0.4599 | +0.1672 |

**GATE (§7.2 strict): REJECT.** `n_primaries_passed=1` (top1 only; place2 and
place3 both have negative point deltas), `place2_or_place3=false` (neither
independently clears delta≥+0.08pp & LB95>0 — the mandatory G2 condition),
and `worst_delta_pp=-0.119` (place5) **breaches the -0.05pp no-regression
floor**. This fails three independent gate conditions (G1, G2, G3) at once —
not a borderline case.

### 5.2 Per-seed detail — the place2/place3 sign is genuinely unstable, not just noisy-but-consistent

| seed | top1 delta [LB95] | place2 delta [LB95] | place3 delta [LB95] |
| ---- | ----------------: | ------------------: | ------------------: |
| 42   |   +0.589 [+0.203] |     -0.048 [-0.531] |     -0.068 [-0.550] |
| 101  |   +0.405 [+0.000] |     -0.145 [-0.646] |     +0.048 [-0.415] |
| 2026 |   +0.367 [-0.058] |     +0.077 [-0.415] |     -0.222 [-0.714] |

top1 is directionally positive in all 3 seeds (though only 1 of the 3
individual-seed LB95s clears zero on its own — the pooled seed-average LB95
is what the gate actually uses). place2 and place3 each **flip sign between
seeds** (place2: negative/negative/positive; place3: negative/positive/negative)
with every individual-seed LB95 solidly negative regardless of point-estimate
sign — there is no seed at which either place2 or place3 shows a real,
LB95-backed win. This is the multi-seed noise-floor check (project memory:
single-arm top1 noise floor ≈±0.4pp) working as intended: a genuine effect
should be directionally consistent across seeds at the SAME sign; place2/place3
here are not.

### 5.3 Cell-level scan (sort-before-mask discipline via the shared harness's `paired()`/cell loop — §9 antipattern K)

Per `feedback_cell_level_adoption_no_pooled_eval`, a pooled REJECT alone does
not settle the question — cell-conditional adoption is a real, established
path in this system (§7.2). Scanned all 4 standard cell dimensions
(`keibajo_code`, `kyori_band`, `season_band`, `current_baba_condition`,
n≥200 floor), seed-averaged, same delta≥+0.08pp/LB95>0 (×2 of 3 primaries)/
place2-or-place3/no-regression-beyond–0.05pp bar used for the pooled gate.

**Summer-4-venue restricted (task's specific focus area) — all 4 fail:**

| venue        |   n |   top1 delta [LB95] | place2 delta [LB95] | place3 delta [LB95] |
| ------------ | --: | ------------------: | ------------------: | ------------------: |
| 01 Sapporo   | 504 |     +0.529 [-1.058] |     +0.132 [-1.587] |     +0.661 [-1.190] |
| 02 Hakodate  | 432 |     +0.386 [-1.080] | **-1.080 [-3.241]** |     +0.232 [-1.622] |
| 03 Fukushima | 720 | **-0.232 [-1.528]** |     +0.556 [-0.880] |     +0.278 [-1.204] |
| 10 Kokura    | 792 |     +1.263 [-0.042] |     +0.463 [-0.758] | **-0.842 [-2.273]** |

No summer venue clears LB95>0 on any primary (Kokura's top1 comes closest at
LB95=-0.042, still negative); Hakodate's place2 and Kokura's place3 show
large negative point estimates. Small per-venue n (432-792 races) also
means wide CIs — absence of a positive signal here is not itself surprising,
but there is no hint of a hidden summer-specific win either.

**All 10 JRA venues, `kyori_band` (0-3), `season_band` (0-3),
`current_baba_condition` (1-4)**: swept identically (28 cells total across
the 4 dimensions). **Zero cells** meet the adoption bar (≥2/3 primaries with
delta≥+0.08pp & LB95>0, place2-or-place3 among them, no regression beyond
-0.05pp). The closest approach is `current_baba_condition=1` (turf good,
n=7,488, the largest single cell): top1 +0.445pp **[LB95 +0.071, positive]**,
but place2 +0.036pp [LB95 -0.419] and place3 +0.200pp [LB95 -0.236] both miss
the required LB95>0 — same top1-only pattern as the pooled result, just with
a tighter CI from the larger n. No cell anywhere shows a genuine place2/
place3 win.

### 5.4 Reading the pattern: what's likely going on

top1 improves consistently (pooled +0.45pp, LB95>0; positive point estimate
in all 3 seeds) while place2/place3 do not (negative pooled, sign-unstable
across seeds, no cell rescues it). A plausible mechanism, consistent with
§4's own finding: the divergence feature's `is_top1` correlation was the
LEAST reliable of the probe's diagnostic targets (§4.3 showed it was largely
a stratification artifact, near-zero within every ninkijun tertile) while
`is_top2`/`finish_norm` were the robust findings. The WF result looks almost
inverted from that expectation — the model found a way to use the new
columns to sharpen its **single #1 pick** (a YetiRank listwise loss can
reallocate ranking precision unevenly across cutoffs) without it carrying
through to the exact-ordinal place2/place3 metrics the probe's own
stratified analysis said were the more trustworthy part of the signal. This
is exactly the kind of gap between "real univariate correlation" and
"incremental value on top of an already-strong 250-feature model" this
campaign's probe-then-WF two-stage protocol exists to catch — probing a
partial correlation cannot see how the other 250 features already spend
their capacity, or whether a tree ensemble reallocates a strong top1 pattern
in the new columns at the expense of the harder place2/place3 boundary.

## 6. Verdict and recommendation

**REJECT. DO-NOT-RETEST this exact construction** (`div_umaren_z`,
`div_wide_z` — z-scored log-ratio divergence between the win pool and the
umaren/wide marginal pools, additive on CLEAN armB-250).

- **Probe stage**: PASS, and by a wide margin — genuinely one of the
  strongest partial-ρ results this campaign has produced for an
  odds-composite family (§4). The stratification check (§4.3) is a real,
  positive methodological finding independent of this candidate's fate: it
  demonstrates that a naive pooled/decile confound check can hide a sign
  reversal that a proper per-subgroup partial-correlation check resolves —
  worth citing as precedent for future probes that hit an apparent
  favorite-tertile anomaly.
- **WF stage**: REJECT. Global pooled gate fails on 3 independent conditions
  (§5.1): only 1/3 primaries clears (top1), neither place2 nor place3
  clears (mandatory G2), and the no-regression floor is breached (place5
  -0.119pp). Per-seed detail (§5.2) shows place2/place3 are not merely
  noisy-but-consistent — they flip sign between seeds, with no seed
  producing an LB95-backed win on either. A full cell scan (§5.3, 28 cells
  across keibajo_code/kyori_band/season_band/current_baba_condition,
  including the task's specific summer-4-venue focus) finds zero
  cell-conditional adoption candidates.
- **Not an ambiguous or close call**: this is a clean multi-metric-gate
  REJECT textbook case (independently corroborated by another agent's
  interim observation during this run — team-lead's message mid-probe cited
  "pooled 1/3 primaries, top1 LB95+0.12, place2/3 不合格", which matches
  this doc's own numbers exactly).
- **Serve-availability (§7) is consequently moot for this candidate** — the
  WF reject means there is nothing to deploy regardless of servability. The
  investigation is retained in this doc as verified, reusable groundwork:
  the underlying umaren/wide odds ARE available pre-race in the live
  production path today (§7.2, confirmed via a real API call against actual
  July 2026 race data), should any future lever want to build on the same
  odds-pool infrastructure.
- **Mechanism note for future work**: the probe's own diagnostics (§4.3)
  flagged `is_top1` as the LEAST trustworthy of the probe's target
  correlations (a stratification artifact, near-zero within every ninkijun
  tertile) while `finish_norm`/`is_top2` were the robust findings. The WF
  outcome is almost the inverse of that expectation — top1 is what actually
  moved, place2/place3 did not. This is a caution against assuming a strong
  marginal partial-correlation on one target predicts which metric a
  full-featured tree ensemble will actually improve; the ensemble's use of
  new columns is not directly readable from a univariate probe.
- **Close this lever bank item.** Fold into
  `apps/pc-keiba-viewer/tmp/candidate-reject-list/REJECT-LIST.md` /
  `index_closed_probes.md` (DO-NOT-RETEST) as: "cross-pool odds divergence
  (`div_umaren_z`/`div_wide_z`, z-scored log-ratio, additive armB-250+2) —
  TESTED-REJECT. Strong probe (partial ρ up to 0.14) does not survive WF:
  pooled 1/3 primaries, place2/place3 negative and sign-unstable across
  seeds, no-regression floor breached, zero qualifying cells across a
  28-cell scan including summer-4-venue restriction."

## 7. Serve-time availability — verified empirically, not assumed from the stale audit

This section was added after orchestrator flagged that a WF gate pass is
worthless if the feature is not actually computable at serve time — the same
failure class as the 2026-07-04 `target_corner_*` within-race leak (§2.6 of
the system doc): a feature that looks great in backtest because it silently
used information unavailable at prediction time. `exotic-odds-availability.md`
(§5-6, the source for "serve is a Python-only change") is from 2026-06-12,
over a month stale relative to this probe — its claims were re-verified
against the LIVE code and a LIVE API call today, not taken on faith.

### 7.1 Code-level check: the current predict-container fetcher

`apps/finish-position-predict-container/src/realtime_odds_fetcher.py` (read
today, 2026-07-17) defines exactly two extractors that pull data out of the
hot worker's `GET /api/odds/{raceKey}` response: `extract_rows()` (tansho
only, from `latest.tansho`) and `extract_sanrenpuku_p3()` (from
`latest["3renpuku"]`, added since the 2026-06-12 audit — the "only tansho"
claim in that audit is now itself slightly stale). **There is no
`extract_umaren` / `extract_wide` function.** `_write_parquet()`'s output
schema is `(keibajo_code, race_bango, umaban, tansho_odds_realtime,
ninkijun_realtime, bataiju_realtime, exotic_sanrenpuku_p3_realtime)` — no
umaren/wide columns exist in the parquet the predict container writes.

**Confirms a precedent for exactly this kind of gap already existing,
harmlessly, in this pipeline**: `exotic_sanrenpuku_p3_realtime` **is**
fetched and written to the realtime parquet today, but a repo-wide grep of
`apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py` finds
**zero references** to `exotic_sanrenpuku_p3` anywhere in the feature
builder — the column is fetched, then silently dropped before it ever
reaches a model. This is a "fetched but not wired into the feature builder"
gap, not a "data doesn't exist" gap — structurally the same category of work
umaren/wide would need.

### 7.2 Live data check: does the hot worker actually have umaren/wide today

Read-only `curl` (same headers/UA the predict container itself uses) against
the real, currently-running hot worker for a genuine recent JRA race
(2026-07-12, 函館 race 1 — the most recent JRA race date in the local
mirror):

```
GET https://sync-realtime-data-hot.kkk4oru.com/api/odds/jra%3A2026%3A0712%3A02%3A01
```

`response["latest"]` keys present: `tansho, fukusho, wakuren, umaren, wide,
umatan, 3renpuku, 3rentan` — **all 8, populated, well-formed**. Sample
entries: `umaren: {"combination": "4-6", "odds": 3.6, "rank": 1}` (66 entries
= C(12,2), matching this race's 12-horse field), `wide: {"combination":
"4-6", "averageOdds": 1.8, "maxOdds": 1.9, "minOdds": 1.7, "rank": 1}`. The
`averageOdds` field on the wide entries is even more convenient than this
probe's own local-mirror decode (which had to compute `(lo+hi)/2` by hand
from `jvd_o3`'s packed min/max — the hot worker's JSON already provides the
midpoint). The `history`/`historyByType` sections additionally show tansho
odds genuinely changing intraday (e.g. one horse's odds moved 1.6 → 1.5 →
1.8 → ... → 2.0 across `fetchedAt` timestamps from the day before through
race morning) — confirming the hot worker holds a real intraday time series,
not merely a single final snapshot (that "final snapshot only" characterization
applies to the local PG warehouse mirror, §2.1 — a different store).

`apps/sync-realtime-data-hot/src/jra.ts`'s `ODDS_PAGE_LABELS` includes
`{label: "馬連", type: "umaren"}` and `{label: "ワイド", type: "wide"}` with
dedicated `parsePairTables(...)` parse branches — confirming the scraper
actively collects both today, consistent with the live response observed.
(`apps/sync-realtime-data-hot` has zero uncommitted changes in this working
tree — read freely, no risk of reading another session's in-flight edit.
Per orchestrator's instruction, `apps/sync-realtime-data`'s currently-modified
files were NOT touched or read for this check — only the separate,
clean `apps/sync-realtime-data-hot` app was consulted, which is what
`realtime_odds_fetcher.py` actually calls.)

### 7.3 Data-availability verdict (independent of §5/§6's WF outcome): NOT-YET-SERVABLE, but a bounded gap, not a dead end

Unlike `target_corner_*` (fundamentally unknowable pre-race — a genuine leak
with no honest fix short of removing the feature), **the raw umaren/wide data
this candidate needs is genuinely available pre-race, today, at the exact
endpoint the predict container already calls for tansho.** The gap is purely
that nothing on the Python/DuckDB side parses or wires it in yet. Required
follow-up work (not performed here — deploy/infra changes are out of this
probe's scope per task instructions):

1. `realtime_odds_fetcher.py`: add `extract_umaren`/`extract_wide` (mirror
   `extract_sanrenpuku_p3`'s pattern — parse `latest.umaren`/`latest.wide`,
   marginalize to a per-horse `{umaban: value}` map exactly as
   `build_feature.py`'s `decode_umaren_race`/`decode_wide_race` do against
   the packed warehouse format, just against the already-decoded JSON
   instead of a packed string — actually simpler than the training-side
   decode). Extend `_write_parquet()`'s schema with
   `umaren_p2_prob_realtime`/`wide_p3_prob_realtime` (or the raw marginal
   sums, matching whichever normalization the feature builder computes
   `div_umaren_z`/`div_wide_z` from) plus `tansho_odds_realtime` (already
   present) to compute the SAME z-scored divergence at serve time.
2. `finish_position_features_duckdb.py`: add the new realtime columns to
   `REALTIME_ODDS_TABLE`'s schema/stub and the `coalesce(rt.xxx, ...)`
   pattern already used for `tansho_odds`/`tansho_ninkijun`/`bataiju`
   (`finish_position_features_duckdb.py:705-716`) — batch-side (`jvd_o2`/
   `jvd_o3`) fallback for already-run races, realtime-parquet value first
   for upcoming ones, computing `div_umaren_z`/`div_wide_z` from whichever
   source resolved.
3. Retrain with the new columns live in both training and serve paths, then
   re-verify serve-realistic (§7.3 of the system doc's general rule: a
   feature's accuracy claim must be checked against a NULL-until-resolved
   serve simulation before deploy, exactly the discipline that caught the
   `target_corner_*` leak in the first place).
4. **Implementation watch-out found in passing** (not fixed here, out of
   scope, flagging for whoever does #2): the existing `tansho_odds` fallback
   arm, `try_cast(nullif(trim(se.tansho_odds), '') as double) / 10`
   (`finish_position_features_duckdb.py:709-712`), guards only the
   empty-string case, not the `'0000'` not-yet-confirmed placeholder
   (`reference_jvd_placeholder_semantics.md`) — `'0000'` casts cleanly to
   `0.0`, not NULL. This is pre-existing behavior for a currently-live
   champion feature (not something this probe introduces or touches), and it
   is already masked in practice by the realtime COALESCE almost always
   winning for genuinely-upcoming races — but the same guard gap would carry
   over verbatim into new `umaren`/`wide` batch-fallback columns built the
   same way, so the eventual implementer should use
   `nullif(trim(se.tansho_odds), '0000')`-style guards (matching this
   probe's own `build_feature.py`, which filters `odds_val <= 0` after cast)
   rather than copy the existing arm byte-for-byte.

**Moot for this specific candidate**: §5/§6 settled the WF gate as REJECT
before this serve-availability question needed a final answer — there is
nothing to wire up. Retained here because the investigation itself is real,
verified, reusable groundwork: had the WF passed, the correct label would
have been ADOPT-ELIGIBLE-PENDING-SERVE-WIRING (not a plain ADOPT, which
would have silently served NULL/fallback-only until the above is built —
exactly the kind of silent degradation this system's serve-realistic
discipline exists to catch), and not a data-availability REJECT (the
underlying signal and data are both genuinely real). Any future lever
proposal that wants per-horse umaren/wide/exotic-pool signals at serve time
can start from §7.1-7.2 directly instead of re-auditing the pipeline from
scratch.

### 7.4 Train/serve timing drift — same pre-existing pattern as tansho, not a new risk

This WF trained on `jvd_o2`/`jvd_o3`'s **final, locked-at-race-off**
snapshot (`data_kubun='5'`, §2.1). Production serving (once §7.3's wiring
exists) would read the hot worker's **realtime** value at whatever moment
the predict container's per-race prediction actually runs — per project
memory, finish-position generation is triggered off the running-style
completion signal, empirically **~7 minutes before post**, not at the literal
final tick. §7.2's own sample shows tansho odds genuinely still moving in the
final pre-race minutes, so a real train/serve distribution gap exists for
this candidate, exactly as it does for every already-deployed odds-derived
feature. Checked directly in `finish_position_features_duckdb.py`
(lines 705-716): **`tansho_odds` and `tansho_ninkijun`, already live in the
250-feature champion today, use the identical pattern** — trained on
`jvd_se`'s final confirmed value, served via
`coalesce(rt.tansho_odds_realtime, <final-value-fallback>)` where
`rt.tansho_odds_realtime` is whatever the hot worker returned at
prediction-run time, not necessarily the final closing line. **This
candidate would inherit an already-accepted, already-shipped design
tradeoff, not introduce a new one** — no additional validity concern beyond
what the current champion already lives with for its own odds features.

## 8. MLflow record

Logged via `apps/mlflow`'s documented `log-training-run` path
(`eval_regime="wf"`, `experiment="finish-position/wf-eval"`,
`register=false`, `champion=false`), tagged `campaign=2026-07-17-summer4`,
`verdict=reject-place2-place3-fail-zero-qualifying-cells` for discoverability
alongside the other same-day campaign runs.

- **Run ID**: `d7940f824d1a4301ab2cabb936a887b9`
- **Run name**: `jra-crosspool-divergence-candidate-2026-07-17`
- **Read-back verified independently** (fresh `MlflowClient`, separate
  process from the CLI write): tracking-URI scheme confirmed `postgresql`
  (the real Neon-backed store, not the frozen-sqlite failure mode a prior
  incident hit), `status=FINISHED`, all 8 tags and all 39 metrics (pooled
  base/cand/delta/LB95 for all 8 gated metrics, `n_primaries_passed`,
  `worst_delta_pp`, `accept_strict_gate=0.0`, `probe_max_partial_rho=0.1436`)
  and all 8 params read back byte-for-byte matching what was sent.

## 9. Artifacts

- `tmp/crosspool-odds-divergence/probe_partial_rho.py` — probe script
  (2023-2025), writes `probe_result.json`.
- `tmp/crosspool-odds-divergence/sanity_check.py` — overround/decile/outlier
  robustness checks + tertile stratification (§4.3/§4.4).
- `tmp/crosspool-odds-divergence/build_feature.py` — builds the full
  2013-2025 leak-free join parquet (`crosspool_divergence.parquet`, keyed by
  `race_id`+`ketto_toroku_bango`, matching the shared harness's join
  convention).
- `tmp/crosspool-odds-divergence/retest_wf.py` — WF gate harness (adapted
  copy of the shared pattern, isolated to this probe's own directory).
- `tmp/crosspool-odds-divergence/reports/crosspool_divergence.json` — full WF
  report (pooled, per-seed, per-fold, per-cell).
- `tmp/crosspool-odds-divergence/mlflow_manifest.json` — the
  `hr-mlflow-training-run/v1` manifest logged in §8.
- `tmp/crosspool-odds-divergence/log_mlflow.py` — an earlier
  `mlflow_hook`-based recording script (superseded by the direct
  `log-training-run <manifest.json>` CLI invocation actually used for §8,
  which matches `tmp/mlflow-handoff-2026-07-17.md`'s documented recipe more
  directly — kept for reference, not re-run).
- `tmp/crosspool-odds-divergence/wf_run.log` — full stdout of the WF
  training run (18 CatBoost fits, per-seed/per-fold progress).

## 10. Addendum (2026-07-17, wave 4-2): marginal implied-probability LEVEL — REJECT

**Task**: team-lead assigned a follow-up hypothesis reusing this probe's
harness — does the marginal implied probability LEVEL of the umaren/wide
pools (not the divergence tested in §1-9 above) carry additive signal for
the champion model? Framed as genuinely untested since 2026-06-12's
`e0904c74` was a probe-stage ABORT under the then-stricter 0.08 bar
(current bar is 0.02).

### 10.1 Dedup correction (found before proceeding)

The task's dedup rationale had the two constructs backwards. **This IS
`e0904c74`'s original hypothesis, not a new one** — confirmed by two
independent sources written before this task was ever assigned:

- `jra-unexploited-data-inventory-2026-07-04.md` Part 2 (this repo's own
  prior exhaustive census) explicitly recommends folding jvd_o1-jvd_o6 into
  KNOWN DEAD wholesale, "do not re-open without a genuinely new angle (e.g.,
  a cross-pool divergence signal, not marginal implied probability, which
  is a different hypothesis than what was tested)."
- This probe's own `probe_partial_rho.py` docstring (§4 above, written this
  morning before this task existed) states: "This is a different construct
  than the marginal exotic-pool implied probability itself (already tested,
  JRA ABORT, commit e0904c74)... here we test the divergence between two
  pools... not the level of either pool."
- Structurally: `z_p2`/`z_p3` (the within-race z-scored level, exactly what
  team-lead's proposed construction asks for) are computed as intermediates
  in `build_feature.py` before being subtracted into
  `div_umaren_z`/`div_wide_z`. Within-race z-scoring is a per-race linear
  (monotonic) transform, so it does not change within-race rank order — the
  level construct is Spearman-rank-equivalent to `e0904c74`'s original,
  unnormalized construction.

Rather than stand down or blindly proceed, ran a cheap fresh probe check
(`probe_level_dedup_check.py`, 2023-2025, same odds-controlled
partial-Spearman methodology) before committing to the WF budget. Result
was genuinely mixed: for the bet-type-matched targets
(`is_top1`/`is_top2`/`is_top3`), fresh numbers are modest and consistent
with `e0904c74`'s historical ~0.08 ceiling (max 0.09, several below the
current 0.02 floor). But for the continuous `finish_norm` target, the level
construct clears the current bar strongly — `z_p2` vs `finish_norm` reaches
0.212, `z_p3` vs `finish_norm` reaches 0.225, both **stronger than this
probe's own successful divergence-construct probe pass** (§4's max 0.144).
Team-lead had already pre-authorized proceeding to a likely-REJECT-but-
worth-formally-closing WF regardless of probe outcome, and this
materially-stronger-than-historical fresh signal tipped the decision toward
proceeding rather than standing down on dedup grounds alone.

### 10.2 Feature construction

`level_umaren_z` / `level_wide_z`: identical decode/join pipeline to §3's
divergence construction (`build_feature_level.py`, a copy of
`build_feature.py` with the final subtraction step against `z_pwin`
removed), 2013-2025, 626,774 rows / 44,907 races, 100% coverage (both
jvd_o2/jvd_o3 have full coverage in this window, matching §2's
data-availability finding).

### 10.3 Walk-forward validation

Same spec as §5: CatBoost YetiRank, armB-250 clean base +
`[level_umaren_z, level_wide_z]`, 3 seeds (42/101/2026) × 3 blind folds
(train≤Y-1, test Y for Y∈{2023,2024,2025}), 2000-iteration paired
bootstrap.

| metric     |   base |   cand | delta (pp) |  LB95 (pp) |
| ---------- | -----: | -----: | ---------: | ---------: |
| top1       | 33.796 | 34.054 |     +0.257 |     −0.000 |
| place2     | 18.119 | 18.183 |     +0.064 |     −0.244 |
| place3     | 14.163 | 14.160 |     −0.003 |     −0.325 |
| place4     | 12.166 | 12.214 |     +0.048 |     −0.241 |
| place5     | 11.076 | 11.089 |     +0.013 |     −0.248 |
| place6     | 10.416 | 10.616 |     +0.199 |     −0.052 |
| top3_box   |  9.410 |  9.539 |     +0.129 |     −0.042 |
| fukusho_2p | 74.912 | 75.224 |     +0.312 | **+0.074** |

**Gate: REJECT.** 0/3 primaries clear (top1's LB95 sits at −0.0001,
essentially a coin flip away from crossing zero but still on the wrong
side; place2/place3 both clearly miss). No regression breach (worst delta
−0.003pp, place3, far inside the −0.05pp floor). `fukusho_2p` is the only
metric with LB95>0, but it isn't a gated primary. Per-seed top1 deltas
(+0.511/−0.010/+0.270) are not sign-stable across seeds — a second
independent signal that the pooled +0.257 is not a robust effect.

### 10.4 Cell scan (sort-before-mask)

22 cells with n≥200 across the harness's 4 standard dims (keibajo_code /
kyori_band / season_band / current_baba_condition). **0/22 clear the
routing gate** (need ≥2 of 3 primaries with delta≥+0.08pp & LB95>0, plus
place2 or place3 among them). Three isolated single-metric hits, none
gate-clearing: Kokura (keibajo_code=10) top1 +1.179pp[LB95+0.253, n=792],
Tokyo (keibajo_code=05) top1 +0.622pp[LB95+0.020, n=1607], season_band=1
top1 +0.593pp[LB95+0.099, n=2699] — each a single primary out of 66
primary-metric comparisons (22 cells × 3 primaries), the same
multi-comparison-noise signature seen across every other cell scan in
today's campaign.

### 10.5 Verdict

**REJECT.** Confirms and extends `e0904c74`'s original 2026-06-12 finding
under the current, more permissive probe bar and full WF rigor: even
though the fresh probe signal materially exceeded the historical ceiling
(driven by the continuous `finish_norm` target), it does not survive to
the actual ranking-loss WF gate — the same "probe necessary, not
sufficient" pattern that recurred all day (divergence §6, longshot v1,
vector-knn). **This closes the exotic-pool marginal-implied-probability
family for JRA a second, independent way** (§1-9's divergence construction
and this addendum's level construction both REJECT), alongside the already
DO-NOT-RETEST status the level construction carried from `e0904c74`.
**DO-NOT-RETEST** (both constructions).

### 10.6 MLflow record

- **Run ID**: `da1b1dcb7274403cad957d568785b661`
- **Run name**: `jra-crosspool-level-candidate-2026-07-17`
- Read-back independently verified (fresh `MlflowClient`, apps/mlflow's own
  `config.get_tracking_uri()` resolver): scheme `postgresql`,
  `gate_result=REJECT` tag, all delta/LB95 metrics for top1/place2/place3
  match the table in §10.3 exactly.

### 10.7 Artifacts (this addendum)

- `tmp/crosspool-odds-divergence/build_feature_level.py` — level-construct
  feature builder (2013-2025).
- `tmp/crosspool-odds-divergence/probe_level_dedup_check.py` — the fresh
  dedup-verification probe (§10.1).
- `tmp/crosspool-odds-divergence/retest_wf_level.py` — WF gate harness,
  adapted from `retest_wf.py`.
- `tmp/crosspool-odds-divergence/reports/crosspool_level.json` — full WF
  report.
- `tmp/crosspool-odds-divergence/log_mlflow_level.py` — MLflow recording
  script.
- `tmp/crosspool-odds-divergence/wf_run_level.log` — full stdout of the WF
  run.

(`tmp/` is gitignored per repo convention — only this doc is committed.)
