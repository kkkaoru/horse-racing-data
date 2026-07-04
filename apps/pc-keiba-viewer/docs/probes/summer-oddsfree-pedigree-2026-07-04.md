# Pedigree Seasonality — Does 夏血統 Exist, and Is It Priced? (2026-07-04)

- **Date**: 2026-07-04
- **Category**: Odds-INDEPENDENT summer-racing tendency probe (angle 4: pedigree
  seasonality, 種牡馬×夏). Task #32.
- **Question**: does "夏に強い血統" (a sire/line that genuinely runs better in
  summer) exist as a real, era-stable tendency in JRA race outcomes — and if
  so, does the betting market already price it into the odds? This is
  deliberately **separate** from today's two model-feature REJECTs
  (`docs/probes/jra-pedigree-winrate-clean-2026-07-04.md`,
  `docs/probes/jra-sire-line-2026-07-04.md`), which asked "does adding this
  column beat a 250-feature CatBoost baseline" and REJECTed. This probe asks
  "does the tendency exist at all, and who prices it" — a descriptive/
  statistical question over raw outcomes, no model training involved.

## TL;DR

**Yes, 夏血統 exists as a real, statistically-validated tendency** — both at
the individual-sire and paternal-line level — and it **explains the two
feature REJECTs**: for essentially every famous, heavily-bet sire/line tested
(Deep Impact, King Kamehameha, Sakura Bakushin O, Kurofune, Stay Gold, Galileo,
Harbinger, ...), the raw summer/yoshiba performance differential is tiny
(point-biserial r ≈ 0.01–0.05) and what little exists **evaporates further**
once the market's own odds/ninkijun are controlled for — i.e. **FULLY-PRICED**.
That is exactly consistent with a feature adding zero incremental value to a
model. Out of 28 candidates checked for odds-independence, only **3 survive as
genuinely odds-free** (small effects the market has NOT absorbed):
**メイショウサムソン** (heat-averse, fade in summer), **エピファネイア**
(modest summer edge), and **ダノンバラード** (large 洋芝-specific edge) — all
three are second-tier-attention sires, not marquee names, which is itself
consistent with an efficient-market story (bettors price what they pay
attention to).

## Data & method

- **Source**: local Postgres (port 15432), `jvd_se` (starts, finish position,
  odds, ninkijun) `join` `jvd_ra` (venue, date, distance, surface) `join`
  `jvd_um` (`ketto_joho_01b`=sire, `ketto_joho_03b`=paternal grandsire "FF" —
  the same 2-gen line proxy used in today's sire-line REJECT probe,
  `ketto_joho_05b`=damsire, unused here). All JRA venues (01-10), flat races
  only (turf/dirt, steeplechase excluded), 2015-2026, finish position not
  null. Extraction: `tmp/candidate-summer-pedigree-seasonality/build_base.py`
  → `base.parquet`, 541,490 horse-starts, 1,096 distinct sires, 385 distinct
  FF lines. Built in 0.7s via DuckDB postgres-attach + window functions (no
  horse-level self-join). `memory_pressure` stayed ≥68% free throughout (well
  above the 25% gate; no memory incident).
- **Summer definition**: `is_summer_month` = kaisai month in {6,7,8,9}, ANY
  JRA venue (confirmed by a venue×month crosstab that Tokyo/Niigata/Chukyo/
  Hanshin all race some of Jun-Sep too, not just the 4 pure local-circuit
  tracks — the broad "hot months" cut is the right primary test for a
  heat-tolerance mechanism). `is_summer_venue` = `keibajo_code` in
  {01 Sapporo, 02 Hakodate, 03 Fukushima, 10 Kokura} used as a secondary,
  stricter robustness cut (`is_summer_month AND is_summer_venue`).
  `is_yoshiba` = `keibajo_code` in {01,02} AND surface = turf (the only JRA
  洋芝), tested separately within a turf-only universe per condition 3.
- **Own-baseline control**: for every entity (sire, or FF line), the summer
  top3-rate is compared against **that same entity's own** non-summer top3-
  rate — this nets out the entity's overall quality by construction; no
  separate covariate-adjustment step is needed for that part.
- **Era replication**: 2015-19 / 2020-23 / 2024-26 (2026 partial-year, current
  date 2026-07-04). A candidate must show the **same sign in all 3 eras**.
- **Significance**: pooled bootstrap 95% CI on the delta must **exclude 0**.
  Vectorized via a closed-form shortcut: for a length-n binary vector with k
  ones, the nonparametric case-resampling bootstrap distribution of the
  resampled count is _exactly_ `Binomial(n, k/n)` (each resampled index is an
  iid draw of one of the n original values) — so per-entity bootstrap CIs for
  hundreds of entities × thousands of iterations vectorize trivially via numpy
  broadcasting, no actual row-resampling needed. 4,000 bootstrap draws/entity.
- **Candidate universe**: individual sires — top 50 by summer-start volume,
  floor n≥200 pooled summer starts. FF lines — **all** 385 groups with floor
  n≥500 pooled summer starts (66 qualified), matching the task's exact
  thresholds. Yoshiba test (rarer population, turf-only) used lighter floors:
  n≥40 (sire, top 40 by volume) / n≥100 (line, all qualifying).
- **Multiple-comparison discipline**: rather than assume a nominal alpha, a
  **null-calibration simulation** (`null_calibration.py`) directly estimates
  how many "survivors" the exact same test (same-sign-3-eras AND
  significant-pooled-delta) would produce on **pure noise**, using the real
  per-entity/per-era sample sizes and each entity's own true overall rate as
  the null model, 2,000 replications (fast normal-approximation proxy for the
  bootstrap-CI-exclusion criterion, valid at these large n — legitimate only
  for this calibration layer; real survivor calls still use the full
  nonparametric bootstrap).

## Step 1 — Individual sire × summer (Jun-Sep, all JRA venues)

50 sires tested (top by summer volume, n≥200 floor) → **8/50 survive**
(same-sign-all-3-eras AND bootstrap CI excludes 0):

| Sire               | n summer / non-summer | Δ pooled (pp) | LB95         | Era Δ (15-19 / 20-23 / 24-26) |
| ------------------ | --------------------- | ------------- | ------------ | ----------------------------- |
| サウスヴィグラス   | 1,448 / 3,358         | +3.85         | +1.21        | +2.60 / +6.76 / +7.28         |
| ジャングルポケット | 1,302 / 2,529         | +3.62         | +1.06        | +2.62 / +5.37 / +16.67        |
| マツリダゴッホ     | 1,323 / 2,562         | +3.37         | +0.91        | +3.50 / +3.17 / +0.38         |
| スクリーンヒーロー | 1,620 / 3,539         | +3.12         | +0.59        | +4.06 / +1.95 / +5.00         |
| エピファネイア     | 2,428 / 5,161         | +2.93         | +0.81        | +5.29 / +3.69 / +1.57         |
| エイシンフラッシュ | 1,476 / 3,022         | +2.50         | +0.18        | +4.65 / +1.02 / +1.23         |
| クロフネ           | 2,080 / 4,762         | **-2.43**     | -4.54..-0.29 | -2.95 / -0.83 / -1.71         |
| メイショウサムソン | 1,079 / 2,270         | **-3.18**     | -6.03..-0.41 | -1.51 / -6.54 / -12.36        |

Notably absent: Deep Impact and King Kamehameha (both individually
directionally negative, same-sign-all-3-eras, but CI includes 0 at this broad
cut — see the strict robustness cut below where they _do_ clear significance).

**Null calibration**: for this exact 50-entity/3-era/n-structure, pure noise
produces a mean of **1.3 survivors** (95th pctile 3, max across 2,000
replications = **6**). The observed **8** never occurred in 2,000 null
replications (empirical p ≈ 0). This is strong evidence the signal is real,
not a multiple-comparison artifact.

## Step 2 — Paternal-line (FF, 父父) × summer, all 385 groups, floor n≥500

66 groups qualified → **9/66 survive**:

| FF line                    | n summer / non-summer | Δ pooled (pp) | LB95         | Era Δ                  |
| -------------------------- | --------------------- | ------------- | ------------ | ---------------------- |
| スウェプトオーヴァーボード | 520 / 1,078           | +4.52         | +0.53        | +14.22 / +2.77 / +4.83 |
| サクラバクシンオー         | 1,659 / 3,168         | +4.39         | +1.95        | +2.94 / +3.84 / +7.33  |
| トニービン                 | 1,348 / 2,595         | +3.66         | +1.07        | +2.70 / +5.37 / +16.67 |
| Dubawi                     | 1,394 / 3,178         | +3.00         | +0.54        | +0.52 / +3.69 / +3.13  |
| **ディープインパクト**     | 13,977 / 30,917       | **+2.10**     | **+1.26**    | +0.30 / +1.89 / +2.94  |
| End Sweep                  | 3,120 / 7,102         | +2.07         | +0.32        | +1.52 / +3.48 / +1.33  |
| **キングカメハメハ**       | 14,345 / 31,892       | **+0.93**     | **+0.10**    | +1.58 / +0.79 / +0.50  |
| Kingmambo                  | 4,190 / 9,565         | **-1.85**     | -3.41..-0.30 | -1.49 / -2.99 / -2.25  |
| French Deputy              | 2,223 / 5,138         | **-2.79**     | -4.85..-0.75 | -3.26 / -1.32 / -3.24  |

**Null calibration**: mean **2.0** survivors under noise (95th pctile 5, max
across 2,000 replications = **8**). Observed **9** never occurred in 2,000
replications (empirical p ≈ 0). Also real, not chance.

The Deep Impact and King Kamehameha **FF-line** results are the single most
statistically bulletproof findings in this probe (n in the tens of thousands,
LB95 well clear of 0) — but see the concentration diagnostic below for why
"line" is a loaded word here, and the odds-independence section for why they
don't matter practically.

### Robustness cut: strict (summer_venue AND summer_month)

Re-run with `keibajo_code ∈ {01,02,03,10}` AND month∈{6..9} (n≥100 sire /
n≥250 line floors): **8/50 sire survivors**, **14/60 line survivors** — mostly
the same names, but with one important **sign-flip discovery**: at this
stricter cut, **Deep Impact and King Kamehameha as individual sires** (their
own direct offspring, not the FF-line proxy) become significant **negative**
(-2.70pp [-4.83,-0.67] and -2.99pp [-5.36,-0.57] respectively) — consistent in
direction with their near-miss-negative result in the broad cut, just cleaner
at the stricter venue restriction. **Their own direct foals underperform in
summer even as their broader male-line descendants (via other sons)
overperform** — an interesting, non-contradictory divergence explained by the
concentration diagnostic next.

## Concentration diagnostic — is "line" really "line"?

For each FF-line survivor, what share of its summer-start volume comes from
its single most-represented individual sire (`line_concentration_and_yoshiba.py`)?

| FF line                    | distinct sires in group | top individual sire | share     |
| -------------------------- | ----------------------- | ------------------- | --------- |
| トニービン                 | 3                       | ジャングルポケット  | **96.6%** |
| French Deputy              | 2                       | クロフネ            | **93.6%** |
| Kingmambo                  | 9                       | キングカメハメハ    | **80.0%** |
| スウェプトオーヴァーボード | 3                       | レッドファルクス    | **64.4%** |
| サクラバクシンオー         | 7                       | ビッグアーサー      | 49.2%     |
| End Sweep                  | 4                       | サウスヴィグラス    | 46.4%     |
| キングカメハメハ           | 20                      | ロードカナロア      | 27.1%     |
| **ディープインパクト**     | **45**                  | キズナ              | **18.4%** |

Four of the nine "line" survivors are **>60% dominated by one individual son**
— i.e. トニービン, French Deputy, Kingmambo, and スウェプトオーヴァーボード
"line" signals are really ジャングルポケット, クロフネ, キングカメハメハ, and
レッドファルクス signals wearing a line label (and indeed ジャングルポケット
and クロフネ already appear as individual-sire survivors above, with matching
signs). This exactly reproduces the finding from today's `jra-sire-line`
feature REJECT: line-pooling mostly re-discovers individual-sire signal rather
than adding a genuine dynasty-level effect.

Two lines are genuinely **broad, multi-sire** signals, not single-sire
artifacts: **ディープインパクト** (45 distinct sons contributing, no single
son over 18.4%) and **キングカメハメハ** (20 distinct sons, top at 27.1%,
Rulership/Duramente close behind Lord Kanaloa). These are the two candidates
where "系統" is the correct frame — and precisely the two that turn out to be
fully priced (see odds-independence, below).

## Step 3 — Yoshiba (洋芝) specialization, turf-only universe

Sapporo+Hakodate turf vs all other JRA turf, same entity's own baseline, same
era+bootstrap discipline:

**Individual sire, 5/40 survive:**

| Sire               | Δ pooled (pp) | LB95         | Era Δ                 |
| ------------------ | ------------- | ------------ | --------------------- |
| **ダノンバラード** | **+21.08**    | **+13.69**   | +35.5 / +21.6 / +19.0 |
| ジャングルポケット | +8.71         | +2.64        | +6.2 / +14.1 / +42.8  |
| キンシャサノキセキ | +6.13         | +1.80        | +4.8 / +9.0 / +6.2    |
| ゴールドシップ     | +4.46         | +0.09        | +43.2 / +1.6 / +6.7   |
| ディープインパクト | **-4.62**     | -7.50..-1.73 | -3.7 / -4.3 / -18.2   |

**FF line, 7/41 survive:**

| FF line            | Δ pooled (pp) | LB95  | Era Δ                 | Note                                                                              |
| ------------------ | ------------- | ----- | --------------------- | --------------------------------------------------------------------------------- |
| マンハッタンカフェ | +12.28        | +5.50 | +8.2 / +16.2 / +7.0   |                                                                                   |
| Hennessy           | +12.11        | +5.98 | +10.4 / +17.4 / +13.7 |                                                                                   |
| トニービン         | +9.10         | +2.89 | +6.7 / +14.1 / +42.8  | = ジャングルポケット (96.6% concentrated)                                         |
| ハーツクライ       | +7.38         | +2.90 | +6.8 / +13.1 / +1.7   |                                                                                   |
| Fuji Kiseki        | +6.13         | +1.84 | +4.8 / +9.0 / +6.2    | identical n to キンシャサノキセキ — **100% concentrated**, same underlying entity |
| サクラバクシンオー | +5.80         | +0.39 | +2.4 / +4.8 / +8.1    |                                                                                   |
| ステイゴールド     | +3.95         | +1.39 | +5.1 / +1.1 / +7.7    |                                                                                   |

**ダノンバラード's +21pp is the largest effect size found in this entire
probe** — over 1,294 turf starts (177 at Sapporo/Hakodate, 1,117 elsewhere),
top3 rate roughly **doubles** in every era (2015-19: 8/13=61.5% vs 18/69=26.1%,
n tiny; 2020-23: 22/56=39.3% vs 75/424=17.7%; 2024-26: 39/108=36.1% vs
107/624=17.1%) — consistent 2x multiplier as his crop volume grows across
eras, not a small-n fluke concentrated in one window.

## Step 4 — Odds-independence: who does the market NOT already price?

For every distinct surviving candidate from steps 1-3 (28 tested; entities
shown ≥90%-concentration-duplicates of an already-tested individual sire —
Fuji Kiseki≈Kinshasa no Kiseki, French Deputy≈Kurofune, Kingmambo≈King
Kamehameha individual, トニービン≈Jungle Pocket, スウェプトオーヴァーボード≈
Red Falx — were not re-tested separately, see concentration diagnostic):

1. **Within-ninkijun-band persistence**: recompute the same delta inside
   ninkijun bands {1; 2-3; 4-6; 7+}. If the market has already priced the
   tendency, it should show up as shorter odds for that entity's summer/
   yoshiba runners, and the residual delta _within_ a band should shrink to 0.
2. **Partial correlation** of (is_summer/is_yoshiba, is_top3) jointly
   controlling `log(odds)` and `ninkijun` via OLS-residualization, compared
   against the raw (unconditional) point-biserial correlation. Verdict:
   **ODDS-FREE** if the partial correlation retains ≥60% of the raw
   correlation's magnitude, same sign, and its Fisher-z 95% CI excludes 0;
   **FULLY-PRICED** if it retains ≤25%, flips sign, or loses significance;
   **PARTIALLY-PRICED** in between.

### Result: 3 ODDS-FREE out of 28

| Entity                                                                                                                                                                                                                                           | Raw r   | Partial r (ctrl odds+ninkijun) | Retention | Partial-r CI95     | Verdict                                               |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------- | ------------------------------ | --------- | ------------------ | ----------------------------------------------------- |
| **エピファネイア** (sire, summer)                                                                                                                                                                                                                | 0.0324  | 0.0239                         | 73.6%     | [0.0013, 0.0464]   | **ODDS-FREE**                                         |
| **ダノンバラード** (sire, yoshiba)                                                                                                                                                                                                               | 0.1761  | 0.1068                         | 60.7%     | [0.0523, 0.1607]   | **ODDS-FREE**                                         |
| **メイショウサムソン** (sire, summer, negative)                                                                                                                                                                                                  | -0.0344 | -0.0351                        | 102.0%    | [-0.0690, -0.0011] | **ODDS-FREE**                                         |
| マンハッタンカフェ (line, yoshiba)                                                                                                                                                                                                               | 0.1137  | 0.0633                         | 55.7%     | [0.0057, 0.1205]   | PARTIALLY-PRICED (borderline, just under the 60% bar) |
| ディープインパクト (line, summer)                                                                                                                                                                                                                | 0.0204  | -0.0003                        | -1.5%     | [-0.0096, 0.0090]  | FULLY-PRICED                                          |
| キングカメハメハ (line, summer)                                                                                                                                                                                                                  | 0.0090  | 0.0004                         | 4.4%      | [-0.0088, 0.0096]  | FULLY-PRICED                                          |
| サクラバクシンオー (line, summer)                                                                                                                                                                                                                | 0.0493  | 0.0185                         | 37.6%     | [-0.0098, 0.0468]  | FULLY-PRICED                                          |
| ジャングルポケット (sire, summer)                                                                                                                                                                                                                | 0.0474  | 0.0223                         | 47.1%     | [-0.0095, 0.0541]  | FULLY-PRICED                                          |
| ディープインパクト (sire, summer, own foals)                                                                                                                                                                                                     | -0.0079 | 0.0042                         | -52.5%    | [-0.0112, 0.0195]  | FULLY-PRICED                                          |
| キングカメハメハ (sire, summer, own foals)                                                                                                                                                                                                       | -0.0150 | 0.0012                         | -8.1%     | [-0.0173, 0.0198]  | FULLY-PRICED                                          |
| クロフネ (sire, summer)                                                                                                                                                                                                                          | -0.0245 | -0.0227                        | 92.4%     | [-0.0465, 0.0011]  | FULLY-PRICED (CI just touches 0)                      |
| ステイゴールド (line, yoshiba)                                                                                                                                                                                                                   | 0.0303  | -0.0041                        | -13.7%    | [-0.0221, 0.0138]  | FULLY-PRICED                                          |
| ...remaining 17 (マツリダゴッホ, スクリーンヒーロー, エイシンフラッシュ, ゴールドシップ×2, ハービンジャー, End Sweep, ステイゴールド(summer), Galileo, ダイワメジャー, King's Best, Pulpit, Dansili, Hennessy, ハーツクライ, キンシャサノキセキ) |         | 11-47% retention or sign-flip  |           |                    | FULLY-PRICED                                          |

Full per-entity ninkijun-band tables and partial-correlation numbers:
`reports/odds_independence.json`, `reports/odds_independence_round2.json`.

**エピファネイア** (n=7,589; ninkijun bands 2-3/4-6/7+ all positive, band 7+
itself significant at +2.09pp[+0.10,+4.27]) is an actively-siring, currently
prominent stallion (sired several recent top JRA performers) — this is a
small, real, currently-underpriced summer edge.

**ダノンバラード** (n=1,294 turf starts) shows the odds-independence
_despite_ being the largest raw effect size in the probe — its retention
(60.7%) sits right at the ODDS-FREE threshold, and n is modest relative to
the marquee sires, so this finding is directionally solid (all 3 eras
positive, band 4-6/7+ individually significant) but statistically the least
robust of the three.

**メイショウサムソン** (n=3,349) is the cleanest of the three in one sense —
partial correlation is _undiminished_ (102% retention) relative to raw,
meaning the market genuinely has not adjusted odds downward for his summer
runners at all. Caveat: his 2024-26 era n is thin (40 summer / 74 non-summer
starts) — he is an aging stallion (foaled 2004) with a shrinking annual crop,
so while the sign is consistent across all 3 eras (and intensifying, not
weakening), the future actionable volume of this signal is limited and
declining.

## Synthesis

**Does 夏血統 exist?** Yes — validated two ways: (a) a purpose-built null-
calibration simulation shows the observed survivor counts (8/50 sires, 9/66
lines) are far outside what pure noise produces at these exact sample sizes
(null means 1.3/2.0, empirical p≈0 for both), and (b) the signal replicates
directionally across three independent 4-6 year eras spanning 2015-2026, not
just a single lucky window.

**Is it priced?** Overwhelmingly yes, for the sires/lines that matter most in
betting volume. Every marquee name tested — Deep Impact (both as an
individual sire and as a 45-son-wide paternal line), King Kamehameha (both
forms), Sakura Bakushin O, Kurofune, Stay Gold, Galileo, Harbinger, Jungle
Pocket — has a raw summer/yoshiba correlation with outcome that is already
tiny (r≈0.01–0.05) and shrinks further, often past zero, once the market's own
odds and ninkijun are controlled for. This is the direct, satisfying
explanation for **why today's two feature-engineering probes REJECTed**:
`sire_yoshiba_top3`, `sire_venue_top3`, `sire_line_yoshiba_top3`, etc. were
built from exactly these kinds of sires (EB-shrunk across the whole
population, dominated by the highest-volume names in both probes' candidate
construction) — a CatBoost model trained on 250 features already containing
`sire_distance_win_rate` / `sire_track_win_rate` / `sim_sire_win_rate` etc.
had no room to gain, because the underlying tendency for the sires that
dominate the training population is already small and already reflected
wherever the model's other features (and the market) pick up correlated
signal (recent form, class level, existing sire×surface/distance rates
interacting with venue via tree splits).

**But it is not _universally_ priced.** Three genuinely odds-free exceptions
survive: エピファネイア (summer-general, modest, currently active/rising
sire), ダノンバラード (洋芝-specific, large effect, smaller/second-tier
sire), and メイショウサムソン (summer-averse/fade signal, aging sire with
shrinking future volume). All three share a pattern: **they are not among the
handful of sires that dominate JRA breeding volume and bettor attention** —
consistent with an efficient-markets story where the crowd prices what it
pays the closest attention to (the sires with thousands of starts and
constant TV/racing-media coverage) and under-prices the same kind of
tendency in less-followed sires. This is a plausible, coherent closure of
condition D for JRA: the tendency is real, the market is _selectively_
efficient (efficient for famous names, inefficient for second-tier ones), and
that selective inefficiency is exactly why a population-wide EB-shrunk
pedigree feature (as tested today) washes out in aggregate — the genuinely
exploitable cases are a small, specific minority of sires, not a general
"pedigree × venue/season" axis.

**Caveat on the 3 odds-free findings**: this probe is intentionally a
descriptive/statistical existence test, not a model-feature validation. It
does not claim that adding narrow, sire-specific columns for exactly these 3
sires to the deployed CatBoost model would produce a WF-validated accept-gate
pass — that would require actual model retraining (out of scope here, and
per `DO-NOT-RETEST` convention on the broader pedigree axis, a narrow
follow-up targeting only these 3 specific sires would be a materially
different, much smaller-scope hypothesis than what was REJECTed today, not a
retest of it). Flagging as a possible narrow follow-up, not executing it here.

## まとめ (日本語)

**「夏に強い血統」は実在するか？** → **実在する。** サイア単体でも父父
(系統)単体でも、同一馬・同一系統の「夏 (6-9月) vs それ以外」の連対率3着内率
差分を自分自身のベースラインと比較する方式で検証し、2015-19/2020-23/2024-26
の3era全てで符号が一致しブートストラップ信頼区間が0を跨がない候補が、
サイア50頭中8頭、系統66群中9群で発見された。この「発見率」が偶然のノイズで
どの程度出るかをnullシミュレーション(同一サンプルサイズ・同一馬/系統の
生涯平均勝率を真の値と仮定した2000回シミュレーション)で直接検証したところ、
偶然の平均発見数はサイアで1.3件・系統で2.0件、2000回中の最大でも6件・8件
にとどまり、観測された8件・9件は偶然では説明できない(実質p≈0)。

**それは市場(オッズ)にすでに織り込まれているか？** → **有名血統は
ほぼ織り込み済み。** ディープインパクト・キングカメハメハ・サクラバクシン
オー・クロフネ・ステイゴールド・ガリレオ・ハービンジャー・ジャングルポケット
など、検証した主要血統はすべて、生の相関係数がもともと非常に小さく(r≈0.01
〜0.05)、人気順・オッズ(log-odds)を統制した偏相関では**さらに縮小しゼロに
近づく**(FULLY-PRICED判定)。これは本日REJECTされた2件の特徴量実験
(`jra-pedigree-winrate-clean`, `jra-sire-line`)の結果と整合する — 検証対象
サイアの多くが同じ「有名・大量出走」血統に偏っていたため、モデルに追加余地が
なかったと解釈できる。

**ただし全面的に織り込み済みではない。** オッズ非依存(ODDS-FREE)と判定
された例外が3件存在する: **エピファネイア**(夏全般、控えめだが現役の
有力サイア)、**ダノンバラード**(洋芝特化、本プローブ最大の効果量+21pp、
中堅サイア)、**メイショウサムソン**(夏に弱い=フェード対象、ただし近年の
産駒数が先細り)。3頭とも「有名血統ほど市場は正確、無名寄りの血統ほど市場は
非効率」という一貫したパターンを示しており、条件Dの血統×季節軸に対する
妥当な結論と考えられる。ただし本プローブは記述統計であり、この3頭限定の
特徴量が実際にモデル精度を上げるかどうかは別途WF検証が必要(未実施、
DO-NOT-RETESTの対象は今回REJECTされた集団プーリング型の構築であり、この
3頭限定の狭い仮説は別物)。

## Artifacts

- Extraction: `tmp/candidate-summer-pedigree-seasonality/build_base.py` →
  `base.parquet` (541,490 rows, 2015-2026)
- Sire/line summer tests + null calibration:
  `tmp/candidate-summer-pedigree-seasonality/analyze_seasonality.py`,
  `tmp/candidate-summer-pedigree-seasonality/null_calibration.py`
- Concentration diagnostic + yoshiba test:
  `tmp/candidate-summer-pedigree-seasonality/line_concentration_and_yoshiba.py`
- Odds-independence checks:
  `tmp/candidate-summer-pedigree-seasonality/odds_independence.py`,
  `tmp/candidate-summer-pedigree-seasonality/odds_independence_round2.py`
- Reports (JSON): `tmp/candidate-summer-pedigree-seasonality/reports/*.json`
  (`sire_summer_month`, `sire_line_summer_month`, `sire_summer_strict`,
  `sire_line_summer_strict`, `sire_yoshiba`, `sire_line_yoshiba`,
  `null_calibration`, `line_concentration`, `line_concentration_top3`,
  `odds_independence`, `odds_independence_round2`)
