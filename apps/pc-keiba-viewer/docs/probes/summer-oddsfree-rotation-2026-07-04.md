# Summer Rotation-Law Odds-Free Probe: 間隔・使い詰め・転戦/滞在 (2026-07-04)

- **Date**: 2026-07-04
- **Category**: JRA descriptive market-efficiency profiling (angle 3 of the
  odds-independent summer-racing-law campaign, task #31). This is **not** a
  WF/model-feature test — it asks whether descriptive rotation "laws" exist
  in JRA summer racing and, if so, whether the betting market already prices
  them (odds-independence check), not whether a GBDT can extract incremental
  serve accuracy from them.
- **Prior art referenced, not re-litigated as model features**: layoff
  showed zero summer-specific elevation as a serve **feature**
  (`jra-summer-upset-divergence-2026-07-04.md` Part 2); `is_meet_repeat`/滞在
  was REJECTED as a serve **feature bundle** with a NEGATIVE target cell
  (`jra-meet-repeat-2026-07-04.md`, Hokkaido x repeat-starter `place2
-1.18pp`); the class-ordinal encoding fix was WF-REJECTED as a serve
  feature swap (`jra-class-ordinal-fix-2026-07-04.md`). This doc asks the
  complementary descriptive question for each: does the underlying
  market-pricing reality explain those REJECTs, and is there anything the
  market itself gets wrong (odds-free)?

## Method

**Data**: local Postgres (`horse_racing`, port 15432, read-only), `jvd_se`
join `jvd_ra`, JRA venues `01`-`10`, 2015-2026, finished starts only
(`ijo_kubun_code='0'`). All rotation columns (rest interval,
starts-in-trailing-60/90d, previous-race venue/distance/class, same-venue-meet
repeat identity, summer-campaign sequence number) are **strictly prior**,
built via DuckDB window functions ordered by `race_date`
(`rows`/`range between unbounded/interval preceding and 1 preceding` — the
current row and any future row are never visible). Class differences use the
**corrected** class ordinal from `jra-class-ordinal-fix-2026-07-04.md`
(701→0 新馬, 703→1 未勝利, 005→2, 010→3, 016→4, 999→5 open), not the buggy
production ordinal.

**Venue taxonomy**: summer venues = `01` Sapporo, `02` Hakodate, `03`
Fukushima, `10` Kokura (matches sibling docs' convention). **Correction over
that convention for this doc's season-matched contrasts**: Fukushima also
runs spring (Apr-May) and autumn (Oct-Nov) meets, and Kokura also runs a
winter meet (Jan-Mar) — neither is "summer" in the literal sense. Where a
fair summer-vs-rest-of-calendar contrast is needed, this doc restricts to
**Jun-Sep** at both the 4 local venues and a main-venue contrast group (`04`
Niigata, `05` Tokyo, `06` Nakayama, `07` Chukyo, `08` Kyoto, `09` Hanshin, the
same calendar months) — this isolates a summer-**venue** effect from a
generic summer-**season** effect, which pure venue-code filtering (as used in
the sibling upset-profile doc) cannot do for Fukushima/Kokura.

**Odds-independence check** (every candidate, every angle): (1)
**within-ninkijun-band persistence** — split into `ninkijun_band` 1-3/4-6/7+
and check whether the tendency's direction/magnitude holds inside each band;
(2) **partial-Spearman** `pr_mkt` — within-race-demeaned rank correlation of
the candidate vs `finish_position`, controlling jointly for `tansho_ninkijun`
rank + `tansho_odds` rank (method identical to
`tmp/candidate-jra-summer-upset/probe_candidates.py`). `raw_rho` = unconditional;
`rho_odds` = candidate's own correlation with market odds (near-0 = market
doesn't independently price this candidate at all). **Era replication**:
2015-19 / 2020-23 / 2024-26, verdict LAW (same sign all 3 eras, each
`|Δpr_mkt|` clears a small noise floor) / PARTIAL (2 of 3 agree) / NOISE
(sign-flip or sub-floor) — computed by `common.era_verdict()`.

**Scripts**: `tmp/candidate-summer-rotation/build_features.py` (PG pull +
window-function build, 554,358 rows, 0.3s), `common.py` (shared
partial-Spearman + rate-table + era-verdict helpers, reused verbatim
methodology from the sibling upset-divergence probe), `angle1_interval.py`,
`angle2_campaign.py`, `angle3_transition.py`, `angle3b_nar_transfer.py`,
`angle4_class.py`. Reports: `tmp/candidate-summer-rotation/angle{1,2,3,3b,4}_*.json`.

## Angle 1 — 間隔 (rest-interval) structure

**Bands**: 連闘 (≤10d) / 中1-2週 (11-20d) / 中3-8週 (21-59d) / 60-89d / 放牧明け
(90d+). Contrast: summer venues Jun-Sep (n=69,304) vs main venues Jun-Sep
(n=82,918, season-matched).

| Band (days)    | Summer top3% (n) | Main top3% (n) |
| -------------- | ---------------- | -------------- |
| ≤10 (連闘)     | 21.67 (4,531)    | 17.46 (1,718)  |
| 11-20          | 25.85 (15,248)   | 23.91 (13,583) |
| 21-59          | 24.15 (30,515)   | 22.92 (39,485) |
| 60-89          | 22.16 (9,291)    | 20.85 (12,842) |
| 90+ (放牧明け) | 19.34 (9,719)    | 17.64 (15,290) |

Both summer and main show the **same shape**: top3 rate peaks at 11-20 days,
declines through 60-89 and 90+, and — counter to a pure "shorter rest is
always better" story — the ≤10-day band is not the best band at either venue
group (likely a fast-repeat-out-of-necessity selection effect offsetting any
freshness gain). Within `ninkijun_band=1-3` (favorites) specifically, the
decline through longer rest IS clean and monotonic (summer: 54.7% → 52.1% →
52.8% → 49.9% → 47.0% from ≤10d to 90+d); within `4-6` and `7+` the curve is
flatter/non-monotonic — the rest-interval effect is strongest for favorites,
not a uniform law across all market tiers.

**Odds-independence (partial-Spearman of continuous `days_since_last_race`
vs finish, controlling ninkijun+odds)**:

| Era     | Summer pr_mkt | Main pr_mkt | Elevation (summer−main) |
| ------- | ------------- | ----------- | ----------------------- |
| 2015-19 | 0.0609        | 0.0769      | −0.0160                 |
| 2020-23 | 0.0523        | 0.0792      | −0.0269                 |
| 2024-26 | 0.0594        | 0.0792      | −0.0198                 |

**Own-effect verdict: LAW** — longer rest correlates with a _worse_ finish
even after controlling for market rank, JRA-wide, all 3 eras (all-venues
pooled `pr_mkt` 0.0638/0.0666/0.0684, monotonically stable). **Elevation
verdict: LAW, but negative** — summer shows _less_ residual rest-interval
signal than season-matched main venues, all 3 eras, not more.

**Verdict: real, PARTIALLY-PRICED odds-free effect (rest interval matters,
market only partially prices it — raw_rho ~0.10-0.12 attenuates to ~0.05-0.08
partial), but the "summer rewards quick back-ups more" / heat-adaptation
hypothesis is REFUTED.** If anything the opposite: the main-venue calendar
shows a slightly _stronger_ rest-interval effect in the same months.
**Serve-feasibility**: none — this is the same `days_since_last_race` /
`is_returning_from_layoff` already live in armB; independently confirms and
replicates (via binned bands + within-band persistence rather than raw
`pr_mkt` alone) the "no summer elevation" finding in
`jra-summer-upset-divergence-2026-07-04.md` Part 2. **DO-NOT-RETEST overlap**:
full overlap with that doc's `layoff_days` REJECT.

## Angle 2 — 使い詰め: Nth start of the summer campaign

`summer_campaign_start_number` = strictly-prior count of this horse's starts
at a summer venue **this calendar year**, capped 1st/2nd/3rd/4th+ for the
rate table (n=124,782 summer-venue starts, all with a valid campaign number).

| Campaign start | Pooled top3% (n) | 3yo top3% (n)  | 4yo+ top3% (n) |
| -------------- | ---------------- | -------------- | -------------- |
| 1st            | 22.17 (63,154)   | 21.11 (28,171) | 21.69 (24,509) |
| 2nd            | 23.42 (31,584)   | 24.22 (14,259) | 21.73 (13,386) |
| 3rd            | 23.22 (15,628)   | 24.13 (7,161)  | 22.04 (7,137)  |
| 4th+           | 22.77 (14,416)   | 25.16 (6,291)  | 20.91 (7,609)  |

Pooled arc is roughly flat, but **within `ninkijun_band=7+` (longshots) the
arc rises cleanly and monotonically**: 8.04% (1st) → 9.02% (2nd) → 9.19%
(3rd) → 10.14% (4th+), n=36,028/17,122/8,304/7,877. `ninkijun_band=1-3`
(favorites) is flat-to-mildly-declining (51.4%→52.0%→50.1%→49.7%);
`4-6` is flat (27.7/27.0/26.8/27.3). **This is asymmetric by market tier**:
longer-tenured summer-campaign horses in the longshot band systematically
outperform their earlier-campaign peers; favorites do not show the same
gain.

**Odds-independence (partial-Spearman, continuous campaign number vs
finish)**:

| Era     | Pooled pr_mkt | 3yo pr_mkt | 4yo+ pr_mkt |
| ------- | ------------- | ---------- | ----------- |
| 2015-19 | −0.0446       | −0.0475    | −0.0395     |
| 2020-23 | −0.0409       | −0.0508    | −0.0412     |
| 2024-26 | −0.0468       | −0.0467    | −0.0489     |

**Verdict: LAW** — negative and stable across all 3 eras, both age groups
(pooled + 3yo + 4yo+ all independently LAW). More summer-campaign starts
correlates with a genuinely better finish beyond what the market already
prices in (raw_rho pooled −0.0591 attenuates only to −0.0432 partial — the
market captures roughly a quarter of this effect, leaving most of it
odds-free). **PARTIALLY-PRICED, tilted toward ODDS-FREE**, concentrated in
the longshot band. Age-conditional: both 3yo and 4yo+ show the same
direction and similar magnitude — this is not a "young horse maturing over
the summer" story specifically, it holds for older campaigners too.
**Serve-feasibility**: `summer_campaign_start_number` (Nth start at a summer
venue this calendar year) is **not** a named armB feature today — armB has
general form-recency features (`days_since_last_race`,
`last_3_avg_finish_norm`, etc.) but nothing that counts specifically-summer-venue
campaign tenure per year. This is a genuinely new, real, era-stable candidate
column — flagging as a **WF-testable candidate** for whoever next runs the
feature-engineering loop (not executed here — this doc is descriptive/market
scope, not a WF slot). **DO-NOT-RETEST overlap**: none identified; distinct
from the REJECTED `is_meet_repeat`/`days_since_meet_first_start` bundle
(angle 3 below) — this is a calendar-year cross-meet cumulative count, not a
within-single-meet repeat flag.

## Angle 3 — 転戦 geography (滞在 / circuit-hop / shipper)

Transition categories (summer-venue starts only, by previous-race venue):
`same_venue` (immediate prior start at the identical venue, any meet),
`summer_to_summer_diff` (circuit-hop between two _different_ summer venues),
`main_to_summer` (shipper-in from a main venue), `debut` (no prior start).

| Category              | Top3% (n)          | Mean ninkijun |
| --------------------- | ------------------ | ------------- |
| same_venue            | 23.93 (33,028)     | 7.12          |
| main_to_summer        | 22.54 (63,845)     | 7.70          |
| debut                 | 22.51 (10,618)     | 7.49          |
| summer_to_summer_diff | **20.98** (17,291) | 7.65          |

`summer_to_summer_diff` (circuit-hopping to a _different_ local venue rather
than staying put or shipping home) is the worst-performing category at a
similar market rating — but the odds-independence check (`is_circuit_hop`
partial-Spearman) finds this weak: pooled `pr_mkt=0.0059`, per-era
`0.0005/0.0085/0.0109` — **PARTIAL at best, closer to NOISE**, small and
`rho_odds≈0` (market doesn't price it either way, but the true effect size is
too small to call a reliable law). Not recommended as an actionable finding.

### 滞在/repeat-starter inversion check (the REJECTED-feature follow-up)

Testing the team-lead's specific question — does the market **overprice**
same-meet repeat starters (滞在), which would explain the REJECTED feature's
negative target cell? Using the exact `is_meet_repeat` definition from
`jra-meet-repeat-2026-07-04.md` (>=1 earlier finished start within the same
`keibajo_code x kaisai_nen x kaisai_kai`):

| Scope            | repeat=0 top3% (n) | repeat=1 top3% (n) | Mean ninkijun (0 vs 1) |
| ---------------- | ------------------ | ------------------ | ---------------------- |
| Summer venues    | 22.46 (103,130)    | 23.80 (21,652)     | 7.60 vs 7.14           |
| Hokkaido (01+02) | 24.21 (33,719)     | 24.81 (8,582)      | 7.01 vs 6.82           |

**Odds-independence (`is_meet_repeat` partial-Spearman)**:

| Era     | Summer pr_mkt | Hokkaido pr_mkt |
| ------- | ------------- | --------------- |
| 2015-19 | −0.0198       | −0.0072         |
| 2020-23 | −0.0264       | −0.0151         |
| 2024-26 | −0.0240       | **−0.0295**     |

**Result: the answer is the OPPOSITE of the hypothesis.** Repeat starters do
not underperform market expectations — after controlling for ninkijun+odds,
they show a small but **era-stable (LAW), negative-direction** `pr_mkt`,
meaning repeat starters finish _slightly better_ than the market's own
pricing implies. The market already shades their odds down somewhat (mean
ninkijun 7.60→7.14 summer, 7.01→6.82 Hokkaido — bettors do notice 滞在
horses and price them shorter), but not quite enough — a small residual
**market UNDERpricing**, not overpricing, and — notably — the Hokkaido-specific
magnitude **grows over time** (−0.0072 in 2015-19 to −0.0295 in 2024-26),
consistent with 滞在-culture becoming a stronger genuine signal in the more
recent local-circuit era. `same_venue_as_prev` (looser continuity, any meet)
shows the same direction and era-stability (`−0.0098/−0.0238/−0.0223`).

**Caveat — this is not uniform across market tiers**: within-`ninkijun_band`
persistence is mixed (summer: `1-3` repeat slightly better 51.6% vs 51.1%;
`4-6` repeat slightly _worse_ 26.5% vs 27.5%; `7+` repeat slightly better
8.85% vs 8.62%) — the aggregate/partial-correlation LAW is real and
era-stable but small, and doesn't read as a clean uniform effect at every
market tier.

**Reconciling with the REJECTED feature (`jra-meet-repeat-2026-07-04.md`)**:
that WF trained 2013+ pooled and tested 2023/2024/2025 folds; since the
underlying signal here is weakest in the earliest years and grows
substantially by 2024-26, a model trained on a decade-plus of mostly-weaker
history may not have learned to weight the flag correctly for the recent,
stronger-signal years — plausible explanation for why a raw additive feature
regressed a small-n cell (n=510) despite a real, if modest, underlying
market inefficiency. This is offered as a reconciliation, not a re-open of
that REJECT — a genuinely different construction (e.g. recency-weighted or
2020+-only training) would be required to test whether the described
inefficiency is model-exploitable, and that is out of scope for this
descriptive doc.

**Verdict: PARTIALLY-PRICED odds-free edge (small, favors repeat starters,
opposite direction from the "market overprices 滞在" hypothesis), era-stable
LAW in the aggregate, not a WF slot on its own** (already tested as a serve
feature and REJECTED; this doc's contribution is characterizing _why_ the
descriptive reality doesn't match the intuitive overpricing story).
**Serve-feasibility**: none new — `is_meet_repeat` already exists as a
tested-and-rejected candidate column
(`tmp/candidate-jra-meet-repeat/build_features.py`). **DO-NOT-RETEST
overlap**: full overlap with `jra-meet-repeat-2026-07-04.md`'s DO-NOT-RETEST
for the identical feature family; this doc does not propose retesting it as
a model feature.

### NAR→JRA transfer (supplementary, "if visible" per task brief)

**Confirmed visible**: 46,725 JRA summer-venue starters (2015-2026) also have

> =1 NAR (`nvd_se`) start somewhere in their history. Built via an ASOF JOIN
> (most-recent NAR race date strictly before the JRA race date, compared
> against the existing strictly-prior JRA `prev_race_date`) to flag starts
> whose single most-recent prior race (either circuit) was NAR rather than
> JRA.

| Group                          | n       | Top3% | Mean ninkijun |
| ------------------------------ | ------- | ----- | ------------- |
| Prior start was NAR            | 2,821   | 9.78  | 10.55         |
| Prior start was JRA (or debut) | 121,961 | 22.99 | 7.45          |

Pooled `pr_mkt=0.0096` (small, `rho_odds=0.0663` — the market already prices
this heavily: mean ninkijun jumps from 7.45 to 10.55). **Verdict:
FULLY-PRICED** — the JRA/NAR quality gap for cross-circuit horses is large
and already reflected almost entirely in the market price; only a marginal
residual (~0.01 partial) remains, and n is too thin (2,821 total across 12
years) to reliably era-split. Not a usable lever either as a feature or as a
descriptive law beyond "the market already knows."

## Angle 4 — Class-program structure

**Class mix, Jun-Sep** (descriptive, not odds-related): summer venues run
noticeably more 未勝利/maiden (42.5% vs 38.2% main), less 新馬/newcomer (8.8%
vs 11.3%), and fewer higher tiers — 3-win 3.5% vs 5.6%, Open 6.2% vs 7.7%.
The summer local-circuit program is structurally skewed toward lower/mid
class racing relative to the main-venue calendar in the same months.

**class_diff (corrected ordinal, current − previous race's class level)
performance**, summer Jun-Sep (n=69,304 with a valid diff):

| class_diff bucket | Top3% (n)      | Mean ninkijun |
| ----------------- | -------------- | ------------- |
| down (a_down)     | 35.45 (1,752)  | 5.04          |
| same (b_same)     | 22.45 (58,106) | 7.53          |
| up (c_up)         | 27.14 (9,446)  | 6.34          |

Moving up in class shows a _higher_ top3 rate than staying put because
up-movers are typically promoted after a recent win (in-form horses) — the
market already recognizes this (mean ninkijun 6.34 vs 7.53).

**Odds-independence (partial-Spearman of continuous `class_diff` vs finish)**:

| Era     | Summer pr_mkt | Main pr_mkt | Elevation (summer−main) |
| ------- | ------------- | ----------- | ----------------------- |
| 2015-19 | 0.0244        | 0.0295      | −0.0051                 |
| 2020-23 | 0.0170        | 0.0325      | −0.0155                 |
| 2024-26 | 0.0223        | 0.0332      | −0.0109                 |

**Own-effect verdict: LAW** — moving up in class correlates with a genuinely
worse finish beyond market pricing, both summer and main, all 3 eras (a real
"class-jump tax" the market only partially prices — raw*rho attenuates
roughly by half). **Elevation verdict: LAW, negative** — the class-jump tax
is \_consistently weaker at summer venues* than at main venues in the same
months, all 3 eras. Plausible mechanism: the competitive step between
adjacent class tiers is smaller in the summer local-circuit fields (weaker
overall depth) than the same nominal step at a main venue.

**Verdict: real, PARTIALLY-PRICED odds-free effect (class-jump difficulty),
present everywhere, but genuinely MUTED at summer venues** — the opposite of
an "inexperienced connections misjudge class jumps more in the chaotic
summer circuit" story. **Serve-feasibility**: `last_race_class_diff`
(buggy ordinal) already exists in armB; the corrected-ordinal swap was
already WF-tested and REJECTED as a serve feature
(`jra-class-ordinal-fix-2026-07-04.md`) — that is a distinct question (does
swapping the ordinal help the model) from this doc's finding (does
class-jump carry real odds-free signal, and is it different by venue-type) —
both can be true simultaneously; this finding is descriptive market
characterization, not a proposal to re-open that WF. **DO-NOT-RETEST
overlap**: full overlap with that doc's REJECT for the corrected-ordinal
model-feature swap.

## Summary table

| Angle                                  | Effect size (pr_mkt)        | Era verdict    | Odds verdict                             | Serve-feasibility                              | DO-NOT-RETEST overlap                     |
| -------------------------------------- | --------------------------- | -------------- | ---------------------------------------- | ---------------------------------------------- | ----------------------------------------- |
| 1. Rest interval (own effect)          | 0.05-0.08                   | LAW            | PARTIALLY-PRICED                         | none (already armB)                            | `layoff_days` REJECT (upset doc)          |
| 1. Rest interval (summer elevation)    | −0.02 to −0.03 (negative)   | LAW            | refutes "summer reward"                  | n/a                                            | same                                      |
| 2. Summer-campaign Nth start           | −0.04 to −0.05              | LAW            | PARTIALLY→ODDS-FREE                      | **new WF-testable candidate, not yet tested**  | none identified                           |
| 3. is_meet_repeat (滞在, inversion)    | −0.01 to −0.03 (Hokkaido↑)  | LAW            | PARTIALLY-PRICED (underpriced, not over) | none new                                       | `jra-meet-repeat` REJECT (feature bundle) |
| 3. summer_to_summer_diff (circuit-hop) | 0.0005-0.011                | PARTIAL/NOISE  | too weak to call                         | none                                           | none                                      |
| 3b. NAR→JRA transfer                   | ~0.01                       | insufficient n | FULLY-PRICED                             | none                                           | none                                      |
| 4. class_diff (own effect)             | 0.017-0.033                 | LAW            | PARTIALLY-PRICED                         | class-ordinal swap already REJECTED as feature | `jra-class-ordinal-fix` REJECT            |
| 4. class_diff (summer elevation)       | −0.005 to −0.016 (negative) | LAW            | class-jump tax muted at summer venues    | n/a                                            | same                                      |

## 日本語まとめ

夏の間隔・使い詰め・転戦/滞在の法則について、市場(オッズ・人気順)の目線から
検証した。全4角度とも「レース単位」「人気帯内persistence」「人気順位+オッズ
順位を統制した偏相関」「2015-19/2020-23/2024-26の3era再現性」でLAW/PARTIAL/
NOISE判定を行った。

1. **間隔(連闘/中1-2週/中3-8週/放牧明け)**: 休養が長いほど成績が悪化する効果
   は市場全体で実在し(3era安定LAW)、市場は半分程度しか織り込んでいない
   (odds-free残差あり)。しかし「夏は連闘馬を優遇する」という仮説は**否定**
   — 同じ6-9月の中央場と比べても夏場開催で特別優遇される様子はなく、
   むしろ夏場のほうがこの効果はやや弱い(3era安定)。
2. **使い詰め(夏季キャンペーンN戦目)**: 人気薄(7人気以上)の馬に限り、
   同一年内の夏場開催でのN戦目が進むほど着順が上昇する明確な右肩上がりが
   見られ、市場はこれをほとんど織り込んでいない(3era安定LAW、3歳・古馬
   問わず同方向)。既存armBにはこの「夏場限定・年内累積出走数」という特徴量
   がなく、新規WF候補として提案する(本ドキュメントでは学習検証まで行わず、
   記述統計のみ)。
3. **転戦/滞在**: REJECT済みの `is_meet_repeat` 特徴量が悪化した理由を検証
   したところ、「市場が滞在馬を過大評価している」という仮説は**否定**され、
   実際は逆(市場はやや過小評価=滞在馬はわずかに市場想定より好走、3era安定
   LAW、特に北海道開催でこの効果は年々拡大)。ただし人気帯別には一様でなく
   (4-6人気帯では逆方向)、小さい効果である点は留意。異なる夏場開催間の
   転戦(circuit-hop)はやや成績が悪いが、統計的に弱くNOISE寄り。NAR→JRA
   転入馬は市場が既にほぼ完全に織り込み済み(FULLY-PRICED)。
4. **クラス(格上げ/格下げ)**: 格上げ(クラス上昇)による「格の壁」税は
   JRA全体で実在し市場は半分程度しか織り込まない(3era安定LAW)が、この
   壁は夏場開催のほうが中央場より一貫して薄い(3era安定)——ローカル
   開催のレース層の薄さを反映している可能性。

いずれも「モデル特徴量として追加すべき」という結論には直結しないが(角度2の
夏季キャンペーンN戦目のみ未検証の新規候補)、記述的な市場効率性の理解として
はすべて有意義な結果。

## Artifacts

- Feature build: `tmp/candidate-summer-rotation/build_features.py` (PG
  jvd_se+jvd_ra, strictly-prior window functions, 554,358 rows, 0.3s)
- Shared helpers: `tmp/candidate-summer-rotation/common.py`
- Angle scripts: `angle1_interval.py`, `angle2_campaign.py`,
  `angle3_transition.py`, `angle3b_nar_transfer.py`, `angle4_class.py`
- Reports: `angle1_interval_report.json`, `angle2_campaign_report.json`,
  `angle3_transition_report.json`, `angle3b_nar_report.json`,
  `angle4_class_report.json`
- Feature parquet: `tmp/candidate-summer-rotation/rotation_features.parquet`
- Related: `jra-summer-upset-divergence-2026-07-04.md` (layoff/prior-finish
  no-summer-elevation precedent, same partial-Spearman methodology),
  `jra-meet-repeat-2026-07-04.md` (REJECTED feature this doc's angle-3
  inversion check follows up on), `jra-class-ordinal-fix-2026-07-04.md`
  (corrected ordinal + REJECTED model-feature swap this doc's angle-4 uses
  the same corrected ordinal for, descriptively only)
