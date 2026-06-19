# JRA Probe — Jockey × Venue × Distance × Surface, **PER-CLASS** partial ρ

**Date**: 2026-06-19
**Category**: JRA finish-position
**Verdict**: **REJECT** — no class or venue yields a usable residual jockey-skill edge.
The only cells whose bootstrap CI excludes 0 in the global-eval sense have the
**wrong sign** (see "Sign caveat"); every cell where the sign is correct
(real skill edge) has |ρ| ≪ 0.08 and most are negligible.

## Hypothesis

Global partial ρ for the 3-way jockey cell is ~0 (odds absorbs it; see
`jra-jockey-triple-interaction.md`). USER directive: re-evaluate **per class**
(`grade_code`) and **per venue** (`keibajo_code`), controlling on **`odds_score`
only**, to find a subclass where the market has _not_ fully absorbed the jockey's
specific edge — and if found, route the model for that subclass.

- cell = `kishu_code × keibajo_code × surface × dist_band`
- `dist_band` = {0:<1400, 1:1400–1799, 2:1800–2199, 3:2200+}; `surface = left(track_code,1)`
- control = **`odds_score` only** (not the existing jockey rates — we want the part
  the _market_ hasn't priced, per the brief)

## Method

- **Feature store**: `tmp/feat-jra-v8-iter18-class/race_year=*/data_0.parquet`
  (263 cols, 2007–2025 used; carries `grade_code`, `keibajo_code`, `race_id`,
  `odds_score`, `finish_position`). The requested
  `tmp/v8/feat-jra-v8-iter19-kohan3f-going/` **does not exist on disk** — confirmed,
  matching the task brief (a prior probe doc claims it exists; that claim is stale).
- **History**: PG `jvd_se ⋈ jvd_ra` (port **15432**, not 5432). `jvd_se` carries
  `kishu_code`/`kakutei_chakujun` but **not** `kyori`/`track_code` → join `jvd_ra`.
  JRA only (`keibajo_code` 01–10), `kishu_code <> '00000'`, placed finishers 1–18.
- **LOYO** (leave-one-year-out), leakage-safe: per cell, win rate over the jockey's
  rides in _all other years_, requiring ≥10 rides in that pool.
- Join store↔history on `race_id`+`umaban` (store has no `kishu_code`;
  `race_id = jra:kaisai_nen:kaisai_tsukihi:keibajo_code:race_bango`).
- **partial ρ**: rank-transform; residualize `jc_win` and `finish_position` on
  `rank(odds_score)`; Spearman of residuals.
- **Bootstrap 95% CI**: n_boot=**5000**, seed=**42**, resample rows with replacement.
- DuckDB `memory_limit='4GB', threads=4`. Script: `/tmp/jra_jockey_triple_perclass.py`.

## Coverage

|                                              |                      |
| -------------------------------------------- | -------------------- |
| JRA LOYO ride rows 2007–2025                 | 923,146              |
| non-null `jc_win` (≥10 other-year rides)     | 875,278 (**94.81%**) |
| eval rows (fin & odds & jc_win all non-null) | 875,278              |

## Sign caveat (decisive)

`jc_win` is a **win rate** → higher means a _better_ finish → a genuine residual
jockey edge must show **negative** partial ρ vs `finish_position`. So:

- **negative** partial ρ with CI excluding 0 = real (correct-sign) residual edge,
- **positive** partial ρ with CI excluding 0 = _anti_-signal (the LOYO rate, after
  odds, ranks finishers slightly the _wrong_ way — not a usable skill lever).

A naive "PASS = LB95 > 0" therefore flags only **wrong-sign** cells. Both
sign-orientations are reported below; "real-edge?" applies the correct sign.

## Results — ALL

| metric                          | value                               |
| ------------------------------- | ----------------------------------- |
| raw Spearman (jc_win vs finish) | **−0.2360**                         |
| partial ρ \| odds_score         | **+0.0019** CI95 [−0.0001, +0.0040] |

Raw signal strong & correct-sign; collapses to ~0 once odds controlled. Matches prior.

## Results — per `grade_code`

| grade | meaning       | n       | raw ρ   | partial ρ   | CI95              | real-edge?                                  |
| ----- | ------------- | ------- | ------- | ----------- | ----------------- | ------------------------------------------- |
| `' '` | maiden/1-win  | 646,289 | −0.2429 | **+0.0056** | [+0.0032,+0.0081] | **no** (wrong sign)                         |
| `E`   | newcomer/新馬 | 187,003 | −0.2194 | **−0.0131** | [−0.0177,−0.0086] | sign ok but \|ρ\|≪0.08                      |
| `C`   | 3-win         | 18,114  | −0.2103 | −0.0280     | [−0.0420,−0.0131] | sign ok, \|ρ\|≪0.08                         |
| `B`   | —             | 9,128   | −0.2597 | −0.0430     | [−0.0634,−0.0223] | sign ok, \|ρ\|≪0.08                         |
| `A`   | —             | 6,811   | −0.2276 | −0.0305     | [−0.0545,−0.0062] | sign ok, \|ρ\|≪0.08                         |
| `L`   | listed        | 5,664   | −0.2071 | −0.0332     | [−0.0590,−0.0072] | sign ok, \|ρ\|≪0.08                         |
| `H`   | —             | 1,078   | −0.2490 | −0.0774     | [−0.1385,−0.0169] | sign ok, **\|ρ\|≈0.08** but n tiny, CI wide |
| `G`   | —             | 596     | −0.2129 | −0.0336     | [−0.1161,+0.0464] | CI straddles 0                              |
| `F`   | —             | 421     | —       | skip (<500) |                   |                                             |
| `D`   | —             | 174     | —       | skip (<500) |                   |                                             |

## Results — per `keibajo_code`

| code | venue     | n       | raw ρ   | partial ρ   | CI95              | real-edge?          |
| ---- | --------- | ------- | ------- | ----------- | ----------------- | ------------------- |
| 01   | Sapporo   | 34,071  | −0.1962 | +0.0105     | [−0.0006,+0.0213] | no (CI straddles 0) |
| 02   | Hakodate  | 32,603  | −0.1637 | **+0.0263** | [+0.0155,+0.0371] | **no** (wrong sign) |
| 03   | Fukushima | 61,202  | −0.1626 | +0.0069     | [−0.0009,+0.0149] | no                  |
| 04   | Niigata   | 86,001  | −0.1895 | +0.0025     | [−0.0042,+0.0091] | no                  |
| 05   | Tokyo     | 138,316 | −0.2805 | **+0.0166** | [+0.0112,+0.0219] | **no** (wrong sign) |
| 06   | Nakayama  | 128,526 | −0.2536 | **+0.0097** | [+0.0040,+0.0153] | **no** (wrong sign) |
| 07   | Chukyo    | 80,857  | −0.2007 | −0.0037     | [−0.0108,+0.0031] | no                  |
| 08   | Kyoto     | 117,613 | −0.2509 | +0.0055     | [−0.0003,+0.0111] | no                  |
| 09   | Hanshin   | 123,811 | −0.2619 | −0.0051     | [−0.0108,+0.0004] | no                  |
| 10   | Kokura    | 72,278  | −0.2054 | −0.0130     | [−0.0203,−0.0060] | sign ok, \|ρ\|≪0.08 |

## PASS list (naive LB95 > 0)

Reporting both interpretations:

- **LB95 > 0 literally** (wrong sign — anti-signal, NOT usable):
  grade `' '`; venues Hakodate, Tokyo, Nakayama.
- **Correct-sign residual edge with |ρ| ≥ 0.08** (the meaningful bar):
  **none**. The closest is grade `H` (ρ=−0.077, n=1078) — under threshold and CI is
  wide; no model-routing case.

## Interpretation

Coverage is fine (94.8% have ≥10 other-year rides). Per-class slicing does **not**
open a usable lever:

1. The few cells that pass a naive "LB95 > 0" do so with the **wrong sign** — after
   odds, the LOYO win-rate orders those finishers slightly _backwards_; nothing to
   route on.
2. Every cell whose sign is correct (graded classes E/C/B/A/L, venue Kokura) has
   |partial ρ| well under 0.08 — odds has absorbed the jockey edge here too.
3. The signal is genuinely strong raw (raw ρ ≈ −0.21 to −0.28 everywhere, correct
   sign) but **fully priced** in every subclass; controlling odds _alone_ collapses
   it. Consistent with the standing market-efficiency frontier
   (`project_science_track_saturation_2026_06_11`,
   `project_relationship_perclass_investigation_2026_06_12`,
   `project_perclass_campaign_complete_2026_06_17`).

**Do not adopt; no per-class model routing warranted.** partial ρ fails the
necessary condition (correct sign + |ρ|≥0.08) in every class and venue.

## Reproduce

`uv run python /tmp/jra_jockey_triple_perclass.py`
(PG port 15432; store `tmp/feat-jra-v8-iter18-class`; bootstrap n=5000 seed=42).
