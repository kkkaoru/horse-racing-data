---
class: 999
label: 障害 (Jumps)
category: jra
n_races_holdout_2023_26: 1064
baseline_top1: 42.01
baseline_place2: 23.03
baseline_place3: 16.35
model_vs_market_top1_delta: +13.63
active_model: iter19-jra-cb-kohan3f-going-v8 (category-global fallback)
per_class_model: none (Phase B registry empty)
candidate7_status: ABORT (2026-06-17)
---

# JRA 999 — 障害 (Jumps)

## Status

Routes to category-global `iter19-jra-cb-kohan3f-going-v8`. No per-class model registered.  
Candidate 7 FEAT probe completed 2026-06-17: **ABORT**.

## Headroom

| Metric | Model% | Notes            |
| ------ | -----: | ---------------- |
| top1   |  42.01 | Mdl−Mkt +13.63pp |
| place2 |  23.03 | —                |
| place3 |  16.35 | —                |

---

## Jump Raw Data Audit

### Data Source: `jvd_um` (競走馬マスタ) — Pre-race static summary fields

The following jump-specific columns exist in `jvd_um` / `nvd_um` (horse master record, present
as snapshot at race-entry time). These are **not** currently read by the feature pipeline
(`stage_um_table` only selects `ketto_toroku_bango, ketto_joho_01b, ketto_joho_05b`):

| Column                             | Logical name     | Format                               | Notes                                 |
| ---------------------------------- | ---------------- | ------------------------------------ | ------------------------------------- |
| `shogai`                           | 障害・着回数     | `char(18)` = 6×3-digit finish counts | Total jump career finish distribution |
| `shogai_ryo`                       | 障良・着回数     | `char(18)`                           | Jump on 良 (good) track               |
| `shogai_yayaomo`                   | 障稍・着回数     | `char(18)`                           | Jump on 稍重 (yielding) track         |
| `shogai_omo`                       | 障重・着回数     | `char(18)`                           | Jump on 重 (heavy) track              |
| `shogai_furyo`                     | 障不・着回数     | `char(18)`                           | Jump on 不良 (soft) track             |
| `shogai_sapporo` … `shogai_kokura` | 各場障害着回数   | `char(18)`                           | Per-venue jump finish counts          |
| `shogai_honshokin_ruikei`          | 障害本賞金累計   | `char(9)`                            | Cumulative jump prize money           |
| `shogai_fukashokin_ruikei`         | 障害付加賞金累計 | `char(9)`                            | Cumulative jump bonus money           |
| `shogai_shutokushokin_ruikei`      | 障害収得賞金累計 | `char(9)`                            | Cumulative jump earnings              |

**Critical limitation**: these are snapshot values at race-entry time. The JV-Data feed updates
`jvd_um` periodically; the snapshot may not perfectly reflect prior-race-only history at
every row. However, they represent a pre-race summary, making them approximately leak-free.

### Data Source: `jvd_se` (馬毎レース情報) — Per-race history

| Column                    | Logical name   | Format    | Jump relevance                                             |
| ------------------------- | -------------- | --------- | ---------------------------------------------------------- |
| `ijo_kubun_code`          | 異常区分コード | `char(1)` | `3`=競走除外, `4`=競走中止 (DNF/fall-stop), `6`=落馬再騎乗 |
| `kakutei_chakujun`        | 確定着順       | `char(2)` | Finish position (00 if DNF)                                |
| `kyoso_joken_code`        | via `jvd_ra`   | —         | Jump race identifier: code `999`                           |
| `shogai_honshokin_ruikei` | 障害本賞金累計 | `char(9)` | Jump earnings snapshot at race time                        |

### No obstacle/elevation course-specific fields

The `jvd_cs` (コース情報) table contains `keibajo_code, kyori, track_code` and includes
`course_elevation_diff_m, course_final_straight_m, course_corner_count` (already in the
feature set via the 244-col parquet). No obstacle-count or hurdle-type columns exist in
the available schema.

### Jump race identification

Jump races have `kyoso_joken_code = '999'` in `jvd_ra`. This was confirmed:
`SELECT count(*) FROM jvd_ra WHERE kyoso_joken_code = '999'` → **13,263 races** in the DB.
`jvd_se` join on this returns **89,340 horse-race entries** (all history), consistent
with ~6.7 starters per jump race.

---

## Candidate 7: Jump-Specific Features — Partial-ρ Probe

**Date run**: 2026-06-17  
**Script**: `tmp/probe_jra_999_jump_features.py`  
**DuckDB settings**: `memory_limit='4GB'; threads=4`

### Features Probed

Five features derived from prior-race history only (strictly leak-free: `race_date <
target race_date`):

| Feature                  | Definition                                              |
| ------------------------ | ------------------------------------------------------- |
| `prior_jump_starts`      | Count of prior 障害 races (including DNFs)              |
| `prior_jump_win_rate`    | 1着 rate in prior jump races (NULL if 0 prior starts)   |
| `prior_jump_place3_rate` | top-3 rate in prior jump races (NULL if 0 prior starts) |
| `jump_experience_ratio`  | prior_jump_starts / total_prior_starts                  |
| `prior_jump_dnf_falls`   | Count of prior 競走中止 / 競走除外 in jump races        |

Controls (partial ρ): `popularity_score` + `career_win_rate`

### Coverage

| Feature                  |        Non-null | Coverage |
| ------------------------ | --------------: | -------: |
| `prior_jump_starts`      | 77,452 / 77,452 |   100.0% |
| `prior_jump_win_rate`    | 64,684 / 77,452 |    83.5% |
| `prior_jump_place3_rate` | 64,684 / 77,452 |    83.5% |
| `jump_experience_ratio`  | 77,435 / 77,452 |  ~100.0% |
| `prior_jump_dnf_falls`   | 77,452 / 77,452 |   100.0% |

NULLs in win/place rate are debut-jump horses (no prior jump history — informative NULLs
that GBDT already handles via current career features).

### Partial-ρ Results

| Feature                  | ρ_full (2013-26) | n_full | ρ_hold (2023-26) | n_hold | Clears ≥0.08? |
| ------------------------ | ---------------: | -----: | ---------------: | -----: | :-----------: |
| `prior_jump_starts`      |          −0.0132 | 54,508 |          −0.0048 | 14,630 |      no       |
| `prior_jump_win_rate`    |          +0.0150 | 45,622 |          +0.0134 | 12,256 |      no       |
| `prior_jump_place3_rate` |          −0.0061 | 45,622 |          −0.0256 | 12,256 |      no       |
| `jump_experience_ratio`  |          −0.0030 | 54,508 |          +0.0072 | 14,630 |      no       |
| `prior_jump_dnf_falls`   |          +0.0055 | 54,508 |          +0.0039 | 14,630 |      no       |

**Best feature**: `prior_jump_win_rate` (ρ_full=+0.0150, ρ_hold=+0.0134) — less than
one-fifth of the 0.08 threshold. All features are noise-level in both windows.

---

## Verdict: ABORT

**All 5 jump-specific features fail decisively** — best |ρ| = 0.0150, vs threshold 0.08.
No feature shows any meaningful incremental signal beyond the odds/career baseline.

### Why the signal is absent

1. **GBDT already captures jump experience via generic form features.** The 244-feature set
   includes `career_win_rate`, `career_top1_count`, `same_track_win_rate`, speed indices,
   and corner pass ratios — all computed from prior races including jump races. A horse's
   jump ability is encoded in these form signals without any special jump flag.

2. **Jump race dynamics at signal level are no different from flat.** The market (odds) also
   prices in jump experience. `popularity_score` is already controlled for, so what remains
   after controlling for odds + career form is truly residual. That residual is noise.

3. **Fall/DNF count is near-zero signal.** Jump DNF/falls are rare events. Even horses with
   falls have ρ ≈ 0.005 — consistent with random noise. The GBDT likely routes these horses
   correctly via their low odds/poor form features.

4. **Course-specific obstacle data does not exist.** The JV-Data schema has no obstacle count,
   hurdle type, or elevation-change-per-obstacle field. The `jvd_cs` columns already in the
   feature set (`course_elevation_diff_m`, `course_corner_count`) are what's available.

### What this means for the campaign

The 999 class Mdl−Mkt gap (+13.63pp) reflects a strong model vs a strong market, not a gap
in jump-specific information. The model is already capturing jump dynamics via the generic
feature set. A dedicated per-class jump model with new features would not improve over the
category-global model.

**Do NOT add to DO-NOT-RETEST registry** — this was a FEAT probe that definitively shows no
available jump-specific signal source. No training was performed.

---

## Evaluation Log

| Date       | Hypothesis                             | Method                | Verdict   | Notes                                                                               |
| ---------- | -------------------------------------- | --------------------- | --------- | ----------------------------------------------------------------------------------- |
| 2026-06-17 | Jump-specific FEAT probe (Candidate 7) | partial-ρ, 5 features | **ABORT** | Best ρ=0.0150, threshold 0.08. No jump-specific signal beyond odds+career baseline. |
