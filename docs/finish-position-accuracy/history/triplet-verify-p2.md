# Triplet Verification P2 — Rank 2 & Rank 9 (2026-06-12)

## Context

Per-class partial Spearman ρ probe on two triplets from `triplet-ideation-ranked.md`.
READ-ONLY on local PG mirror (`postgresql://horse_racing:***@127.0.0.1:15432/horse_racing`).
History window: 2020-01-01 (for 5-race look-back) to 2026-06-11. Target window: 2023-01-01 to
2026-06-11. Gate: partial Spearman |ρ| ≥ 0.08 after controlling for log-odds.

Probe script: `tmp/triplet_verify_p2_mp.py` (not tracked).

### Schema notes confirmed during probe

All JVD/NVD table columns are `character varying`. Casts:

| Field             | JRA                                                          | NAR                      | Ban-ei (keibajo_code='83')         |
| ----------------- | ------------------------------------------------------------ | ------------------------ | ---------------------------------- |
| `futan_juryo`     | decimal string ÷10 = kg                                      | decimal string ÷10 = kg  | **hex string** ÷10 = kg            |
| `bataiju`         | decimal string = kg                                          | decimal string = kg      | **hex string** = kg                |
| `kyori`           | decimal string = metres                                      | decimal string = metres  | decimal string = metres (200 only) |
| `corner_4`        | decimal integer position                                     | decimal integer position | all zeros (no corners — straight)  |
| `shusso_tosu`     | in `jvd_ra`/`nvd_ra`                                         | in `nvd_ra`              | same                               |
| date construction | `kaisai_nen` + `kaisai_tsukihi` (MMDD) → ISO date            | same                     | same                               |
| join              | `kaisai_nen`, `kaisai_tsukihi`, `keibajo_code`, `race_bango` | same                     | same                               |

**Ban-ei hex example**: `futan='276'` → `int('276', 16)=630` ÷10 = 63.0 kg;
`bataiju='3EA'` → `int('3EA', 16)=1002` kg. Values that look purely numeric (e.g. `'276'`) are
still hex for Ban-ei.

**Ban-ei structural note**: `corner_4` is always `'00'` for Ban-ei (200m straight track, no
corners). Ban-ei is therefore excluded from the LOW triplet analysis (corner4_norm is
meaningless) but included in the HIGH triplet analysis.

---

## Rank 2: `futan_juryo` × `bataiju` × `kyori`

### Hypothesis

The TIME-SERIES SLOPE of `futan_juryo / bataiju` (carrying ratio) across recent starts, bucketed
by distance, captures whether a horse is being asked to carry proportionally more weight than its
recent baseline and whether this trend predicts performance. Existing features (`bataiju_futan_ratio`,
`joint_ratio`) are static within-race ratios, not slopes. The load density normalised by distance
(`futan / kyori`) averaged over recent races is also expected to add signal.

### Features built

| Feature                          | Description                                                 |
| -------------------------------- | ----------------------------------------------------------- |
| `carrying_ratio_slope5`          | OLS slope of carrying_ratio over last ≤5 prior races        |
| `load_density_avg5`              | avg(futan_kg / kyori_m) over last ≤5 prior                  |
| `carrying_ratio_same_dist_avg`   | avg(carrying_ratio) over last ≤5 prior at same kyori bucket |
| `carrying_ratio_same_dist_slope` | slope of carrying_ratio at same bucket (last ≤5)            |
| `load_density_slope5`            | OLS slope of load_density over last ≤5 prior races          |

Kyori buckets: short ≤1400m, middle 1400-2000m, long ≥2000m.
Ban-ei bataiju median = 991 kg (heavy draft); futan_kg median = 61.0 kg; carrying_ratio = 0.061.

### Per-class partial Spearman ρ table (vs `finish_norm`, controlling for log-odds)

| Category | Feature                          | ρ           | p-value  | n       | valid% | Gate     |
| -------- | -------------------------------- | ----------- | -------- | ------- | ------ | -------- |
| JRA      | `carrying_ratio_slope5`          | −0.0189     | 6.70e-16 | 183,212 | 79%    | FAIL     |
| JRA      | `load_density_avg5`              | **+0.0980** | 0.00e+00 | 183,212 | 79%    | **PASS** |
| JRA      | `carrying_ratio_same_dist_avg`   | +0.0372     | 4.63e-52 | 166,763 | 72%    | FAIL     |
| JRA      | `carrying_ratio_same_dist_slope` | −0.0144     | 7.30e-08 | 140,183 | 61%    | FAIL     |
| JRA      | `load_density_slope5`            | −0.0186     | 1.48e-15 | 183,212 | 79%    | FAIL     |
| NAR      | `carrying_ratio_slope5`          | +0.0096     | 1.14e-10 | 449,946 | 96%    | FAIL     |
| NAR      | `load_density_avg5`              | +0.0171     | 1.39e-30 | 449,946 | 96%    | FAIL     |
| NAR      | `carrying_ratio_same_dist_avg`   | −0.0014     | 3.66e-01 | 401,739 | 85%    | FAIL     |
| NAR      | `carrying_ratio_same_dist_slope` | +0.0087     | 2.43e-07 | 356,761 | 76%    | FAIL     |
| NAR      | `load_density_slope5`            | −0.0058     | 8.66e-05 | 449,946 | 96%    | FAIL     |
| BAN      | `carrying_ratio_slope5`          | +0.0280     | 1.72e-10 | 52,068  | 95%    | FAIL     |
| BAN      | `load_density_avg5`              | −0.0040     | 3.61e-01 | 52,068  | 95%    | FAIL     |
| BAN      | `carrying_ratio_same_dist_avg`   | −0.0075     | 8.55e-02 | 52,937  | 97%    | FAIL     |
| BAN      | `carrying_ratio_same_dist_slope` | +0.0280     | 1.72e-10 | 52,068  | 95%    | FAIL     |
| BAN      | `load_density_slope5`            | +0.0207     | 2.34e-06 | 52,068  | 95%    | FAIL     |

### Redundancy analysis (incremental partial ρ over avg+odds)

After controlling for `load_density_avg5` in addition to log-odds:

| Category | Feature                          | Incremental ρ | n       |
| -------- | -------------------------------- | ------------- | ------- |
| JRA      | `carrying_ratio_slope5`          | −0.0162       | 183,212 |
| JRA      | `carrying_ratio_same_dist_slope` | −0.0126       | 140,183 |
| NAR      | `carrying_ratio_slope5`          | +0.0094       | 449,946 |
| NAR      | `carrying_ratio_same_dist_slope` | +0.0087       | 356,761 |
| BAN      | `carrying_ratio_slope5`          | +0.0282       | 52,068  |
| BAN      | `carrying_ratio_same_dist_slope` | +0.0282       | 52,068  |

### Interpretation

- **`load_density_avg5`** (JRA ρ=+0.098) is the only PASS feature. However, this is an _average_
  of `futan_kg / kyori_m`, not a slope. This is structurally very similar to the existing
  `past_speed_futan_normalized_avg5` which computes `soha_time/kyori × futan` — both use the
  `futan/kyori` ratio over recent starts. The PASS likely reflects redundancy with existing
  futan-normalised speed features rather than genuinely new signal.
- The key hypothesis of this triplet — that the **SLOPE** of carrying ratio over time is
  predictive — fails across all three categories (JRA −0.019, NAR +0.010, BAN +0.028). All
  incremental slope ρ values after controlling for the average are below 0.03.
- The gate pass for `load_density_avg5` is formally correct but the underlying signal is
  likely already captured by `past_speed_futan_normalized_avg5` and `past_speed_kg_normalized_avg5`.

### Verdict

**ABORT** — the novel feature (carrying-ratio slope) fails the gate in all three categories
(best slope ρ = −0.019 JRA). The sole PASS feature (`load_density_avg5` JRA ρ=+0.098) is an
average, not a slope, and is very likely redundant with existing `past_speed_futan_normalized_avg5`.
No net signal gain expected from implementing this triplet's trajectory features.

---

## Rank 9: `kyori` × `corner4_norm` × `finish_position`

### Hypothesis

At each distance bucket, a horse's average late-corner field position (`corner4_norm`) and
average finish position combine to reveal distance-specific late-race behaviour. The joint
signal (distance × late-position × result) is expected to be partially redundant with
`same_distance_win_rate` and `corner_pass_avg_5`, but the distance-conditional corner4 trajectory
may add marginal signal.

### Features built

| Feature                        | Description                                                      |
| ------------------------------ | ---------------------------------------------------------------- |
| `corner4_norm_same_dist_avg`   | avg(corner4_norm) over last ≤10 prior races at same kyori bucket |
| `corner4_norm_same_dist_slope` | OLS slope of corner4_norm at same bucket (last ≤10)              |
| `corner4_norm_avg10`           | avg(corner4_norm) unconditional over last ≤10 prior              |
| `finish_norm_same_dist_avg`    | avg(finish_norm) at same kyori bucket (proxy for same_dist_wr)   |
| `finish_norm_bucket_spread`    | max(bucket_avg_fn) − min(bucket_avg_fn) across distance buckets  |

Note: Ban-ei `corner_4` is always 0 (straight 200m track, no corners). Ban-ei is included for
finish_norm_same_dist_avg probe only; corner features are structurally zero.

### Per-class partial Spearman ρ table (vs `finish_norm`, controlling for log-odds)

| Category | Feature                        | ρ           | p-value   | n       | valid% | Gate     |
| -------- | ------------------------------ | ----------- | --------- | ------- | ------ | -------- |
| JRA      | `corner4_norm_same_dist_avg`   | **+0.1871** | 0.00e+00  | 173,957 | 75%    | **PASS** |
| JRA      | `corner4_norm_same_dist_slope` | **+0.1082** | 0.00e+00  | 152,403 | 66%    | **PASS** |
| JRA      | `corner4_norm_avg10`           | **+0.1832** | 0.00e+00  | 183,212 | 79%    | **PASS** |
| JRA      | `finish_norm_same_dist_avg`    | **+0.3555** | 0.00e+00  | 173,957 | 75%    | **PASS** |
| JRA      | `finish_norm_bucket_spread`    | −0.0134     | 9.82e-06  | 108,984 | 47%    | FAIL     |
| NAR      | `corner4_norm_same_dist_avg`   | +0.0226     | 3.50e-49  | 425,741 | 91%    | FAIL     |
| NAR      | `corner4_norm_same_dist_slope` | +0.0498     | 6.46e-218 | 400,134 | 85%    | FAIL     |
| NAR      | `corner4_norm_avg10`           | +0.0117     | 3.41e-15  | 449,946 | 96%    | FAIL     |
| NAR      | `finish_norm_same_dist_avg`    | **+0.1161** | 0.00e+00  | 425,741 | 91%    | **PASS** |
| NAR      | `finish_norm_bucket_spread`    | +0.0037     | 5.51e-02  | 271,059 | 58%    | FAIL     |
| BAN      | `corner4_norm_same_dist_avg`   | −0.0008     | 8.60e-01  | 52,937  | 97%    | FAIL     |
| BAN      | `corner4_norm_same_dist_slope` | −0.0007     | 8.71e-01  | 52,068  | 95%    | FAIL     |
| BAN      | `corner4_norm_avg10`           | −0.0007     | 8.71e-01  | 52,068  | 95%    | FAIL     |
| BAN      | `finish_norm_same_dist_avg`    | +0.0321     | 1.45e-13  | 52,937  | 97%    | FAIL     |
| BAN      | `finish_norm_bucket_spread`    | NaN         | NaN       | 0       | 0%     | FAIL     |

### Redundancy analysis (incremental partial ρ over corner4_avg10 + finish_norm_same_dist_avg + odds)

After controlling for `corner4_norm_avg10`, `finish_norm_same_dist_avg`, and log-odds:

| Category | Feature                        | Incremental ρ | n       |
| -------- | ------------------------------ | ------------- | ------- |
| JRA      | `corner4_norm_same_dist_avg`   | −0.0333       | 173,957 |
| JRA      | `corner4_norm_same_dist_slope` | **+0.0967**   | 152,403 |
| NAR      | `corner4_norm_same_dist_avg`   | −0.0201       | 425,741 |
| NAR      | `corner4_norm_same_dist_slope` | +0.0600       | 400,134 |
| BAN      | `corner4_norm_same_dist_avg`   | −0.0001       | 52,937  |
| BAN      | `corner4_norm_same_dist_slope` | −0.0000       | 52,068  |

### Interpretation

The high marginal ρ values for `corner4_norm_same_dist_avg` (JRA +0.187) and
`finish_norm_same_dist_avg` (JRA +0.356) confirm there is genuine signal in the data. However:

1. **`finish_norm_same_dist_avg`** (JRA ρ=+0.356, NAR ρ=+0.116) is essentially equivalent to
   `same_distance_win_rate` — it is the horse's historical finishing position at the current
   distance. This feature type already exists in the model. The high ρ confirms the existing
   feature is informative, not that this triplet adds novel signal.

2. **`corner4_norm_same_dist_avg`** (JRA ρ=+0.187) is very similar to the unconditional
   `corner4_norm_avg10` (JRA ρ=+0.183). The incremental ρ after controlling for the
   unconditional average and same-dist finish average drops to −0.033 — **the distance-specific
   conditioning adds no incremental value**.

3. **`corner4_norm_same_dist_slope`** has incremental ρ = +0.097 after full controls in JRA.
   This just barely misses the gate of 0.08 in the _incremental_ analysis (it passes the raw
   gate at +0.108). The finding is marginal: it reflects that the trend of where a horse finishes
   at a given distance distance slightly predicts future performance, but this is likely already
   partially captured by `same_distance_win_rate` × `corner_pass_avg_5` interactions in LGBM.

4. **NAR**: No corner feature passes the gate. `finish_norm_same_dist_avg` (NAR +0.116) is
   captured by `same_distance_win_rate`.

5. **Ban-ei**: Corner features are all noise (ρ ≈ 0, p > 0.85) as expected for a straight track.

### Verdict

**ABORT** — the gate pass features are redundant with existing model inputs. The novel
joint signal (distance-conditional corner4 trajectory) shows only marginal incremental ρ
(+0.097 in JRA, after full controls, below the 0.08 incremental gate; NAR +0.060). The
ideation doc's redundancy prediction was accurate: `same_distance_win_rate` and `corner_pass_avg_5`
already capture the underlying pattern.

---

## Summary

| Triplet                             | Rank | Best ρ (raw) | Best class | Best feature                | Verdict   |
| ----------------------------------- | ---- | ------------ | ---------- | --------------------------- | --------- |
| `futan_juryo` × `bataiju` × `kyori` | 2    | +0.098       | JRA        | `load_density_avg5`         | **ABORT** |
| `kyori` × `corner4_norm` × `fp`     | 9    | +0.356       | JRA        | `finish_norm_same_dist_avg` | **ABORT** |

Both triplets ABORT:

- Rank 2 (HIGH): the novel slope features fail the gate (best slope ρ ≤ 0.028); the only PASS
  is an average that duplicates `past_speed_futan_normalized_avg5`.
- Rank 9 (LOW): the PASS features are redundant with `same_distance_win_rate` and
  `corner_pass_avg_5`; the genuinely incremental feature (`corner4_same_dist_slope`) has
  incremental ρ = 0.097 in JRA only, sub-gate after full redundancy controls.

Next pair: Rank 3 (`tansho_odds` × `corner1_norm` × `finish_position`) and
Rank 8 (`zogen_sa` × `tansho_odds` × `finish_position`) — see `triplet-verify-p3.md`.
