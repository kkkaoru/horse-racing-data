# F1: Pedigree Silent-NULL Fix (NAR / Ban-ei)

**Date:** 2026-06-12
**Branch:** docs/jes-journal-collection
**Status:** Fixed + committed

---

## Root Cause

`add-near-miss-features.py` and `add-baba-pedigree-affinity-features.py` staged
horse pedigree (sire / damsire registration numbers) by querying `pg.jvd_um`
only. JRA horse masters live in `pg.jvd_um`; NAR and Ban-ei horse masters live
in `pg.nvd_um`. Every NAR / Ban-ei horse therefore received `sire_id = NULL`
and `damsire_id = NULL` in the staging temp table, silently propagating NULL
through all downstream sire/damsire feature calculations.

The main pipeline (`finish_position_features_duckdb.py`) already handled this
correctly via `COALESCE(jvd_um, nvd_um)` — the bug was isolated to the two
secondary feature-layer scripts.

---

## Affected Columns

### `add-near-miss-features.py` (v6 layer)

| Column                         | Derivation                                    |
| ------------------------------ | --------------------------------------------- |
| `sire_distance_place2_rate`    | sire_distance_p2 / sire_distance_starts       |
| `sire_grade_place2_rate`       | sire_grade_p2 / sire_grade_starts             |
| `damsire_distance_place2_rate` | damsire_distance_p2 / damsire_distance_starts |

### `add-baba-pedigree-affinity-features.py` (v7 layer)

| Column                           | Derivation                                    |
| -------------------------------- | --------------------------------------------- |
| `sire_baba_win_rate`             | sire wins / starts on same baba condition     |
| `sire_baba_career_starts`        | sire starts on same baba                      |
| `damsire_baba_win_rate`          | damsire wins / starts on same baba            |
| `damsire_baba_career_starts`     | damsire starts on same baba                   |
| `sire_horse_baba_combined_score` | mean(horse_baba_win_rate, sire_baba_win_rate) |

---

## NULL Rate Before Fix (estimated)

NAR horses are registered in `pg.nvd_um` only. The join on `pg.jvd_um` returns
no match, so `sire_id = NULL` and `damsire_id = NULL` for **100% of NAR /
Ban-ei horses**. All five sire/damsire columns above are NULL for every NAR /
Ban-ei row in the materialized feature parquets built with these scripts.

JRA horses (in `pg.jvd_um`) were unaffected — non-NULL rate for JRA was
determined by actual data completeness in jvd_um (typically high).

Ban-ei horses also live in `pg.nvd_um` (keibajo_code = '83') and were equally
affected.

---

## Fix

Extracted `stage_horse_pedigree` into a new shared module:
`src/scripts/finish-position-features/pedigree_staging.py`

The new implementation unions both sources with QUALIFY deduplication:

```sql
with combined as (
  select ketto_toroku_bango,
         nullif(trim(ketto_joho_01a), '') as sire_id,
         nullif(trim(ketto_joho_04a), '') as damsire_id,
         1 as priority
  from pg.jvd_um where ketto_toroku_bango is not null
  union all
  select ketto_toroku_bango,
         nullif(trim(ketto_joho_01a), '') as sire_id,
         nullif(trim(ketto_joho_04a), '') as damsire_id,
         2 as priority
  from pg.nvd_um where ketto_toroku_bango is not null
)
select ketto_toroku_bango, sire_id, damsire_id
from combined
qualify row_number() over (partition by ketto_toroku_bango order by priority) = 1
```

Both `add-near-miss-features.py` and `add-baba-pedigree-affinity-features.py`
now import and call this shared function.

---

## Dead-Heat Handling

`finish_position` is stored as an integer. Tied horses share the same integer
value and receive the same relevance label (tier map: `{1: 3, 2: 2, 3: 1}`,
default 0). Binary objectives (`binary-top1`, `binary-place2`, etc.) also
treat ties symmetrically — both horses in a 1st-place dead-heat get label=1.
This is correct behavior: the model should assign high probability to both.
No fractional label splitting is applied, which is standard for LambdaRank /
binary cross-entropy objectives.

---

## NULL Rate After Fix (expected)

After the fix, NAR / Ban-ei horses will receive non-NULL `sire_id` /
`damsire_id` wherever `pg.nvd_um` carries that data. The realized gain depends
on `nvd_um` data completeness. If `nvd_um` is sparse for older NAR horses, the
improvement will be partial but always >= 0pp.

---

## Retrain Recommendation

The affected columns (`sire_*_place2_rate`, `damsire_*_place2_rate`,
`sire_baba_win_rate`, etc.) were previously 100% NULL for NAR / Ban-ei in
v6 / v7 feature layers. The model learned to handle these as constant-missing,
effectively ignoring them. With the fix, these columns become informative for
NAR / Ban-ei horses.

**Recommendation: NAR + Ban-ei full retrain is warranted.**

The saturation analysis (2026-06-11) identified the empirical frontier for the
current feature set, but that frontier was measured with silently-NULL pedigree
features for all NAR / Ban-ei horses. The v6/v7 pedigree columns are new
genuine signal for these categories.

Retrain scope:

- NAR models (all walk-forward folds from iter 1 with corrected feature parquets)
- Ban-ei models (same)
- JRA models: **no retrain needed** — jvd_um was already correct

Expected gain: unknown until measured. Pedigree affinity (sire × track
condition) is a known strong signal in NAR racing where horse quality
differentiation is lower than JRA. The gain could be meaningful (+0.5–2pp
place2/place3 for NAR) but must be confirmed empirically via walk-forward eval.

---

## Files Changed

- `src/scripts/finish-position-features/pedigree_staging.py` (new)
- `src/scripts/finish-position-features/add-near-miss-features.py` — import changed, local `stage_horse_pedigree` removed
- `src/scripts/finish-position-features/add-baba-pedigree-affinity-features.py` — same
- `tests/test_pedigree_staging.py` (new, 6 test cases)
- `pyproject.toml` — added `--cov=pedigree_staging`
