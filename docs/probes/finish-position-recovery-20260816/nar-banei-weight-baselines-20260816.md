# NAR / Ban-ei weight-rescore baselines (saved 09:10 JST)

JRA bataiju still empty (`skip:weights-empty`). If JRA never rescores
today, the same HIT / rank / odds-vs-weight checks move to NAR 12:04 and
Ban-ei 13:54. Baselines taken **now** so a later overwrite cannot erase
them.

Neon has no pedigree columns. Compare `predicted_rank` /
`prediction_generated_at` / `odds_score` only. Cache health is R2
`pedigree_score_for_race` pos/null/zero (NAR/Ban-ei were already live
on production HITs).

## Chosen races

| cat       | R1 (first post) | gen (JST) | contrast | gen (JST) | TSV prefix                  |
| --------- | --------------- | --------- | -------- | --------- | --------------------------- |
| nar 35    | 35/01           | 05:31:18  | 35/06    | 05:31:20  | `neon-nar-3501` / `3506`    |
| nar 44    | 44/01           | 05:31:24  | 44/06    | 05:31:27  | `neon-nar-4401` / `4406`    |
| nar 55    | 55/01           | 05:31:30  | 55/06    | 05:31:32  | `neon-nar-5501` / `5506`    |
| ban-ei 83 | 83/01           | 05:50:12  | 83/06    | 05:50:14  | `neon-ban-ei-8301` / `8306` |

Files: `docs/probes/finish-position-recovery-20260816/neon-*-ranks-before-weight-20260816.tsv`.

**Not used as contrast:** 35/04 gen **07:16:43** (already overwritten
before this snapshot). 55/09 and 55/10 are 05:56 (also later than the
05:31 card).

Observation at the window: `nar-weight-window-checklist-20260816.md`.
Last-Modified = overwrite only. Neon gen = UPSERT only. HIT/MISS is
not stored. NAR success vs JRA miss would point at JRA-specific;
NAR miss would point at a shared structure.
