# NAR 12:04 observation (prep 10:21 JST)

Cause of the JRA miss is **unknown**. Stop new JRA hypotheses.
NAR is the next structural test: same rescore wait path, no JRA
`rec INNER JOIN jra_um` pedigree death.

## (1) Baselines already on disk

Saved 09:10 JST, commit `3e41ca7f`. Files under
`docs/probes/finish-position-recovery-20260816/`:

| race      |   n | Neon gen (UTC) | TSV                                              |
| --------- | --: | -------------- | ------------------------------------------------ |
| **35/01** |   9 | 20:31:18Z      | `neon-nar-3501-ranks-before-weight-20260816.tsv` |
| 35/06     |  11 | 20:31:20Z      | `neon-nar-3506-…`                                |
| 44/01     |  14 | 20:31:24Z      | `neon-nar-4401-…`                                |
| 44/06     |  13 | 20:31:27Z      | `neon-nar-4406-…`                                |
| 55/01     |  11 | 20:31:30Z      | `neon-nar-5501-…`                                |
| 55/06     |  12 | 20:31:32Z      | `neon-nar-5506-…`                                |

35/01 model `iter12-nar-xgb-hpo-v8-stage1-marketfree-184`, every
`odds_score=0.5048`. Contrast 35/04 is **not** a baseline (already
07:16:43 overwrite).

## (2) What to look at around 12:04

Same two facts as JRA, with the 10:13 correction:

| #   | look                                                 | means                     | does **not** mean     |
| --- | ---------------------------------------------------- | ------------------------- | --------------------- |
| A   | R2 HEAD Last-Modified vs 07:49 seed                  | PUT after seed, or not    | HIT / MISS / “unread” |
| B   | Neon `count`, `min/max/distinct generated_at` vs TSV | successful UPSERT, or not | why                   |

Add only what is visible without logs:

| #   | look                                                                                         | when it helps                                                                                                           |
| --- | -------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| C   | Neon ranks / `odds_score` vs TSV                                                             | **only if** B moved. One `generated_at` that changes both weight-ish ranks and odds is **not** separable                |
| C2  | On a landing: is `odds_score` **per-horse** (min≠max) or still the morning constant?         | 01/02 was 0.11–0.65 vs R1 0.5664. Spread ⇒ `apply_fresh_snapshots` injected real odds. Flat ⇒ UPSERT without new market |
| D   | D1 `finish_position_predict_retry_errors` for 35/01 (and 44/01, 55/01) after the NAR trigger | consumer `catch` after start. Absence ≠ success                                                                         |
| E   | GraphQL `internalError` in the NAR window (fix-dev)                                          | same unknown as JRA 09:00–09:55; do not attribute without a matching race                                               |
| F   | `bunx wrangler containers list` **once at NAR weight trigger** (read-only)                   | LIVE INSTANCES vs max 10, same clock as landing yes/no. JRA 9/10 was **after** 09:37, not at 09:10                      |

Do **not** infer HIT from A. GET does not rewrite the object.

Fill `weight-rescore-missed-post-table-20260816.md` with A + B + in-time.

## (3) Branch that changes tomorrow

Same queue, same held `mode=rescore`, NAR pedigree is already live.

- **NAR UPSERT before 12:04** (B moves) → JRA-specific (pedigree / JRA
  data / JRA card), not “rescore never finishes”.
- **NAR also no UPSERT** → category-independent (queue / consumer /
  held request / something else). Tomorrow’s fix is not “JRA pedigree
  first”.

Ban-ei 13:54 is the third copy of the same branch (`83/01` TSV already
saved).

Pre-window snapshot **11:30:46 JST** (not the trigger clock): predict
container `LIVE INSTANCES 9`, `LAST MODIFIED` 00:37:10Z = 09:37 JST.
Take F again when NAR weights actually fire.
