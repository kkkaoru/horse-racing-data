# 09:09 JRA weight window — who looks at what

Read-only. First likely JRA publish ~09:09 (04/01 posts 09:40).
Not a guarantee. If still empty at 09:20, say so; do not invent fire.

Do not deploy. Do not PUT. Do not POST `/run`.

## Split

| who                       | owns                                                                                                                  | does not own                    |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------- | ------------------------------- |
| **pi-fix-developer**      | (1) `jvd_se.bataiju` landed? (2) `weight-rescore-trigger` in D1? (3) Neon `prediction_generated_at` + 80/940 coverage | cache schema / rank permutation |
| **pi-optimize-developer** | feat-cache HIT vs MISS, rank / pedigree quality on the object scored                                                  | D1 trigger counts               |
| **advisor**               | go / no-go if overwrite looks worse than the 05:04 host rows                                                          | —                               |

## 1. Did weights land? (fix)

```sql
SELECT keibajo_code, race_bango, COUNT(*) AS n,
       COUNT(*) FILTER (
         WHERE NULLIF(TRIM(bataiju), '') IS NOT NULL
           AND TRIM(bataiju) NOT IN ('', '000')
       ) AS with_weight
FROM jvd_se
WHERE kaisai_nen = '2026' AND kaisai_tsukihi = '0816'
  AND keibajo_code IN ('01', '04', '07')
GROUP BY 1, 2
ORDER BY 1, 2;
```

08:00 local-PG was 0/500. First interesting row is `04/01`.

## 2. Did the trigger fire? (fix)

Remote D1 `sync-realtime-data` (`d12ebd45-…`):

```sql
SELECT race_key, status, message, created_at
FROM fetch_logs
WHERE job_type = 'weight-rescore-trigger'
  AND created_at >= '2026-08-16 09:00:00+09:00'
ORDER BY created_at;
```

Pair with `job_type='fetch-weights' AND status='ok'` for the same
`race_key`. Baseline `weight-rescore-observe-plan-20260816.md`:
**no 0816 trigger at 08:02**; `fetch-weights` was still
`skip:weights-empty`. Fire only after a real write.

## 3. Cache HIT / MISS (optimize)

HEAD `feat-cache/catalog-v1/jra/20260816/{keibajo}/{race}/features.parquet`.
Compare Last-Modified to the rescore stamp. Seed / quality notes:
`feat-cache-healthy-seed-0816.md`, `feat-cache-0412-provenance-20260816.md`.
Do not PUT a replacement during the window.

## 4. Neon clock and ranks

**generated_at + coverage (fix):**

```sql
SELECT keibajo_code, race_bango, COUNT(*) AS n,
       MIN(prediction_generated_at) AS gen,
       MIN(model_version) AS model
FROM race_finish_position_model_predictions
WHERE kaisai_nen = '2026' AND kaisai_tsukihi = '0816'
  AND keibajo_code IN ('01', '04', '07')
GROUP BY 1, 2
ORDER BY 1, 2;
```

Controls still on the 05:04 host write at 08:02: **`04/04`**, **`01/01`**.
Already dirty: `04/01` 07:07, `04/02` 06:33, `04/03` 06:14.
Coverage must stay **80 / 940**. A new stamp without a matching
trigger row is an unexplained overwrite.

**Ranks / pedigree on the scored vector (optimize):** compare HIT
parquet vs the 05:04 host body. Report permutation, not a vibe.

## 5. Baseline files

| file                                      | what                                         |
| ----------------------------------------- | -------------------------------------------- |
| `weight-rescore-observe-plan-20260816.md` | Neon stamps + D1 at 08:02 (fix)              |
| `nar-banei-weight-window-20260816.md`     | NAR 12:04 / Ban-ei 13:54; 44/44 HIT at 07:49 |
| `feat-cache-healthy-seed-0816.md`         | later seed inventory (optimize)              |
| `overnight-fp-index.md`                   | symptom table                                |
