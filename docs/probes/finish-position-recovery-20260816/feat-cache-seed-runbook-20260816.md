# Replay: seed healthy per-race feat-cache from a full-day host generate

Use this instead of tonight’s trial-and-error. Goal: a later scoped
`mode=rescore` **HITs** a live-pedigree parquet so it does not rebuild
with `LAYER_CHAIN` + `--target-race` (that path zeros JRA’s five
sire/damsire components).

Do **not** treat this as 8/17+ automation. That still needs the ordered
image + PREWARM + `DAY_BASE_SPLIT` list in
`macless-automation-checklist-20260816.md`.

## 1. Build the per-race objects

**Material:** the **last layer of a full-day host generate**
(`target_race=None`), not `feat-*-v7-final`.

| category | tonight’s file                                                                       | rows | cols (after hive) |
| -------- | ------------------------------------------------------------------------------------ | ---: | ----------------: |
| jra      | `/tmp/predict-upcoming/feat-jra-layer-16/.../data_0.parquet`                         |  490 |               390 |
| nar      | `/tmp/predict-upcoming/feat-nar-v7-final/...` (NAR’s last layer; name is misleading) |  333 |               327 |
| ban-ei   | `/tmp/predict-upcoming/feat-ban-ei-v7-final/...`                                     |  117 |               271 |

`feat-jra-v7-final` on 8/16 was a **1×8** leftover / rescore snapshot.
Do not seed from it.

**Split:** same as production `_split_parquet_by_race`:

```sql
COPY (
  SELECT * FROM read_parquet($path, hive_partitioning = false)
  WHERE race_id = $race_id
) TO $dest (FORMAT PARQUET)
```

`hive_partitioning=false` so `race_year` is not a physical column.
Production HIT objects are 390/327/271 cols without `race_year`.

**dtype:** production HIT uses numpy `int32`/`int64`; host pandas often
has nullable `Int32`/`Int64`. DuckDB COPY of the host layer is enough
for the reader (`pd.read_parquet` + 250 model names). Do **not** force
cast NA integer cols to numpy int (`course_full_gate_count` can be all
NA). Tonight: 04/01 after COPY was 13×390, reader missing-features **0**.

**PUT:** `bunx wrangler r2 object put --remote --force` from
`apps/finish-position-cron` into bucket `pc-keiba-features-archive`,
key `feat-cache/catalog-v1/{cat}/{YYYYMMDD}/{keibajo}/{bango}/features.parquet`.

Container / host SigV4 PUT is **403** (read-only token; `41adee27`
removed that path). Worker `FEATURES_CACHE.put` is the other writer;
it only sees NDJSON / focused-full pickup, not a local file.

Script used tonight:
`docs/probes/finish-position-recovery-20260816/seed-healthy-feat-cache-0816.py`
(slice) + `put-healthy-feat-cache-0816.sh` (wrangler).

## 2. Checks before trusting a seed (do these)

On **one** race (tonight: 04/01), same functions as catalog rescore:

1. `_fetch_watermarked_per_race_cache` (needs `R2_CATALOG_*` or the
   entry-list check fail-closes even if GET succeeded).
2. Rows / one `race_id` / col count match the slice (13 / `jra:…:04:01` / 390).
3. `_load_model_metadata` names: **missing 0**.
4. `apply_fresh_snapshots` can write `weight_diff_from_avg` when given a
   synthetic bataiju.
5. `score_races` returns one row per horse.

Record: `seed-cache-reader-verify-0401-20260816.md`.

`tansho_ninkijun` may be absent (`tansho_ninkijun_1` NaN) on a morning
full-day file. That is missing odds, not a corrupt parquet.

## 3. Do not

- Seed from `feat-jra-v7-final` without counting rows/cols.
- PUT without the reader check above.
- DELETE / TRUNCATE R2 or Neon. A bad object can only be **overwritten**.
- Use layer-16 vs an old production HIT **values** as a “must match”
  gate. On 8/16 the old HIT was the **dead** `--target-race` vector;
  the host full-day file was the healthy one.
- Enable `DAY_BASE_SPLIT_ENABLED` on the same night as the first image
  deploy (`macless-automation-checklist-20260816.md`).

## 4. Keep a copy of existing HITs before overwrite

Tonight the only pre-seed JRA HITs were 04/12 and 07/01 (dead pedigree).
Copies:

`docs/probes/finish-position-recovery-20260816/r2-hit-before-overwrite-0816/jra-04-12.parquet`
`docs/probes/finish-position-recovery-20260816/r2-hit-before-overwrite-0816/jra-07-01.parquet`

Without those, “what production used to serve” is gone.

## 5. After seed (8/16 specific)

80/80 keys under `feat-cache/catalog-v1/{jra,nar,ban-ei}/20260816/…`.
Weight rescore should HIT and overlay odds/bataiju. Queue wait is
unchanged. Observe **04/01 and 07/01** ranks (04/01 Neon was overwritten
at 07:07:10Z). Baselines: `neon-*-ranks-before-weight-20260816.tsv`.
