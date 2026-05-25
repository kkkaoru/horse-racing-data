# WIN5 overlay daily generation

`generate-win5-overlay.ts` produces a per-WIN5-day `rs-overlay` variant of the
production WIN5 model (`win5-xgb-v7-lineage-v1`) and upserts it into
`race_finish_position_model_predictions`. The viewer's
`buildModelScoreLookupFromPool` picks up the overlay automatically when its
rows exist, otherwise falls back to the base `win5-xgb-v7-lineage-v1` rows.

## Manual run

```sh
cd apps/pc-keiba-viewer
DATABASE_URL_LOCAL="postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing" \
  bun run src/scripts/generate-win5-overlay.ts [--date YYYYMMDD] [--force]
```

- `--date` defaults to today (JST). Use `--date 20260524` to backfill a specific
  day.
- `--force` re-runs the pipeline even if an overlay row already exists for the
  date.

The script is idempotent: it short-circuits when `jvd_wf` (or `jvd_ra`
fallback) shows no WIN5 races for the date, or when the overlay rows have
already been imported.

## Pipeline stages

1. `bun run src/scripts/build-corner-feature-table.ts` — refreshes the last
   14 days of `race_entry_corner_features`.
2. `python src/scripts/finish_position_features_duckdb.py` — rebuilds the
   base finish-position parquet through the target date.
3. `python src/scripts/finish-position-features/add-grade-race-lineage-features.py`
   — applies the v7 lineage layer (the column set my XGB model was trained on).
4. `python src/scripts/finish_position_xgboost_predict_only.py` — trains on
   2007-01-01 through `<date-1>`, predicts the target date.
5. `bun run src/scripts/finish-position-features/import-finish-position-predictions.ts`
   — upserts under `model_version = win5-xgb-v7-lineage-v1-rs-overlay-<YYYYMMDD>`.

Total runtime: 10-15 minutes (dominated by base-feature rebuild + XGB training).

## macOS scheduling (launchd)

A reference plist lives at `scripts/com.kkkaoru.win5-overlay.plist`. It runs
the overlay generator every Saturday and Sunday at 09:00 JST.

```sh
# Install (one-time)
cp apps/pc-keiba-viewer/scripts/com.kkkaoru.win5-overlay.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.kkkaoru.win5-overlay.plist

# Inspect
launchctl list | grep win5-overlay
tail -f /tmp/win5-overlay-stderr.log

# Unload
launchctl unload ~/Library/LaunchAgents/com.kkkaoru.win5-overlay.plist
```

The plist hard-codes the repo path. Edit it if your checkout lives elsewhere.

## Linux scheduling (cron)

Equivalent crontab entry (run as the user that owns the repo + Postgres):

```cron
0 0 * * 6,0 cd /path/to/horse-racing-data/apps/pc-keiba-viewer && \
  DATABASE_URL_LOCAL="postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing" \
  /usr/local/bin/bun run src/scripts/generate-win5-overlay.ts \
  >> /var/log/win5-overlay.log 2>&1
```

(`0 0 * * 6,0` UTC = 09:00 JST on Sat/Sun.)

## Verifying after a run

```sql
select kaisai_nen, kaisai_tsukihi, count(*) as rows,
       count(distinct (keibajo_code, race_bango)) as races
from race_finish_position_model_predictions
where model_version like 'win5-xgb-v7-lineage-v1-rs-overlay-%'
group by kaisai_nen, kaisai_tsukihi
order by kaisai_nen desc, kaisai_tsukihi desc
limit 5;
```

The viewer at `/win5/<year>/<month>/<day>` automatically prefers the overlay
when present (see `buildModelScoreLookupFromPool`).
