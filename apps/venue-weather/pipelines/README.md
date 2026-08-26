# Venue Weather R2 Catalog Runbook

Venue weather no longer uses D1 as the runtime store.

## Runtime Store

- Latest object: `venue-weather-live/v1/race_date={YYYY-MM-DD}/keibajo_code={code}/{forecast|actual}.json`
- Snapshot object: `venue-weather-snapshots/v1/race_date={YYYY-MM-DD}/keibajo_code={code}/{forecast|actual}/{fetchedAt}.json`
- Catalog staging object: `venue-weather-catalog-staging/v1/race_date={YYYY-MM-DD}/keibajo_code={code}/{forecast|actual}/{fetchedAt}.ndjson`
- KV response cache: `weather:{YYYY-MM-DD}`, TTL 3600s
- Cache API read cache: `/weather/{YYYY-MM-DD}`, TTL 60s

The Worker read path is Cache API -> KV -> R2 list/get -> empty payload.

## Catalog Resources

- R2 bucket: `pc-keiba-venue-weather-archive`
- Pipeline stream: `venue_weather_ingest_stream`
- Runtime stream: `venue_weather_ingest_stream`
- Sink: `venue_weather_hourly_sink`
- Pipeline: `venue_weather_hourly_ingest_pipeline`
- Namespace/table: `weather.venue_weather_hourly`

## V2 humidity / wet-bulb migration

Cloudflare structured-stream schemas and Pipeline SQL are immutable after creation ([stream management](https://developers.cloudflare.com/pipelines/streams/manage-streams/), [pipeline management](https://developers.cloudflare.com/pipelines/pipelines/manage-pipelines/)), so v2 uses isolated resources rather than mutating v1:

- Stream: `venue_weather_ingest_stream_v2` (`e031c664c15249f7ba1d8d3bf0376290`)
- Sink: `venue_weather_hourly_v2_sink` (`79c55459e1af4e36a2c28ee831d1537a`)
- Pipeline: `venue_weather_hourly_ingest_pipeline_v2` (`f42853b3fd5f443990cfad6eb420481a`)
- Catalog table: `weather.venue_weather_hourly_v2`
- Runtime prefixes: `venue-weather-live/v2` and `venue-weather-snapshots/v2`

V1 remains the mandatory serving source. A v2 fetch/write failure is caught only after the v1 upsert succeeds, so it cannot remove current weather availability. The HTTP read path enriches v1 only when the v2 venue-hour keys exactly match the complete v1 response; partial v2 data falls back to byte-compatible v1 fields. Missing v2 fields mean “v2 unavailable”; they must never be coerced to zero humidity, wet-bulb temperature, dew point, or radiation.

## Historical Backfill

V2 uses a separate, secret-protected Worker endpoint backed by the v2 Pipeline binding. The tool validates every selected venue-date as exactly 24 complete hours before sending anything, writes an atomic checkpoint only after a successful batch, and can resume deterministically.

```sh
VENUE_WEATHER_V2_BACKFILL_TOKEN=... bun run --filter venue-weather backfill:r2-catalog:v2 -- \
  --data-dir apps/venue-weather/data \
  --from-date 2025-01-01 \
  --to-date 2025-01-01 \
  --checkpoint /secure/path/venue-weather-v2.checkpoint.json
```

Bounded v2 closure check (2026-08-25): backfill date `2025-01-01` produced exactly 600 R2 SQL rows, 25 venues, 24 hours (00–23), 600 unique venue-hour keys, and 600 non-null values for each of relative humidity, dew point, wet-bulb temperature, and shortwave radiation. V1 was not modified.

Use Pipeline HTTP ingest for v1 historical catalog migration. Runtime Worker writes still use the Pipeline binding plus R2 latest objects.

```sh
bun run --filter venue-weather backfill:r2-catalog -- \
  --data-dir apps/venue-weather/data \
  --pipeline-url https://9171219290d34ec1b67e17371a726ff0.ingest.cloudflare.com
```
