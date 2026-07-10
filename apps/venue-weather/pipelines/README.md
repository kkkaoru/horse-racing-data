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

## Historical Backfill

Use Pipeline HTTP ingest for historical catalog migration. Runtime Worker writes still use the Pipeline binding plus R2 latest objects.

```sh
bun run --filter venue-weather backfill:r2-catalog -- \
  --data-dir apps/venue-weather/data \
  --pipeline-url https://9171219290d34ec1b67e17371a726ff0.ingest.cloudflare.com
```
