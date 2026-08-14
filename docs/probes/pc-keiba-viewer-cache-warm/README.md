# PC Keiba Viewer race-detail cache warm

Reusable low-concurrency warm-up for a complete race date. It exists to avoid a
first user seeing the section route's established cold-compute fallback
(`503 section_unavailable`, `Retry-After: 30`) when no fresh or stale cache entry
exists.

## Safety gate

Do **not** run before finish-position generation is complete. The script first
fetches every race's `finish-prediction` section and aborts before warming pages
or other sections unless every race has runners and non-empty
`modelPredictionFeatures`. The section route itself does not cache an empty
model-feature payload.

The operator must still get a completion GO from the prediction recovery owner.
The preflight protects against a mistaken GO; it does not replace ownership.

## Usage

From the repository root:

```sh
EXPECTED_RACE_COUNT=68 CONCURRENCY=2 \
  bash docs/probes/pc-keiba-viewer-cache-warm/warm-race-detail-cache.sh 20260815
```

The script reads Cloudflare Access credentials from
`apps/pc-keiba-viewer/.env.local`. Override `VIEWER_ENV_FILE` or `VIEWER_ORIGIN`
when needed. It discovers race URLs from the date page instead of hard-coding
venues/race numbers.

Warm targets per race:

- race-detail page;
- `finish-prediction` during the readiness preflight;
- `overall-score`;
- `pace-prediction`;
- `similar`;
- `bloodline`;
- `time-score`;
- `premium-data-top`.

`CONCURRENCY` defaults to 2. Keep it low: this is cache preparation, not a load
test. Curl uses bounded timeouts and three retries, including `503` responses.

Artifacts are written under `tmp/race-detail-cache-warm/YYYYMMDD/`:

- discovered race list;
- preflight and warmed JSON bodies;
- rendered page bodies;
- `results.tsv` and `failures.tsv`.

Success requires the discovered count (when `EXPECTED_RACE_COUNT` is set),
preflight count, and completed warm count to agree with no failed endpoint.
