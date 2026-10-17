# finish-position-clef-rerank

Race-day rerank of the served finish-position prediction with Cloudflare-hosted
[clef-flash](https://huggingface.co/Cloudflare/clef-flash) (`@cf/cloudflare/clef-flash`).

## Flow

1. A training host (this Mac) builds one bundle per race and uploads it to
   `DETAIL_SECTION_CACHE_KV` as `clefrr:v1:bundle:{YYYYMMDD}:{venue}:{race}`. The day's
   list goes in `clefrr:v1:index:{YYYYMMDD}`. A bundle holds:
   - the fusion weights;
   - a point-in-time feature-ranker score for each runner, covering heatmap-equivalent
     partnerships (horse, horse×jockey, jockey×venue, jockey×trainer×venue, owner×venue,
     jockey×trainer×owner, gate) and pedigree combinations (sire×damsire, sire line×damsire,
     sire×dam's damsire, each also by surface/distance band, venue and going);
   - the anonymized clef-flash System One request, with no names, dates, odds or popularity.
2. The Worker reads the served prediction (`pred:fp:v1:...`) and injects its ranks into the
   request. It then calls clef-flash and fuses the within-race z-scores:
   `base·z(served rank) + feature·z(feature score) + clefTop3·z(logit P(top3)) + clefWin·z(log P(win))`.
   The result is stored in `clefrr:v1:result:...`. A cached result is reused until the
   served prediction is regenerated, for example by the post-weight rescore.
3. A cron trigger warms every indexed race every 10 minutes from 08:00 to 17:50 JST. The
   viewer reads `GET /api/clef-rerank/{YYYYMMDD}/{venue}/{race}` and hides the panel when
   the Worker answers 404 or fails.

`POST /api/clef-rerank/probe` forwards one System One request to the hosted model. Use it
for local-vs-hosted parity checks. Like every route here, it sits behind Cloudflare Access.

## Commands

```sh
bun run --filter finish-position-clef-rerank check
cd apps/finish-position-clef-rerank && bunx wrangler deploy
bunx wrangler kv bulk put <bulk.json> --namespace-id a013fdf6e7224c3ab0c227be3c899073 --remote
```
