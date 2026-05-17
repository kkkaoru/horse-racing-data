# R2 Model Upload

Local development helpers for uploading large model files to Cloudflare R2.

Wrangler's `r2 object put` command cannot upload objects larger than 300 MiB. For local development, prefer chunk uploads. The browser still downloads the model from a single application URL; the app route joins the versioned R2 chunks at read time.

```sh
R2_UPLOAD_FILE=/path/to/gemma-4-E2B-it-web.task \
R2_UPLOAD_KEY=models/gemma-4-e2b/v20260518/gemma-4-E2B-it-web.task \
  bun local-dev/r2-model-upload/upload-model-chunks.mjs
```

Optional environment variables:

- `R2_UPLOAD_BUCKET`: defaults to `pc-keiba-finish-position-models`
- `R2_UPLOAD_CHUNK_SIZE`: defaults to 128 MiB and must be at most 300 MiB
- `R2_UPLOAD_WRANGLER_CONFIG`: defaults to `wrangler.jsonc`

The script uploads:

- `models/gemma-4-e2b/<version>/gemma-4-E2B-it-web.task.chunks/<index>`
- `models/gemma-4-e2b/<version>/gemma-4-E2B-it-web.task.manifest.json`

The older multipart helper is kept here for cases where a temporary remote dev Worker can accept the file body without timing out.

```sh
UPLOAD_TOKEN=$(openssl rand -hex 32)
R2_UPLOAD_TOKEN="$UPLOAD_TOKEN" \
  bunx wrangler dev local-dev/r2-model-upload/worker.js \
  --remote \
  --config local-dev/r2-model-upload/wrangler.jsonc \
  --port 8788 \
  --var UPLOAD_TOKEN:$UPLOAD_TOKEN \
  --show-interactive-dev-session=false
```

In another shell:

```sh
R2_UPLOAD_TOKEN="$UPLOAD_TOKEN" \
R2_UPLOAD_FILE=/path/to/gemma-4-E2B-it-web.task \
R2_UPLOAD_KEY=models/gemma-4-e2b/v20260518/gemma-4-E2B-it-web.task \
  bun local-dev/r2-model-upload/upload-model.mjs
```

The default multipart chunk size is 5 MiB, which is the R2 multipart minimum and avoids remote dev request timeouts.
