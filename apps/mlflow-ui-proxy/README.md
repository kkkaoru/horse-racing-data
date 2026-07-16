# mlflow-ui-proxy

Cloudflare Worker + Container deployment for the production MLflow UI and
production-prediction preview sync.

The Worker is the public Basic-auth gate. Authenticated UI/API requests are
forwarded to an MLflow server running in the bound Cloudflare Container. The
same Container reads production prediction rows from racing Neon and writes
run tags/metrics to the MLflow Neon backend on a Cloudflare Cron schedule.
Neither path depends on a Mac process, launchd, or Cloudflare Tunnel.

## Resources

- Worker: `mlflow-ui-proxy`
- Container Durable Object: `MlflowContainer`
- MLflow backend: Neon PostgreSQL
- Artifact store: Cloudflare R2 bucket `mlflow-artifacts`, prefix `mlflow`
- Preview sync: every 10 minutes during JST 02:00-21:59
- Sync range: current JST date through two days ahead

The preview sync records genuinely served finish-position and running-style
prediction runs as soon as they exist in racing Neon. It does not read local
PostgreSQL, emit result-based evaluations, or upload prediction artifacts.

## Configuration

Non-secret values are defined in `wrangler.jsonc`.

| Name                              | Kind   | Purpose                             |
| --------------------------------- | ------ | ----------------------------------- |
| `MLFLOW_UI_USERNAME`              | secret | Basic-auth username                 |
| `MLFLOW_UI_PASSWORD`              | secret | Basic-auth password                 |
| `HORSE_RACING_MLFLOW_BACKEND_URI` | secret | MLflow Neon PostgreSQL DSN          |
| `NEON_PRIMARY_URL`                | secret | Racing Neon PostgreSQL DSN          |
| `R2_ACCOUNT_ID`                   | secret | Cloudflare account ID for R2 S3 API |
| `R2_ACCESS_KEY_ID`                | secret | R2 S3 access key                    |
| `R2_SECRET_ACCESS_KEY`            | secret | R2 S3 secret key                    |
| `HORSE_RACING_MLFLOW_R2_BUCKET`   | var    | MLflow artifact bucket              |
| `HORSE_RACING_MLFLOW_R2_PREFIX`   | var    | Artifact key prefix                 |

Set secrets without committing their values:

```sh
bunx wrangler secret put MLFLOW_UI_USERNAME --config apps/mlflow-ui-proxy/wrangler.jsonc
bunx wrangler secret put MLFLOW_UI_PASSWORD --config apps/mlflow-ui-proxy/wrangler.jsonc
bunx wrangler secret put HORSE_RACING_MLFLOW_BACKEND_URI --config apps/mlflow-ui-proxy/wrangler.jsonc
bunx wrangler secret put NEON_PRIMARY_URL --config apps/mlflow-ui-proxy/wrangler.jsonc
bunx wrangler secret put R2_ACCOUNT_ID --config apps/mlflow-ui-proxy/wrangler.jsonc
bunx wrangler secret put R2_ACCESS_KEY_ID --config apps/mlflow-ui-proxy/wrangler.jsonc
bunx wrangler secret put R2_SECRET_ACCESS_KEY --config apps/mlflow-ui-proxy/wrangler.jsonc
```

## Verify And Deploy

```sh
bun run --filter mlflow-ui-proxy check
bun run --filter mlflow-ui-proxy deploy
```

`check` runs TypeScript checking, lint, formatting verification, and Vitest
coverage. Lines, branches, functions, and statements must all remain at or
above 95%.

## Request Handling

- Every public UI/API request requires valid Basic auth.
- Paths under `/__internal/` return 404 and are never proxied.
- Browser `Origin` and `Referer` headers are removed before forwarding because
  MLflow rejects the Worker's public origin as cross-origin.
- Hop-by-hop, host, and client authorization headers are not forwarded to the
  Container.
- `MLFLOW_SERVER_ENABLE_JOB_EXECUTION=false` disables unused MLflow background
  schedulers that would otherwise keep Neon active.
