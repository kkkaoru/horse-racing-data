# mlflow-ui

Launcher/operator for the MLflow tracking UI server (`mlflow server`), used to
browse and manage model runs for this repo's finish-position and
running-style prediction models.

This package only starts/stops/supervises the `mlflow server` process. It
does not contain the MLflow client library used by training code -- that
lives in the sibling `apps/mlflow` package, which owns the sqlite backend
store (`apps/mlflow/data/mlflow.db`) and local artifact store
(`apps/mlflow/data/mlartifacts/`) that this launcher points `mlflow server`
at by default.

## Run

```sh
cd apps/mlflow-ui
uv run python -m mlflow_ui.cli start      # start mlflow server in the background
uv run python -m mlflow_ui.cli status     # check whether it's running
uv run python -m mlflow_ui.cli stop       # stop it (SIGTERM, PID-file based)
uv run python -m mlflow_ui.cli foreground # run in the foreground (for launchd)
uv run python -m mlflow_ui.cli plist      # print a launchd LaunchAgent plist
uv run python -m mlflow_ui.cli plist --output ~/Library/LaunchAgents/com.horse-racing.mlflow-ui.plist
```

`plist` only ever generates plist text -- it never installs the file into
`~/Library/LaunchAgents` on its own. Copying it there (and running
`launchctl load`/`bootstrap`) is a separate, explicit operator step.

## Security

`mlflow server` has **no built-in authentication**. `HORSE_RACING_MLFLOW_UI_HOST`
must stay `127.0.0.1` unless the operator has explicitly accepted the risk of
exposing the tracking UI (and any artifacts it can serve) beyond localhost.

## Environment variables

| Variable                                           | Default                           | Purpose                                                                                                                                                                                                                                                                   |
| -------------------------------------------------- | --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `HORSE_RACING_MLFLOW_DATA_DIR`                     | `<repo_root>/apps/mlflow/data`    | Directory holding the sqlite DB, local artifacts, PID file, and log file.                                                                                                                                                                                                 |
| `HORSE_RACING_MLFLOW_BACKEND_URI`                  | `sqlite:////<data_dir>/mlflow.db` | Full backend store URI override. Deliberately not `MLFLOW_TRACKING_URI` -- that env var is read by MLflow's _client_ SDK for a different purpose (pointing a client at a tracking server), and reusing it for the _server's_ own DB connection string would be ambiguous. |
| `HORSE_RACING_MLFLOW_ARTIFACTS_MODE`               | `local`                           | `local` or `r2`.                                                                                                                                                                                                                                                          |
| `HORSE_RACING_MLFLOW_R2_BUCKET`                    | (none)                            | Required when mode is `r2`.                                                                                                                                                                                                                                               |
| `HORSE_RACING_MLFLOW_R2_PREFIX`                    | `mlflow`                          | Key prefix within the R2 bucket.                                                                                                                                                                                                                                          |
| `HORSE_RACING_MLFLOW_UI_HOST`                      | `127.0.0.1`                       | Never default to `0.0.0.0`; see Security above.                                                                                                                                                                                                                           |
| `HORSE_RACING_MLFLOW_UI_PORT`                      | `5252`                            | Not `5000` -- macOS Control Center's AirPlay Receiver squats that port on modern macOS.                                                                                                                                                                                   |
| `R2_ACCOUNT_ID` (fallback `CLOUDFLARE_ACCOUNT_ID`) | (none)                            | R2 account id, used to build `https://<account_id>.r2.cloudflarestorage.com`. Required when mode is `r2`.                                                                                                                                                                 |
| `R2_ACCESS_KEY_ID`                                 | (none)                            | Required when mode is `r2`.                                                                                                                                                                                                                                               |
| `R2_SECRET_ACCESS_KEY`                             | (none)                            | Required when mode is `r2`.                                                                                                                                                                                                                                               |

All `HORSE_RACING_MLFLOW_*` and R2 vars are repo-wide-precedent names shared
with (or namespaced against) other packages -- see `apps/finish-position-predict-container`
and `apps/pc-keiba-viewer` for the shared `R2_*` credential names, and note
that bucket-name env vars are intentionally package-specific
(`HORSE_RACING_MLFLOW_R2_BUCKET`) to avoid collisions with other apps'
`R2_BUCKET`/`R2_BUCKET_NAME` vars in a shared shell.

Missing R2 configuration when `HORSE_RACING_MLFLOW_ARTIFACTS_MODE=r2` is
detected at config-load time (fails fast, before any subprocess is
launched), naming exactly which variable(s) are missing.
