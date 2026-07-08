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

In production this repo runs the server under **launchd supervision**
(`KeepAlive`/`RunAtLoad`), not as a manually-started background process --
see "Ops: launchd supervision" below. Manual start/stop remains available for
local development or ad-hoc debugging:

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
`launchctl bootstrap`) is a separate, explicit operator step (see below).
Manual `start`/`stop` and the launchd-supervised process both use the exact
same PID-file/log-file paths under `HORSE_RACING_MLFLOW_DATA_DIR`, so do not
run both at once against the same data dir -- `start` refuses to launch a
second copy while a PID file names a live process, but stopping the
launchd-managed one requires `launchctl`, not `cli.py stop` (`KeepAlive`
would just restart it).

## Ops: launchd supervision

```sh
cd apps/mlflow-ui
uv run python -m mlflow_ui.cli plist --output ~/Library/LaunchAgents/com.horse-racing.mlflow-ui.plist
chmod 600 ~/Library/LaunchAgents/com.horse-racing.mlflow-ui.plist  # embeds the backend DSN and R2 credentials
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.horse-racing.mlflow-ui.plist

# status / logs
launchctl print gui/$(id -u)/com.horse-racing.mlflow-ui | grep -E "state|pid"
tail -f "$(uv run python -c 'from mlflow_ui.config import load_config; print(load_config().data_dir)')"/mlflow-ui-launchd.*.log

# stop supervision entirely (KeepAlive means a plain `stop` won't hold)
launchctl bootout gui/$(id -u)/com.horse-racing.mlflow-ui
```

The generated plist's `ProgramArguments` invoke `uv run --project <this dir>
python -m mlflow_ui.cli foreground` with the default `uv_path="uv"` --
launchd's GUI-domain agents get a minimal `PATH` that does **not** include
Homebrew's `/opt/homebrew/bin`, so when installing this for real, generate
the plist with an explicit absolute `uv` path (`generate_plist(cfg,
uv_path=...)` via the Python API, or add `PATH` to the plist's
`EnvironmentVariables`) rather than relying on the bare-name default -- a
bare `uv` will fail to resolve under launchd and crash-loop under
`KeepAlive`. `foreground` execs `mlflow server` directly (`os.execvpe`, no
wrapper subprocess left running), so `KeepAlive` supervises the real server
process, not a shell around it.

## Security

`mlflow server` has **no built-in authentication**. `HORSE_RACING_MLFLOW_UI_HOST`
must stay `127.0.0.1` unless the operator has explicitly accepted the risk of
exposing the tracking UI (and any artifacts it can serve) beyond localhost.

The backend store URI (which may embed a plaintext database password, e.g. a
Neon Postgres DSN) and the artifacts destination are **never** passed as
`mlflow server` argv flags -- a process's argv is visible to every local user
via `ps`, regardless of which user owns the process. Instead
`server.build_command()` omits `--backend-store-uri`/`--artifacts-destination`
entirely, and `config.server_env()` injects the equivalent
`MLFLOW_BACKEND_STORE_URI`/`MLFLOW_ARTIFACTS_DESTINATION` env vars that
mlflow's CLI accepts as click `envvar=` aliases for the same two options.
Both `start()` (background subprocess) and `run_foreground()` (launchd exec)
always merge `server_env()` into the child's environment. Verify with `ps
-p <pid> -o command=` on the actual `mlflow server` process (not the `uv
run`/`python -m mlflow_ui.cli foreground` wrapper) -- it should show only
`--host`/`--port`, never a URI.

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

## Neon cost hygiene

`mlflow server` always launches with `MLFLOW_SERVER_ENABLE_JOB_EXECUTION=false`
injected into its subprocess environment, regardless of artifacts mode. This
disables mlflow's huey-based background job schedulers
(`online_scoring_scheduler` / `trace_archival_scheduler`), which otherwise
poll the backend database every minute and prevent a Neon serverless backend
from ever auto-suspending. This repo does not use mlflow's GenAI
online-scoring or trace-archival features, so disabling the subsystem has no
functional cost. An operator who sets `MLFLOW_SERVER_ENABLE_JOB_EXECUTION`
themselves before starting the server is never overridden.
