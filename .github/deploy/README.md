# Production delivery

`Production` runs on updates to `main`. It also supports manual selection of one service or all services, with a validation-only option. The production GitHub environment is restricted to `main` and holds deployment credentials.

The workflow compares against the last successful automatic run, including changes from failed or superseded runs. A complete NUL-delimited Git diff covers deletions, renames, and large pushes. Shared packages and lockfile changes select all services; cross-workspace container inputs are listed in `plan.py`. Documentation and CI-only changes do not redeploy unchanged applications. All selected services pass checks before any deployment begins. An outdated checkout aborts without becoming a successful deployment baseline. Deployment runs share one concurrency group and are never cancelled in progress.

## Mac dependencies and their replacements

| Existing Mac dependency                             | CI replacement                                                                         |
| --------------------------------------------------- | -------------------------------------------------------------------------------------- |
| Interactive Wrangler OAuth login                    | `production` environment `CLOUDFLARE_API_TOKEN` and account variable                   |
| Bun installation and local dependencies             | Bun 1.3.14 and frozen workspace lockfile                                               |
| Local Python environments                           | uv 0.11.8, Python 3.12, project lockfiles                                              |
| Colima for Wrangler Containers                      | GitHub-hosted Ubuntu Docker daemon, via the existing compatibility wrapper             |
| Gitignored prediction models                        | Private R2 objects addressed by SHA-256, restored and checked before deployment        |
| Private source checkouts                            | Existing pinned `core:prepare` using `PRIVATE_CORE_READ_TOKEN`                         |
| Manually paused prediction queues                   | Existing `finish-position-cron` guarded deployment, including its finally-based resume |
| Runtime Worker secrets                              | Retained in Cloudflare; ordinary deployments do not overwrite them                     |
| macOS Metal training and Windows desktop automation | Remain on compatible hosts; not required for deployment of already trained models      |

## Credentials

GitHub `production` environment:

- `CLOUDFLARE_API_TOKEN`: existing deployment credential; account in `vars.CLOUDFLARE_ACCOUNT_ID`.
- `PRIVATE_CORE_READ_TOKEN`: existing access to the pinned private JV-Link/UmaConn core.
- `FINISH_POSITION_CRON_TRIGGER_TOKEN`: required by prediction drain/stop operations.
- `JRA_VAN_WORKER_API_TOKEN`, `UMMACON_WORKER_API_TOKEN`: private worker smoke tests.
- `R2_ENDPOINT_URL`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `PRODUCTION_ARTIFACT_BUCKET`: immutable model artifact retrieval. Read access suffices on a deployment runner.
- `PC_KEIBA_VIEWER_INTERNAL_TOKEN`: authenticated application diagnostics.

Devin Secrets hold its production diagnostic token, Cloudflare account, application tokens and R2 access. Production deployments use the GitHub workflow's deployment credential. Never copy a Wrangler OAuth cache, a browser login, an entire `.env`, or Devin identifiers into Git. Keep each credential's actual value only in the owning secret store. The knowledge note and playbook are maintained in the authorized Devin organization; their identifiers are not repository configuration.

## Model artifact lifecycle

`models.py` uses the production artifact manifest and existing selector-closure validator to select exactly the finish-position artifacts required by the checked-out code. It downloads `ci-artifacts/sha256/<digest>` from the private artifact bucket, checks size and SHA-256 before replacing a local file, then the existing deployment and Dockerfile verifiers check the complete model tree. It never trains or silently substitutes a model during deployment.

Before merging a model/selector update, publish its selected bytes with a credential that has object-write access:

```sh
export PYTHONPATH=apps/finish-position-predict-container/src
uv run --project apps/pc-keiba-viewer python .github/deploy/models.py \
  --root apps/finish-position-predict-container/models --publish
```

Set the four artifact environment variables from your secret manager. If the S3 credential is read-only, the Mac's authenticated Wrangler can publish each already verified source file to the same digest-addressed key with `bunx wrangler r2 object put ... --file ... --remote`. Do not grant runners object-write access merely to download models. Model promotion still requires the existing evaluation and artifact-manifest checks; uploading an object alone does not activate it.

## Verification and operation

The JV-Link Wine Container additionally requires the private SDK installer. `sdk.py` restores the pinned installer from the same private digest-addressed R2 namespace and checks its byte length and SHA-256. Keep the installer out of Git and update its verified digest/size only when intentionally upgrading the SDK.

Validation-only runs also build the selected Workers, OpenNext application and Container images with Wrangler `--dry-run`. They do not publish versions or alter production queues. Deploying runs perform the same builds before the first production mutation; Docker layers are reused by the subsequent guarded deployment.

Existing prediction regression tests also require historical artifacts listed in `test-artifacts.json`. CI restores these with `--extra-manifest .github/deploy/test-artifacts.json`, using the same byte and digest checks. Publish that manifest's bytes when updating those regression inputs. This supplemental manifest does not change production selectors or activate historical models.

`Production configuration checks` tests service selection, download corruption/network failure, failed validation, stale main, validate-only behavior, deployment ordering, and the guarded prediction deployment. Python helper coverage must remain at least 95%. No production secrets are used by PR checks.

```sh
gh workflow run production.yml --ref main -f target=pipeline-health-monitor -F validate_only=true
gh workflow run production.yml --ref main -f target=pipeline-health-monitor -F validate_only=false
gh run list --workflow production.yml
```

Watch the completed job and Cloudflare deployment history. An accepted workflow dispatch is not deployment success. Private native workers also run their existing smoke tests. For incident diagnosis, use Devin's stored credentials with the documented service APIs; do not print token values. A failed prediction deployment must finish queue cleanup before retrying. Rollbacks are explicit reviewed source changes or the existing Cloudflare rollback procedure, followed by service-specific checks.
