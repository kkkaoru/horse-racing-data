# Devin integration

Target repository: `kkkaoru/horse-racing-data`.

Select the owner's authorized organization through local Devin configuration. Keep Devin organization identifiers, organization URLs, session identifiers, blueprint identifiers, and build identifiers out of Git-managed files.

These files prepare the repository for Devin. Their presence alone does not connect the GitHub App, activate a blueprint, or prove that a cloud build succeeds.

## Activate and verify

1. In the requested organization's Connections settings, connect the Devin GitHub App and grant access to this repository. Verify the repository appears in that organization.
2. Sync `.devin/blueprint.yaml` as the repository's git-backed blueprint, or import it in the repository blueprint editor. Preserve other repositories and organization settings.
3. Build the environment and inspect the complete build result. The blueprint installs Bun 1.3.14, uv 0.11.8, Python 3.12, workspace dependencies, viewer Python dependencies, and root script dependencies. Validate Linux compatibility of the locked ML dependencies; a local macOS install is not evidence of a working Linux snapshot.
4. Enable repository indexing and verify indexing succeeds for the current default-branch commit. Regenerate DeepWiki after changing `wiki.json`.
5. Enable Devin Review for this repository and verify a review appears on a real PR. Configure autofix only for actionable review/CI feedback and retain the existing branch/review protections.
6. Run a bounded validation task in a fresh cloud session: clone the repository, report the commit, run schema-package tests and the affected package's full checks, then make one useful regression-test improvement in a draft PR. Verify PR creation, checks, and the review integration. Do not report completion from a session-start response alone.

## Useful task recipes

- **Bug fix:** provide the failing input, expected behavior, affected package, and reproduction. Require a failing regression test before the fix, full package checks, and a PR describing the evidence.
- **Coverage improvement:** identify reachable uncovered behavior, add meaningful cases, preserve thresholds and measurement scope, and include the four final metrics.
- **Data freshness incident:** trace source timestamps, ingestion, catalog, prediction, and viewer caches. Include the race identifier and observation time. Require evidence before changing production state.
- **Prediction experiment:** specify cohort, temporal split, baseline artifact, metrics, and compute budget. Prevent future-data leakage; report accuracy and calibration against the same holdout. Preserve production models until the experiment's promotion criteria pass.
- **Dependency maintenance:** scope the task to one dependency family, update with Bun or uv, run affected checks, and report compatibility changes in a PR.

## Additional tools and credentials

The repository's `packages/pc-keiba-viewer-plugin/README.md` documents the viewer MCP and its authentication. Add that MCP in Devin's settings and verify a read-only query after authenticating. Add other external tools only with the access required for the intended workflow; store service credentials in Devin Secrets, not these files.

Windows desktop automation requires a Windows environment; Apple MLX/Metal work requires compatible hardware. Provision a compatible runner before claiming those workflows work in Devin Cloud. Production data services also require their real connection configuration; tests with fixtures verify code behavior, not connectivity.

## Completion evidence

Verify the authorized organization, GitHub repository access, active blueprint/version, successful build, indexed commit, validation session, PR, review result, and any remaining external-service limitations. Keep Devin identifiers and session URLs in Devin or local notes outside the repository. Do not substitute local file validation for these remote checks.

## References

- [Environment setup](https://docs.devin.ai/onboard-devin/environment)
- [Blueprint reference](https://docs.devin.ai/onboard-devin/environment/blueprint-reference)
- [DeepWiki configuration](https://docs.devin.ai/work-with-devin/deepwiki)
- [Devin Review](https://docs.devin.ai/work-with-devin/devin-review)
