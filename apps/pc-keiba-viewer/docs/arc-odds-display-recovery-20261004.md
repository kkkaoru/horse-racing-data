# Arc realtime odds display recovery — 2026-10-04

## Actual failure

Production `/races/2026/10/04/A8/05` had prices inside its serialized payload, but the visible runner table showed `-` for horses 1–9 and the realtime panel showed `オッズ推移はまだありません。`. Payload presence was not proof of display success.

The deployed Arc-specific loader bypassed `REALTIME_HOT`, replacing the complete seven-bet-type payload with the official entry card's single-win values and empty histories. Its `01` horse keys also failed the runner table's `1` lookup.

## Fix and deployment

- Always read the existing JRA hot service; use the official card only if it returns no payload.
- Normalize fallback horse keys using `String(h.number)`.
- Preserve race entry/cancellation and weight data.
- Source and regression test updated in the production release source at `.cache/statistics-rollout-20261004/release/apps/pc-keiba-viewer/src/lib/realtime-payload.server{,.test}.ts`, and verified in `.cache/a8-odds-display-recovery/release/`.
- The ordinary working-tree loader does not contain this deployed Arc override. Do not reintroduce the bypass when reconciling production release sources.
- To avoid publishing unrelated pending release changes, applied only the equivalent three loader changes to all three server bundle copies. `prepare.mjs` requires the exact production SHA-256; `verify-bundle.mjs` executes each patched copy with mocked services, testing both hot and fallback paths.
- Previous version: `fecd3a43-1d9e-4a02-9c32-ea63c7602251`.
- Deployed version: `54fdcf4d-c4b6-46bc-a5b2-537d8f549526`.
- All 35 bindings and existing static assets retained; runtime settings checked unchanged. No database schema, model, or frontend bundle changes.

## Verification

- 6,075 TypeScript tests passed; type checking and lint passed.
- Coverage: statements 98.82%, branches 96.75%, functions 99.18%, lines 98.94%. Thresholds unchanged.
- `bun run prod get` retrieved production HTML; an inert HTML parser excluded every script/embedded payload before assertions.
- All 16 runner rows have numeric single-win prices; the realtime panel contains 16 visible current-odds labels and the single-win chart container, with no empty-history message.
- Available numeric-history tabs: single-win, quinella, exacta, trio, trifecta. Place/wide latest values are returned by the API, but their existing history points have null numeric odds, so this change does not claim range-history chart support.
- Refreshed the official acquisition and repeated both visible-HTML/API checks: fetch time advanced to `2026-10-04T20:11:14+09:00`, win odds remained 16 rows, and each win history had two snapshots. The visible checks passed again.
- No Playwright or interactive Access login was used. This is visible server-HTML verification, not screenshot/browser-interaction verification.

Evidence and runnable checks: `.cache/a8-odds-display-recovery/` (`visible-audit.json`, `verify-visible.mjs`, `patch-audit.json`, `deploy.log`, `settings-before.json`, `settings-after.json`). Raw HTML/API: `/tmp/a8-visible-after.html`, `/tmp/a8-visible-after-api.json`. Test logs: `/tmp/a8-display-{target-tests,tsc,lint,coverage}.log`.

Python was not modified. The first copied-release Python run reached 96.51% coverage but failed sibling-module imports; a second run exposed non-relocatable virtualenv imports. The repository-root package check then passed: 5,315 tests, 97.43% coverage, 27 existing runtime warnings (`/tmp/a8-display-python-root.log`). The failed copied-environment logs are retained, not relabeled as passes.
