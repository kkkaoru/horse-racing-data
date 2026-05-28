Use bun/bunx, not use npm/npx

## Per-package rules — coverage thresholds are enforced

All listed packages enforce minimum coverage via their own config + `lefthook.yml` pre-commit. The thresholds CANNOT be lowered without explicit user approval, and the coverage `include` / `source` must not be shrunk to hide regressions.

- `apps/pc-keiba-viewer/` — **TypeScript + Python**. See `apps/pc-keiba-viewer/CLAUDE.md`. TS enforced by `vitest.config.ts` (all 4 metrics >= 95). Python enforced by `pyproject.toml --cov-fail-under=95` over `corner_lightgbm`, `finish_position_lightgbm`, `finish_position_features_duckdb`, `finish_position_transformer`.
- `apps/local-postgresql/` — **TypeScript**. See `apps/local-postgresql/CLAUDE.md`. Enforced by `vitest.config.ts` (all 4 metrics >= 95).
- `apps/sync-realtime-data/` — **TypeScript (Cloudflare Workers)**. See `apps/sync-realtime-data/CLAUDE.md`. Enforced by `vitest.config.ts` (all 4 metrics >= 95). Branches was raised from 90 to 95 by removing dead `??` arms and testing reachable fallback arms — same playbook applies if it ever regresses.
- `scripts/` (repo root Python) — **Python**. See `scripts/CLAUDE.md`. Enforced by `pyproject.toml --cov-fail-under=95` over `pc_keiba_auto_update`.

Other apps (`apps/horse-racing-duckdb/`, `packages/*`) currently have no enforced threshold — when AI edits them, do not regress whatever level exists, but the hard rules above apply to the listed packages.
