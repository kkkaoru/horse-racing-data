// run with: bun run test:coverage
import { defineConfig } from "vitest/config";

// All 4 metrics: 95%. v8 counts every `?? null`, `?? ""`, and ternary as a
// branch, so do not regress by adding dead defensive fallbacks — refactor
// unreachable arms with `!` / direct access instead.
const COVERAGE_THRESHOLD = 95;

export default defineConfig({
  test: {
    coverage: {
      provider: "v8",
      reporter: ["text", "json-summary"],
      all: true,
      include: ["src/**/*.ts", "scripts/**/*.ts"],
      exclude: [
        "src/**/*.d.ts",
        "src/**/*.test.ts",
        "scripts/**/*.test.ts",
        "src/types.ts",
        "src/dsnp-parquetjs.d.ts",
        "src/postgres.ts",
        "src/finish-position-lite-pool.ts",
        "src/scripts/backfill-nar-realtime-date.ts",
        "scripts/backfill-premium-data-top.ts",
        "scripts/sync-running-style-d1-to-neon.ts",
      ],
      thresholds: {
        lines: COVERAGE_THRESHOLD,
        branches: COVERAGE_THRESHOLD,
        functions: COVERAGE_THRESHOLD,
        statements: COVERAGE_THRESHOLD,
      },
    },
    include: ["test/**/*.test.ts", "scripts/**/*.test.ts", "src/**/*.test.ts"],
    testTimeout: 30000,
  },
});
