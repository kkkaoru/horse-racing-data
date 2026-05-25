// run with: bun run test:coverage
import { defineConfig } from "vitest/config";

// Lines/Statements/Functions: 95% (semantic correctness).
// Branches: 87% — v8 counts every `?? null`, `?? ""`, and ternary as a branch,
// inflating the metric for parser-heavy code. Push higher when the dead
// defensive fallbacks have been refactored away.
const COVERAGE_THRESHOLD = 95;
const BRANCHES_THRESHOLD = 87;

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
      ],
      thresholds: {
        lines: COVERAGE_THRESHOLD,
        branches: BRANCHES_THRESHOLD,
        functions: COVERAGE_THRESHOLD,
        statements: COVERAGE_THRESHOLD,
      },
    },
    include: ["test/**/*.test.ts", "scripts/**/*.test.ts", "src/**/*.test.ts"],
    testTimeout: 30000,
  },
});
