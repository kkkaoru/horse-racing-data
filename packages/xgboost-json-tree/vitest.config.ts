// Run with: bun run --filter xgboost-json-tree test:coverage
import { defineConfig } from "vitest/config";

// All 4 metrics: 95%. v8 counts every `?? null`, `?? ""`, and ternary as a
// branch, so do not regress by adding dead defensive fallbacks — refactor
// unreachable arms with `!` / direct access instead.
const COVERAGE_THRESHOLD = 95;

export default defineConfig({
  test: {
    include: ["src/**/*.test.ts"],
    coverage: {
      provider: "v8",
      reporter: ["text", "json-summary"],
      all: true,
      include: ["src/**/*.ts"],
      exclude: ["src/**/*.test.ts", "src/**/*.d.ts"],
      thresholds: {
        lines: COVERAGE_THRESHOLD,
        branches: COVERAGE_THRESHOLD,
        functions: COVERAGE_THRESHOLD,
        statements: COVERAGE_THRESHOLD,
      },
    },
  },
});
