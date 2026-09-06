// Run with bun.
import { defineConfig } from "vitest/config";

const COVERAGE_THRESHOLD: number = 95;

export default defineConfig({
  test: {
    coverage: {
      provider: "v8",
      reporter: ["text", "json-summary"],
      include: ["src/**/*.ts"],
      exclude: ["src/**/*.test.ts", "src/worker.ts"],
      thresholds: {
        lines: COVERAGE_THRESHOLD,
        branches: COVERAGE_THRESHOLD,
        functions: COVERAGE_THRESHOLD,
        statements: COVERAGE_THRESHOLD,
      },
    },
    include: ["src/**/*.test.ts", "tests/**/*.test.ts"],
  },
});
