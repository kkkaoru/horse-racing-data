import { defineConfig } from "vitest/config";

const COVERAGE_THRESHOLD = 95;

export default defineConfig({
  resolve: {
    alias: {
      "cloudflare:workers": new URL("./test-support/cloudflare-workers.ts", import.meta.url)
        .pathname,
    },
  },
  test: {
    coverage: {
      exclude: ["src/**/*.test.ts"],
      include: [
        "src/**/*.ts",
        "scripts/run-vector-backfill.ts",
        "scripts/run-d1-backfill.ts",
        "scripts/run-d1-capture.ts",
      ],
      provider: "v8",
      reporter: ["text", "json-summary"],
      thresholds: {
        branches: COVERAGE_THRESHOLD,
        functions: COVERAGE_THRESHOLD,
        lines: COVERAGE_THRESHOLD,
        statements: COVERAGE_THRESHOLD,
      },
    },
    include: [
      "src/**/*.test.ts",
      "scripts/run-vector-backfill.test.ts",
      "scripts/run-d1-backfill.test.ts",
      "scripts/run-d1-capture.test.ts",
    ],
  },
});
