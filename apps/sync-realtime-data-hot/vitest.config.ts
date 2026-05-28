// run with: bun run test:coverage
import { defineConfig } from "vitest/config";

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
        "src/index.ts",
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
