// run with: bun run test:coverage
import { defineConfig } from "vitest/config";

const COVERAGE_THRESHOLD: number = 95;

export default defineConfig({
  test: {
    coverage: {
      provider: "v8",
      reporter: ["text", "json-summary"],
      all: true,
      include: ["src/**/*.ts"],
      exclude: ["src/run-mcp-stdio-proxy.ts", "src/**/*.test.ts"],
      thresholds: {
        lines: COVERAGE_THRESHOLD,
        branches: COVERAGE_THRESHOLD,
        functions: COVERAGE_THRESHOLD,
        statements: COVERAGE_THRESHOLD,
      },
    },
    include: ["src/**/*.test.ts"],
  },
});
