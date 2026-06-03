// run with: bun run test:coverage
import { defineConfig } from "vitest/config";

const COVERAGE_THRESHOLD = 95;

export default defineConfig({
  test: {
    coverage: {
      provider: "v8",
      reporter: ["text", "json-summary"],
      all: true,
      include: ["src/**/*.ts"],
      exclude: [
        "src/**/*.d.ts",
        "src/**/*.test.ts",
        "src/types.ts",
        "src/index.ts",
        // Thin Durable-Object Container binding wrapper: extends Container from
        // @cloudflare/containers, which cannot be instantiated in the vitest
        // pool. Its only logic (start() options) is built + tested in
        // dispatch.ts. Documented exclusion per CLAUDE.md coverage rules.
        "src/container-class.ts",
      ],
      thresholds: {
        lines: COVERAGE_THRESHOLD,
        branches: COVERAGE_THRESHOLD,
        functions: COVERAGE_THRESHOLD,
        statements: COVERAGE_THRESHOLD,
      },
    },
    include: ["src/**/*.test.ts"],
    testTimeout: 30000,
  },
});
