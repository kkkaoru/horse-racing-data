// Run with bun.
import { defineConfig } from "vitest/config";
import { wasmModulePlugin } from "./vitest-wasm-module-plugin";

const COVERAGE_THRESHOLD: number = 95;

export default defineConfig({
  plugins: [wasmModulePlugin()],
  resolve: {
    alias: {
      "cloudflare:sockets": new URL("src/cloudflare-sockets-test.ts", import.meta.url).pathname,
    },
  },
  test: {
    coverage: {
      provider: "v8",
      reporter: ["text"],
      include: [
        "src/acquisition.ts",
        "src/auth.ts",
        "src/compatibility-attestation.ts",
        "src/rust-core.ts",
        "src/socket-fetch.ts",
        "src/streaming.ts",
        "src/worker.ts",
      ],
      thresholds: {
        branches: COVERAGE_THRESHOLD,
        functions: COVERAGE_THRESHOLD,
        lines: COVERAGE_THRESHOLD,
        statements: COVERAGE_THRESHOLD,
      },
    },
    include: ["src/**/*.test.ts"],
  },
});
