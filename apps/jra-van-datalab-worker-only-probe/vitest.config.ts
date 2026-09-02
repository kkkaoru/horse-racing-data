// Run with bun.
import { defineConfig } from "vitest/config";
import { wasmModulePlugin } from "./vitest-wasm-module-plugin";

const COVERAGE_THRESHOLD: number = 100;

export default defineConfig({
  plugins: [wasmModulePlugin()],
  test: {
    coverage: {
      provider: "v8",
      reporter: ["text"],
      include: [
        "src/acquisition.ts",
        "src/compatibility-attestation.ts",
        "src/compatibility.ts",
        "src/course.ts",
        "src/encoding.ts",
        "src/jvfile.ts",
        "src/movie.ts",
        "src/network.ts",
        "src/probe.ts",
        "src/protocol.ts",
        "src/realtime.ts",
        "src/runtime.ts",
        "src/rust-core.ts",
        "src/streaming.ts",
      ],
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
