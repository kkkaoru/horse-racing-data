// Run with bun. Production Worker end-to-end integration suite.
import { defineConfig } from "vitest/config";
import { wasmModulePlugin } from "./vitest-wasm-module-plugin";

export default defineConfig({
  plugins: [wasmModulePlugin()],
  test: {
    include: ["scripts/e2e-production.test.ts"],
    testTimeout: 60_000,
  },
});
