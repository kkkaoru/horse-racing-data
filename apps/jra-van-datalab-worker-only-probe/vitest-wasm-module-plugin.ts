// Run with bun. Emulates Wrangler's static Wasm module import in Vitest.
import { readFile } from "node:fs/promises";
import type { Plugin } from "vite";

export const wasmModulePlugin = (): Plugin => ({
  enforce: "pre",
  name: "wrangler-static-wasm-module",
  async load(id) {
    if (!id.endsWith(".wasm")) return undefined;
    const base64 = (await readFile(id)).toString("base64");
    return `const bytes = Uint8Array.from(atob(${JSON.stringify(base64)}), (value) => value.charCodeAt(0));\nexport default new WebAssembly.Module(bytes);`;
  },
});
