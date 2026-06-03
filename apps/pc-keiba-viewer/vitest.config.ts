import { defineConfig } from "vitest/config";

export default defineConfig({
  esbuild: {
    jsx: "automatic",
  },
  resolve: {
    alias: {
      "cloudflare:workers": new URL("./src/test-stubs/cloudflare-workers.ts", import.meta.url)
        .pathname,
      "server-only": new URL("./src/test-stubs/server-only.ts", import.meta.url).pathname,
    },
  },
  test: {
    coverage: {
      provider: "v8",
      include: [
        "src/lib/**/*.ts",
        "src/app/races/calendar.tsx",
        "src/app/races/detail/runners-table.tsx",
        "src/app/races/detail/finish-position-bucket-section.tsx",
      ],
      exclude: [
        "src/lib/paddock-server.ts",
        "src/lib/**/*.server.ts",
        "src/lib/favorites-indexeddb.ts",
        "src/lib/race-pace-prediction.ts",
        "src/lib/race-detail-section-cache.ts",
        "src/lib/win5/index.ts",
      ],
      thresholds: {
        branches: 95,
        functions: 95,
        lines: 95,
        statements: 95,
      },
    },
    environment: "happy-dom",
    include: ["src/**/*.test.ts", "src/**/*.test.tsx"],
  },
});
