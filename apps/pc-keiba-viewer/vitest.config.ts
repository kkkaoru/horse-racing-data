import { defineConfig } from "vitest/config";

export default defineConfig({
  esbuild: {
    jsx: "automatic",
  },
  test: {
    coverage: {
      provider: "v8",
      include: [
        "src/lib/**/*.ts",
        "src/app/races/calendar.tsx",
        "src/app/races/detail/runners-table.tsx",
      ],
      exclude: [
        "src/lib/paddock-server.ts",
        "src/lib/**/*.server.ts",
        "src/lib/favorites-indexeddb.ts",
        "src/lib/race-pace-prediction.ts",
        "src/lib/race-detail-section-cache.ts",
      ],
      thresholds: {
        branches: 95,
        functions: 90,
        lines: 90,
        statements: 90,
      },
    },
    environment: "jsdom",
    include: ["src/**/*.test.ts", "src/**/*.test.tsx"],
  },
});
