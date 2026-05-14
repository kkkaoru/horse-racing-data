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
        "src/app/races/[year]/[month]/[day]/race-date-filter.tsx",
        "src/app/races/detail/runners-table.tsx",
      ],
      exclude: ["src/lib/paddock-server.ts"],
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
