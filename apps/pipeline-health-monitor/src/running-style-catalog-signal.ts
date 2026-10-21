// Run with bun.
// daily-keiba-sync appends the production running-style predictions to the R2 Catalog
// once a day (JST 05:30) and records each successful run in its KV namespace.
// Training reads the Catalog, so a silently stopped sync would starve it of rs_* rows.
import type { IncidentSignal } from "./incident-engine";

interface RunningStyleCatalogHealth {
  at: string;
  objects: number;
}

interface HealthRule {
  failed: (health: RunningStyleCatalogHealth, now: Date) => boolean;
  describe: (health: RunningStyleCatalogHealth) => string;
}

export const RUNNING_STYLE_CATALOG_HEALTH_KEY = "running-style-catalog:last-success";
// One missed daily run is already 27.5 h old at the monitor's first JST 09:00 tick.
const MAX_SUCCESS_AGE_MS = 26 * 3_600_000;
const SIGNAL_KEY = "running-style-catalog-sync";
const SIGNAL_TITLE = "running-style Catalog sync";
const MISSING_DESCRIPTION = "No successful running-style Catalog sync is recorded.";
const HEALTHY_DESCRIPTION = "The running-style Catalog sync succeeded within the last 26 hours.";
const HEALTH_RULES: readonly HealthRule[] = [
  {
    failed: (health, now) => now.getTime() - Date.parse(health.at) > MAX_SUCCESS_AGE_MS,
    describe: (health) => `The last successful running-style Catalog sync was at ${health.at}.`,
  },
  {
    failed: (health) => health.objects === 0,
    describe: (health) =>
      `The running-style Catalog sync at ${health.at} found no Parquet objects in its lookback window.`,
  },
];

const isHealth = (value: unknown): value is RunningStyleCatalogHealth =>
  typeof value === "object" &&
  value !== null &&
  "at" in value &&
  typeof value.at === "string" &&
  !Number.isNaN(Date.parse(value.at)) &&
  "objects" in value &&
  typeof value.objects === "number";

const parseHealth = (raw: string): RunningStyleCatalogHealth => {
  const value: unknown = JSON.parse(raw);
  if (!isHealth(value)) throw new Error("Running-style Catalog health record is malformed");
  return value;
};

const describeProblem = (raw: string | null, now: Date): string | null => {
  if (raw === null) return MISSING_DESCRIPTION;
  const health = parseHealth(raw);
  return HEALTH_RULES.find((rule) => rule.failed(health, now))?.describe(health) ?? null;
};

export const buildRunningStyleCatalogSignal = (raw: string | null, now: Date): IncidentSignal => {
  const problem = describeProblem(raw, now);
  return {
    description: problem ?? HEALTHY_DESCRIPTION,
    fields: [{ name: "KV key", value: RUNNING_STYLE_CATALOG_HEALTH_KEY }],
    key: SIGNAL_KEY,
    ok: problem === null,
    severity: "critical",
    stage: problem === null ? "healthy" : "stale",
    title: problem === null ? `${SIGNAL_TITLE} recovered` : `${SIGNAL_TITLE} stalled`,
  };
};
