// Run with bun.
// daily-keiba-sync acquires JV-Link (JST 20:00) and NV-Link (JST 01:00) data once a day and
// records each successful scheduled run in its KV namespace. A failed or wedged acquisition
// leaves its run in a non-terminal state with no alert, so staleness of that record is the
// signal that the upstream feed (or its license) stopped working.
import type { IncidentSignal } from "./incident-engine";

export type SyncProvider = "jv" | "nv";

interface ProviderSyncHealth {
  at: string;
}

export const SYNC_PROVIDERS: readonly SyncProvider[] = ["jv", "nv"];
// One missed daily run is at least 26 h old by the monitor's last JST 22:55 tick.
const MAX_SUCCESS_AGE_MS: number = 26 * 3_600_000;
const PROVIDER_LABELS: Readonly<Record<SyncProvider, string>> = {
  jv: "JV-Link",
  nv: "NV-Link",
};

export const providerSyncHealthKey = (provider: SyncProvider): string =>
  `provider-sync:last-success:${provider}`;

const isHealth = (value: unknown): value is ProviderSyncHealth =>
  typeof value === "object" &&
  value !== null &&
  "at" in value &&
  typeof value.at === "string" &&
  !Number.isNaN(Date.parse(value.at));

const parseHealth = (raw: string): ProviderSyncHealth => {
  const value: unknown = JSON.parse(raw);
  if (!isHealth(value)) throw new Error("Provider sync health record is malformed");
  return value;
};

const describeProblem = (provider: SyncProvider, raw: string | null, now: Date): string | null => {
  const label: string = PROVIDER_LABELS[provider];
  if (raw === null) return `No successful scheduled ${label} sync is recorded.`;
  const health: ProviderSyncHealth = parseHealth(raw);
  return now.getTime() - Date.parse(health.at) > MAX_SUCCESS_AGE_MS
    ? `The last successful scheduled ${label} sync was at ${health.at}.`
    : null;
};

export const buildProviderSyncSignal = (
  provider: SyncProvider,
  raw: string | null,
  now: Date,
): IncidentSignal => {
  const problem: string | null = describeProblem(provider, raw, now);
  const title: string = `${PROVIDER_LABELS[provider]} daily sync`;
  return {
    description:
      problem ??
      `The scheduled ${PROVIDER_LABELS[provider]} sync succeeded within the last 26 hours.`,
    fields: [{ name: "KV key", value: providerSyncHealthKey(provider) }],
    key: `provider-sync:${provider}`,
    ok: problem === null,
    severity: "critical",
    stage: problem === null ? "healthy" : "stale",
    title: problem === null ? `${title} recovered` : `${title} stalled`,
  };
};
