// Run with bun. Convert readiness and queue-canary payloads into incident signals.

import type { IncidentSignal } from "./incident-engine";
import type {
  DeliveryCanaryResponse,
  PredictionReadinessRace,
  PredictionReadinessResponse,
} from "./types";

const CANARY_STALE_MS = 10 * 60 * 1000;

const readinessSignal = (race: PredictionReadinessRace, runYmd: string): IncidentSignal => ({
  description:
    race.expectedCount === 0
      ? "No eligible entry rows are available, so readiness cannot be proven."
      : `${race.missingCount} of ${race.expectedCount} required predictions are missing.`,
  fields: [
    { name: "Run date", value: runYmd },
    { name: "Race", value: race.raceKey },
    { name: "Post", value: race.raceStartAtJst },
    { name: "Coverage", value: `${race.predictionCount}/${race.expectedCount}` },
    { name: "Minutes to post", value: String(race.minutesToPost) },
  ],
  key: `finish-position-readiness:${runYmd}:${race.raceKey}`,
  ok: race.complete,
  severity: race.deadline === "T-120" ? "warning" : "critical",
  stage: race.deadline,
  title: `finish-position predictions incomplete ${runYmd} ${race.raceKey}`,
});

export const buildReadinessSignals = (response: PredictionReadinessResponse): IncidentSignal[] =>
  response.races.map((race) => readinessSignal(race, response.runYmd));

export const buildCanarySignal = (response: DeliveryCanaryResponse, now: Date): IncidentSignal => {
  const overdue = response.canaries
    .filter(
      (canary) =>
        canary.consumedAt === null &&
        now.getTime() - Date.parse(canary.enqueuedAt) >= CANARY_STALE_MS,
    )
    .toSorted((left, right) => left.enqueuedAt.localeCompare(right.enqueuedAt));
  const oldest = overdue[0];
  const newest = response.canaries.toSorted((left, right) =>
    right.enqueuedAt.localeCompare(left.enqueuedAt),
  )[0];
  const noCanaries = newest === undefined;
  const staleHeartbeat =
    newest !== undefined && now.getTime() - Date.parse(newest.enqueuedAt) >= CANARY_STALE_MS;
  return {
    description: noCanaries
      ? "No delivery canary record is available."
      : oldest
        ? `Canary ${oldest.id} has not been consumed within 10 minutes.`
        : staleHeartbeat
          ? `No new delivery canary has been enqueued since ${newest.enqueuedAt}.`
          : "Primary prediction queue consumed delivery canaries within the SLO.",
    fields: oldest
      ? [
          { name: "Canary ID", value: oldest.id },
          { name: "Enqueued", value: oldest.enqueuedAt },
        ]
      : [],
    key: "finish-position-delivery-canary",
    ok: !noCanaries && oldest === undefined && !staleHeartbeat,
    severity: "critical",
    stage: "10-minute-delivery-slo",
    title: "finish-position queue delivery canary overdue",
  };
};

export const buildEndpointFailureSignal = (name: string, error: unknown): IncidentSignal => ({
  description: `Monitor endpoint failed and cannot be evaluated: ${String(error)}`,
  fields: [{ name: "Endpoint", value: name }],
  key: `finish-position-monitor-endpoint:${name}`,
  ok: false,
  severity: "critical",
  stage: "endpoint-failure",
  title: `monitor endpoint unavailable: ${name}`,
});

export const buildEndpointRecoverySignal = (name: string): IncidentSignal => ({
  description: "Monitor endpoint recovered and can be evaluated again.",
  fields: [{ name: "Endpoint", value: name }],
  key: `finish-position-monitor-endpoint:${name}`,
  ok: true,
  severity: "critical",
  stage: "healthy",
  title: `monitor endpoint recovered: ${name}`,
});
