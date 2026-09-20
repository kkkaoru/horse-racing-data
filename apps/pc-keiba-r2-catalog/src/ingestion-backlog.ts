// Runs with bun; assesses read-only Queue gauges, never consumes or replays messages.
interface BacklogPolicy {
  retentionSeconds: number;
  warningAgeSeconds: number;
  criticalAgeSeconds: number;
  maxObservationAgeSeconds: number;
}
interface BacklogObservation {
  metrics: unknown;
  observedAtMs: number;
  nowMs: number;
  policy: BacklogPolicy;
}
interface BacklogAssessment {
  status: "unknown" | "clear" | "pending" | "warning" | "critical";
  reason: string;
  backlogCount: number | null;
  oldestAgeSeconds: number | null;
  retentionRemainingSeconds: number | null;
}
const unknownAssessment = (reason: string): BacklogAssessment => ({
  status: "unknown",
  reason,
  backlogCount: null,
  oldestAgeSeconds: null,
  retentionRemainingSeconds: null,
});
const integer = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const assessIngestionBacklog = (input: BacklogObservation): BacklogAssessment => {
  const { metrics, policy, observedAtMs, nowMs } = input;
  if (
    !Object.values(policy).every(integer) ||
    policy.warningAgeSeconds === 0 ||
    policy.maxObservationAgeSeconds === 0 ||
    policy.warningAgeSeconds >= policy.criticalAgeSeconds ||
    policy.criticalAgeSeconds >= policy.retentionSeconds
  )
    throw new Error("Invalid backlog monitoring policy");
  if (
    !integer(observedAtMs) ||
    !integer(nowMs) ||
    observedAtMs > nowMs ||
    nowMs - observedAtMs > policy.maxObservationAgeSeconds * 1000
  )
    return unknownAssessment("stale-or-invalid-observation");
  if (
    !record(metrics) ||
    !integer(metrics.backlog_count) ||
    !integer(metrics.backlog_bytes) ||
    !integer(metrics.oldest_message_timestamp_ms)
  )
    return unknownAssessment("invalid-gauges");
  if (metrics.backlog_count === 0) {
    if (metrics.backlog_bytes !== 0 || metrics.oldest_message_timestamp_ms !== 0)
      return unknownAssessment("inconsistent-empty-gauges");
    return {
      status: "clear",
      reason: "empty-at-observation",
      backlogCount: 0,
      oldestAgeSeconds: null,
      retentionRemainingSeconds: null,
    };
  }
  if (
    metrics.oldest_message_timestamp_ms === 0 ||
    metrics.oldest_message_timestamp_ms > observedAtMs
  )
    return unknownAssessment("invalid-oldest-timestamp");
  const age: number = (nowMs - metrics.oldest_message_timestamp_ms) / 1000;
  const status: BacklogAssessment["status"] =
    age >= policy.criticalAgeSeconds ? "critical" : "pending";
  const assessedStatus: BacklogAssessment["status"] =
    status === "pending" && age >= policy.warningAgeSeconds ? "warning" : status;
  return {
    status: assessedStatus,
    reason: "unarchived-backlog",
    backlogCount: metrics.backlog_count,
    oldestAgeSeconds: age,
    retentionRemainingSeconds: Math.max(0, policy.retentionSeconds - age),
  };
};
