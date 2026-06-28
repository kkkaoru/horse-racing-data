// Run with bun.
import type { AlertField, AlertMessage, AlertSeverity, HealthCheck } from "./types";

interface BuildAlertInput {
  check: HealthCheck;
  severity: AlertSeverity;
  failureCount: number;
  nowJst: Date;
}

// Map-style lookup per coding rule (replace if/switch chains with Map).
// Modeled as a Record so TS proves exhaustiveness — no defensive
// undefined-fallback arm is needed.
const TITLE_PREFIX_BY_SEVERITY: Record<AlertSeverity, string> = {
  warning: "[WARNING]",
  critical: "[CRITICAL]",
  recovery: "[RECOVERY]",
};

const JST_OFFSET_HOURS = 9;
const MS_PER_HOUR = 3_600_000;
const ISO_FRACTIONAL_TRIM_LENGTH = 19;
const JST_ISO_SUFFIX = "+09:00";

const formatJstIso = (now: Date): string => {
  const shifted = new Date(now.getTime() + JST_OFFSET_HOURS * MS_PER_HOUR);
  const iso = shifted.toISOString();
  return `${iso.slice(0, ISO_FRACTIONAL_TRIM_LENGTH)}${JST_ISO_SUFFIX}`;
};

const resolveTitlePrefix = (severity: AlertSeverity): string => TITLE_PREFIX_BY_SEVERITY[severity];

const buildFields = (input: BuildAlertInput): AlertField[] => [
  { name: "Check", value: input.check.name },
  { name: "Value", value: String(input.check.value) },
  { name: "Threshold", value: String(input.check.threshold) },
  { name: "Failure Count", value: String(input.failureCount) },
];

const buildDescription = (check: HealthCheck): string =>
  `${check.message} (value=${check.value}, threshold=${check.threshold})`;

export const buildAlertMessage = (input: BuildAlertInput): AlertMessage => ({
  checkName: input.check.name,
  severity: input.severity,
  title: `${resolveTitlePrefix(input.severity)} ${input.check.name}`,
  description: buildDescription(input.check),
  fields: buildFields(input),
  timestampJst: formatJstIso(input.nowJst),
});
