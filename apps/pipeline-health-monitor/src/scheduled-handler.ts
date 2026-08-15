// Run with bun.
import { buildAlertMessage } from "./alert-message";
import { getFailureCount, incrementFailureCounter, resetFailureCounter } from "./alert-state";
import { evaluateChecks } from "./checks";
import { fetchDeliveryCanaries, fetchPredictionReadiness } from "./finish-position-client";
import {
  buildCanarySignal,
  buildEndpointFailureSignal,
  buildEndpointRecoverySignal,
  buildReadinessSignals,
} from "./finish-position-signals";
import {
  processIncidentSignal,
  sendDailyMonitorHeartbeat,
  type IncidentSignal,
} from "./incident-engine";
import { fetchQueueHealth } from "./queue-health-client";
import type { AlertSeverity, Env, HealthCheck } from "./types";

interface RunScheduledInput {
  env: Env;
  now: Date;
}

interface ProcessCheckInput {
  env: Env;
  check: HealthCheck;
  now: Date;
}

const WARNING_THRESHOLD = 2;
const CRITICAL_THRESHOLD = 3;
const STILL_FAILING_INTERVAL = 4;
const NO_PREVIOUS_FAILURES = 0;
const FIRST_OVER_CRITICAL = 0;

const enqueueAlert = async (input: {
  env: Env;
  check: HealthCheck;
  severity: AlertSeverity;
  failureCount: number;
  now: Date;
}): Promise<void> => {
  const message = buildAlertMessage({
    check: input.check,
    severity: input.severity,
    failureCount: input.failureCount,
    nowJst: input.now,
  });
  await input.env.ALERT_QUEUE.send(message);
};

const handleOkPath = async (input: ProcessCheckInput): Promise<void> => {
  const previousCount = await getFailureCount(input.env, input.check.name);
  if (previousCount === NO_PREVIOUS_FAILURES) {
    return;
  }
  await enqueueAlert({
    env: input.env,
    check: input.check,
    severity: "recovery",
    failureCount: previousCount,
    now: input.now,
  });
  await resetFailureCounter(input.env, input.check.name);
};

const isStillFailingTick = (newCount: number): boolean => {
  const overshoot = newCount - CRITICAL_THRESHOLD;
  return overshoot > FIRST_OVER_CRITICAL && overshoot % STILL_FAILING_INTERVAL === 0;
};

const handleNotOkPath = async (input: ProcessCheckInput): Promise<void> => {
  const newCount = await incrementFailureCounter(input.env, input.check.name);
  if (newCount < WARNING_THRESHOLD) {
    return;
  }
  if (newCount === WARNING_THRESHOLD) {
    console.warn(
      `pipeline-health-monitor warning: ${input.check.name} failed ${newCount}/${CRITICAL_THRESHOLD}`,
    );
    return;
  }
  if (newCount === CRITICAL_THRESHOLD) {
    await enqueueAlert({
      env: input.env,
      check: input.check,
      severity: "critical",
      failureCount: newCount,
      now: input.now,
    });
    return;
  }
  if (isStillFailingTick(newCount)) {
    await enqueueAlert({
      env: input.env,
      check: input.check,
      severity: "critical",
      failureCount: newCount,
      now: input.now,
    });
  }
};

const processCheck = async (input: ProcessCheckInput): Promise<void> => {
  if (input.check.skipped === true) {
    return;
  }
  if (input.check.ok) {
    await handleOkPath(input);
    return;
  }
  await handleNotOkPath(input);
};

interface ProcessIncidentInput {
  env: Env;
  now: Date;
  signal: IncidentSignal;
}

const processIncidentSafely = async (input: ProcessIncidentInput): Promise<void> => {
  console.log(
    "pipeline-health-monitor signal",
    JSON.stringify({
      key: input.signal.key,
      ok: input.signal.ok,
      severity: input.signal.severity,
      stage: input.signal.stage,
    }),
  );
  try {
    await processIncidentSignal(input.env, input.signal, input.now);
  } catch (error) {
    console.error(
      "pipeline-health-monitor incident delivery failed",
      input.signal.key,
      String(error),
    );
  }
};

const processCheckSafely = async (input: ProcessCheckInput): Promise<void> => {
  console.log(
    "pipeline-health-monitor check",
    JSON.stringify({
      check: input.check.name,
      ok: input.check.ok,
      skipped: input.check.skipped === true,
    }),
  );
  try {
    await processCheck(input);
  } catch (error) {
    await processIncidentSafely({
      env: input.env,
      now: input.now,
      signal: buildEndpointFailureSignal("queue-health-alert-delivery", error),
    });
  }
};

const QUARTER_HOUR_MINUTES = 15;

export const isQuarterHourTick = (now: Date): boolean =>
  now.getUTCMinutes() % QUARTER_HOUR_MINUTES === 0;

const processCanary = async (input: RunScheduledInput): Promise<void> => {
  try {
    const response = await fetchDeliveryCanaries(input.env);
    await processIncidentSafely({
      env: input.env,
      now: input.now,
      signal: buildEndpointRecoverySignal("delivery-canaries"),
    });
    await processIncidentSafely({
      env: input.env,
      now: input.now,
      signal: buildCanarySignal(response, input.now),
    });
  } catch (error) {
    await processIncidentSafely({
      env: input.env,
      now: input.now,
      signal: buildEndpointFailureSignal("delivery-canaries", error),
    });
  }
};

const processReadiness = async (input: RunScheduledInput): Promise<void> => {
  try {
    const response = await fetchPredictionReadiness(input.env);
    await processIncidentSafely({
      env: input.env,
      now: input.now,
      signal: buildEndpointRecoverySignal("prediction-readiness"),
    });
    await Promise.all(
      buildReadinessSignals(response).map((signal) =>
        processIncidentSafely({ env: input.env, now: input.now, signal }),
      ),
    );
  } catch (error) {
    await processIncidentSafely({
      env: input.env,
      now: input.now,
      signal: buildEndpointFailureSignal("prediction-readiness", error),
    });
  }
};

const processExistingChecks = async (input: RunScheduledInput): Promise<void> => {
  try {
    const metrics = await fetchQueueHealth(input.env);
    console.log("pipeline-health-monitor queue-health", JSON.stringify({ ok: true }));
    await processIncidentSafely({
      env: input.env,
      now: input.now,
      signal: buildEndpointRecoverySignal("queue-health"),
    });
    const checks = evaluateChecks({ metrics, nowJst: input.now });
    await Promise.all(
      checks.map((check) => processCheckSafely({ env: input.env, check, now: input.now })),
    );
  } catch (error) {
    await processIncidentSafely({
      env: input.env,
      now: input.now,
      signal: buildEndpointFailureSignal("queue-health", error),
    });
  }
};

export const runScheduled = async (input: RunScheduledInput): Promise<void> => {
  try {
    await sendDailyMonitorHeartbeat(input.env, input.now);
  } catch (error) {
    console.error("pipeline-health-monitor heartbeat delivery failed", String(error));
  }
  await processCanary(input);
  if (!isQuarterHourTick(input.now)) return;
  await Promise.all([processExistingChecks(input), processReadiness(input)]);
};
