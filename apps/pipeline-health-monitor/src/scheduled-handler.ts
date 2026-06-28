// Run with bun.
import { buildAlertMessage } from "./alert-message";
import { getFailureCount, incrementFailureCounter, resetFailureCounter } from "./alert-state";
import { evaluateChecks } from "./checks";
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

export const runScheduled = async (input: RunScheduledInput): Promise<void> => {
  const metrics = await fetchQueueHealth(input.env);
  const checks = evaluateChecks({ metrics, nowJst: input.now });
  await Promise.all(checks.map((check) => processCheck({ env: input.env, check, now: input.now })));
};
