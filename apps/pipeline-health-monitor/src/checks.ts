// Run with bun.
import type { CheckEvaluationInput, HealthCheck } from "./types";

interface JstWindowInput {
  now: Date;
  startHour: number;
  startMin: number;
  endHour: number;
  endMin: number;
}

interface StalenessCheckInput {
  name: string;
  iso: string | null;
  nowJst: Date;
  thresholdMin: number;
}

export const CHECK_FETCH_RESULTS_STALENESS = "fetch-results-staleness";
export const CHECK_FETCH_WEIGHTS_STALENESS = "fetch-weights-staleness";
export const CHECK_RACES_QUEUED = "races-queued-not-fetched-today";
export const CHECK_RACES_STUCK = "races-stuck-over-thirty-min";

const FETCH_RESULTS_STALENESS_MIN = 30;
const FETCH_WEIGHTS_STALENESS_MIN = 30;
const RACES_QUEUED_THRESHOLD = 10;
const RACES_STUCK_THRESHOLD = 10;

const FETCH_RESULTS_WINDOW_START_HOUR = 13;
const FETCH_RESULTS_WINDOW_START_MIN = 0;
const FETCH_RESULTS_WINDOW_END_HOUR = 21;
const FETCH_RESULTS_WINDOW_END_MIN = 30;
const FETCH_WEIGHTS_WINDOW_START_HOUR = 11;
const FETCH_WEIGHTS_WINDOW_START_MIN = 0;
const FETCH_WEIGHTS_WINDOW_END_HOUR = 21;
const FETCH_WEIGHTS_WINDOW_END_MIN = 30;

const JST_OFFSET_HOURS = 9;
const MS_PER_HOUR = 3_600_000;
const MS_PER_MINUTE = 60_000;
const MINUTES_PER_HOUR = 60;
const NEGATIVE_SENTINEL = -1;

const OUTSIDE_WINDOW_MESSAGE = "outside window";

const getJstMinuteOfDay = (now: Date): number => {
  const jstMs = now.getTime() + JST_OFFSET_HOURS * MS_PER_HOUR;
  const dayMs = jstMs % (24 * MS_PER_HOUR);
  return Math.floor(dayMs / MS_PER_MINUTE);
};

export const isWithinJstWindow = (input: JstWindowInput): boolean => {
  const minuteOfDay = getJstMinuteOfDay(input.now);
  const startMinute = input.startHour * MINUTES_PER_HOUR + input.startMin;
  const endMinute = input.endHour * MINUTES_PER_HOUR + input.endMin;
  return minuteOfDay >= startMinute && minuteOfDay <= endMinute;
};

const buildSkippedCheck = (name: string): HealthCheck => ({
  name,
  ok: true,
  skipped: true,
  value: NEGATIVE_SENTINEL,
  threshold: NEGATIVE_SENTINEL,
  message: OUTSIDE_WINDOW_MESSAGE,
});

const buildNullStalenessCheck = (input: StalenessCheckInput): HealthCheck => ({
  name: input.name,
  ok: false,
  value: NEGATIVE_SENTINEL,
  threshold: input.thresholdMin,
  message: "no successful fetch recorded yet",
});

const computeStalenessMinutes = (iso: string, nowJst: Date): number =>
  Math.floor((nowJst.getTime() - new Date(iso).getTime()) / MS_PER_MINUTE);

const evaluateStalenessCheck = (input: StalenessCheckInput): HealthCheck => {
  if (input.iso === null) {
    return buildNullStalenessCheck(input);
  }
  const staleness = computeStalenessMinutes(input.iso, input.nowJst);
  const ok = staleness < input.thresholdMin;
  return {
    name: input.name,
    ok,
    value: staleness,
    threshold: input.thresholdMin,
    message: ok ? "within freshness threshold" : "exceeded freshness threshold",
  };
};

const evaluateCounterCheck = (input: {
  name: string;
  value: number;
  threshold: number;
}): HealthCheck => {
  const ok = input.value < input.threshold;
  return {
    name: input.name,
    ok,
    value: input.value,
    threshold: input.threshold,
    message: ok ? "below counter threshold" : "exceeded counter threshold",
  };
};

const evaluateFetchResultsCheck = (input: CheckEvaluationInput): HealthCheck => {
  const inWindow = isWithinJstWindow({
    now: input.nowJst,
    startHour: FETCH_RESULTS_WINDOW_START_HOUR,
    startMin: FETCH_RESULTS_WINDOW_START_MIN,
    endHour: FETCH_RESULTS_WINDOW_END_HOUR,
    endMin: FETCH_RESULTS_WINDOW_END_MIN,
  });
  if (!inWindow) {
    return buildSkippedCheck(CHECK_FETCH_RESULTS_STALENESS);
  }
  return evaluateStalenessCheck({
    name: CHECK_FETCH_RESULTS_STALENESS,
    iso: input.metrics.lastSuccessfulFetchResultsAt,
    nowJst: input.nowJst,
    thresholdMin: FETCH_RESULTS_STALENESS_MIN,
  });
};

const evaluateFetchWeightsCheck = (input: CheckEvaluationInput): HealthCheck => {
  const inWindow = isWithinJstWindow({
    now: input.nowJst,
    startHour: FETCH_WEIGHTS_WINDOW_START_HOUR,
    startMin: FETCH_WEIGHTS_WINDOW_START_MIN,
    endHour: FETCH_WEIGHTS_WINDOW_END_HOUR,
    endMin: FETCH_WEIGHTS_WINDOW_END_MIN,
  });
  if (!inWindow) {
    return buildSkippedCheck(CHECK_FETCH_WEIGHTS_STALENESS);
  }
  return evaluateStalenessCheck({
    name: CHECK_FETCH_WEIGHTS_STALENESS,
    iso: input.metrics.lastSuccessfulFetchWeightsAt,
    nowJst: input.nowJst,
    thresholdMin: FETCH_WEIGHTS_STALENESS_MIN,
  });
};

const evaluateRacesQueuedCheck = (input: CheckEvaluationInput): HealthCheck =>
  evaluateCounterCheck({
    name: CHECK_RACES_QUEUED,
    value: input.metrics.racesQueuedNotFetchedToday,
    threshold: RACES_QUEUED_THRESHOLD,
  });

const evaluateRacesStuckCheck = (input: CheckEvaluationInput): HealthCheck =>
  evaluateCounterCheck({
    name: CHECK_RACES_STUCK,
    value: input.metrics.racesStuckOverThirtyMin,
    threshold: RACES_STUCK_THRESHOLD,
  });

export const evaluateChecks = (input: CheckEvaluationInput): HealthCheck[] => [
  evaluateFetchResultsCheck(input),
  evaluateFetchWeightsCheck(input),
  evaluateRacesQueuedCheck(input),
  evaluateRacesStuckCheck(input),
];
