// bun run

import type { RaceSource } from "./codes";
import { getNextOddsFetchAt } from "./odds-schedule";

export type ScheduleTaskKind =
  | "odds"
  | "horse-weight"
  | "paddock"
  | "result"
  | "trend-cache-warm"
  | "running-style-features"
  | "running-style";

export interface RaceScheduleSourceRace {
  source: RaceSource;
  raceStartAt: string;
  keibajoCode: string;
}

export interface RaceScheduleVenueContext {
  venueLastRaceStartAt?: string | null;
}

export interface RaceScheduleSlot<TRace extends RaceScheduleSourceRace> {
  kind: ScheduleTaskKind;
  label: string;
  race: TRace;
  scheduledAt: string;
}

export const SCHEDULE_TASK_KINDS: readonly ScheduleTaskKind[] = [
  "odds",
  "horse-weight",
  "paddock",
  "result",
  "trend-cache-warm",
  "running-style-features",
  "running-style",
];

export const SCHEDULE_TASK_LABELS: Record<ScheduleTaskKind, string> = {
  odds: "オッズ更新",
  "horse-weight": "馬体重取得",
  paddock: "パドック取得",
  result: "結果取得",
  "trend-cache-warm": "トレンドキャッシュ温め",
  "running-style-features": "脚質特徴量生成",
  "running-style": "脚質予測生成",
};

const MINUTE_MS = 60_000;
const THREE_MINUTE_MS = 3 * MINUTE_MS;
const HORSE_WEIGHT_WINDOW_BEFORE_MINUTES = 20;
const PADDOCK_WINDOW_BEFORE_MINUTES = 35;
const PADDOCK_WINDOW_AFTER_MINUTES = 2;
const RESULT_INTERVAL_MINUTES = 5;
const RESULT_MAX_SLOTS = 3;
const TREND_CACHE_WARM_BEFORE_MINUTES = 20;
const RUNNING_STYLE_FEATURES_BEFORE_MINUTES = 90;
const RUNNING_STYLE_BEFORE_MINUTES = 60;
const MAX_ODDS_SLOTS = 64;

const getRaceStartMs = (race: RaceScheduleSourceRace): number =>
  new Date(race.raceStartAt).getTime();

const ceilToTickMs = (timeMs: number, tickMs: number): number =>
  Math.ceil(timeMs / tickMs) * tickMs;

const buildSlot = <TRace extends RaceScheduleSourceRace>(
  kind: ScheduleTaskKind,
  race: TRace,
  scheduledAtMs: number,
): RaceScheduleSlot<TRace> => ({
  kind,
  label: SCHEDULE_TASK_LABELS[kind],
  race,
  scheduledAt: new Date(scheduledAtMs).toISOString(),
});

const enumerateOddsSlots = <TRace extends RaceScheduleSourceRace>(
  race: TRace,
  venue: RaceScheduleVenueContext,
): RaceScheduleSlot<TRace>[] => {
  const raceStartMs = getRaceStartMs(race);
  if (!Number.isFinite(raceStartMs)) {
    return [];
  }
  const slots: RaceScheduleSlot<TRace>[] = [];
  const startScanFromMs = raceStartMs - 30 * 60 * MINUTE_MS;
  let cursorMs = startScanFromMs;
  while (slots.length < MAX_ODDS_SLOTS) {
    const next = getNextOddsFetchAt(race.raceStartAt, cursorMs, race.source, {
      keibajoCode: race.keibajoCode,
      venueLastRaceStartAt: venue.venueLastRaceStartAt ?? null,
    });
    if (!next) {
      break;
    }
    const nextMs = new Date(next).getTime();
    if (nextMs <= cursorMs - 1) {
      break;
    }
    slots.push(buildSlot("odds", race, nextMs));
    cursorMs = nextMs + 1;
  }
  return slots;
};

const enumerateThreeMinuteSlotsInWindow = <TRace extends RaceScheduleSourceRace>(
  kind: ScheduleTaskKind,
  race: TRace,
  windowStartMs: number,
  windowEndMs: number,
): RaceScheduleSlot<TRace>[] => {
  if (windowEndMs <= windowStartMs) {
    return [];
  }
  const slots: RaceScheduleSlot<TRace>[] = [];
  let cursorMs = ceilToTickMs(windowStartMs, THREE_MINUTE_MS);
  while (cursorMs <= windowEndMs) {
    slots.push(buildSlot(kind, race, cursorMs));
    cursorMs += THREE_MINUTE_MS;
  }
  return slots;
};

const enumerateHorseWeightSlots = <TRace extends RaceScheduleSourceRace>(
  race: TRace,
): RaceScheduleSlot<TRace>[] => {
  const raceStartMs = getRaceStartMs(race);
  if (!Number.isFinite(raceStartMs)) {
    return [];
  }
  return enumerateThreeMinuteSlotsInWindow(
    "horse-weight",
    race,
    raceStartMs - HORSE_WEIGHT_WINDOW_BEFORE_MINUTES * MINUTE_MS,
    raceStartMs - MINUTE_MS,
  );
};

const enumeratePaddockSlots = <TRace extends RaceScheduleSourceRace>(
  race: TRace,
): RaceScheduleSlot<TRace>[] => {
  if (race.source !== "jra") {
    return [];
  }
  const raceStartMs = getRaceStartMs(race);
  if (!Number.isFinite(raceStartMs)) {
    return [];
  }
  return enumerateThreeMinuteSlotsInWindow(
    "paddock",
    race,
    raceStartMs - PADDOCK_WINDOW_BEFORE_MINUTES * MINUTE_MS,
    raceStartMs + PADDOCK_WINDOW_AFTER_MINUTES * MINUTE_MS,
  );
};

const enumerateResultSlots = <TRace extends RaceScheduleSourceRace>(
  race: TRace,
): RaceScheduleSlot<TRace>[] => {
  const raceStartMs = getRaceStartMs(race);
  if (!Number.isFinite(raceStartMs)) {
    return [];
  }
  return Array.from({ length: RESULT_MAX_SLOTS }, (_unused, index) =>
    buildSlot("result", race, raceStartMs + index * RESULT_INTERVAL_MINUTES * MINUTE_MS),
  );
};

const enumerateTrendCacheWarmSlots = <TRace extends RaceScheduleSourceRace>(
  race: TRace,
): RaceScheduleSlot<TRace>[] => {
  const raceStartMs = getRaceStartMs(race);
  if (!Number.isFinite(raceStartMs)) {
    return [];
  }
  return [
    buildSlot("trend-cache-warm", race, raceStartMs - TREND_CACHE_WARM_BEFORE_MINUTES * MINUTE_MS),
  ];
};

const enumerateRunningStyleFeaturesSlots = <TRace extends RaceScheduleSourceRace>(
  race: TRace,
): RaceScheduleSlot<TRace>[] => {
  const raceStartMs = getRaceStartMs(race);
  if (!Number.isFinite(raceStartMs)) {
    return [];
  }
  return [
    buildSlot(
      "running-style-features",
      race,
      raceStartMs - RUNNING_STYLE_FEATURES_BEFORE_MINUTES * MINUTE_MS,
    ),
  ];
};

const enumerateRunningStyleSlots = <TRace extends RaceScheduleSourceRace>(
  race: TRace,
): RaceScheduleSlot<TRace>[] => {
  const raceStartMs = getRaceStartMs(race);
  if (!Number.isFinite(raceStartMs)) {
    return [];
  }
  return [buildSlot("running-style", race, raceStartMs - RUNNING_STYLE_BEFORE_MINUTES * MINUTE_MS)];
};

export const enumerateRaceScheduleSlots = <TRace extends RaceScheduleSourceRace>(
  race: TRace,
  venue: RaceScheduleVenueContext = {},
): RaceScheduleSlot<TRace>[] => [
  ...enumerateOddsSlots(race, venue),
  ...enumerateHorseWeightSlots(race),
  ...enumeratePaddockSlots(race),
  ...enumerateResultSlots(race),
  ...enumerateTrendCacheWarmSlots(race),
  ...enumerateRunningStyleFeaturesSlots(race),
  ...enumerateRunningStyleSlots(race),
];

const compareSlotsByScheduledAt = <TRace extends RaceScheduleSourceRace>(
  left: RaceScheduleSlot<TRace>,
  right: RaceScheduleSlot<TRace>,
): number =>
  new Date(left.scheduledAt).getTime() - new Date(right.scheduledAt).getTime() ||
  getRaceStartMs(left.race) - getRaceStartMs(right.race);

export const buildSortedRaceScheduleSlots = <TRace extends RaceScheduleSourceRace>(
  races: readonly TRace[],
  getVenueContext: (race: TRace) => RaceScheduleVenueContext = () => ({}),
): RaceScheduleSlot<TRace>[] =>
  races
    .flatMap((race) => enumerateRaceScheduleSlots(race, getVenueContext(race)))
    .toSorted(compareSlotsByScheduledAt);
