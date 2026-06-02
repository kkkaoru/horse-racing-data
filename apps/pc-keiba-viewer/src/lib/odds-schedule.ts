import type { RaceSource } from "./codes";

const JST_OFFSET_MS = 9 * 60 * 60_000;
const HOUR_MS = 60 * 60_000;
const HOURLY_THRESHOLD_MINUTES = 60;
const FIVE_MIN_THRESHOLD_MINUTES = 15;
const ONE_MIN_THRESHOLD_MINUTES = 1;
const HOURLY_INTERVAL_MINUTES = 60;
const FIVE_MIN_INTERVAL_MINUTES = 5;
const ONE_MIN_INTERVAL_MINUTES = 1;

export const NAR_ODDS_SALE_START_RULE = {
  createdAt: "2026-05-22",
  defaultSaleStartHhmm: "1000",
  id: "nar-same-day-venue-sale-start",
  nightRaceExceptionKeibajoCodes: ["48", "54"],
  nightRaceLastStartThresholdHhmm: "1900",
  nightRaceSaleStartHhmm: "1200",
} as const;

interface OddsScheduleOptions {
  keibajoCode?: string | null;
  venueLastRaceStartAt?: string | null;
}

export const getOddsFetchIntervalMinutes = (minutesUntilRace: number): number | null => {
  if (minutesUntilRace >= HOURLY_THRESHOLD_MINUTES) return HOURLY_INTERVAL_MINUTES;
  if (minutesUntilRace >= FIVE_MIN_THRESHOLD_MINUTES) return FIVE_MIN_INTERVAL_MINUTES;
  if (minutesUntilRace >= ONE_MIN_THRESHOLD_MINUTES) return ONE_MIN_INTERVAL_MINUTES;
  return null;
};

const getJstDateString = (date: Date): string =>
  new Date(date.getTime() + JST_OFFSET_MS).toISOString().slice(0, 10);

const ceilToJstHour = (date: Date): Date =>
  new Date(Math.ceil((date.getTime() + JST_OFFSET_MS) / HOUR_MS) * HOUR_MS - JST_OFFSET_MS);

const floorToHourlySlotFromStart = (date: Date, start: Date): Date =>
  new Date(start.getTime() + Math.floor((date.getTime() - start.getTime()) / HOUR_MS) * HOUR_MS);

const ceilToHourlySlotFromStart = (date: Date, start: Date): Date =>
  new Date(start.getTime() + Math.ceil((date.getTime() - start.getTime()) / HOUR_MS) * HOUR_MS);

const getNextJraAdvanceOddsFetchAt = (raceStart: Date, nowMs: number): string | null => {
  const raceDate = getJstDateString(raceStart);
  const raceDayStart = new Date(`${raceDate}T00:00:00+09:00`);
  const saleStart = new Date(raceDayStart.getTime() - 5 * HOUR_MS);
  const oneHourBeforeRace = new Date(raceStart.getTime() - HOUR_MS);
  if (nowMs < saleStart.getTime()) {
    return saleStart.toISOString();
  }
  if (nowMs >= oneHourBeforeRace.getTime()) {
    return null;
  }
  const nextHourlySlot = ceilToJstHour(new Date(nowMs));
  return new Date(Math.min(nextHourlySlot.getTime(), oneHourBeforeRace.getTime())).toISOString();
};

const getHhmmFromRaceStart = (raceStartAt: string | null | undefined): string | null => {
  const hhmm = raceStartAt?.slice(11, 16).replace(":", "") ?? null;
  return hhmm && /^\d{4}$/u.test(hhmm) ? hhmm : null;
};

const getNarOddsSaleStartAt = (
  raceStart: Date,
  options: OddsScheduleOptions | undefined,
): Date | null => {
  const raceDate = getJstDateString(raceStart);
  const venueLastHhmm = getHhmmFromRaceStart(options?.venueLastRaceStartAt);
  const isNightRace =
    Boolean(
      venueLastHhmm && venueLastHhmm >= NAR_ODDS_SALE_START_RULE.nightRaceLastStartThresholdHhmm,
    ) &&
    !(NAR_ODDS_SALE_START_RULE.nightRaceExceptionKeibajoCodes as readonly string[]).includes(
      options?.keibajoCode ?? "",
    );
  const hhmm = isNightRace
    ? NAR_ODDS_SALE_START_RULE.nightRaceSaleStartHhmm
    : NAR_ODDS_SALE_START_RULE.defaultSaleStartHhmm;
  const saleStart = new Date(`${raceDate}T${hhmm.slice(0, 2)}:${hhmm.slice(2, 4)}:00+09:00`);
  return Number.isNaN(saleStart.getTime()) ? null : saleStart;
};

const getNextNarOddsFetchAt = (
  raceStart: Date,
  nowMs: number,
  options: OddsScheduleOptions | undefined,
): string | null => {
  const saleStart = getNarOddsSaleStartAt(raceStart, options);
  if (!saleStart) {
    return null;
  }
  if (nowMs < saleStart.getTime()) {
    return saleStart.toISOString();
  }
  const oneHourBeforeRace = new Date(raceStart.getTime() - HOUR_MS);
  if (nowMs < oneHourBeforeRace.getTime()) {
    const nextHourlySlot = floorToHourlySlotFromStart(new Date(nowMs), saleStart);
    if (nextHourlySlot.getTime() <= nowMs) {
      const followingHourlySlot = ceilToHourlySlotFromStart(new Date(nowMs + 1), saleStart);
      return new Date(
        Math.min(followingHourlySlot.getTime(), oneHourBeforeRace.getTime()),
      ).toISOString();
    }
    return new Date(Math.min(nextHourlySlot.getTime(), oneHourBeforeRace.getTime())).toISOString();
  }
  return null;
};

export const getNextOddsFetchAt = (
  raceStartAt: string,
  nowMs = Date.now(),
  source?: RaceSource,
  options?: OddsScheduleOptions,
): string | null => {
  const raceStartMs = new Date(raceStartAt).getTime();
  if (!Number.isFinite(raceStartMs)) {
    return null;
  }
  if (source === "jra") {
    const jraAdvanceFetchAt = getNextJraAdvanceOddsFetchAt(new Date(raceStartMs), nowMs);
    if (jraAdvanceFetchAt) {
      return jraAdvanceFetchAt;
    }
  }
  if (source === "nar") {
    const narAdvanceFetchAt = getNextNarOddsFetchAt(new Date(raceStartMs), nowMs, options);
    if (narAdvanceFetchAt) {
      return narAdvanceFetchAt;
    }
  }
  const minutesUntilRace = (raceStartMs - nowMs) / 60_000;
  const intervalMinutes = getOddsFetchIntervalMinutes(minutesUntilRace);
  if (!intervalMinutes) {
    return null;
  }
  const slotMinutesBeforeRace = Math.floor(minutesUntilRace / intervalMinutes) * intervalMinutes;
  return new Date(raceStartMs - slotMinutesBeforeRace * 60_000).toISOString();
};
