import type { RaceSource } from "./codes";

const JST_OFFSET_MS = 9 * 60 * 60_000;
const HOUR_MS = 60 * 60_000;

export const getOddsFetchIntervalMinutes = (minutesUntilRace: number): number | null => {
  if (minutesUntilRace >= 60) {
    return 60;
  }
  if (minutesUntilRace >= 10) {
    return 10;
  }
  if (minutesUntilRace >= 1) {
    return 1;
  }
  return null;
};

const getJstDateString = (date: Date): string =>
  new Date(date.getTime() + JST_OFFSET_MS).toISOString().slice(0, 10);

const ceilToJstHour = (date: Date): Date =>
  new Date(Math.ceil((date.getTime() + JST_OFFSET_MS) / HOUR_MS) * HOUR_MS - JST_OFFSET_MS);

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

export const getNextOddsFetchAt = (
  raceStartAt: string,
  nowMs = Date.now(),
  source?: RaceSource,
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
  const minutesUntilRace = (raceStartMs - nowMs) / 60_000;
  const intervalMinutes = getOddsFetchIntervalMinutes(minutesUntilRace);
  if (!intervalMinutes) {
    return null;
  }
  const slotMinutesBeforeRace = Math.floor(minutesUntilRace / intervalMinutes) * intervalMinutes;
  return new Date(raceStartMs - slotMinutesBeforeRace * 60_000).toISOString();
};
