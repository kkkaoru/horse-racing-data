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

export const getNextOddsFetchAt = (raceStartAt: string, nowMs = Date.now()): string | null => {
  const raceStartMs = new Date(raceStartAt).getTime();
  if (!Number.isFinite(raceStartMs)) {
    return null;
  }
  const minutesUntilRace = (raceStartMs - nowMs) / 60_000;
  const intervalMinutes = getOddsFetchIntervalMinutes(minutesUntilRace);
  if (!intervalMinutes) {
    return null;
  }
  const slotMinutesBeforeRace = Math.floor(minutesUntilRace / intervalMinutes) * intervalMinutes;
  return new Date(raceStartMs - slotMinutesBeforeRace * 60_000).toISOString();
};
