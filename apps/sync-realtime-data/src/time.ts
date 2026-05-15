const JST_TIME_ZONE = "Asia/Tokyo";
const JST_OFFSET_MS = 9 * 60 * 60 * 1000;

const jstFormatter = new Intl.DateTimeFormat("ja-JP-u-ca-gregory", {
  day: "2-digit",
  hour: "2-digit",
  hour12: false,
  minute: "2-digit",
  month: "2-digit",
  timeZone: JST_TIME_ZONE,
  year: "numeric",
});

const partsToRecord = (date: Date): Record<string, string> =>
  Object.fromEntries(jstFormatter.formatToParts(date).map((part) => [part.type, part.value]));

export const getJstDateParts = (date = new Date()) => {
  const parts = partsToRecord(date);
  const year = parts.year;
  const month = parts.month;
  const day = parts.day;
  const hour = parts.hour;
  const minute = parts.minute;
  return {
    day,
    hour,
    minute,
    month,
    year,
    yyyymmdd: `${year}${month}${day}`,
  };
};

export const getTodayJst = (date = new Date()): string => getJstDateParts(date).yyyymmdd;

export const toJstIsoString = (date = new Date()): string => {
  const jstDate = new Date(date.getTime() + JST_OFFSET_MS);
  return `${jstDate.toISOString().slice(0, 19)}+09:00`;
};

export const parseRaceStartJst = (
  year: string,
  monthDay: string,
  hhmm: string | null | undefined,
): Date | null => {
  if (!hhmm || !/^\d{4}$/.test(hhmm)) {
    return null;
  }
  const iso = `${year}-${monthDay.slice(0, 2)}-${monthDay.slice(2, 4)}T${hhmm.slice(0, 2)}:${hhmm.slice(2, 4)}:00+09:00`;
  const date = new Date(iso);
  return Number.isNaN(date.getTime()) ? null : date;
};

export const formatRaceStartJst = (year: string, monthDay: string, hhmm: string): string =>
  `${year}-${monthDay.slice(0, 2)}-${monthDay.slice(2, 4)}T${hhmm.slice(0, 2)}:${hhmm.slice(2, 4)}:00+09:00`;

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

export const getOddsFetchSlotAt = (raceStart: Date, now: Date): string | null => {
  const minutes = (raceStart.getTime() - now.getTime()) / 60_000;
  const interval = getOddsFetchIntervalMinutes(minutes);
  if (!interval) {
    return null;
  }
  const slotMinutesBeforeRace = Math.ceil(minutes / interval) * interval;
  return toJstIsoString(new Date(raceStart.getTime() - slotMinutesBeforeRace * 60_000));
};

const floorToHourJstSlot = (date: Date): string =>
  `${toJstIsoString(date).slice(0, 14)}00:00+09:00`;

export const getJraAdvanceOddsFetchSlotAt = (raceStart: Date, now: Date): string | null => {
  const raceDate = toJstIsoString(raceStart).slice(0, 10);
  const raceDayStart = new Date(`${raceDate}T00:00:00+09:00`);
  const saleStart = new Date(raceDayStart.getTime() - 5 * 60 * 60_000);
  const oneHourBeforeRace = new Date(raceStart.getTime() - 60 * 60_000);
  if (now.getTime() < saleStart.getTime() || now.getTime() >= oneHourBeforeRace.getTime()) {
    return null;
  }
  return floorToHourJstSlot(now);
};

export const isJstPollingWindow = (date = new Date()): boolean => {
  const { hour } = getJstDateParts(date);
  const parsedHour = Number(hour);
  return parsedHour >= 6 && parsedHour <= 21;
};
