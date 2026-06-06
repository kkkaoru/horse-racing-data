const JST_TIME_ZONE = "Asia/Tokyo";
const JST_OFFSET_MS = 9 * 60 * 60 * 1000;
const FINAL_ODDS_FETCH_DELAY_MINUTES = 2;
const ONE_HOUR_MS = 60 * 60_000;
const JST_POLLING_HOUR_START = 6;
const JST_POLLING_HOUR_END = 22;

export const NAR_ODDS_SALE_START_RULE = {
  createdAt: "2026-05-22",
  defaultSaleStartHhmm: "1000",
  id: "nar-same-day-venue-sale-start",
  nightRaceExceptionKeibajoCodes: ["48", "54"],
  nightRaceLastStartThresholdHhmm: "1900",
  nightRaceSaleStartHhmm: "1200",
} as const;

interface NarOddsSaleStartInput {
  keibajoCode: string;
  raceStartAtJst: string;
  venueLastRaceStartAtJst?: string | null;
}

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

const extractHhmmFromJstIso = (value: string | null | undefined): string | null => {
  const hhmm = value?.slice(11, 16).replace(":", "") ?? null;
  return hhmm && /^\d{4}$/u.test(hhmm) ? hhmm : null;
};

export const isNarNightRaceMeeting = (venueLastRaceStartAtJst: string | null | undefined) => {
  const hhmm = extractHhmmFromJstIso(venueLastRaceStartAtJst);
  return Boolean(hhmm && hhmm >= NAR_ODDS_SALE_START_RULE.nightRaceLastStartThresholdHhmm);
};

export const getNarOddsSaleStartAt = ({
  keibajoCode,
  raceStartAtJst,
  venueLastRaceStartAtJst,
}: NarOddsSaleStartInput): Date | null => {
  const raceDate = raceStartAtJst.slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/u.test(raceDate)) {
    return null;
  }
  const isNightRace =
    isNarNightRaceMeeting(venueLastRaceStartAtJst) &&
    !(NAR_ODDS_SALE_START_RULE.nightRaceExceptionKeibajoCodes as readonly string[]).includes(
      keibajoCode,
    );
  const hhmm = isNightRace
    ? NAR_ODDS_SALE_START_RULE.nightRaceSaleStartHhmm
    : NAR_ODDS_SALE_START_RULE.defaultSaleStartHhmm;
  const saleStart = new Date(`${raceDate}T${hhmm.slice(0, 2)}:${hhmm.slice(2, 4)}:00+09:00`);
  return Number.isNaN(saleStart.getTime()) ? null : saleStart;
};

const floorToHourlySlotFromStart = (date: Date, start: Date): Date =>
  new Date(
    start.getTime() + Math.floor((date.getTime() - start.getTime()) / ONE_HOUR_MS) * ONE_HOUR_MS,
  );

const ceilToNextHourlySlotFromStart = (date: Date, start: Date): Date =>
  new Date(
    start.getTime() +
      (Math.floor((date.getTime() - start.getTime()) / ONE_HOUR_MS) + 1) * ONE_HOUR_MS,
  );

export const getNarOddsFetchSlotAt = (
  raceStart: Date,
  now: Date,
  saleStart: Date | null,
): string | null => {
  if (!saleStart || now.getTime() < saleStart.getTime()) {
    return null;
  }
  const oneHourBeforeRace = new Date(raceStart.getTime() - ONE_HOUR_MS);
  if (now.getTime() < oneHourBeforeRace.getTime()) {
    return toJstIsoString(floorToHourlySlotFromStart(now, saleStart));
  }
  return getOddsFetchSlotAt(raceStart, now);
};

export const getOddsFetchSlotAt = (raceStart: Date, now: Date): string | null => {
  const minutes = (raceStart.getTime() - now.getTime()) / 60_000;
  const interval = getOddsFetchIntervalMinutes(minutes);
  if (interval) {
    const slotMinutesBeforeRace = Math.ceil(minutes / interval) * interval;
    return toJstIsoString(new Date(raceStart.getTime() - slotMinutesBeforeRace * 60_000));
  }

  const finalOddsSlot = new Date(raceStart.getTime() + FINAL_ODDS_FETCH_DELAY_MINUTES * 60_000);
  return now.getTime() >= finalOddsSlot.getTime() ? toJstIsoString(finalOddsSlot) : null;
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

const ceilToNextHourJstSlot = (date: Date): Date => {
  const current = toJstIsoString(date);
  const floored = new Date(`${current.slice(0, 14)}00:00+09:00`);
  return floored.getTime() > date.getTime() ? floored : new Date(floored.getTime() + 60 * 60_000);
};

export const getNextOddsFetchSlotAt = (
  raceStart: Date,
  now: Date,
  source: "jra" | "nar",
  options: { narSaleStartAt?: Date | null } = {},
): string | null => {
  const raceStartMs = raceStart.getTime();
  if (source === "jra") {
    const raceDate = toJstIsoString(raceStart).slice(0, 10);
    const raceDayStart = new Date(`${raceDate}T00:00:00+09:00`);
    const saleStart = new Date(raceDayStart.getTime() - 5 * 60 * 60_000);
    const oneHourBeforeRace = new Date(raceStartMs - 60 * 60_000);
    if (now.getTime() < oneHourBeforeRace.getTime()) {
      const nextAdvanceSlot =
        now.getTime() < saleStart.getTime() ? saleStart : ceilToNextHourJstSlot(now);
      return nextAdvanceSlot.getTime() < oneHourBeforeRace.getTime()
        ? toJstIsoString(nextAdvanceSlot)
        : toJstIsoString(oneHourBeforeRace);
    }
  }
  if (source === "nar" && options.narSaleStartAt) {
    const saleStart = options.narSaleStartAt;
    const oneHourBeforeRace = new Date(raceStartMs - ONE_HOUR_MS);
    if (now.getTime() < saleStart.getTime()) {
      return toJstIsoString(saleStart);
    }
    if (now.getTime() < oneHourBeforeRace.getTime()) {
      const nextHourlySlot = ceilToNextHourlySlotFromStart(now, saleStart);
      return nextHourlySlot.getTime() < oneHourBeforeRace.getTime()
        ? toJstIsoString(nextHourlySlot)
        : toJstIsoString(oneHourBeforeRace);
    }
  }

  const regularOffsets = [60, 50, 40, 30, 20, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, -2];
  const nextSlot = regularOffsets
    .map((minutesBeforeRace) => new Date(raceStartMs - minutesBeforeRace * 60_000))
    .find((slot) => slot.getTime() > now.getTime());
  return nextSlot ? toJstIsoString(nextSlot) : null;
};

export const isJstPollingWindow = (date = new Date()): boolean => {
  const { hour } = getJstDateParts(date);
  const parsedHour = Number(hour);
  return parsedHour >= JST_POLLING_HOUR_START && parsedHour <= JST_POLLING_HOUR_END;
};
