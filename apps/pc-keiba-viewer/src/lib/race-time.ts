// This file runs with bun.

import { cleanText } from "./format";

const ENCODED_RACE_TIME_PATTERN: RegExp = /^\d{1,4}$/u;
const TENTHS_PER_SECOND: number = 10;
const SECONDS_PER_MINUTE: number = 60;
const TENTHS_PER_MINUTE: number = SECONDS_PER_MINUTE * TENTHS_PER_SECOND;
const PADDED_RACE_TIME_LENGTH: number = 4;
const SECONDS_START_INDEX: number = 1;
const TENTHS_INDEX: number = 3;

export const parseEncodedRaceTimeTenths = (value: string | null | undefined): number | null => {
  const cleaned = cleanText(value, "");
  if (!ENCODED_RACE_TIME_PATTERN.test(cleaned) || /^0+$/u.test(cleaned)) {
    return null;
  }
  const padded = cleaned.padStart(PADDED_RACE_TIME_LENGTH, "0");
  const minutes = Number(padded.slice(0, SECONDS_START_INDEX));
  const seconds = Number(padded.slice(SECONDS_START_INDEX, TENTHS_INDEX));
  const tenths = Number(padded.slice(TENTHS_INDEX));
  return seconds < SECONDS_PER_MINUTE
    ? minutes * TENTHS_PER_MINUTE + seconds * TENTHS_PER_SECOND + tenths
    : null;
};

export const formatRaceTimeTenths = (tenths: number | null): string => {
  if (tenths === null || !Number.isFinite(tenths) || tenths <= 0) {
    return "-";
  }
  const rounded = Math.round(tenths);
  const minutes = Math.floor(rounded / TENTHS_PER_MINUTE);
  const seconds = Math.floor((rounded % TENTHS_PER_MINUTE) / TENTHS_PER_SECOND);
  const remainder = rounded % TENTHS_PER_SECOND;
  return minutes > 0
    ? `${minutes}:${String(seconds).padStart(2, "0")}.${remainder}`
    : `${seconds}.${remainder}`;
};

export const formatEncodedRaceTime = (value: string | null | undefined): string =>
  formatRaceTimeTenths(parseEncodedRaceTimeTenths(value));
