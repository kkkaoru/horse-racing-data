import { type RaceSource } from "./codes";
import { cleanText } from "./format";

const SEX_LABELS: Record<string, string> = {
  "1": "牡",
  "2": "牝",
  "3": "セ",
};
const JRA_KEIBAJO_CODE_MAX: number = 10;
const NAR_KEIBAJO_CODE_MIN: number = 30;

export const isBanEiKeibajoCode = (value: string | null | undefined): boolean =>
  ["81", "82", "83", "84"].includes(cleanText(value, ""));

/** Overseas venues use alphabetic keibajo codes (e.g. "A6" = Ascot). */
export const isOverseasKeibajoCode = (value: string | null | undefined): boolean => {
  const cleaned = cleanText(value, "");
  return cleaned.length > 0 && !/^\d+$/u.test(cleaned);
};

export const inferRaceSourceFromKeibajoCode = (
  value: string | null | undefined,
): RaceSource | null => {
  if (isOverseasKeibajoCode(value)) {
    return "jra";
  }
  const parsed = Number(cleanText(value, ""));
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return null;
  }
  if (parsed <= JRA_KEIBAJO_CODE_MAX) {
    return "jra";
  }
  if (parsed >= NAR_KEIBAJO_CODE_MIN) {
    return "nar";
  }
  return null;
};

export const formatRunnerNumber = (value: string | null | undefined): string => {
  const parsed = Number(cleanText(value, ""));
  return Number.isFinite(parsed) && parsed > 0 ? String(parsed) : "-";
};

export const formatSexAge = (
  sexCode: string | null | undefined,
  age: string | null | undefined,
): string => {
  const sex = SEX_LABELS[cleanText(sexCode, "")] ?? "";
  const parsedAge = Number(cleanText(age, ""));
  const ageLabel = Number.isFinite(parsedAge) && parsedAge > 0 ? `${parsedAge}歳` : "";

  if (sex && ageLabel) {
    return `${sex} / ${ageLabel}`;
  }
  return sex || ageLabel || "-";
};

export const formatHorseWeight = (
  weight: string | null | undefined,
  sign: string | null | undefined,
  diff: string | null | undefined,
  decodeHex = false,
): string => {
  const cleanWeight = cleanText(weight, "");
  if (!cleanWeight || cleanWeight === "000" || cleanWeight.toUpperCase() === "FFF") {
    return "-";
  }

  const cleanDiff = cleanText(diff, "");
  const cleanSign = cleanText(sign, "");
  const parsedWeight = decodeHex ? Number.parseInt(cleanWeight, 16) : Number(cleanWeight);
  const displayWeight = Number.isFinite(parsedWeight) ? String(parsedWeight) : cleanWeight;
  if (!cleanDiff || cleanDiff === "000" || cleanDiff.toUpperCase() === "FFF") {
    return `${displayWeight}kg`;
  }

  const parsedDiff = decodeHex ? Number.parseInt(cleanDiff, 16) : Number(cleanDiff);
  const displayDiff = Number.isFinite(parsedDiff) ? String(parsedDiff) : String(Number(cleanDiff));
  return `${displayWeight}kg (${cleanSign}${displayDiff})`;
};

export const formatCarriedWeight = (
  weight: string | null | undefined,
  decodeHex = false,
): string => {
  const cleaned = cleanText(weight, "");
  if (!cleaned || cleaned.toUpperCase() === "FFF") {
    return "-";
  }

  const parsed = decodeHex ? Number.parseInt(cleaned, 16) : Number(cleaned);
  if (!Number.isFinite(parsed)) {
    return cleaned;
  }
  return decodeHex ? String(parsed) : (parsed / 10).toFixed(1);
};

export const formatRunnerValue = (value: string | null | undefined, emptyValue: string): string => {
  const cleaned = cleanText(value, "");
  return cleaned === emptyValue ? "-" : cleanText(value);
};
