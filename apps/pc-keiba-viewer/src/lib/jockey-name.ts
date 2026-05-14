import { cleanText } from "./format";

const SAME_JOCKEY_PREFIX_LENGTH = 3;

export const normalizeJockeyNameForComparison = (value: string | null | undefined): string => {
  return cleanText(value, "").replace(/\s+/gu, "");
};

const getJockeyNamePrefix = (value: string): string =>
  Array.from(value).slice(0, SAME_JOCKEY_PREFIX_LENGTH).join("");

export const isSameJockeyName = (
  left: string | null | undefined,
  right: string | null | undefined,
): boolean => {
  const normalizedLeft = normalizeJockeyNameForComparison(left);
  const normalizedRight = normalizeJockeyNameForComparison(right);
  if (normalizedLeft === "" || normalizedRight === "") {
    return false;
  }
  if (normalizedLeft === normalizedRight) {
    return true;
  }
  if (
    Array.from(normalizedLeft).length < SAME_JOCKEY_PREFIX_LENGTH ||
    Array.from(normalizedRight).length < SAME_JOCKEY_PREFIX_LENGTH
  ) {
    return false;
  }
  return getJockeyNamePrefix(normalizedLeft) === getJockeyNamePrefix(normalizedRight);
};
