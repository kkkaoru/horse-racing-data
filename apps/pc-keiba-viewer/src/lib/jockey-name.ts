import { cleanText } from "./format";

const SAME_JOCKEY_PREFIX_LENGTH = 3;
const JOCKEY_NAME_CHARACTER_REPLACEMENTS: Record<string, string> = {
  櫻: "桜",
  邊: "辺",
  邉: "辺",
};
const JOCKEY_NAME_ALIASES: Record<string, string> = {
  デム: "デムーロ",
};

export const normalizeJockeyNameForDisplay = (value: string | null | undefined): string => {
  const cleaned = cleanText(value, "")
    .replace(/^[牡牝騙せセ]\d+\/[^\s　]+\s*[0-9]+(?:\.[0-9]+)?kg\s*/u, "")
    .replace(/^[A-Za-zＡ-Ｚａ-ｚ][.．]/u, "")
    .replace(/[△▲☆★◇◆□■▽▼]/gu, "")
    .replace(/[\s\p{Separator}\u200B-\u200D\uFEFF]+/gu, "");
  return cleaned;
};

export const normalizeJockeyNameForComparison = (value: string | null | undefined): string => {
  const normalized = Array.from(normalizeJockeyNameForDisplay(value))
    .map((character) => JOCKEY_NAME_CHARACTER_REPLACEMENTS[character] ?? character)
    .join("");
  return JOCKEY_NAME_ALIASES[normalized] ?? normalized;
};

const getJockeyNamePrefix = (value: string): string =>
  Array.from(value).slice(0, SAME_JOCKEY_PREFIX_LENGTH).join("");

const isHanCharacter = (value: string): boolean => /\p{Script=Han}/u.test(value);

const hasSameLocalKeibaAbbreviatedName = (left: string, right: string): boolean => {
  const leftCharacters = Array.from(left);
  const rightCharacters = Array.from(right);
  if (leftCharacters.length < 3 || rightCharacters.length < 3) {
    return false;
  }
  const leftPrefix = leftCharacters.slice(0, 2);
  const rightPrefix = rightCharacters.slice(0, 2);
  return (
    leftPrefix.every(isHanCharacter) &&
    rightPrefix.every(isHanCharacter) &&
    leftPrefix.join("") === rightPrefix.join("") &&
    leftCharacters.at(-1) === rightCharacters.at(-1)
  );
};

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
  if (hasSameLocalKeibaAbbreviatedName(normalizedLeft, normalizedRight)) {
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

export const getPreferredJockeyName = (
  storedName: string | null | undefined,
  realtimeName: string | null | undefined,
): string => {
  const cleanedStoredName = normalizeJockeyNameForDisplay(storedName);
  const cleanedRealtimeName = normalizeJockeyNameForDisplay(realtimeName);
  if (cleanedStoredName !== "" && isSameJockeyName(cleanedStoredName, cleanedRealtimeName)) {
    return cleanedStoredName;
  }
  return cleanedRealtimeName || cleanedStoredName;
};
