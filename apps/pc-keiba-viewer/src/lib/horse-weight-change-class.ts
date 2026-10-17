// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)
import { cleanText } from "./format";
import type { LiveHorseWeight } from "./horse-weight-class";
import { formatRunnerNumber, isBanEiKeibajoCode } from "./runner-format";

export interface HorseWeightChangeClass {
  key: string;
  label: string;
  minKg: number | null;
}

export interface ParseHorseWeightChangeKgInput {
  keibajoCode: string | null | undefined;
  zogenFugo: string | null | undefined;
  zogenSa: string | null | undefined;
}

export interface ResolveCurrentHorseWeightChangeKgInput extends ParseHorseWeightChangeKgInput {
  horseNumber: string;
  liveWeightChangeKgByHorse: Map<string, number> | undefined;
}

const NEGATIVE_SIGN = "-";
const HEX_RADIX = 16;
// Ban-ei stores the change as hex ('00A' = 10kg) and marks unmeasured as FFF;
// JRA and NAR store decimal kilograms and mark unmeasured as 999. Blank is a
// debut or a missing weigh-in. Mirrors the R2 Catalog weight-change stats SQL.
const BAN_EI_WEIGHT_CHANGE_PATTERN: RegExp = /^[0-9A-F]{1,3}$/u;
const DECIMAL_WEIGHT_CHANGE_PATTERN: RegExp = /^\d{1,3}$/u;
const BAN_EI_UNMEASURED_WEIGHT_CHANGE = "FFF";
const DECIMAL_UNMEASURED_WEIGHT_CHANGE = "999";

const WEIGHT_CHANGE_CLASS_MINUS_20: HorseWeightChangeClass = {
  key: "minus20",
  label: "-20kg未満",
  minKg: null,
};

// Ordered from the highest lower bound; each class covers [minKg, previous minKg).
export const HORSE_WEIGHT_CHANGE_CLASSES: readonly HorseWeightChangeClass[] = [
  { key: "plus20", label: "+20kg以上", minKg: 20 },
  { key: "plus10", label: "+20kg未満+10kg以上", minKg: 10 },
  { key: "plus5", label: "+10kg未満+5kg以上", minKg: 5 },
  { key: "plus2", label: "+5kg未満+2kg以上", minKg: 2 },
  { key: "even", label: "+2kg未満-2kg以上", minKg: -2 },
  { key: "minus2", label: "-2kg未満-5kg以上", minKg: -5 },
  { key: "minus5", label: "-5kg未満-10kg以上", minKg: -10 },
  { key: "minus10", label: "-10kg未満-20kg以上", minKg: -20 },
  WEIGHT_CHANGE_CLASS_MINUS_20,
];

export const parseHorseWeightChangeKg = (input: ParseHorseWeightChangeKgInput): number | null => {
  const cleaned = cleanText(input.zogenSa, "").toUpperCase();
  const banEi = isBanEiKeibajoCode(input.keibajoCode);
  const valid = banEi
    ? BAN_EI_WEIGHT_CHANGE_PATTERN.test(cleaned) && cleaned !== BAN_EI_UNMEASURED_WEIGHT_CHANGE
    : DECIMAL_WEIGHT_CHANGE_PATTERN.test(cleaned) && cleaned !== DECIMAL_UNMEASURED_WEIGHT_CHANGE;
  if (!valid) {
    return null;
  }
  const magnitude = banEi ? Number.parseInt(cleaned, HEX_RADIX) : Number(cleaned);
  return cleanText(input.zogenFugo, "") === NEGATIVE_SIGN ? -magnitude : magnitude;
};

export const getHorseWeightChangeClass = (changeKg: number): HorseWeightChangeClass =>
  HORSE_WEIGHT_CHANGE_CLASSES.find(
    (changeClass) => changeClass.minKg !== null && changeKg >= changeClass.minKg,
  ) ?? WEIGHT_CHANGE_CLASS_MINUS_20;

// A live entry without a weight is a scratch and one without an amount is a
// debut; neither has a change. The amount is unsigned and the sign is '-' only
// for losses (a null sign with amount 0 is "±0").
const liveWeightChangeKg = (horse: LiveHorseWeight): number | null => {
  if (horse.weight === null || horse.changeAmount === undefined || horse.changeAmount === null) {
    return null;
  }
  if (!Number.isFinite(horse.changeAmount)) {
    return null;
  }
  return horse.changeSign === NEGATIVE_SIGN ? -horse.changeAmount : horse.changeAmount;
};

export const indexLiveHorseWeightChangeKg = (
  horses: readonly LiveHorseWeight[],
): Map<string, number> =>
  horses.reduce((index, horse) => {
    const changeKg = liveWeightChangeKg(horse);
    const horseNumber = formatRunnerNumber(horse.horseNumber);
    return changeKg === null || horseNumber === "-"
      ? index
      : new Map(index).set(horseNumber, changeKg);
  }, new Map<string, number>());

export const resolveCurrentHorseWeightChangeKg = (
  input: ResolveCurrentHorseWeightChangeKgInput,
): number | null =>
  input.liveWeightChangeKgByHorse?.get(formatRunnerNumber(input.horseNumber)) ??
  parseHorseWeightChangeKg(input);
