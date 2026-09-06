// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)
import { cleanText } from "./format";
import { formatRunnerNumber, isBanEiKeibajoCode } from "./runner-format";

export interface HorseWeightClass {
  key: string;
  label: string;
  maxKg: number | null;
  minKg: number | null;
}

export interface ParseHorseWeightKgInput {
  bataiju: string | null | undefined;
  keibajoCode: string | null | undefined;
}

export interface LiveHorseWeight {
  horseNumber: string;
  weight: number | null;
}

export interface ResolveCurrentHorseWeightKgInput {
  bataiju: string | null | undefined;
  horseNumber: string;
  keibajoCode: string;
  liveWeightKgByHorse: Map<string, number>;
}

const SENTINEL_EMPTY_WEIGHT = "000";
const SENTINEL_UNKNOWN_WEIGHT = "FFF";

const HORSE_WEIGHT_CLASS_GE_540: HorseWeightClass = {
  key: "ge540",
  label: "540kg以上",
  maxKg: null,
  minKg: 540,
};

const BAN_EI_HORSE_WEIGHT_CLASS_GE_1200: HorseWeightClass = {
  key: "ge1200",
  label: "1200kg以上",
  maxKg: null,
  minKg: 1200,
};

export const HORSE_WEIGHT_CLASSES: readonly HorseWeightClass[] = [
  { key: "le399", label: "399kg以下", maxKg: 400, minKg: null },
  { key: "400-419", label: "400-419kg", maxKg: 420, minKg: 400 },
  { key: "420-439", label: "420-439kg", maxKg: 440, minKg: 420 },
  { key: "440-459", label: "440-459kg", maxKg: 460, minKg: 440 },
  { key: "460-479", label: "460-479kg", maxKg: 480, minKg: 460 },
  { key: "480-499", label: "480-499kg", maxKg: 500, minKg: 480 },
  { key: "500-519", label: "500-519kg", maxKg: 520, minKg: 500 },
  { key: "520-539", label: "520-539kg", maxKg: 540, minKg: 520 },
  HORSE_WEIGHT_CLASS_GE_540,
];

export const BAN_EI_HORSE_WEIGHT_CLASSES: readonly HorseWeightClass[] = [
  { key: "le899", label: "899kg以下", maxKg: 900, minKg: null },
  { key: "900-949", label: "900-949kg", maxKg: 950, minKg: 900 },
  { key: "950-999", label: "950-999kg", maxKg: 1000, minKg: 950 },
  { key: "1000-1049", label: "1000-1049kg", maxKg: 1050, minKg: 1000 },
  { key: "1050-1099", label: "1050-1099kg", maxKg: 1100, minKg: 1050 },
  { key: "1100-1149", label: "1100-1149kg", maxKg: 1150, minKg: 1100 },
  { key: "1150-1199", label: "1150-1199kg", maxKg: 1200, minKg: 1150 },
  BAN_EI_HORSE_WEIGHT_CLASS_GE_1200,
];

const matchesHorseWeightClass = (kg: number, weightClass: HorseWeightClass): boolean => {
  if (!Number.isFinite(kg)) {
    return false;
  }
  if (weightClass.maxKg !== null && kg >= weightClass.maxKg) {
    return false;
  }
  return true;
};

export const parseHorseWeightKg = (input: ParseHorseWeightKgInput): number | null => {
  const cleanWeight = cleanText(input.bataiju, "");
  if (
    cleanWeight === "" ||
    cleanWeight === SENTINEL_EMPTY_WEIGHT ||
    cleanWeight.toUpperCase() === SENTINEL_UNKNOWN_WEIGHT
  ) {
    return null;
  }
  const parsed = isBanEiKeibajoCode(input.keibajoCode)
    ? Number.parseInt(cleanWeight, 16)
    : Number(cleanWeight);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
};

export const getHorseWeightClass = (
  kg: number,
  keibajoCode: string | null | undefined,
): HorseWeightClass => {
  const banEi = isBanEiKeibajoCode(keibajoCode);
  const classes = banEi ? BAN_EI_HORSE_WEIGHT_CLASSES : HORSE_WEIGHT_CLASSES;
  const fallback = banEi ? BAN_EI_HORSE_WEIGHT_CLASS_GE_1200 : HORSE_WEIGHT_CLASS_GE_540;
  return classes.find((weightClass) => matchesHorseWeightClass(kg, weightClass)) ?? fallback;
};

export const indexLiveHorseWeightKg = (horses: readonly LiveHorseWeight[]): Map<string, number> =>
  horses.reduce((index, horse) => {
    if (horse.weight === null || !Number.isFinite(horse.weight) || horse.weight <= 0) {
      return index;
    }
    const horseNumber = formatRunnerNumber(horse.horseNumber);
    if (horseNumber === "-") {
      return index;
    }
    return new Map(index).set(horseNumber, horse.weight);
  }, new Map<string, number>());

export const resolveCurrentHorseWeightKg = (
  input: ResolveCurrentHorseWeightKgInput,
): number | null => {
  const liveWeights = input.liveWeightKgByHorse;
  const liveWeightKg =
    liveWeights instanceof Map ? liveWeights.get(formatRunnerNumber(input.horseNumber)) : undefined;
  if (liveWeightKg !== undefined) {
    return liveWeightKg;
  }
  return parseHorseWeightKg({ bataiju: input.bataiju, keibajoCode: input.keibajoCode });
};
