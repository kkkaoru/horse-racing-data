// Run with bun (exercised via `bunx vitest run`).
// Compact horse-weight minus carried-weight rows for one race card.
import { cleanText } from "./format";
import { formatRunnerNumber, isBanEiKeibajoCode } from "./runner-format";

export interface WeightFutanDiffRunner {
  bataiju: string | null;
  futanJuryo: string | null;
  umaban: string | null;
}

export interface WeightFutanDiffRow {
  d: number | null;
  f: number | null;
  u: string;
  w: number | null;
}

export interface BuildWeightFutanDiffRowsInput {
  keibajoCode: string;
  runners: readonly WeightFutanDiffRunner[];
}

const HEX_RADIX: number = 16;
const FUTAN_DECIGRAM_DIVISOR: number = 10;
const NON_BANEI_UNMEASURED_WEIGHT: number = 999;
const INVALID_WEIGHT_FFF: string = "FFF";
const ALL_ZERO_PATTERN: RegExp = /^0+$/;
const EMPTY_UMABAN: string = "-";

const isInvalidSentinel = (cleaned: string): boolean =>
  cleaned.length === 0 ||
  ALL_ZERO_PATTERN.test(cleaned) ||
  cleaned.toUpperCase() === INVALID_WEIGHT_FFF;

const parseKg = (
  value: string | null,
  decodeHex: boolean,
  rejectUnmeasured: boolean,
): number | null => {
  const cleaned = cleanText(value, "");
  if (isInvalidSentinel(cleaned)) {
    return null;
  }
  const parsed = decodeHex ? Number.parseInt(cleaned, HEX_RADIX) : Number(cleaned);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return null;
  }
  if (rejectUnmeasured && parsed === NON_BANEI_UNMEASURED_WEIGHT) {
    return null;
  }
  return parsed;
};

const parseHorseWeightKg = (value: string | null, keibajoCode: string): number | null => {
  const decodeHex = isBanEiKeibajoCode(keibajoCode);
  return parseKg(value, decodeHex, !decodeHex);
};

const parseFutanKg = (value: string | null, keibajoCode: string): number | null => {
  const decodeHex = isBanEiKeibajoCode(keibajoCode);
  const parsed = parseKg(value, decodeHex, false);
  if (parsed === null) {
    return null;
  }
  return decodeHex ? parsed : parsed / FUTAN_DECIGRAM_DIVISOR;
};

export const buildWeightFutanDiffRows = (
  input: BuildWeightFutanDiffRowsInput,
): WeightFutanDiffRow[] =>
  input.runners.flatMap((runner) => {
    const umaban = formatRunnerNumber(runner.umaban);
    if (umaban === EMPTY_UMABAN) {
      return [];
    }
    const weightKg = parseHorseWeightKg(runner.bataiju, input.keibajoCode);
    const futanKg = parseFutanKg(runner.futanJuryo, input.keibajoCode);
    return [
      {
        d: weightKg === null || futanKg === null ? null : weightKg - futanKg,
        f: futanKg,
        u: umaban,
        w: weightKg,
      },
    ];
  });
