// Run with bun. Derives wakuban (frame number, 1..8) from umaban (horse
// number) + horse_count for a single race. The rule matches JRA's published
// frame chart and also applies to NAR / Ban-ei since the official frame
// distribution algorithm is shared: frames are filled as evenly as possible
// across 8 fixed frame numbers, with overflow horses shifted to the higher
// frames. Both viewer (D1 today-trend) and the realtime DO use this helper
// so the trend section's frame filter never drops rows for a race that
// happens to live in a non-JRA source.

const FRAME_COUNT = 8;
const MIN_UMABAN = 1;
const MAX_HORSE_COUNT = 18;

export interface DeriveWakubanInput {
  horseCount: number;
  horseNumber: number;
}

interface BaseAndExtra {
  base: number;
  extra: number;
  threshold: number;
}

const isWithinBounds = (input: DeriveWakubanInput): boolean =>
  Number.isInteger(input.horseNumber) &&
  Number.isInteger(input.horseCount) &&
  input.horseNumber >= MIN_UMABAN &&
  input.horseNumber <= input.horseCount &&
  input.horseCount >= MIN_UMABAN &&
  input.horseCount <= MAX_HORSE_COUNT;

const computeBaseAndExtra = (horseCount: number): BaseAndExtra => {
  const base = Math.floor(horseCount / FRAME_COUNT);
  const extra = horseCount - base * FRAME_COUNT;
  const threshold = (FRAME_COUNT - extra) * base;
  return { base, extra, threshold };
};

export const deriveWakuban = (input: DeriveWakubanInput): number | null => {
  if (!isWithinBounds(input)) return null;
  if (input.horseCount <= FRAME_COUNT) return input.horseNumber;
  const { base, extra, threshold } = computeBaseAndExtra(input.horseCount);
  if (input.horseNumber <= threshold) {
    return Math.floor((input.horseNumber - 1) / base) + 1;
  }
  return FRAME_COUNT - extra + Math.floor((input.horseNumber - threshold - 1) / (base + 1)) + 1;
};

export const deriveWakubanString = (input: DeriveWakubanInput): string | null => {
  const value = deriveWakuban(input);
  return value === null ? null : String(value);
};
