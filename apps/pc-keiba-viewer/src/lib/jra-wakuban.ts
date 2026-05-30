// Run with bun. JRA's umaban (horse_number) -> wakuban (frame_number)
// derivation: frames are distributed as evenly as possible across the 8
// fixed frame numbers, with overflow horses shifted to the higher frames.
// The rule matches JRA's published frame chart.
//
// Phase B (2026-05-30): race_entry_snapshots does not store wakuban, so
// the today-trend snapshot path derives wakuban from horse_number + the
// distinct horse_count of the race. NAR keeps its own snapshot wakuban
// path elsewhere and is intentionally out of scope for this helper.

const JRA_FRAME_COUNT = 8;
const MIN_UMABAN = 1;
const JRA_MAX_HORSE_COUNT = 18;

export interface DeriveJraWakubanParams {
  horseCount: number;
  horseNumber: number;
}

interface BaseAndExtra {
  base: number;
  extra: number;
  threshold: number;
}

const isWithinJraBounds = (params: DeriveJraWakubanParams): boolean =>
  Number.isInteger(params.horseNumber) &&
  Number.isInteger(params.horseCount) &&
  params.horseNumber >= MIN_UMABAN &&
  params.horseNumber <= params.horseCount &&
  params.horseCount >= MIN_UMABAN &&
  params.horseCount <= JRA_MAX_HORSE_COUNT;

const computeBaseAndExtra = (horseCount: number): BaseAndExtra => {
  const base = Math.floor(horseCount / JRA_FRAME_COUNT);
  const extra = horseCount - base * JRA_FRAME_COUNT;
  const threshold = (JRA_FRAME_COUNT - extra) * base;
  return { base, extra, threshold };
};

export const deriveJraWakuban = (params: DeriveJraWakubanParams): number | null => {
  if (!isWithinJraBounds(params)) return null;
  if (params.horseCount <= JRA_FRAME_COUNT) return params.horseNumber;
  const { base, extra, threshold } = computeBaseAndExtra(params.horseCount);
  if (params.horseNumber <= threshold) {
    return Math.floor((params.horseNumber - 1) / base) + 1;
  }
  return (
    JRA_FRAME_COUNT - extra + Math.floor((params.horseNumber - threshold - 1) / (base + 1)) + 1
  );
};
