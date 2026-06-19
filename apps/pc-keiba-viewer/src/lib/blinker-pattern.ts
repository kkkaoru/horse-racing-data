// Run with bun (exercised via `bunx vitest run`).
// Blinker (ブリンカー) wearing-state helpers: classify how a horse's
// `blinker_shiyo_kubun` for the upcoming race relates to its past races into one
// of six display patterns (A-F). Only JRA populates this column ('1' = wearing,
// '0' = not wearing); NAR/Ban-ei rows are all '0', so they resolve to no pattern.

export type BlinkerPattern = "A" | "B" | "C" | "D" | "E" | "F";

// The raw column value that marks the horse as wearing a blinker.
const WEARING_VALUE = "1";

// Re-attachment gap (in past races since the most recent wearing race) at or
// above which the resumption counts as a "long rest" (pattern D) instead of a
// "recent rest" (pattern C).
const LONG_REST_GAP = 3;

export const BLINKER_PATTERN_LABELS: Record<BlinkerPattern, string> = {
  A: "初装着(初出走以外)",
  B: "初装着(初出走)",
  C: "再装着(近1-2走休止)",
  D: "再装着(3走以上休止)",
  E: "ブリンカー解除",
  F: "継続着用",
};

export const isWearingBlinker = (value: string | null | undefined): boolean =>
  value === WEARING_VALUE;

// Classify the upcoming-race blinker state relative to past races. `past` is the
// horse's previous races MOST-RECENT-FIRST, each entry the race's raw
// `blinkerShiyoKubun`. Returns null when no meaningful pattern applies (e.g. the
// horse has never worn a blinker and is not wearing one now).
export const classifyBlinkerPattern = (
  current: string | null | undefined,
  past: ReadonlyArray<string | null | undefined>,
): BlinkerPattern | null => {
  const wearsNow = isWearingBlinker(current);
  const hasPast = past.length > 0;
  const woreEver = past.some(isWearingBlinker);
  if (!wearsNow) {
    return hasPast && past.every(isWearingBlinker) ? "E" : null;
  }
  if (!hasPast) {
    return "B";
  }
  if (!woreEver) {
    return "A";
  }
  if (isWearingBlinker(past[0])) {
    return "F";
  }
  const gap = past.findIndex(isWearingBlinker);
  return gap >= LONG_REST_GAP ? "D" : "C";
};
