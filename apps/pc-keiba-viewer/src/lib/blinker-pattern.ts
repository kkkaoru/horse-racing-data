// Run with bun (exercised via `bunx vitest run`).
// Blinker (ブリンカー) wearing-state helpers: classify how a horse's
// `blinker_shiyo_kubun` for the upcoming race relates to its past races into one
// of six display patterns. Only JRA populates this column ('1' = wearing,
// '0' = not wearing); NAR/Ban-ei rows are all '0', so they resolve to no pattern.
//
// The "A".."F" identifiers are kept INTERNAL only: they are the className suffix
// (pattern-A..pattern-F) plus the label-map keys. The user never sees the letter;
// the badges render the Japanese labels below.

export type BlinkerPattern = "A" | "B" | "C" | "D" | "E" | "F";

// The raw column value that marks the horse as wearing a blinker.
const WEARING_VALUE = "1";

// Re-attachment gap (in past races since the most recent wearing race) at or
// above which the resumption counts as a "long rest" (pattern D) instead of a
// "recent rest" (pattern C). The same threshold defines the "recent 3+ consecutive
// wore" consistency used by removal (E) and continuation (F).
const CONSISTENT_WEAR_RUN = 3;

// Full, self-explanatory labels (paddock badges). Readable without knowing the
// internal A-F scheme.
export const BLINKER_PATTERN_LABELS: Record<BlinkerPattern, string> = {
  A: "初ブリンカー",
  B: "初ブリンカー(初出走)",
  C: "ブリンカー再装着",
  D: "ブリンカー再装着(3走以上ぶり)",
  E: "ブリンカー解除",
  F: "ブリンカー継続",
};

// Compact labels (runners-table badges). Same meaning, fewer characters.
export const BLINKER_PATTERN_SHORT_LABELS: Record<BlinkerPattern, string> = {
  A: "初装着",
  B: "初装着(新馬)",
  C: "再装着",
  D: "再装着(久々)",
  E: "解除",
  F: "継続",
};

export const isWearingBlinker = (value: string | null | undefined): boolean =>
  value === WEARING_VALUE;

// Count of most-recent past races that consecutively wore the blinker. Equals
// past.length when every past race wore it (no non-wearing race found).
const countLeadingWore = (past: ReadonlyArray<string | null | undefined>): number => {
  const firstUnworn = past.findIndex((value) => !isWearingBlinker(value));
  return firstUnworn === -1 ? past.length : firstUnworn;
};

// Classify the upcoming-race blinker state relative to past races. `past` is the
// horse's previous races MOST-RECENT-FIRST, each entry the race's raw
// `blinkerShiyoKubun`. Returns null when no meaningful pattern applies (e.g. the
// horse has never worn a blinker and is not wearing one now).
export const classifyBlinkerPattern = (
  current: string | null | undefined,
  past: ReadonlyArray<string | null | undefined>,
): BlinkerPattern | null => {
  const wears = isWearingBlinker(current);
  const hasPast = past.length > 0;
  const woreEver = past.some(isWearingBlinker);
  const leadingWore = countLeadingWore(past);
  // Either every past race wore it, or the most recent CONSISTENT_WEAR_RUN+ in a
  // row wore it: a settled "this horse wears a blinker" history.
  const consistentlyWorn =
    hasPast && (leadingWore === past.length || leadingWore >= CONSISTENT_WEAR_RUN);
  if (!wears) {
    return consistentlyWorn ? "E" : null;
  }
  if (!hasPast) {
    return "B";
  }
  if (!woreEver) {
    return "A";
  }
  if (consistentlyWorn) {
    return "F";
  }
  if (isWearingBlinker(past[0])) {
    return "F";
  }
  const gap = past.findIndex(isWearingBlinker);
  return gap >= CONSISTENT_WEAR_RUN ? "D" : "C";
};
