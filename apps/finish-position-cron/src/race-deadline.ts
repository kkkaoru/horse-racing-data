// Run with bun. Pure hard-deadline guard for per-race prediction work.

interface RaceDeadlineInput {
  nowMs: number;
  raceStartAtJst: string | undefined;
}

export class RaceDeadlineExceededError extends Error {
  constructor() {
    super("Race start deadline has been reached");
    this.name = "RaceDeadlineExceededError";
  }
}

export const isBeforeRaceStartDeadline = (input: RaceDeadlineInput): boolean => {
  if (input.raceStartAtJst === undefined) return false;
  const raceStartMs = Date.parse(input.raceStartAtJst);
  return Number.isFinite(raceStartMs) && input.nowMs < raceStartMs;
};

export const assertBeforeRaceStartDeadline = (input: RaceDeadlineInput): void => {
  if (!isBeforeRaceStartDeadline(input)) throw new RaceDeadlineExceededError();
};
