// Run with bun: bun run --filter pc-keiba-viewer test src/lib/runner-finish-position.test.ts
const EMPTY_ENTRY_MARKER = "00";

const TREND_MIN_VALID_VALUE = 0;

const TREND_NULL_FALLBACK_NUMBER = 0;

export interface PickFinishPositionInput {
  entryValue: string | null;
  trendValue: number | null;
}

interface PickFinishPositionEmptyEntryInput {
  entryValue: typeof EMPTY_ENTRY_MARKER | null;
  trendValue: number | null;
}

const isEntryValueNull = (value: string | null): value is null => value === null;

const isEntryValueEmptyMarker = (value: string | null): value is typeof EMPTY_ENTRY_MARKER =>
  value === EMPTY_ENTRY_MARKER;

const isEntryEmpty = (value: string | null): value is typeof EMPTY_ENTRY_MARKER | null =>
  isEntryValueNull(value) || isEntryValueEmptyMarker(value);

const isTrendValueNull = (value: number | null): value is null => value === null;

const safeTrendValue = (value: number | null): number =>
  isTrendValueNull(value) ? TREND_NULL_FALLBACK_NUMBER : value;

const isTrendUsable = (value: number | null): boolean =>
  safeTrendValue(value) > TREND_MIN_VALID_VALUE;

const parseEntryNumber = (value: string): number => Number(value);

const toDisplayEntryValue = (value: string): string => String(parseEntryNumber(value));

const trendAsDisplay = (value: number | null): string => String(value);

const pickTrendOrEmptyEntry = (input: PickFinishPositionEmptyEntryInput): string | null =>
  isTrendUsable(input.trendValue) ? trendAsDisplay(input.trendValue) : input.entryValue;

const buildEmptyEntryInput = (
  input: PickFinishPositionInput,
  entryValue: typeof EMPTY_ENTRY_MARKER | null,
): PickFinishPositionEmptyEntryInput => ({
  entryValue,
  trendValue: input.trendValue,
});

export const pickFinishPosition = (input: PickFinishPositionInput): string | null =>
  isEntryEmpty(input.entryValue)
    ? pickTrendOrEmptyEntry(buildEmptyEntryInput(input, input.entryValue))
    : toDisplayEntryValue(input.entryValue);
