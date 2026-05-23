import type { Win5PayoutInfo } from "./types";

const PAYOUT_FIELD_LENGTH = 9;
const HORSE_NUMBER_COUNT = 5;
const HORSE_NUMBER_WIDTH = 2;
const WINNING_HORSES_LENGTH = HORSE_NUMBER_COUNT * HORSE_NUMBER_WIDTH;

export const parseWin5PayoutField = (value: string | null | undefined): Win5PayoutInfo | null => {
  const cleaned = (value ?? "").trim();
  if (cleaned.length < WINNING_HORSES_LENGTH + PAYOUT_FIELD_LENGTH) {
    return null;
  }

  const winningHorseNumbers = Array.from({ length: HORSE_NUMBER_COUNT }, (_, index) => {
    const start = index * HORSE_NUMBER_WIDTH;
    return cleaned.slice(start, start + HORSE_NUMBER_WIDTH).replace(/^0+/u, "") || "0";
  });

  const payoutRaw = cleaned.slice(WINNING_HORSES_LENGTH, WINNING_HORSES_LENGTH + PAYOUT_FIELD_LENGTH);
  const payoutYen = Number.parseInt(payoutRaw, 10);
  const ticketRaw = cleaned.slice(WINNING_HORSES_LENGTH + PAYOUT_FIELD_LENGTH);
  const winningTicketCount = ticketRaw.length > 0 ? Number.parseInt(ticketRaw, 10) : 0;

  if (!Number.isFinite(payoutYen)) {
    return null;
  }

  return {
    payoutYen,
    winningHorseNumbers,
    winningTicketCount: Number.isFinite(winningTicketCount) ? winningTicketCount : 0,
  };
};

export const planCoversWinningCombination = (
  selections: ReadonlyArray<{ horseNumbers: readonly string[] }>,
  winningHorseNumbers: readonly string[],
): boolean => {
  if (selections.length !== winningHorseNumbers.length) {
    return false;
  }
  return selections.every((selection, index) =>
    selection.horseNumbers.some(
      (horseNumber) =>
        horseNumber.replace(/^0+/u, "") === winningHorseNumbers[index]?.replace(/^0+/u, ""),
    ),
  );
};
