// Run with bun (bunx vitest). Decoder and labels for the clef-flash bet suggestion Worker payload.
import type { ClefRerankRaceAddress } from "./clef-rerank";

export interface ClefBetRunner {
  horseNumber: number;
  frameNumber: number;
  horseName: string;
  jockey: string;
  trainer: string;
  owner: string;
}

export interface ClefBetTicket {
  horses: number[];
  odds: number;
  maxOdds: number | null;
  stake: number;
  payout: number;
  payoutMax: number | null;
}

export interface ClefBetSuggestion {
  betType: string;
  style: string;
  axis: number;
  legs: number[][];
  budget: number;
  totalStake: number;
  expectedReturn: number;
  tickets: ClefBetTicket[];
}

export interface ClefBetCard {
  oddsFetchedAt: string;
  runners: ClefBetRunner[];
  suggestion: ClefBetSuggestion;
}

export const DEFAULT_BET_BUDGET: number = 2000;
export const BET_BUDGET_STEP: number = 100;
export const MIN_BET_BUDGET: number = 100;
export const MAX_BET_BUDGET: number = 1_000_000;

const CLEF_BETS_ENDPOINT: string = "/api/clef-rerank/bets";
const TWO_DIGITS: number = 2;
const BET_TYPE_LABELS: Record<string, string> = {
  tansho: "単勝",
  fukusho: "複勝",
  umaren: "馬連",
  wide: "ワイド",
  umatan: "馬単",
  "3renpuku": "3連複",
  "3rentan": "3連単",
};
const STYLE_LABELS: Record<string, string> = {
  single: "通常",
  box: "ボックス",
  nagashi: "軸流し",
  multi: "軸流しマルチ",
  formation: "フォーメーション",
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isFiniteNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value);

const isNumberList = (value: unknown): value is number[] =>
  Array.isArray(value) && value.every(isFiniteNumber);

const isNullableNumber = (value: unknown): value is number | null =>
  value === null || isFiniteNumber(value);

const isRunner = (value: unknown): value is ClefBetRunner =>
  isRecord(value) &&
  isFiniteNumber(value.horseNumber) &&
  isFiniteNumber(value.frameNumber) &&
  typeof value.horseName === "string" &&
  typeof value.jockey === "string" &&
  typeof value.trainer === "string" &&
  typeof value.owner === "string";

const isTicket = (value: unknown): value is ClefBetTicket =>
  isRecord(value) &&
  isNumberList(value.horses) &&
  isFiniteNumber(value.odds) &&
  isNullableNumber(value.maxOdds) &&
  isFiniteNumber(value.stake) &&
  isFiniteNumber(value.payout) &&
  isNullableNumber(value.payoutMax);

const isSuggestion = (value: unknown): value is ClefBetSuggestion =>
  isRecord(value) &&
  typeof value.betType === "string" &&
  typeof value.style === "string" &&
  isFiniteNumber(value.axis) &&
  Array.isArray(value.legs) &&
  value.legs.every(isNumberList) &&
  isFiniteNumber(value.budget) &&
  isFiniteNumber(value.totalStake) &&
  isFiniteNumber(value.expectedReturn) &&
  Array.isArray(value.tickets) &&
  value.tickets.length > 0 &&
  value.tickets.every(isTicket);

const pad = (value: string): string => value.padStart(TWO_DIGITS, "0");

export const buildClefBetsUrl = (race: ClefRerankRaceAddress, budget: number): string =>
  `${CLEF_BETS_ENDPOINT}/${race.year}${pad(race.month)}${pad(race.day)}/${race.keibajoCode}/${pad(race.raceNumber)}?budget=${String(budget)}`;

export const readClefBets = (value: unknown): ClefBetCard | null =>
  isRecord(value) &&
  typeof value.oddsFetchedAt === "string" &&
  Array.isArray(value.runners) &&
  value.runners.every(isRunner) &&
  isSuggestion(value.suggestion)
    ? {
        oddsFetchedAt: value.oddsFetchedAt,
        runners: value.runners,
        suggestion: value.suggestion,
      }
    : null;

// Clamps a typed or slid budget onto the 100-yen grid the Worker accepts.
export const normalizeBudget = (value: number): number =>
  Number.isFinite(value)
    ? Math.min(
        MAX_BET_BUDGET,
        Math.max(MIN_BET_BUDGET, Math.round(value / BET_BUDGET_STEP) * BET_BUDGET_STEP),
      )
    : DEFAULT_BET_BUDGET;

export const betTypeLabel = (betType: string): string => BET_TYPE_LABELS[betType] ?? betType;

export const betStyleLabel = (style: string): string => STYLE_LABELS[style] ?? style;

export const formatYen = (value: number): string => `${value.toLocaleString("ja-JP")}円`;

export const formatOdds = (ticket: Pick<ClefBetTicket, "odds" | "maxOdds">): string =>
  ticket.maxOdds === null
    ? ticket.odds.toFixed(1)
    : `${ticket.odds.toFixed(1)}〜${ticket.maxOdds.toFixed(1)}`;

export const formatPayout = (ticket: Pick<ClefBetTicket, "payout" | "payoutMax">): string =>
  ticket.payoutMax === null
    ? formatYen(ticket.payout)
    : `${formatYen(ticket.payout)}〜${formatYen(ticket.payoutMax)}`;

export const formatLegs = (suggestion: Pick<ClefBetSuggestion, "legs" | "style">): string =>
  suggestion.style === "box" || suggestion.legs.length === 1
    ? suggestion.legs.flat().join(", ")
    : suggestion.legs
        .map((leg, index) => `${String(index + 1)}列目: ${leg.join(", ")}`)
        .join(" ／ ");

export const summarizeSuggestion = (suggestion: ClefBetSuggestion): string =>
  [
    `${betTypeLabel(suggestion.betType)} ${betStyleLabel(suggestion.style)}`,
    `軸 ${String(suggestion.axis)}`,
    formatLegs(suggestion),
    `${String(suggestion.tickets.length)}点`,
    `合計 ${formatYen(suggestion.totalStake)}`,
    `期待払戻 ${formatYen(suggestion.expectedReturn)}`,
  ].join(" ／ ");
