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

export interface ClefBetPlanSet {
  budget: number;
  totalStake: number;
  expectedReturn: number;
  plans: ClefBetSuggestion[];
}

export interface ClefBetCard {
  oddsFetchedAt: string;
  runners: ClefBetRunner[];
  plans: ClefBetPlanSet;
}

export const DEFAULT_BET_BUDGET: number = 2000;
export const BET_BUDGET_STEP: number = 100;
export const MIN_BET_BUDGET: number = 200;
export const MAX_BET_BUDGET: number = 1_000_000;

const CLEF_BETS_ENDPOINT: string = "/api/clef-rerank/bets";
const TWO_DIGITS: number = 2;
const BET_TYPE_LABELS: Record<string, string> = {
  tansho: "単勝",
  fukusho: "複勝",
  umaren: "馬連",
  wide: "ワイド",
  wakuren: "枠連",
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

const MIN_PLANS: number = 1;
const ORDERED_BET_TYPES: ReadonlySet<string> = new Set(["umatan", "3rentan"]);

const isPlanSet = (value: unknown): value is ClefBetPlanSet =>
  isRecord(value) &&
  isFiniteNumber(value.budget) &&
  isFiniteNumber(value.totalStake) &&
  isFiniteNumber(value.expectedReturn) &&
  Array.isArray(value.plans) &&
  value.plans.length >= MIN_PLANS &&
  value.plans.every(isSuggestion);

const pad = (value: string): string => value.padStart(TWO_DIGITS, "0");

// Bet types in display order; the ids match the realtime odds and the Worker `types` parameter.
export const BET_TYPES: readonly string[] = [
  "tansho",
  "fukusho",
  "wide",
  "wakuren",
  "umaren",
  "umatan",
  "3renpuku",
  "3rentan",
];
export const BET_TYPES_STORAGE_KEY: string = "pc-keiba:bet-types";

const orderBetTypes = (types: readonly unknown[]): string[] =>
  BET_TYPES.filter((type) => types.includes(type));

// The last choice on this device wins for every race; anything unreadable means all types.
export const readSelectedBetTypes = (stored: string | null): string[] => {
  try {
    const parsed: unknown = stored === null ? null : JSON.parse(stored);
    if (Array.isArray(parsed)) return orderBetTypes(parsed);
  } catch {
    // Unreadable storage falls back to every bet type below.
  }
  return [...BET_TYPES];
};

// Before any odds arrive every remembered type is requested. Selecting every type with odds is
// the default view the cron warm already caches, so it is requested as all types.
export const requestedBetTypes = (
  selected: readonly string[],
  available: readonly string[],
): readonly string[] => {
  if (available.length === 0) return selected;
  if (available.every((type) => selected.includes(type))) return BET_TYPES;
  return selected.filter((type) => available.includes(type));
};

export const buildClefBetsUrl = (
  race: ClefRerankRaceAddress,
  budget: number,
  types: readonly string[] = BET_TYPES,
): string => {
  const selected = orderBetTypes(types);
  const filter = selected.length === BET_TYPES.length ? "" : `&types=${selected.join(",")}`;
  return `${CLEF_BETS_ENDPOINT}/${race.year}${pad(race.month)}${pad(race.day)}/${race.keibajoCode}/${pad(race.raceNumber)}?budget=${String(budget)}${filter}`;
};

export const readClefBets = (value: unknown): ClefBetCard | null =>
  isRecord(value) &&
  typeof value.oddsFetchedAt === "string" &&
  Array.isArray(value.runners) &&
  value.runners.every(isRunner) &&
  isPlanSet(value.plans)
    ? {
        oddsFetchedAt: value.oddsFetchedAt,
        runners: value.runners,
        plans: value.plans,
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

const joinHorses = (horses: readonly number[]): string => horses.join("・");

const legName = (ordered: boolean, index: number): string =>
  ordered ? `${String(index + 1)}着` : `${String(index + 1)}頭目`;

// Spells out the ticket structure the way a betting slip names it: 軸流し, マルチ, フォーメーション, ボックス.
export const describePlan = (
  plan: Pick<ClefBetSuggestion, "betType" | "style" | "axis" | "legs">,
): string => {
  const ordered = ORDERED_BET_TYPES.has(plan.betType);
  const partners = joinHorses(plan.legs[1] ?? []);
  const descriptions: Record<string, () => string> = {
    single: () => `${String(plan.axis)}番`,
    box: () => `ボックス ${joinHorses(plan.legs.flat())}`,
    nagashi: () => `${ordered ? "1着軸" : "軸"} ${String(plan.axis)} → 相手 ${partners}`,
    multi: () => `軸 ${String(plan.axis)} マルチ → 相手 ${partners}`,
    formation: () =>
      `フォーメーション ${plan.legs.map((leg, index) => `${legName(ordered, index)} ${joinHorses(leg)}`).join(" ／ ")}`,
  };
  return descriptions[plan.style]?.() ?? `${String(plan.axis)}番`;
};

export interface ClefBetLegGroup {
  label: string;
  horses: number[];
}

// Horse groups shown in each column: one block per leg, labelled like a betting slip.
export const planGroups = (
  plan: Pick<ClefBetSuggestion, "betType" | "style" | "axis" | "legs">,
): ClefBetLegGroup[] => {
  const ordered = ORDERED_BET_TYPES.has(plan.betType);
  const partners = plan.legs[1] ?? [];
  const groups: Record<string, () => ClefBetLegGroup[]> = {
    box: () => [{ label: "ボックス", horses: plan.legs.flat() }],
    nagashi: () => [
      { label: ordered ? "1着軸" : "軸", horses: [plan.axis] },
      { label: "相手", horses: partners },
    ],
    multi: () => [
      { label: "軸（マルチ）", horses: [plan.axis] },
      { label: "相手", horses: partners },
    ],
    formation: () =>
      plan.legs.map((leg, index) => ({ label: legName(ordered, index), horses: leg })),
  };
  return groups[plan.style]?.() ?? [{ label: "", horses: [plan.axis] }];
};

export const formatPlanOdds = (plan: Pick<ClefBetSuggestion, "tickets">): string => {
  const low = Math.min(...plan.tickets.map((ticket) => ticket.odds));
  const high = Math.max(...plan.tickets.map((ticket) => ticket.maxOdds ?? ticket.odds));
  return low === high ? low.toFixed(1) : `${low.toFixed(1)}〜${high.toFixed(1)}`;
};

export const formatPlanStake = (
  plan: Pick<ClefBetSuggestion, "tickets" | "totalStake">,
): string => {
  const stakes = new Set(plan.tickets.map((ticket) => ticket.stake));
  const [only] = stakes;
  if (plan.tickets.length === 1) return formatYen(plan.totalStake);
  return stakes.size === 1 && only !== undefined
    ? `各${formatYen(only)} × ${String(plan.tickets.length)}点 = ${formatYen(plan.totalStake)}`
    : `${String(plan.tickets.length)}点 計${formatYen(plan.totalStake)}`;
};

export const formatPlanPayout = (plan: Pick<ClefBetSuggestion, "tickets">): string => {
  const low = Math.min(...plan.tickets.map((ticket) => ticket.payout));
  const high = Math.max(...plan.tickets.map((ticket) => ticket.payoutMax ?? ticket.payout));
  return low === high ? formatYen(low) : `${formatYen(low)}〜${formatYen(high)}`;
};

export const summarizePlanSet = (set: ClefBetPlanSet): string =>
  [
    `${String(set.plans.length)}種類`,
    `${String(set.plans.reduce((sum, plan) => sum + plan.tickets.length, 0))}点`,
    `合計 ${formatYen(set.totalStake)}`,
    `期待払戻 ${formatYen(set.expectedReturn)}`,
  ].join(" ／ ");

export const OPTIONAL_BET_COLUMNS: readonly string[] = ["馬名", "騎手", "調教師", "馬主"];
export const BET_COLUMNS_STORAGE_KEY: string = "pc-keiba:bet-hidden-columns";
const MOBILE_HIDDEN_BET_COLUMNS: readonly string[] = ["調教師", "馬主"];

const isOptionalColumn = (value: unknown): value is string =>
  typeof value === "string" && OPTIONAL_BET_COLUMNS.includes(value);

// The last choice on this device wins; otherwise phones start without 調教師 and 馬主.
export const readHiddenBetColumns = (stored: string | null, mobile: boolean): string[] => {
  try {
    const parsed: unknown = stored === null ? null : JSON.parse(stored);
    if (Array.isArray(parsed)) return parsed.filter(isOptionalColumn);
  } catch {
    // Unreadable storage falls back to the device default below.
  }
  return mobile ? [...MOBILE_HIDDEN_BET_COLUMNS] : [];
};

const EDGE_SPACES: RegExp = /^[\s\u3000]+|[\s\u3000]+$/gu;

// Source names are padded with half- and full-width spaces; only the edges are removed.
export const trimName = (value: string): string => value.replace(EDGE_SPACES, "");
