export const PADDOCK_HISTORY_LIMIT = 100;

export type PaddockMetric = "attention" | "kaeshi" | "paddock" | "preference";
export type PaddockOfficialRank = 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10;

export interface PaddockHorseScore {
  attention: number;
  horseName: string;
  horseNumber: string;
  kaeshi: number;
  officialRank: PaddockOfficialRank | null;
  paddock: number;
  preference: number;
  total: number;
}

export interface PaddockHistoryEntry {
  at: string;
  category?: PaddockMetric;
  delta?: -1 | 1;
  horseName: string;
  horseNumber: string;
  id: string;
  officialRank?: PaddockOfficialRank | null;
  scores: Pick<
    PaddockHorseScore,
    "attention" | "kaeshi" | "officialRank" | "paddock" | "preference" | "total"
  >;
  type: "official-rank" | "score";
}

export interface PaddockState {
  history: PaddockHistoryEntry[];
  horses: Record<string, PaddockHorseScore>;
  raceKey: string;
  updatedAt: string;
}

export interface PaddockScoreAction {
  category: PaddockMetric;
  delta: -1 | 1;
  horseName: string;
  horseNumber: string;
  type?: "score";
}

export interface PaddockOfficialRankAction {
  horseName: string;
  horseNumber: string;
  rank: PaddockOfficialRank | null;
  type: "official-rank";
}

export type PaddockAction = PaddockOfficialRankAction | PaddockScoreAction;

const PADDOCK_METRICS = new Set<PaddockMetric>(["attention", "kaeshi", "paddock", "preference"]);
const PADDOCK_OFFICIAL_RANKS: PaddockOfficialRank[] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

const isPaddockMetric = (value: unknown): value is PaddockMetric =>
  value === "attention" || value === "kaeshi" || value === "paddock" || value === "preference";

const isPaddockOfficialRank = (value: unknown): value is PaddockOfficialRank =>
  PADDOCK_OFFICIAL_RANKS.some((rank) => rank === value);

export const createPaddockState = (
  raceKey: string,
  now = new Date().toISOString(),
): PaddockState => ({
  history: [],
  horses: {},
  raceKey,
  updatedAt: now,
});

export const getPaddockKvKey = (raceKey: string): string => `paddock:${raceKey}`;

export const getRacePaddockKey = ({
  day,
  keibajoCode,
  month,
  raceNumber,
  year,
}: {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  year: string;
}): string => `${year}${month}${day}:${keibajoCode}:${raceNumber}`;

const normalizeHorseNumber = (value: string): string =>
  value.trim().replace(/^0+/u, "") || value.trim();

const calculateTotal = (
  horse: Pick<PaddockHorseScore, "attention" | "kaeshi" | "paddock" | "preference">,
): number => horse.paddock + horse.kaeshi + horse.attention * 0.5 + horse.preference * 0.3;

export const normalizePaddockHorseScore = (
  value: Partial<PaddockHorseScore> | undefined,
  fallback: { horseName: string; horseNumber: string },
): PaddockHorseScore => {
  const horse = {
    attention: value?.attention ?? 0,
    horseName: value?.horseName || fallback.horseName,
    horseNumber: value?.horseNumber || fallback.horseNumber,
    kaeshi: value?.kaeshi ?? 0,
    officialRank: value?.officialRank ?? null,
    paddock: value?.paddock ?? 0,
    preference: value?.preference ?? 0,
  };
  return {
    ...horse,
    total: calculateTotal(horse),
  };
};

export const isPaddockAction = (value: unknown): value is PaddockAction => {
  if (typeof value !== "object" || value === null) {
    return false;
  }

  if (
    "type" in value &&
    value.type === "official-rank" &&
    "horseNumber" in value &&
    typeof value.horseNumber === "string" &&
    "horseName" in value &&
    typeof value.horseName === "string" &&
    "rank" in value
  ) {
    return value.rank === null || isPaddockOfficialRank(value.rank);
  }
  return (
    "horseNumber" in value &&
    typeof value.horseNumber === "string" &&
    "horseName" in value &&
    typeof value.horseName === "string" &&
    "delta" in value &&
    (value.delta === 1 || value.delta === -1) &&
    "category" in value &&
    isPaddockMetric(value.category) &&
    PADDOCK_METRICS.has(value.category)
  );
};

export const applyPaddockAction = (
  state: PaddockState,
  action: PaddockAction,
  now = new Date().toISOString(),
): PaddockState => {
  const horseNumber = normalizeHorseNumber(action.horseNumber);
  const current = normalizePaddockHorseScore(state.horses[horseNumber], {
    horseName: action.horseName,
    horseNumber,
  });
  if (action.type === "official-rank") {
    const nextHorse = {
      ...current,
      horseName: action.horseName || current.horseName,
      officialRank: action.rank,
    };
    const nextHorses = Object.fromEntries(
      Object.entries(state.horses).map(([key, horse]) => [
        key,
        action.rank !== null && horse.officialRank === action.rank
          ? { ...horse, officialRank: null }
          : horse,
      ]),
    );
    const historyEntry: PaddockHistoryEntry = {
      at: now,
      horseName: nextHorse.horseName,
      horseNumber,
      id: `${now}:${horseNumber}:official-rank:${state.history.length}`,
      officialRank: action.rank,
      scores: {
        attention: nextHorse.attention,
        kaeshi: nextHorse.kaeshi,
        officialRank: nextHorse.officialRank,
        paddock: nextHorse.paddock,
        preference: nextHorse.preference,
        total: nextHorse.total,
      },
      type: "official-rank",
    };
    return {
      history: [historyEntry, ...state.history].slice(0, PADDOCK_HISTORY_LIMIT),
      horses: {
        ...nextHorses,
        [horseNumber]: nextHorse,
      },
      raceKey: state.raceKey,
      updatedAt: now,
    };
  }
  const nextHorse = {
    ...current,
    horseName: action.horseName || current.horseName,
    [action.category]: current[action.category] + action.delta,
  };
  nextHorse.total = calculateTotal(nextHorse);
  const historyEntry: PaddockHistoryEntry = {
    at: now,
    category: action.category,
    delta: action.delta,
    horseName: nextHorse.horseName,
    horseNumber,
    id: `${now}:${horseNumber}:${action.category}:${state.history.length}`,
    scores: {
      attention: nextHorse.attention,
      kaeshi: nextHorse.kaeshi,
      officialRank: nextHorse.officialRank,
      paddock: nextHorse.paddock,
      preference: nextHorse.preference,
      total: nextHorse.total,
    },
    type: "score",
  };
  return {
    history: [historyEntry, ...state.history].slice(0, PADDOCK_HISTORY_LIMIT),
    horses: {
      ...state.horses,
      [horseNumber]: nextHorse,
    },
    raceKey: state.raceKey,
    updatedAt: now,
  };
};

export interface PaddockNotifyGateInput {
  officialRank: PaddockOfficialRank | null;
  total: number;
}

const hasPaddockTotalInput = (input: PaddockNotifyGateInput): boolean => input.total > 0;
const hasOfficialRankInput = (input: PaddockNotifyGateInput): boolean =>
  input.officialRank !== null;

// Notification gate: skip the Discord notification only when every horse has
// both no positive paddock evaluation total and no official rank assigned.
// Allowing the paddock-only-empty or official-rank-only-empty cases lets the
// editor publish the official-rank verdict even before paddock scoring starts.
export const shouldSkipPaddockDiscordNotification = (
  inputs: readonly PaddockNotifyGateInput[],
): boolean => !inputs.some((input) => hasPaddockTotalInput(input) || hasOfficialRankInput(input));

export const isPaddockHorseNotifiable = (input: PaddockNotifyGateInput): boolean =>
  hasPaddockTotalInput(input) || hasOfficialRankInput(input);

export const isPaddockState = (value: unknown): value is PaddockState => {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const candidate = value as Partial<PaddockState>;
  return (
    typeof candidate.raceKey === "string" &&
    typeof candidate.updatedAt === "string" &&
    typeof candidate.horses === "object" &&
    candidate.horses !== null &&
    Array.isArray(candidate.history)
  );
};
