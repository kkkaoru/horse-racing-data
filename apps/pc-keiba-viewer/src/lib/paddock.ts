export const PADDOCK_HISTORY_LIMIT = 100;

export type PaddockMetric = "attention" | "paddock" | "preference";
export type PaddockOfficialRank = 1 | 2 | 3 | 4 | 5 | 6;

export interface PaddockHorseScore {
  attention: number;
  horseName: string;
  horseNumber: string;
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
    "attention" | "officialRank" | "paddock" | "preference" | "total"
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

const PADDOCK_METRICS = new Set<PaddockMetric>(["attention", "paddock", "preference"]);

const isPaddockMetric = (value: unknown): value is PaddockMetric =>
  value === "attention" || value === "paddock" || value === "preference";

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
  horse: Pick<PaddockHorseScore, "attention" | "paddock" | "preference">,
): number => horse.paddock + horse.attention + horse.preference * 0.5;

export const normalizePaddockHorseScore = (
  value: Partial<PaddockHorseScore> | undefined,
  fallback: { horseName: string; horseNumber: string },
): PaddockHorseScore => {
  const horse = {
    attention: value?.attention ?? 0,
    horseName: value?.horseName || fallback.horseName,
    horseNumber: value?.horseNumber || fallback.horseNumber,
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
    return (
      value.rank === null ||
      value.rank === 1 ||
      value.rank === 2 ||
      value.rank === 3 ||
      value.rank === 4 ||
      value.rank === 5 ||
      value.rank === 6
    );
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
