// Run with bun (bunx vitest). clef-flash bet suggestions: builds the decision request from the
// viewer's race data, then turns the answers into tickets and a budget split.

export type BetType =
  | "tansho"
  | "fukusho"
  | "umaren"
  | "wide"
  | "umatan"
  | "3renpuku"
  | "3rentan"
  | "wakuren";
export type BetStyle = "single" | "box" | "nagashi" | "multi" | "formation";

export interface OddsEntry {
  combination: string;
  odds: number | null;
  minOdds: number | null;
  maxOdds: number | null;
}

export type OddsBook = Partial<Record<BetType, OddsEntry[]>>;

export interface RunnerProfile {
  horseNumber: number;
  frameNumber: number;
  horseName: string;
  jockey: string;
  trainer: string;
  owner: string;
}

export interface BetSources {
  heatmap: unknown;
  realtime: unknown;
  recent: unknown;
  training: unknown;
  trends: unknown;
  ranks: ReadonlyMap<number, number>;
}

export interface BetRace {
  raceKey: string;
  oddsFetchedAt: string;
  runners: RunnerProfile[];
  odds: OddsBook;
  ranks: ReadonlyMap<number, number>;
  request: Record<string, unknown>;
}

export interface BetAnswers {
  winner: Record<string, number>;
  top3: Record<string, number>;
  betType: Record<string, number>;
  style: Record<string, number>;
  axis: Record<string, number>;
}

export interface BetTicket {
  horses: number[];
  odds: number;
  maxOdds: number | null;
  probability: number;
  stake: number;
  payout: number;
  payoutMax: number | null;
}

export interface BetSuggestion {
  betType: BetType;
  style: BetStyle;
  axis: number;
  legs: number[][];
  budget: number;
  totalStake: number;
  expectedReturn: number;
  tickets: BetTicket[];
}

export interface SuggestInput {
  race: Pick<BetRace, "runners" | "odds" | "ranks">;
  answers: BetAnswers;
  budget: number;
  // Bet types the viewer selected; absent means every type with odds.
  types?: ReadonlySet<BetType>;
  // Profit in yen that every finishing order hitting a ticket must clear over the whole budget,
  // counting all tickets it hits; absent means none.
  target?: number;
  // Every selected bet type with odds must be part of the suggestion.
  requireAll?: boolean;
}

interface Candidate {
  horses: number[];
  odds: number;
  maxOdds: number | null;
  probability: number;
}

interface Plan {
  legs: number[][];
  combos: number[][];
}

interface PlanInput {
  betType: BetType;
  style: BetStyle;
  axis: number;
  order: readonly number[];
}

interface Allocation {
  candidate: Candidate;
  units: number;
}

interface Weighted {
  candidate: Candidate;
  share: number;
}

const BET_TYPES: readonly BetType[] = [
  "tansho",
  "fukusho",
  "umaren",
  "wide",
  "umatan",
  "3renpuku",
  "3rentan",
  "wakuren",
];
// 枠連 is offered only on request and stays out of the clef-flash question, so cached answers
// and the default suggestions keep their bet-type ranking.
const PROMPT_EXCLUDED_TYPES: ReadonlySet<BetType> = new Set(["wakuren"]);
const BET_TYPE_LABELS: Record<BetType, string> = {
  tansho: "単勝: 1着を当てる",
  fukusho: "複勝: 3着以内を当てる",
  umaren: "馬連: 1-2着の組み合わせ（順不同）",
  wide: "ワイド: 3着以内の2頭（順不同）",
  umatan: "馬単: 1-2着を着順通り",
  "3renpuku": "3連複: 1-3着の組み合わせ（順不同）",
  "3rentan": "3連単: 1-3着を着順通り",
  wakuren: "枠連: 1-2着の枠の組み合わせ（順不同）",
};
const STYLE_LABELS: Record<Exclude<BetStyle, "single">, string> = {
  box: "ボックス: 上位馬の全組み合わせ",
  nagashi: "軸流し: 軸馬から相手へ流す",
  multi: "マルチ: 軸馬の着順を問わず流す（馬単・3連単）",
  formation: "フォーメーション: 着順ごとに候補を絞る",
};
const SINGLE_TYPES: ReadonlySet<BetType> = new Set(["tansho", "fukusho"]);
const ORDERED_TYPES: ReadonlySet<BetType> = new Set(["umatan", "3rentan"]);
const TRIO_TYPES: ReadonlySet<BetType> = new Set(["3renpuku", "3rentan"]);
const HEATMAP_COLUMNS: readonly string[] = [
  "horse",
  "jockey",
  "trainer",
  "jockeyVenue",
  "horseJockey",
  "jockeyTrainerVenue",
  "sire",
  "sireDamSire",
  "frame",
  "owner",
];
const RECENT_LIMIT: number = 5;
const TREND_PLACES: number = 3;
const PAIR_PARTNERS: number = 4;
const TRIO_PARTNERS: number = 5;
const BOX_SIZE: number = 4;
const FORMATION_SECOND: number = 3;
const UNIT_YEN: number = 100;
const PROBABILITY_FLOOR: number = 1e-4;
const ODDS_WEIGHT: number = 0.5;
const RANK_WEIGHT: number = 0.25;
const CLEF_WEIGHT: number = 0.25;
const RANK_DECAY: number = 0.3;
const MODEL_SELECTOR: string = "clef-flash";
const EDGE_SPACES: RegExp = /^[\u3000\s]+|[\u3000\s]+$/gu;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const records = (value: unknown): Record<string, unknown>[] =>
  Array.isArray(value) ? value.filter(isRecord) : [];

const field = (value: unknown, key: string): unknown => (isRecord(value) ? value[key] : undefined);

const text = (value: unknown): string =>
  typeof value === "string" ? value.replace(EDGE_SPACES, "") : "";

const finite = (value: unknown): number | null =>
  typeof value === "number" && Number.isFinite(value) ? value : null;

const toNumber = (value: unknown): number => Number(text(value));

const isBetType = (value: string): value is BetType => BET_TYPES.some((type) => type === value);

// Parses the viewer's comma-separated bet type list; an unknown or empty list is rejected.
export const parseBetTypes = (value: string): Set<BetType> | null => {
  const ids = value.split(",").filter((id) => id !== "");
  return ids.length > 0 && ids.every(isBetType) ? new Set(ids.filter(isBetType)) : null;
};

const isStyle = (value: string): value is Exclude<BetStyle, "single"> =>
  Object.hasOwn(STYLE_LABELS, value);

export const readRunners = (heatmap: unknown): RunnerProfile[] =>
  records(field(heatmap, "runners"))
    .map((runner) => ({
      horseNumber: toNumber(runner.umaban),
      frameNumber: toNumber(runner.wakuban),
      horseName: text(runner.bamei),
      jockey: text(runner.kishumeiRyakusho),
      trainer: text(runner.chokyoshimeiRyakusho),
      owner: text(runner.banushimei),
    }))
    .filter((runner) => runner.horseNumber > 0);

const readOddsEntry = (entry: Record<string, unknown>): OddsEntry => ({
  combination: text(entry.combination),
  odds: finite(entry.odds) ?? finite(entry.averageOdds),
  minOdds: finite(entry.minOdds),
  maxOdds: finite(entry.maxOdds),
});

export const readOdds = (realtime: unknown): { book: OddsBook; fetchedAt: string } => {
  const odds = field(realtime, "odds");
  const latest = field(odds, "latest");
  const fetchedAt = field(odds, "fetchedAt");
  return {
    book: Object.fromEntries(
      BET_TYPES.flatMap((type) => {
        const entries = records(field(latest, type)).map(readOddsEntry);
        return entries.length > 0 ? [[type, entries]] : [];
      }),
    ),
    fetchedAt: typeof fetchedAt === "string" ? fetchedAt : "",
  };
};

const heatmapSummary = (heatmap: unknown): Map<number, Record<string, unknown>> =>
  new Map(
    records(field(field(heatmap, "presentation"), "rows")).map((row) => [
      toNumber(row.horseNumber),
      Object.fromEntries(
        HEATMAP_COLUMNS.map((column) => {
          const cell = field(row.cells, column);
          return [
            column,
            {
              n: field(cell, "name") ?? null,
              s: field(cell, "starts") ?? null,
              w: field(cell, "winRate") ?? null,
              p: field(cell, "showRate") ?? null,
            },
          ];
        }),
      ),
    ]),
  );

const groupBy = (
  rows: readonly Record<string, unknown>[],
  key: string,
): Map<number, Record<string, unknown>[]> =>
  rows.reduce((groups, row) => {
    const horse = toNumber(row[key]);
    groups.set(horse, [...(groups.get(horse) ?? []), row]);
    return groups;
  }, new Map<number, Record<string, unknown>[]>());

const compareRaceDateDesc = (a: Record<string, unknown>, b: Record<string, unknown>): number =>
  `${text(b.kaisaiNen)}${text(b.kaisaiTsukihi)}`.localeCompare(
    `${text(a.kaisaiNen)}${text(a.kaisaiTsukihi)}`,
  );

const recentSummary = (recent: unknown): Map<number, unknown[]> =>
  new Map(
    [...groupBy(records(field(recent, "results")), "currentUmaban")].map(([horse, rows]) => [
      horse,
      rows
        .toSorted(compareRaceDateDesc)
        .slice(0, RECENT_LIMIT)
        .map((row) => ({
          d: `${text(row.kaisaiNen)}${text(row.kaisaiTsukihi)}`,
          v: text(row.keibajoCode),
          m: text(row.kyori),
          f: toNumber(row.kakuteiChakujun),
          pop: toNumber(row.tanshoNinkijun),
          j: text(row.kishumeiRyakusho),
        })),
    ]),
  );

const trainingSummary = (training: unknown): Map<number, unknown> =>
  new Map(
    records(field(training, "trainings")).map((row) => [
      toNumber(row.umaban),
      {
        grade: text(row.premiumEvaluationGrade),
        note: text(row.premiumEvaluationText),
        index: row.premiumWorkoutIndex ?? null,
        f3: text(row.timeGokei3f),
      },
    ]),
  );

const styleSummary = (trends: unknown): Map<number, string> =>
  new Map(
    records(field(trends, "currentRunningStyles")).map((row) => [
      toNumber(row.horseNumber),
      text(row.predictedLabel),
    ]),
  );

// Earlier finishers at the same venue today describe the track bias.
const trendSummary = (trends: unknown): unknown[] =>
  records(field(trends, "starterRows"))
    .filter((row) => {
      const place = finite(row.finishPosition);
      return place !== null && place >= 1 && place <= TREND_PLACES;
    })
    .map((row) => ({
      r: text(row.raceBango),
      f: row.finishPosition,
      frame: toNumber(row.wakuban),
      c4: row.corner4 ?? null,
      pop: row.tanshoPopularity ?? null,
    }));

const horseCriteria = (runners: readonly RunnerProfile[]): Record<string, string> =>
  Object.fromEntries(
    runners.map((runner) => [
      String(runner.horseNumber),
      `${runner.horseNumber} ${runner.horseName}`,
    ]),
  );

const winOdds = (odds: OddsBook, horse: number): number | null =>
  odds.tansho?.find((entry) => entry.combination === String(horse))?.odds ?? null;

export const buildBetRequest = (sources: BetSources): Omit<BetRace, "raceKey"> => {
  const runners = readRunners(sources.heatmap);
  const { book, fetchedAt } = readOdds(sources.realtime);
  const heat = heatmapSummary(sources.heatmap);
  const recent = recentSummary(sources.recent);
  const training = trainingSummary(sources.training);
  const styles = styleSummary(sources.trends);
  const types = BET_TYPES.filter(
    (type) => (book[type]?.length ?? 0) > 0 && !PROMPT_EXCLUDED_TYPES.has(type),
  );
  const horses = horseCriteria(runners);
  const state = {
    race: { todayPlacings: trendSummary(sources.trends) },
    runners: runners.map((runner) => ({
      no: runner.horseNumber,
      frame: runner.frameNumber,
      name: runner.horseName,
      jockey: runner.jockey,
      trainer: runner.trainer,
      modelRank: sources.ranks.get(runner.horseNumber) ?? null,
      winOdds: winOdds(book, runner.horseNumber),
      style: styles.get(runner.horseNumber) ?? null,
      heatmap: heat.get(runner.horseNumber) ?? null,
      recent: recent.get(runner.horseNumber) ?? [],
      training: training.get(runner.horseNumber) ?? null,
    })),
  };
  const questions = {
    winner: {
      type: "choice",
      instructions:
        "Which horse wins this race? Use the model rank, win odds, heatmap win/show rates, recent finishes, training and today's placings.",
      criteria: horses,
    },
    ...Object.fromEntries(
      runners.map((runner) => [
        `top3_${String(runner.horseNumber)}`,
        {
          type: "noul",
          instructions: `Does horse ${String(runner.horseNumber)} (${runner.horseName}) finish in the top 3?`,
        },
      ]),
    ),
    bet_type: {
      type: "choice",
      instructions:
        "Which bet type gives the best expected return for a small fixed budget given the odds and how predictable this race is?",
      criteria: Object.fromEntries(types.map((type) => [type, BET_TYPE_LABELS[type]])),
    },
    bet_style: {
      type: "choice",
      instructions:
        "Which ticket structure suits this race: a confident single axis, an open box, or a formation?",
      criteria: STYLE_LABELS,
    },
    axis: {
      type: "choice",
      instructions: "Which horse is the most reliable axis to build tickets around?",
      criteria: horses,
    },
  };
  return {
    runners,
    odds: book,
    ranks: sources.ranks,
    oddsFetchedAt: fetchedAt,
    request: { model: MODEL_SELECTOR, state, questions },
  };
};

const probabilitiesOf = (answers: unknown, id: string): Record<string, number> | null => {
  const probabilities = field(field(answers, id), "probabilities");
  return isRecord(probabilities) &&
    Object.values(probabilities).every((value) => finite(value) !== null)
    ? Object.fromEntries(Object.entries(probabilities).map(([key, value]) => [key, Number(value)]))
    : null;
};

const answersRoot = (value: unknown): unknown =>
  isRecord(field(value, "answers"))
    ? field(value, "answers")
    : field(field(value, "result"), "answers");

export const parseBetAnswers = (value: unknown, horses: readonly number[]): BetAnswers | null => {
  const answers = answersRoot(value);
  const winner = probabilitiesOf(answers, "winner");
  const betType = probabilitiesOf(answers, "bet_type");
  const style = probabilitiesOf(answers, "bet_style");
  const axis = probabilitiesOf(answers, "axis");
  const top3 = Object.fromEntries(
    horses.map((horse) => [
      String(horse),
      finite(field(field(answers, `top3_${String(horse)}`), "noul")),
    ]),
  );
  return winner === null ||
    betType === null ||
    style === null ||
    axis === null ||
    Object.values(top3).some((value) => value === null)
    ? null
    : {
        winner,
        top3: Object.fromEntries(Object.entries(top3).map(([k, v]) => [k, Number(v)])),
        betType,
        style,
        axis,
      };
};

const argmax = (probabilities: Record<string, number>): string | null =>
  Object.entries(probabilities).reduce<[string, number] | null>(
    (best, entry) => (best === null || entry[1] > best[1] ? entry : best),
    null,
  )?.[0] ?? null;

const floorLog = (value: number): number => Math.log(Math.max(value, PROBABILITY_FLOOR));

// Blends odds-implied, model-rank and clef-flash win probabilities in log space.
export const winProbabilities = (
  race: Pick<BetRace, "runners" | "odds" | "ranks">,
  winner: Record<string, number>,
): Map<number, number> => {
  const horses = race.runners.map((runner) => runner.horseNumber);
  const fallbackRank = horses.length;
  const weights = horses.map((horse): [number, number] => {
    const odds = winOdds(race.odds, horse);
    const rank = race.ranks.get(horse) ?? fallbackRank;
    return [
      horse,
      Math.exp(
        ODDS_WEIGHT * floorLog(odds === null ? 1 / horses.length : 1 / odds) +
          RANK_WEIGHT * -RANK_DECAY * (rank - 1) +
          CLEF_WEIGHT * floorLog(winner[String(horse)] ?? 0),
      ),
    ];
  });
  const total = weights.reduce((sum, [, weight]) => sum + weight, 0);
  return new Map(weights.map(([horse, weight]) => [horse, weight / total]));
};

const orderedProbability = (p: ReadonlyMap<number, number>, horses: readonly number[]): number =>
  horses.reduce(
    (acc, horse) => ({
      value: acc.value * ((p.get(horse) ?? 0) / Math.max(1 - acc.used, PROBABILITY_FLOOR)),
      used: acc.used + (p.get(horse) ?? 0),
    }),
    { value: 1, used: 0 },
  ).value;

const permutations = (horses: readonly number[]): number[][] =>
  horses.length <= 1
    ? [[...horses]]
    : horses.flatMap((horse, index) =>
        permutations(horses.filter((_, other) => other !== index)).map((rest) => [horse, ...rest]),
      );

const setProbability = (p: ReadonlyMap<number, number>, horses: readonly number[]): number =>
  permutations(horses).reduce((sum, order) => sum + orderedProbability(p, order), 0);

const othersOf = (p: ReadonlyMap<number, number>, horses: readonly number[]): number[] =>
  [...p.keys()].filter((horse) => !horses.includes(horse));

const pairsOf = (horses: readonly number[]): number[][] =>
  horses.flatMap((first, index) => horses.slice(index + 1).map((second) => [first, second]));

const frameKey = (frames: ReadonlyMap<number, number>, horses: readonly number[]): string =>
  horses
    .map((horse) => Number(frames.get(horse)))
    .toSorted((a, b) => a - b)
    .join("-");

// 枠連 pays when the first two horses come from the two frames, so a frame pair sums the
// probabilities of every horse pair drawn from those frames (a frame may pair with itself).
const framePairProbability = (
  p: ReadonlyMap<number, number>,
  frames: ReadonlyMap<number, number>,
  pair: readonly number[],
): number =>
  pairsOf([...p.keys()])
    .filter((horses) => frameKey(frames, horses) === pair.join("-"))
    .reduce((sum, horses) => sum + setProbability(p, horses), 0);

// Harville probability that a ticket of the given type pays out. For 枠連 the combination
// holds frame numbers, resolved through the horse-to-frame map.
export const ticketProbability = (
  p: ReadonlyMap<number, number>,
  betType: BetType,
  horses: readonly number[],
  frames: ReadonlyMap<number, number>,
): number => {
  const table: Record<BetType, () => number> = {
    tansho: () => p.get(horses[0] ?? 0) ?? 0,
    fukusho: () =>
      pairsOf(othersOf(p, horses)).reduce(
        (sum, pair) => sum + setProbability(p, [...horses, ...pair]),
        0,
      ),
    umaren: () => setProbability(p, horses),
    wide: () =>
      othersOf(p, horses).reduce((sum, third) => sum + setProbability(p, [...horses, third]), 0),
    umatan: () => orderedProbability(p, horses),
    "3renpuku": () => setProbability(p, horses),
    "3rentan": () => orderedProbability(p, horses),
    wakuren: () => framePairProbability(p, frames, horses),
  };
  return table[betType]();
};

const unique = (combos: readonly number[][], ordered: boolean): number[][] => [
  ...new Map(
    combos
      .filter((combo) => new Set(combo).size === combo.length)
      .map((combo) => (ordered ? combo : combo.toSorted((a, b) => a - b)))
      .map((combo) => [combo.join("-"), combo]),
  ).values(),
];

const product = (legs: readonly number[][]): number[][] =>
  legs.reduce<number[][]>(
    (acc, leg) => acc.flatMap((prefix) => leg.map((horse) => [...prefix, horse])),
    [[]],
  );

const subsets = (horses: readonly number[], size: number): number[][] =>
  size === 0
    ? [[]]
    : horses.flatMap((horse, index) =>
        subsets(horses.slice(index + 1), size - 1).map((rest) => [horse, ...rest]),
      );

const legSizeOf = (betType: BetType): number =>
  SINGLE_TYPES.has(betType) ? 1 : TRIO_TYPES.has(betType) ? 3 : 2;

// Builds the legs (display) and the concrete combinations for one bet type and style.
export const planTickets = ({ betType, style, axis, order }: PlanInput): Plan => {
  const size = legSizeOf(betType);
  const ordered = ORDERED_TYPES.has(betType);
  const partners = order
    .filter((horse) => horse !== axis)
    .slice(0, size === 3 ? TRIO_PARTNERS : PAIR_PARTNERS);
  if (size === 1) return { legs: [[axis]], combos: [[axis]] };
  const box = [axis, ...partners].slice(0, BOX_SIZE);
  const nagashi = subsets(partners, size - 1).flatMap((rest) =>
    ordered ? permutations(rest).map((tail) => [axis, ...tail]) : [[axis, ...rest]],
  );
  const builders: Record<Exclude<BetStyle, "single">, () => Plan> = {
    box: () => ({
      legs: [box],
      combos: subsets(box, size).flatMap((combo) => (ordered ? permutations(combo) : [combo])),
    }),
    nagashi: () => ({ legs: [[axis], partners], combos: nagashi }),
    multi: () => ({
      legs: [[axis], partners],
      combos: ordered ? nagashi.flatMap((combo) => permutations(combo)) : nagashi,
    }),
    formation: () => {
      const legs =
        size === 3
          ? [[axis], partners.slice(0, FORMATION_SECOND), partners]
          : [
              [axis, ...partners.slice(0, 1)],
              [axis, ...partners],
            ];
      return { legs, combos: product(legs) };
    },
  };
  const plan = builders[style === "single" ? "nagashi" : style]();
  return { legs: plan.legs, combos: unique(plan.combos, ordered) };
};

const oddsKey = (betType: BetType, horses: readonly number[]): string =>
  (ORDERED_TYPES.has(betType) ? [...horses] : horses.toSorted((a, b) => a - b)).join("-");

const frameMap = (race: Pick<BetRace, "runners">): Map<number, number> =>
  new Map(race.runners.map((runner) => [runner.horseNumber, runner.frameNumber]));

// A horse-pair plan becomes the distinct frame pairs its tickets fall in.
const toFramePairs = (
  combos: readonly number[][],
  frames: ReadonlyMap<number, number>,
): number[][] => [
  ...new Map(
    combos.map((combo) => {
      const key = frameKey(frames, combo);
      return [key, key.split("-").map(Number)];
    }),
  ).values(),
];

const toCandidate = (
  race: Pick<BetRace, "odds" | "runners">,
  p: ReadonlyMap<number, number>,
  betType: BetType,
  horses: number[],
): Candidate | null => {
  const entry = race.odds[betType]?.find((row) => row.combination === oddsKey(betType, horses));
  const odds = entry?.minOdds ?? entry?.odds ?? null;
  return entry === undefined || odds === null
    ? null
    : {
        horses,
        odds,
        maxOdds: entry.maxOdds,
        probability: ticketProbability(p, betType, horses, frameMap(race)),
      };
};

const compareProbabilityDesc = (a: Candidate, b: Candidate): number =>
  b.probability - a.probability || a.odds - b.odds;

const compareRemainderDesc = (a: Weighted, b: Weighted): number =>
  b.share - Math.floor(b.share) - (a.share - Math.floor(a.share));

interface KeptCandidates {
  kept: Candidate[];
  used: number;
}

// Dutching split in 100-yen units: every ticket gets its minimum (one unit, or what reaches the
// target profit), the rest follows 1/odds so each hit pays back about the same, and leftover
// units go to the largest remainders. The likeliest tickets are kept while their minimums fit.
export const allocateUnits = (
  candidates: readonly Candidate[],
  units: number,
  minimumUnits: (candidate: Candidate) => number,
): Allocation[] => {
  const { kept, used } = candidates
    .toSorted(compareProbabilityDesc)
    .reduce<KeptCandidates>(
      (acc, candidate) =>
        acc.used + minimumUnits(candidate) > units
          ? acc
          : { kept: [...acc.kept, candidate], used: acc.used + minimumUnits(candidate) },
      { kept: [], used: 0 },
    );
  const spare = units - used;
  const totalWeight = kept.reduce((sum, candidate) => sum + 1 / candidate.odds, 0);
  const weighted = kept.map((candidate) => ({
    candidate,
    share: (spare * (1 / candidate.odds)) / totalWeight,
  }));
  const floored = weighted.reduce((sum, row) => sum + Math.floor(row.share), 0);
  const bonus = new Set(
    weighted
      .toSorted(compareRemainderDesc)
      .slice(0, spare - floored)
      .map((row) => row.candidate),
  );
  return weighted.map((row) => ({
    candidate: row.candidate,
    units: minimumUnits(row.candidate) + Math.floor(row.share) + (bonus.has(row.candidate) ? 1 : 0),
  }));
};

const compareWinDesc = (a: [number, number], b: [number, number]): number =>
  b[1] - a[1] || a[0] - b[0];

const pickType = (race: Pick<BetRace, "odds">, answers: BetAnswers): BetType => {
  const choice = argmax(
    Object.fromEntries(
      Object.entries(answers.betType).filter(
        ([type]) => isBetType(type) && (race.odds[type]?.length ?? 0) > 0,
      ),
    ),
  );
  return choice !== null && isBetType(choice) ? choice : "tansho";
};

const pickStyle = (betType: BetType, answers: BetAnswers): BetStyle => {
  const choice = argmax(answers.style);
  return SINGLE_TYPES.has(betType)
    ? "single"
    : choice !== null && isStyle(choice)
      ? choice
      : "nagashi";
};

export const suggestBets = ({ race, answers, budget }: SuggestInput): BetSuggestion | null => {
  const p = winProbabilities(race, answers.winner);
  const order = [...p].toSorted(compareWinDesc).map(([horse]) => horse);
  const chosenAxis = Number(argmax(answers.axis));
  const axis = p.has(chosenAxis) ? chosenAxis : (order[0] ?? 0);
  const betType = pickType(race, answers);
  const style = pickStyle(betType, answers);
  const plan = planTickets({ betType, style, axis, order });
  const candidates = plan.combos
    .map((horses) => toCandidate(race, p, betType, horses))
    .filter((candidate) => candidate !== null);
  const units = Math.floor(budget / UNIT_YEN);
  if (candidates.length === 0 || units < 1) return null;
  const tickets = allocateUnits(candidates, units, () => 1).map(({ candidate, units: count }) => ({
    horses: candidate.horses,
    odds: candidate.odds,
    maxOdds: candidate.maxOdds,
    probability: candidate.probability,
    stake: count * UNIT_YEN,
    payout: Math.floor(count * UNIT_YEN * candidate.odds),
    payoutMax: candidate.maxOdds === null ? null : Math.floor(count * UNIT_YEN * candidate.maxOdds),
  }));
  return {
    betType,
    style,
    axis,
    legs: plan.legs,
    budget,
    totalStake: tickets.reduce((sum, ticket) => sum + ticket.stake, 0),
    expectedReturn: Math.round(
      tickets.reduce((sum, ticket) => sum + ticket.probability * ticket.payout, 0),
    ),
    tickets,
  };
};

export interface BetPlanSet {
  budget: number;
  totalStake: number;
  expectedReturn: number;
  plans: [BetSuggestion, ...BetSuggestion[]];
}

interface PlanSlot {
  betType: BetType;
  style: BetStyle;
  units: number;
}

interface PlanContext {
  race: Pick<BetRace, "odds" | "runners">;
  p: ReadonlyMap<number, number>;
  order: readonly number[];
  axis: number;
  minimumUnits: (candidate: Candidate) => number;
}

interface PlanAccumulator {
  plans: BetSuggestion[];
  carry: number;
}

// Plan count grows with the budget (100-yen units); a single bet type is never suggested.
const PLAN_COUNT_STEPS: readonly (readonly [number, number])[] = [
  [30, 4],
  [10, 3],
  [0, 2],
];
const MIN_PARTNER_POOL: number = 3;
// The default view always suggests at least two bet types; a viewer's selection needs only as
// many as its selected types that have odds.
const MIN_PLAN_TYPES: number = 2;

const compareEntryDesc = (a: [string, number], b: [string, number]): number =>
  b[1] - a[1] || a[0].localeCompare(b[0]);

const rankedTypes = (
  race: Pick<BetRace, "odds">,
  answers: BetAnswers,
  selected: ReadonlySet<BetType>,
): BetType[] =>
  BET_TYPES.filter((type) => selected.has(type) && (race.odds[type]?.length ?? 0) > 0).toSorted(
    (a, b) => (answers.betType[b] ?? 0) - (answers.betType[a] ?? 0),
  );

const rankedStyles = (answers: BetAnswers): Exclude<BetStyle, "single">[] =>
  Object.entries(answers.style)
    .toSorted(compareEntryDesc)
    .map(([style]) => style)
    .filter(isStyle);

const toSuggestion = (
  slot: PlanSlot & { axis: number; legs: number[][] },
  candidates: readonly Candidate[],
  minimumUnits: (candidate: Candidate) => number,
): BetSuggestion => {
  const tickets = allocateUnits(candidates, slot.units, minimumUnits).map(
    ({ candidate, units: count }) => ({
      horses: candidate.horses,
      odds: candidate.odds,
      maxOdds: candidate.maxOdds,
      probability: candidate.probability,
      stake: count * UNIT_YEN,
      payout: Math.floor(count * UNIT_YEN * candidate.odds),
      payoutMax:
        candidate.maxOdds === null ? null : Math.floor(count * UNIT_YEN * candidate.maxOdds),
    }),
  );
  return {
    betType: slot.betType,
    style: slot.style,
    axis: slot.axis,
    legs: slot.legs,
    budget: slot.units * UNIT_YEN,
    totalStake: tickets.reduce((sum, ticket) => sum + ticket.stake, 0),
    expectedReturn: Math.round(
      tickets.reduce((sum, ticket) => sum + ticket.probability * ticket.payout, 0),
    ),
    tickets,
  };
};

// Shrinks the partner pool until every ticket of the structure can get at least 100 yen, so the
// displayed legs always match the tickets bought.
const buildPlan = (context: PlanContext, slot: PlanSlot): BetSuggestion | null => {
  const pools = Array.from(
    { length: Math.max(context.order.length - MIN_PARTNER_POOL + 1, 1) },
    (_, index) => context.order.length - index,
  );
  const fitted = pools
    .map((size) => {
      const plan = planTickets({
        betType: slot.betType,
        style: slot.style,
        axis: context.axis,
        order: context.order.slice(0, size),
      });
      const combos =
        slot.betType === "wakuren"
          ? toFramePairs(plan.combos, frameMap(context.race))
          : plan.combos;
      const candidates = combos
        .map((horses) => toCandidate(context.race, context.p, slot.betType, horses))
        .filter((candidate) => candidate !== null);
      return { plan, combos, candidates };
    })
    .find(
      ({ combos, candidates }) =>
        candidates.length > 0 &&
        candidates.length === combos.length &&
        candidates.reduce((sum, candidate) => sum + context.minimumUnits(candidate), 0) <=
          slot.units,
    );
  return fitted === undefined
    ? null
    : toSuggestion(
        { ...slot, axis: context.axis, legs: fitted.plan.legs },
        fitted.candidates,
        context.minimumUnits,
      );
};

const splitUnits = (weights: readonly number[], units: number): number[] => {
  const total = weights.reduce((sum, weight) => sum + weight, 0);
  const base = weights.map((weight) =>
    Math.max(1, Math.floor(units * (total > 0 ? weight / total : 1 / weights.length))),
  );
  const used = base.reduce((sum, value) => sum + value, 0);
  return base.map((value, index) => (index === 0 ? value + units - used : value));
};

interface PricedTicket {
  betType: BetType;
  candidate: Candidate;
}

interface TargetContext {
  tickets: readonly PricedTicket[];
  sets: readonly number[][];
  units: number;
  need: number;
}

interface TargetInput {
  plans: readonly BetSuggestion[];
  race: Pick<BetRace, "runners">;
  budget: number;
  target: number;
}

const TOP_PLACES: number = 3;

const ticketHits = (
  betType: BetType,
  horses: readonly number[],
  order: readonly number[],
  frames: ReadonlyMap<number, number>,
): boolean => {
  const top2 = new Set(order.slice(0, 2));
  const top3 = new Set(order);
  const table: Record<BetType, () => boolean> = {
    tansho: () => horses[0] === order[0],
    fukusho: () => top3.has(horses[0] ?? 0),
    wide: () => horses.every((horse) => top3.has(horse)),
    umaren: () => horses.every((horse) => top2.has(horse)),
    umatan: () => horses[0] === order[0] && horses[1] === order[1],
    wakuren: () => frameKey(frames, order.slice(0, 2)) === horses.join("-"),
    "3renpuku": () => horses.every((horse) => top3.has(horse)),
    "3rentan": () => horses.every((horse, index) => order[index] === horse),
  };
  return table[betType]();
};

// The distinct groups of tickets a single finishing order pays out together.
const hitSets = (tickets: readonly PricedTicket[], race: Pick<BetRace, "runners">): number[][] => {
  const frames = frameMap(race);
  const horses = race.runners.map((runner) => runner.horseNumber);
  const orders = subsets(horses, Math.min(TOP_PLACES, horses.length)).flatMap(permutations);
  return [
    ...new Map(
      orders
        .map((order) =>
          tickets.flatMap((ticket, index) =>
            ticketHits(ticket.betType, ticket.candidate.horses, order, frames) ? [index] : [],
          ),
        )
        .filter((set) => set.length > 0)
        .map((set) => [set.join(","), set]),
    ).values(),
  ];
};

const setPayout = (
  context: TargetContext,
  units: readonly number[],
  set: readonly number[],
): number =>
  set.reduce(
    (sum, index) =>
      sum +
      Math.floor((units[index] ?? 0) * UNIT_YEN * (context.tickets[index]?.candidate.odds ?? 0)),
    0,
  );

const compareOddsIndex =
  (context: TargetContext) =>
  (best: number, index: number): number =>
    (context.tickets[index]?.candidate.odds ?? 0) > (context.tickets[best]?.candidate.odds ?? 0)
      ? index
      : best;

// Raises the worst-paying finishing order until every order that hits a ticket returns the
// budget plus the target, adding units to its highest-odds ticket; null when the budget runs out.
const raiseToTarget = (context: TargetContext, units: readonly number[]): number[] | null => {
  const worst = context.sets.reduce<number[] | null>(
    (lowest, set) =>
      lowest === null || setPayout(context, units, set) < setPayout(context, units, lowest)
        ? set
        : lowest,
    null,
  );
  const used = units.reduce((sum, count) => sum + count, 0);
  if (worst === null || setPayout(context, units, worst) >= context.need) return [...units];
  const best = worst.reduce(compareOddsIndex(context), worst[0] ?? 0);
  const odds = context.tickets[best]?.candidate.odds ?? 0;
  const add = Math.ceil((context.need - setPayout(context, units, worst)) / (UNIT_YEN * odds));
  return used + add > context.units
    ? null
    : raiseToTarget(
        context,
        units.map((count, index) => (index === best ? count + add : count)),
      );
};

const toTicket = (candidate: Candidate, count: number): BetTicket => ({
  horses: candidate.horses,
  odds: candidate.odds,
  maxOdds: candidate.maxOdds,
  probability: candidate.probability,
  stake: count * UNIT_YEN,
  payout: Math.floor(count * UNIT_YEN * candidate.odds),
  payoutMax: candidate.maxOdds === null ? null : Math.floor(count * UNIT_YEN * candidate.maxOdds),
});

// Re-stakes every ticket of every plan together so that the whole suggestion, not each ticket,
// reaches the target; spare units still follow 1/odds.
const applyTarget = ({ plans, race, budget, target }: TargetInput): BetSuggestion[] | null => {
  const grouped = plans.map((plan) =>
    plan.tickets.map((ticket) => ({
      betType: plan.betType,
      candidate: {
        horses: ticket.horses,
        odds: ticket.odds,
        maxOdds: ticket.maxOdds,
        probability: ticket.probability,
      },
    })),
  );
  const tickets = grouped.flat();
  const units = Math.floor(budget / UNIT_YEN);
  const context = { tickets, sets: hitSets(tickets, race), units, need: budget + target };
  const minimums =
    tickets.length > units
      ? null
      : raiseToTarget(
          context,
          tickets.map(() => 1),
        );
  if (minimums === null) return null;
  const minimumOf = new Map(
    tickets.map((ticket, index) => [ticket.candidate, minimums[index] ?? 1]),
  );
  const counts = new Map(
    allocateUnits(
      tickets.map((ticket) => ticket.candidate),
      units,
      (candidate) => minimumOf.get(candidate) ?? 1,
    ).map((allocation) => [allocation.candidate, allocation.units]),
  );
  return plans.map((plan, index) => {
    const restaked = (grouped[index] ?? []).map((ticket) =>
      toTicket(ticket.candidate, counts.get(ticket.candidate) ?? 0),
    );
    const totalStake = restaked.reduce((sum, ticket) => sum + ticket.stake, 0);
    return {
      ...plan,
      budget: totalStake,
      totalStake,
      expectedReturn: Math.round(
        restaked.reduce((sum, ticket) => sum + ticket.probability * ticket.payout, 0),
      ),
      tickets: restaked,
    };
  });
};

interface RequiredPlanInput {
  requireAll: boolean;
  profitTarget: number | undefined;
  selected: ReadonlySet<BetType> | undefined;
  typeCount: number;
}

// A profit target may leave room for only one bet type, which is still a valid suggestion,
// unless every selected type is required.
const requiredPlanCount = ({
  requireAll,
  profitTarget,
  selected,
  typeCount,
}: RequiredPlanInput): number => {
  if (requireAll) return typeCount;
  if (profitTarget !== undefined) return 1;
  return selected === undefined ? MIN_PLAN_TYPES : Math.min(typeCount, MIN_PLAN_TYPES);
};

interface FitInput extends Omit<TargetInput, "plans"> {
  required: number;
}

// Drops the lowest-ranked bet type until the rest can reach the target together.
const fitTarget = (plans: readonly BetSuggestion[], input: FitInput): BetSuggestion[] | null => {
  if (plans.length < Math.max(input.required, 1)) return null;
  return applyTarget({ ...input, plans }) ?? fitTarget(plans.slice(0, -1), input);
};

export const suggestPlans = ({
  race,
  answers,
  budget,
  types: selected,
  target: profitTarget,
  requireAll = false,
}: SuggestInput): BetPlanSet | null => {
  const p = winProbabilities(race, answers.winner);
  const order = [...p].toSorted(compareWinDesc).map(([horse]) => horse);
  const chosenAxis = Number(argmax(answers.axis));
  const axis = p.has(chosenAxis) ? chosenAxis : (order[0] ?? 0);
  const units = Math.floor(budget / UNIT_YEN);
  const types = rankedTypes(race, answers, selected ?? new Set(BET_TYPES));
  const styles = rankedStyles(answers);
  const target = requireAll
    ? types.length
    : Math.min(
        types.length,
        units,
        PLAN_COUNT_STEPS.find(([threshold]) => units >= threshold)?.[1] ?? 2,
      );
  // A required type the answers rank low still needs room for its tickets, so the split is
  // nearly even when every selected type must appear.
  const shares = splitUnits(
    types.slice(0, target).map((type) => (requireAll ? 1 : 0) + (answers.betType[type] ?? 0)),
    units,
  );
  const buildAll = (pool: number): BetSuggestion[] => {
    const context: PlanContext = {
      race,
      p,
      order: order.slice(0, pool),
      axis,
      minimumUnits: () => 1,
    };
    return types.reduce<PlanAccumulator>(
      (acc, betType, index) => {
        const slotUnits = (shares[index] ?? 0) + acc.carry;
        if (acc.plans.length >= target || slotUnits < 1) return acc;
        const fitting = styles.filter((style) => style !== "multi" || ORDERED_TYPES.has(betType));
        const multiHorse = acc.plans.filter((plan) => plan.style !== "single").length;
        const style = SINGLE_TYPES.has(betType)
          ? "single"
          : (fitting[multiHorse % Math.max(fitting.length, 1)] ?? "nagashi");
        const plan = buildPlan(context, { betType, style, units: slotUnits });
        return plan === null
          ? { plans: acc.plans, carry: slotUnits }
          : { plans: [...acc.plans, plan], carry: 0 };
      },
      { plans: [], carry: 0 },
    ).plans;
  };
  const required = requiredPlanCount({
    requireAll,
    profitTarget,
    selected,
    typeCount: types.length,
  });
  // With a target, fewer partners mean fewer tickets to fund, so the pool narrows until the
  // whole suggestion can reach it.
  const pools = Array.from(
    { length: Math.max(order.length - MIN_PARTNER_POOL + 1, 1) },
    (_, index) => order.length - index,
  );
  // Tries the widest pool first and stops at the first that fits.
  const fitFromPool = (remaining: readonly number[], goal: number): BetSuggestion[] | null => {
    const [pool, ...rest] = remaining;
    if (pool === undefined) return null;
    const built = buildAll(pool);
    const fitted =
      built.length >= required ? fitTarget(built, { race, budget, target: goal, required }) : null;
    return fitted ?? fitFromPool(rest, goal);
  };
  const plans =
    profitTarget === undefined ? buildAll(order.length) : (fitFromPool(pools, profitTarget) ?? []);
  const [first, ...rest] = plans;
  return first === undefined || plans.length < required
    ? null
    : {
        budget,
        totalStake: plans.reduce((sum, plan) => sum + plan.totalStake, 0),
        expectedReturn: plans.reduce((sum, plan) => sum + plan.expectedReturn, 0),
        plans: [first, ...rest],
      };
};
