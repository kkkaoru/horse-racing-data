// Run with bun (bunx vitest). clef-flash bet suggestions: builds the decision request from the
// viewer's race data, then turns the answers into tickets and a budget split.

export type BetType = "tansho" | "fukusho" | "umaren" | "wide" | "umatan" | "3renpuku" | "3rentan";
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
];
const BET_TYPE_LABELS: Record<BetType, string> = {
  tansho: "単勝: 1着を当てる",
  fukusho: "複勝: 3着以内を当てる",
  umaren: "馬連: 1-2着の組み合わせ（順不同）",
  wide: "ワイド: 3着以内の2頭（順不同）",
  umatan: "馬単: 1-2着を着順通り",
  "3renpuku": "3連複: 1-3着の組み合わせ（順不同）",
  "3rentan": "3連単: 1-3着を着順通り",
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
const FULL_WIDTH_SPACE: RegExp = /[　\s]+$/u;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const records = (value: unknown): Record<string, unknown>[] =>
  Array.isArray(value) ? value.filter(isRecord) : [];

const field = (value: unknown, key: string): unknown => (isRecord(value) ? value[key] : undefined);

const text = (value: unknown): string =>
  typeof value === "string" ? value.replace(FULL_WIDTH_SPACE, "").trim() : "";

const finite = (value: unknown): number | null =>
  typeof value === "number" && Number.isFinite(value) ? value : null;

const toNumber = (value: unknown): number => Number(text(value));

const isBetType = (value: string): value is BetType => BET_TYPES.some((type) => type === value);

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
  const types = BET_TYPES.filter((type) => (book[type]?.length ?? 0) > 0);
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

// Harville probability that a ticket of the given type pays out.
export const ticketProbability = (
  p: ReadonlyMap<number, number>,
  betType: BetType,
  horses: readonly number[],
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

const toCandidate = (
  race: Pick<BetRace, "odds">,
  p: ReadonlyMap<number, number>,
  betType: BetType,
  horses: number[],
): Candidate | null => {
  const entry = race.odds[betType]?.find((row) => row.combination === oddsKey(betType, horses));
  const odds = entry?.minOdds ?? entry?.odds ?? null;
  return entry === undefined || odds === null
    ? null
    : { horses, odds, maxOdds: entry.maxOdds, probability: ticketProbability(p, betType, horses) };
};

const compareProbabilityDesc = (a: Candidate, b: Candidate): number =>
  b.probability - a.probability || a.odds - b.odds;

const compareRemainderDesc = (a: Weighted, b: Weighted): number =>
  b.share - Math.floor(b.share) - (a.share - Math.floor(a.share));

// Dutching split in 100-yen units: every ticket gets one unit, the rest follows 1/odds so
// each hit pays back about the same, and leftover units go to the largest remainders.
export const allocateUnits = (candidates: readonly Candidate[], units: number): Allocation[] => {
  const kept = candidates.toSorted(compareProbabilityDesc).slice(0, units);
  const spare = units - kept.length;
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
    units: 1 + Math.floor(row.share) + (bonus.has(row.candidate) ? 1 : 0),
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
  const tickets = allocateUnits(candidates, units).map(({ candidate, units: count }) => ({
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
