// Run with bun (bunx vitest).
import { expect, it } from "vitest";

import {
  allocateUnits,
  buildBetRequest,
  parseBetAnswers,
  planTickets,
  readOdds,
  readRunners,
  suggestBets,
  suggestPlans,
  ticketProbability,
  winProbabilities,
  type OddsBook,
  type RunnerProfile,
} from "./bets";

const runners = (): RunnerProfile[] => [
  { horseNumber: 1, frameNumber: 1, horseName: "H1", jockey: "J1", trainer: "T1", owner: "O1" },
  { horseNumber: 2, frameNumber: 2, horseName: "H2", jockey: "J2", trainer: "T2", owner: "O2" },
  { horseNumber: 3, frameNumber: 3, horseName: "H3", jockey: "J3", trainer: "T3", owner: "O3" },
  { horseNumber: 4, frameNumber: 4, horseName: "H4", jockey: "J4", trainer: "T4", owner: "O4" },
];

const odds = (): OddsBook => ({
  tansho: [
    { combination: "1", odds: 2, minOdds: null, maxOdds: null },
    { combination: "2", odds: 4, minOdds: null, maxOdds: null },
    { combination: "3", odds: 8, minOdds: null, maxOdds: null },
  ],
  fukusho: [{ combination: "1", odds: 1.3, minOdds: 1.2, maxOdds: 1.5 }],
  umaren: [
    { combination: "1-2", odds: 5, minOdds: null, maxOdds: null },
    { combination: "1-3", odds: 9, minOdds: null, maxOdds: null },
    { combination: "1-4", odds: 20, minOdds: null, maxOdds: null },
  ],
  "3rentan": [{ combination: "1-2-3", odds: 30, minOdds: null, maxOdds: null }],
});

const ranks = (): Map<number, number> =>
  new Map([
    [1, 1],
    [2, 2],
    [3, 3],
  ]);

it("reads runner profiles, trimming padding and dropping unusable rows", () => {
  expect(
    readRunners({
      runners: [
        {
          umaban: "01",
          wakuban: "1",
          bamei: "アルファ　　",
          kishumeiRyakusho: "騎手A",
          chokyoshimeiRyakusho: "調教A",
          banushimei: "馬主A　",
        },
        { umaban: "00", wakuban: "3" },
        "bad",
      ],
    }),
  ).toStrictEqual([
    {
      horseNumber: 1,
      frameNumber: 1,
      horseName: "アルファ",
      jockey: "騎手A",
      trainer: "調教A",
      owner: "馬主A",
    },
  ]);
  expect(readRunners(null)).toStrictEqual([]);
});

it("reads the latest odds book and skips empty bet types", () => {
  expect(
    readOdds({
      odds: {
        fetchedAt: "2026-10-18T09:00:00+09:00",
        latest: {
          tansho: [{ combination: "1", odds: 2.5 }],
          wide: [{ combination: "1-2", averageOdds: 3, minOdds: 2.5, maxOdds: 3.5 }],
          umaren: [],
        },
      },
    }),
  ).toStrictEqual({
    book: {
      tansho: [{ combination: "1", odds: 2.5, minOdds: null, maxOdds: null }],
      wide: [{ combination: "1-2", odds: 3, minOdds: 2.5, maxOdds: 3.5 }],
    },
    fetchedAt: "2026-10-18T09:00:00+09:00",
  });
  expect(readOdds({ odds: { fetchedAt: 1 } })).toStrictEqual({ book: {}, fetchedAt: "" });
});

it("builds the clef-flash state and questions from the viewer data", () => {
  const built = buildBetRequest({
    heatmap: {
      runners: [
        {
          umaban: "01",
          wakuban: "1",
          bamei: "アルファ",
          kishumeiRyakusho: "騎手A",
          chokyoshimeiRyakusho: "調教A",
          banushimei: "馬主A",
        },
        {
          umaban: "02",
          wakuban: "2",
          bamei: "ベータ",
          kishumeiRyakusho: "騎手B",
          chokyoshimeiRyakusho: "調教B",
          banushimei: "馬主B",
        },
      ],
      presentation: {
        rows: [
          {
            horseNumber: "1",
            cells: { horse: { name: "アルファ", starts: 4, winRate: 25, showRate: 50 } },
          },
        ],
      },
    },
    realtime: {
      odds: {
        fetchedAt: "2026-10-18T09:00:00+09:00",
        latest: {
          tansho: [{ combination: "1", odds: 2.5 }],
          wide: [{ combination: "1-2", averageOdds: 3, minOdds: 2.5, maxOdds: 3.5 }],
        },
      },
    },
    recent: {
      results: [
        {
          currentUmaban: "01",
          kaisaiNen: "2026",
          kaisaiTsukihi: "0901",
          keibajoCode: "05",
          kyori: "1600",
          kakuteiChakujun: "02",
          tanshoNinkijun: "03",
          kishumeiRyakusho: "騎手A",
        },
        {
          currentUmaban: "01",
          kaisaiNen: "2026",
          kaisaiTsukihi: "0920",
          keibajoCode: "06",
          kyori: "1800",
          kakuteiChakujun: "01",
          tanshoNinkijun: "01",
          kishumeiRyakusho: "騎手C",
        },
      ],
    },
    training: {
      trainings: [
        {
          umaban: "02",
          premiumEvaluationGrade: "A",
          premiumEvaluationText: "好調",
          premiumWorkoutIndex: 7,
          timeGokei3f: "0380",
        },
      ],
    },
    trends: {
      currentRunningStyles: [{ horseNumber: "1", predictedLabel: "nige" }],
      starterRows: [
        { raceBango: "01", finishPosition: 1, wakuban: "3", corner4: "02", tanshoPopularity: 1 },
        { raceBango: "01", finishPosition: 4, wakuban: "5" },
        { raceBango: "02", finishPosition: null },
        { raceBango: "02", finishPosition: 3, wakuban: "8" },
      ],
    },
    ranks: new Map([[2, 1]]),
  });
  expect(built.oddsFetchedAt).toBe("2026-10-18T09:00:00+09:00");
  expect(built.request.state).toStrictEqual({
    race: {
      todayPlacings: [
        { r: "01", f: 1, frame: 3, c4: "02", pop: 1 },
        { r: "02", f: 3, frame: 8, c4: null, pop: null },
      ],
    },
    runners: [
      {
        no: 1,
        frame: 1,
        name: "アルファ",
        jockey: "騎手A",
        trainer: "調教A",
        modelRank: null,
        winOdds: 2.5,
        style: "nige",
        heatmap: {
          horse: { n: "アルファ", s: 4, w: 25, p: 50 },
          jockey: { n: null, s: null, w: null, p: null },
          trainer: { n: null, s: null, w: null, p: null },
          jockeyVenue: { n: null, s: null, w: null, p: null },
          horseJockey: { n: null, s: null, w: null, p: null },
          jockeyTrainerVenue: { n: null, s: null, w: null, p: null },
          sire: { n: null, s: null, w: null, p: null },
          sireDamSire: { n: null, s: null, w: null, p: null },
          frame: { n: null, s: null, w: null, p: null },
          owner: { n: null, s: null, w: null, p: null },
        },
        recent: [
          { d: "20260920", v: "06", m: "1800", f: 1, pop: 1, j: "騎手C" },
          { d: "20260901", v: "05", m: "1600", f: 2, pop: 3, j: "騎手A" },
        ],
        training: null,
      },
      {
        no: 2,
        frame: 2,
        name: "ベータ",
        jockey: "騎手B",
        trainer: "調教B",
        modelRank: 1,
        winOdds: null,
        style: null,
        heatmap: null,
        recent: [],
        training: { grade: "A", note: "好調", index: 7, f3: "0380" },
      },
    ],
  });
  expect(built.request.questions).toStrictEqual({
    winner: {
      type: "choice",
      instructions:
        "Which horse wins this race? Use the model rank, win odds, heatmap win/show rates, recent finishes, training and today's placings.",
      criteria: { "1": "1 アルファ", "2": "2 ベータ" },
    },
    top3_1: { type: "noul", instructions: "Does horse 1 (アルファ) finish in the top 3?" },
    top3_2: { type: "noul", instructions: "Does horse 2 (ベータ) finish in the top 3?" },
    bet_type: {
      type: "choice",
      instructions:
        "Which bet type gives the best expected return for a small fixed budget given the odds and how predictable this race is?",
      criteria: { tansho: "単勝: 1着を当てる", wide: "ワイド: 3着以内の2頭（順不同）" },
    },
    bet_style: {
      type: "choice",
      instructions:
        "Which ticket structure suits this race: a confident single axis, an open box, or a formation?",
      criteria: {
        box: "ボックス: 上位馬の全組み合わせ",
        nagashi: "軸流し: 軸馬から相手へ流す",
        multi: "マルチ: 軸馬の着順を問わず流す（馬単・3連単）",
        formation: "フォーメーション: 着順ごとに候補を絞る",
      },
    },
    axis: {
      type: "choice",
      instructions: "Which horse is the most reliable axis to build tickets around?",
      criteria: { "1": "1 アルファ", "2": "2 ベータ" },
    },
  });
  expect(built.request.model).toBe("clef-flash");
});

it("builds an empty request when every source is missing", () => {
  const built = buildBetRequest({
    heatmap: null,
    realtime: null,
    recent: null,
    training: null,
    trends: null,
    ranks: new Map(),
  });
  expect(built.runners).toStrictEqual([]);
  expect(built.odds).toStrictEqual({});
  expect(built.oddsFetchedAt).toBe("");
  expect(built.request.state).toStrictEqual({ race: { todayPlacings: [] }, runners: [] });
});

it("parses binding and REST answer shapes", () => {
  const answers = {
    winner: { probabilities: { "1": 0.6, "2": 0.4 } },
    bet_type: { probabilities: { tansho: 1 } },
    bet_style: { probabilities: { box: 1 } },
    axis: { probabilities: { "1": 1 } },
    top3_1: { noul: 0.8 },
    top3_2: { noul: 0.3 },
  };
  expect(parseBetAnswers({ answers }, [1, 2])).toStrictEqual({
    winner: { "1": 0.6, "2": 0.4 },
    top3: { "1": 0.8, "2": 0.3 },
    betType: { tansho: 1 },
    style: { box: 1 },
    axis: { "1": 1 },
  });
  expect(parseBetAnswers({ result: { answers } }, [1])).toStrictEqual({
    winner: { "1": 0.6, "2": 0.4 },
    top3: { "1": 0.8 },
    betType: { tansho: 1 },
    style: { box: 1 },
    axis: { "1": 1 },
  });
});

it("rejects incomplete or malformed answers", () => {
  const answers = {
    winner: { probabilities: { "1": 0.6 } },
    bet_type: { probabilities: { tansho: 1 } },
    bet_style: { probabilities: { box: 1 } },
    axis: { probabilities: { "1": 1 } },
    top3_1: { noul: 0.8 },
  };
  expect(parseBetAnswers({ answers }, [1, 2])).toBeNull();
  expect(
    parseBetAnswers({ answers: { ...answers, winner: { probabilities: { "1": "x" } } } }, [1]),
  ).toBeNull();
  expect(parseBetAnswers({ answers: { ...answers, bet_type: {} } }, [1])).toBeNull();
  expect(parseBetAnswers({ answers: { ...answers, bet_style: null } }, [1])).toBeNull();
  expect(parseBetAnswers({ answers: { ...answers, axis: { probabilities: [] } } }, [1])).toBeNull();
  expect(parseBetAnswers("text", [1])).toBeNull();
});

it("blends odds, model ranks and clef answers into win probabilities", () => {
  expect([
    ...winProbabilities(
      { runners: runners(), odds: odds(), ranks: ranks() },
      {
        "1": 0.5,
        "2": 0.3,
        "3": 0.1,
      },
    ),
  ]).toStrictEqual([
    [1, 0.5175160109810499],
    [2, 0.2987958210944743],
    [3, 0.14893855004693082],
    [4, 0.03474961787754513],
  ]);
});

it("scores every bet type with Harville probabilities", () => {
  const p = new Map([
    [1, 0.5175160109810499],
    [2, 0.2987958210944743],
    [3, 0.14893855004693082],
    [4, 0.03474961787754513],
  ]);
  expect(ticketProbability(p, "tansho", [1])).toBe(0.5175160109810499);
  expect(ticketProbability(p, "fukusho", [1])).toBe(0.9828147891515896);
  expect(ticketProbability(p, "umaren", [1, 2])).toBe(0.5410136398562542);
  expect(ticketProbability(p, "wide", [1, 2])).toBe(0.9308849359548772);
  expect(ticketProbability(p, "umatan", [1, 2])).toBe(0.3204906793799254);
  expect(ticketProbability(p, "3renpuku", [1, 2, 3])).toBe(0.7713789680549261);
  expect(ticketProbability(p, "3rentan", [1, 2, 3])).toBe(0.2598611420090362);
  expect(ticketProbability(p, "tansho", [])).toBe(0);
  expect(ticketProbability(p, "umatan", [9, 1])).toBe(0);
});

it("plans boxes for unordered and ordered pair types", () => {
  expect(
    planTickets({ betType: "umaren", style: "box", axis: 2, order: [1, 2, 3, 4, 5, 6, 7] }),
  ).toStrictEqual({
    legs: [[2, 1, 3, 4]],
    combos: [
      [1, 2],
      [2, 3],
      [2, 4],
      [1, 3],
      [1, 4],
      [3, 4],
    ],
  });
  expect(
    planTickets({ betType: "umatan", style: "box", axis: 2, order: [1, 2, 3, 4, 5, 6, 7] }),
  ).toStrictEqual({
    legs: [[2, 1, 3, 4]],
    combos: [
      [2, 1],
      [1, 2],
      [2, 3],
      [3, 2],
      [2, 4],
      [4, 2],
      [1, 3],
      [3, 1],
      [1, 4],
      [4, 1],
      [3, 4],
      [4, 3],
    ],
  });
});

it("plans axis flows for trio types", () => {
  expect(
    planTickets({ betType: "3renpuku", style: "nagashi", axis: 2, order: [1, 2, 3, 4, 5, 6, 7] }),
  ).toStrictEqual({
    legs: [[2], [1, 3, 4, 5, 6]],
    combos: [
      [1, 2, 3],
      [1, 2, 4],
      [1, 2, 5],
      [1, 2, 6],
      [2, 3, 4],
      [2, 3, 5],
      [2, 3, 6],
      [2, 4, 5],
      [2, 4, 6],
      [2, 5, 6],
    ],
  });
  expect(
    planTickets({ betType: "3rentan", style: "nagashi", axis: 2, order: [1, 2, 3, 4] }),
  ).toStrictEqual({
    legs: [[2], [1, 3, 4]],
    combos: [
      [2, 1, 3],
      [2, 3, 1],
      [2, 1, 4],
      [2, 4, 1],
      [2, 3, 4],
      [2, 4, 3],
    ],
  });
});

it("plans multi flows only for ordered types", () => {
  expect(
    planTickets({ betType: "umatan", style: "multi", axis: 2, order: [1, 2, 3] }),
  ).toStrictEqual({
    legs: [[2], [1, 3]],
    combos: [
      [2, 1],
      [1, 2],
      [2, 3],
      [3, 2],
    ],
  });
  expect(
    planTickets({ betType: "umaren", style: "multi", axis: 2, order: [1, 2, 3] }),
  ).toStrictEqual({
    legs: [[2], [1, 3]],
    combos: [
      [1, 2],
      [2, 3],
    ],
  });
  expect(
    planTickets({ betType: "3rentan", style: "multi", axis: 2, order: [1, 2, 3] }),
  ).toStrictEqual({
    legs: [[2], [1, 3]],
    combos: [
      [2, 1, 3],
      [2, 3, 1],
      [1, 2, 3],
      [1, 3, 2],
      [3, 2, 1],
      [3, 1, 2],
    ],
  });
});

it("plans formations for trio and pair types", () => {
  expect(
    planTickets({ betType: "3rentan", style: "formation", axis: 2, order: [1, 2, 3, 4, 5] }),
  ).toStrictEqual({
    legs: [[2], [1, 3, 4], [1, 3, 4, 5]],
    combos: [
      [2, 1, 3],
      [2, 1, 4],
      [2, 1, 5],
      [2, 3, 1],
      [2, 3, 4],
      [2, 3, 5],
      [2, 4, 1],
      [2, 4, 3],
      [2, 4, 5],
    ],
  });
  expect(
    planTickets({ betType: "wide", style: "formation", axis: 2, order: [1, 2, 3] }),
  ).toStrictEqual({
    legs: [
      [2, 1],
      [2, 1, 3],
    ],
    combos: [
      [1, 2],
      [2, 3],
      [1, 3],
    ],
  });
});

it("plans single-horse types and treats a single style on pair types as a flow", () => {
  expect(planTickets({ betType: "tansho", style: "box", axis: 2, order: [1, 2] })).toStrictEqual({
    legs: [[2]],
    combos: [[2]],
  });
  expect(
    planTickets({ betType: "umaren", style: "single", axis: 2, order: [1, 2, 3] }),
  ).toStrictEqual({
    legs: [[2], [1, 3]],
    combos: [
      [1, 2],
      [2, 3],
    ],
  });
});

it("splits units by inverse odds with ties broken by lower odds", () => {
  expect(
    allocateUnits(
      [
        { horses: [3], odds: 3, maxOdds: null, probability: 0.2 },
        { horses: [2], odds: 4, maxOdds: null, probability: 0.5 },
        { horses: [1], odds: 2, maxOdds: null, probability: 0.5 },
      ],
      7,
    ).map((row) => [row.candidate.horses, row.units]),
  ).toStrictEqual([
    [[1], 3],
    [[2], 2],
    [[3], 2],
  ]);
});

it("keeps only the most likely tickets when units are scarce", () => {
  expect(
    allocateUnits(
      [
        { horses: [1], odds: 2, maxOdds: null, probability: 0.5 },
        { horses: [2], odds: 4, maxOdds: null, probability: 0.6 },
      ],
      1,
    ),
  ).toStrictEqual([
    { candidate: { horses: [2], odds: 4, maxOdds: null, probability: 0.6 }, units: 1 },
  ]);
});

it("suggests an axis flow with a dutched budget", () => {
  expect(
    suggestBets({
      race: { runners: runners(), odds: odds(), ranks: ranks() },
      answers: {
        winner: { "1": 0.5, "2": 0.3, "3": 0.1 },
        top3: { "1": 0.9, "2": 0.7, "3": 0.4, "4": 0.1 },
        axis: { "1": 0.6, "2": 0.4 },
        style: { nagashi: 0.7, box: 0.3 },
        betType: { umaren: 0.6, tansho: 0.4 },
      },
      budget: 1000,
    }),
  ).toStrictEqual({
    betType: "umaren",
    style: "nagashi",
    axis: 1,
    legs: [[1], [2, 3, 4]],
    budget: 1000,
    totalStake: 1000,
    expectedReturn: 2252,
    tickets: [
      {
        horses: [1, 2],
        odds: 5,
        maxOdds: null,
        probability: 0.5410136398562542,
        stake: 500,
        payout: 2500,
        payoutMax: null,
      },
      {
        horses: [1, 3],
        odds: 9,
        maxOdds: null,
        probability: 0.2503196284168927,
        stake: 300,
        payout: 2700,
        payoutMax: null,
      },
      {
        horses: [1, 4],
        odds: 20,
        maxOdds: null,
        probability: 0.05590360578457285,
        stake: 200,
        payout: 4000,
        payoutMax: null,
      },
    ],
  });
});

it("skips bet types without odds and shows payout ranges", () => {
  expect(
    suggestBets({
      race: { runners: runners(), odds: odds(), ranks: ranks() },
      answers: {
        winner: { "1": 0.5, "2": 0.3, "3": 0.1 },
        top3: { "1": 0.9, "2": 0.7, "3": 0.4, "4": 0.1 },
        axis: { "1": 0.6, "2": 0.4 },
        style: { nagashi: 0.7 },
        betType: { fukusho: 0.9, wide: 0.95 },
      },
      budget: 300,
    }),
  ).toStrictEqual({
    betType: "fukusho",
    style: "single",
    axis: 1,
    legs: [[1]],
    budget: 300,
    totalStake: 300,
    expectedReturn: 354,
    tickets: [
      {
        horses: [1],
        odds: 1.2,
        maxOdds: 1.5,
        probability: 0.9828147891515896,
        stake: 300,
        payout: 360,
        payoutMax: 450,
      },
    ],
  });
});

it("falls back to the favourite, a win bet and a flow on unusable answers", () => {
  expect(
    suggestBets({
      race: { runners: runners(), odds: odds(), ranks: ranks() },
      answers: {
        winner: { "1": 0.5, "2": 0.3, "3": 0.1 },
        top3: {},
        axis: { "9": 1 },
        style: { odd: 1 },
        betType: {},
      },
      budget: 200,
    }),
  ).toStrictEqual({
    betType: "tansho",
    style: "single",
    axis: 1,
    legs: [[1]],
    budget: 200,
    totalStake: 200,
    expectedReturn: 207,
    tickets: [
      {
        horses: [1],
        odds: 2,
        maxOdds: null,
        probability: 0.5175160109810499,
        stake: 200,
        payout: 400,
        payoutMax: null,
      },
    ],
  });
  expect(
    suggestBets({
      race: { runners: runners(), odds: odds(), ranks: ranks() },
      answers: {
        winner: {},
        top3: {},
        axis: { "1": 1 },
        style: { odd: 1 },
        betType: { umaren: 1 },
      },
      budget: 200,
    })?.style,
  ).toBe("nagashi");
});

it("returns null without odds for the planned tickets or without budget", () => {
  expect(
    suggestBets({
      race: { runners: runners(), odds: odds(), ranks: ranks() },
      answers: {
        winner: {},
        top3: {},
        axis: { "4": 1 },
        style: { box: 1 },
        betType: { tansho: 1 },
      },
      budget: 1000,
    }),
  ).toBeNull();
  expect(
    suggestBets({
      race: { runners: runners(), odds: odds(), ranks: ranks() },
      answers: { winner: {}, top3: {}, axis: {}, style: {}, betType: {} },
      budget: 50,
    }),
  ).toBeNull();
  expect(
    suggestBets({
      race: { runners: [], odds: odds(), ranks: ranks() },
      answers: { winner: {}, top3: {}, axis: {}, style: {}, betType: {} },
      budget: 1000,
    }),
  ).toBeNull();
});

it("orders tied horses by number and looks up ordered odds as written", () => {
  const suggestion = suggestBets({
    race: {
      runners: runners().slice(0, 3),
      odds: { "3rentan": [{ combination: "1-2-3", odds: 30, minOdds: null, maxOdds: null }] },
      ranks: new Map(),
    },
    answers: { winner: {}, top3: {}, axis: {}, style: { nagashi: 1 }, betType: { "3rentan": 1 } },
    budget: 100,
  });
  expect(suggestion?.legs).toStrictEqual([[1], [2, 3]]);
  expect(
    suggestion?.tickets.map((ticket) => [ticket.horses, ticket.stake, ticket.payout]),
  ).toStrictEqual([[[1, 2, 3], 100, 3000]]);
  expect(suggestion?.tickets[0]?.probability).toBeCloseTo(0.1667, 4);
});

it("keeps a training row without a workout index", () => {
  const built = buildBetRequest({
    heatmap: { runners: [{ umaban: "1", wakuban: "1", bamei: "A" }] },
    realtime: null,
    recent: null,
    training: { trainings: [{ umaban: "1" }] },
    trends: null,
    ranks: new Map(),
  });
  expect(built.request.state).toStrictEqual({
    race: { todayPlacings: [] },
    runners: [
      {
        no: 1,
        frame: 1,
        name: "A",
        jockey: "",
        trainer: "",
        modelRank: null,
        winOdds: null,
        style: null,
        heatmap: null,
        recent: [],
        training: { grade: "", note: "", index: null, f3: "" },
      },
    ],
  });
});

const planOdds = (): OddsBook => ({
  tansho: [
    { combination: "1", odds: 2, minOdds: null, maxOdds: null },
    { combination: "2", odds: 4, minOdds: null, maxOdds: null },
    { combination: "3", odds: 8, minOdds: null, maxOdds: null },
  ],
  fukusho: [{ combination: "1", odds: 1.3, minOdds: 1.2, maxOdds: 1.5 }],
  umaren: [
    { combination: "1-2", odds: 5, minOdds: null, maxOdds: null },
    { combination: "1-3", odds: 9, minOdds: null, maxOdds: null },
    { combination: "1-4", odds: 20, minOdds: null, maxOdds: null },
    { combination: "2-3", odds: 12, minOdds: null, maxOdds: null },
    { combination: "2-4", odds: 30, minOdds: null, maxOdds: null },
    { combination: "3-4", odds: 40, minOdds: null, maxOdds: null },
  ],
  "3rentan": [{ combination: "1-2-3", odds: 30, minOdds: null, maxOdds: null }],
});

it("suggests two bet types for a small budget", () => {
  const result = suggestPlans({
    race: { runners: runners(), odds: planOdds(), ranks: ranks() },
    answers: {
      winner: { "1": 0.5, "2": 0.3, "3": 0.1 },
      top3: {},
      axis: { "1": 1 },
      style: { nagashi: 0.7, box: 0.3 },
      betType: { umaren: 0.5, tansho: 0.3, fukusho: 0.2 },
    },
    budget: 500,
  });
  expect(result?.totalStake).toBe(500);
  expect(result?.expectedReturn).toBe(982);
  expect(
    result?.plans.map((plan) => [plan.betType, plan.style, plan.legs, plan.totalStake]),
  ).toStrictEqual([
    ["umaren", "nagashi", [[1], [2, 3, 4]], 400],
    ["tansho", "single", [[1]], 100],
  ]);
});

it("suggests three bet types from ten units", () => {
  const result = suggestPlans({
    race: { runners: runners(), odds: planOdds(), ranks: ranks() },
    answers: {
      winner: { "1": 0.5, "2": 0.3, "3": 0.1 },
      top3: {},
      axis: { "1": 1 },
      style: { nagashi: 0.7, box: 0.3 },
      betType: { umaren: 0.5, tansho: 0.3, fukusho: 0.2 },
    },
    budget: 1000,
  });
  expect(
    result?.plans.map((plan) => [plan.betType, plan.style, plan.legs, plan.totalStake]),
  ).toStrictEqual([
    ["umaren", "nagashi", [[1], [2, 3, 4]], 500],
    ["tansho", "single", [[1]], 300],
    ["fukusho", "single", [[1]], 200],
  ]);
});

it("suggests four bet types from thirty units and rotates the multi-horse styles", () => {
  const book = planOdds();
  const result = suggestPlans({
    race: {
      runners: runners(),
      odds: {
        ...book,
        wide: [
          { combination: "1-2", odds: 5, minOdds: 5, maxOdds: 6 },
          { combination: "1-3", odds: 9, minOdds: 9, maxOdds: 10 },
          { combination: "1-4", odds: 20, minOdds: 20, maxOdds: 21 },
          { combination: "2-3", odds: 12, minOdds: 12, maxOdds: 13 },
          { combination: "2-4", odds: 30, minOdds: 30, maxOdds: 31 },
          { combination: "3-4", odds: 40, minOdds: 40, maxOdds: 41 },
        ],
      },
      ranks: ranks(),
    },
    answers: {
      winner: { "1": 0.5, "2": 0.3, "3": 0.1 },
      top3: {},
      axis: { "1": 1 },
      style: { nagashi: 0.6, formation: 0.4 },
      betType: { umaren: 0.4, tansho: 0.3, fukusho: 0.2, wide: 0.1 },
    },
    budget: 3000,
  });
  expect(result?.totalStake).toBe(3000);
  expect(
    result?.plans.map((plan) => [plan.betType, plan.style, plan.legs, plan.totalStake]),
  ).toStrictEqual([
    ["umaren", "nagashi", [[1], [2, 3, 4]], 1200],
    ["tansho", "single", [[1]], 900],
    ["fukusho", "single", [[1]], 600],
    [
      "wide",
      "formation",
      [
        [1, 2],
        [1, 2, 3],
      ],
      300,
    ],
  ]);
});

it("skips the multi style for unordered bet types", () => {
  const result = suggestPlans({
    race: { runners: runners(), odds: planOdds(), ranks: ranks() },
    answers: {
      winner: { "1": 0.5, "2": 0.3, "3": 0.1 },
      top3: {},
      axis: { "1": 1 },
      style: { multi: 0.9, box: 0.1 },
      betType: { umaren: 0.5, tansho: 0.3, fukusho: 0.2 },
    },
    budget: 3000,
  });
  expect(result?.plans[0]).toStrictEqual(
    expect.objectContaining({ betType: "umaren", style: "box", legs: [[1, 2, 3, 4]] }),
  );
});

it("shrinks the partner pool so every ticket gets at least 100 yen", () => {
  const result = suggestPlans({
    race: { runners: runners(), odds: planOdds(), ranks: ranks() },
    answers: {
      winner: { "1": 0.5, "2": 0.3, "3": 0.1 },
      top3: {},
      axis: { "1": 1 },
      style: { nagashi: 1 },
      betType: { umaren: 0.6, tansho: 0.4 },
    },
    budget: 300,
  });
  expect(
    result?.plans.map((plan) => [plan.betType, plan.legs, plan.tickets.length, plan.totalStake]),
  ).toStrictEqual([
    ["umaren", [[1], [2, 3]], 2, 200],
    ["tansho", [[1]], 1, 100],
  ]);
});

it("carries the units of a bet type without odds for every ticket to the next type", () => {
  const result = suggestPlans({
    race: { runners: runners(), odds: planOdds(), ranks: ranks() },
    answers: {
      winner: { "1": 0.5, "2": 0.3, "3": 0.1 },
      top3: {},
      axis: { "1": 1 },
      style: { nagashi: 1 },
      betType: { "3rentan": 0.6, tansho: 0.3, fukusho: 0.1 },
    },
    budget: 1000,
  });
  expect(result?.plans.map((plan) => [plan.betType, plan.totalStake])).toStrictEqual([
    ["tansho", 900],
    ["fukusho", 100],
  ]);
});

it("returns null when fewer than two bet types fit", () => {
  expect(
    suggestPlans({
      race: { runners: runners(), odds: { tansho: planOdds().tansho }, ranks: ranks() },
      answers: {
        winner: { "1": 1 },
        top3: {},
        axis: { "1": 1 },
        style: { nagashi: 1 },
        betType: { tansho: 1 },
      },
      budget: 1000,
    }),
  ).toBeNull();
  expect(
    suggestPlans({
      race: { runners: runners(), odds: planOdds(), ranks: ranks() },
      answers: {
        winner: { "1": 1 },
        top3: {},
        axis: { "1": 1 },
        style: { nagashi: 1 },
        betType: { tansho: 0.5, fukusho: 0.5 },
      },
      budget: 100,
    }),
  ).toBeNull();
});

it("splits units evenly when clef-flash gives the bet types no weight", () => {
  const result = suggestPlans({
    race: { runners: runners(), odds: planOdds(), ranks: ranks() },
    answers: { winner: { "1": 0.5 }, top3: {}, axis: {}, style: {}, betType: {} },
    budget: 400,
  });
  expect(result?.plans.map((plan) => [plan.betType, plan.style, plan.totalStake])).toStrictEqual([
    ["tansho", "single", 200],
    ["fukusho", "single", 200],
  ]);
});

it("orders tied styles by name and falls back to a flow without any style answer", () => {
  const tied = suggestPlans({
    race: { runners: runners(), odds: planOdds(), ranks: ranks() },
    answers: {
      winner: { "1": 0.5, "2": 0.3, "3": 0.1 },
      top3: {},
      axis: { "1": 1 },
      style: { nagashi: 0.5, box: 0.5 },
      betType: { umaren: 0.6, tansho: 0.4 },
    },
    budget: 1000,
  });
  expect(tied?.plans[0].style).toBe("box");
  const none = suggestPlans({
    race: { runners: runners(), odds: planOdds(), ranks: ranks() },
    answers: {
      winner: { "1": 0.5, "2": 0.3, "3": 0.1 },
      top3: {},
      axis: { "1": 1 },
      style: {},
      betType: { umaren: 0.6, tansho: 0.4 },
    },
    budget: 1000,
  });
  expect(none?.plans[0].style).toBe("nagashi");
});
