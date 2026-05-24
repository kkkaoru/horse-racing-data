import type { Pool, QueryResultRow } from "pg";
import { expect, test, vi } from "vitest";

import {
  buildJraRaceId,
  buildWin5LegInputsWithPool,
  padRaceBango,
  type Win5ModelScoreLookup,
} from "./leg-inputs";
import type { Win5Schedule } from "./types";

interface QueryCall {
  text: string;
  values: readonly unknown[];
}

interface FakeQueryResult<TRow extends QueryResultRow> {
  rows: TRow[];
}

const buildSchedule = (): Win5Schedule => ({
  fetchedAt: "2026-05-24T00:00:00.000Z",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0524",
  saleDeadline: null,
  legs: [
    {
      legIndex: 1,
      keibajoCode: "05",
      kaisaiKai: "01",
      kaisaiNichime: "01",
      raceBango: "1",
    },
  ],
  source: "jvd_wf",
});

const buildSingleLegMetaResult = (): FakeQueryResult<QueryResultRow> => ({
  rows: [
    {
      kaisai_kai: "01",
      kaisai_nichime: "01",
      kyosomei_hondai: "Race Name",
    },
  ],
});

const buildSingleRunnerResult = (): FakeQueryResult<QueryResultRow> => ({
  rows: [
    {
      umaban: "01",
      ketto_toroku_bango: "2020001234",
      bamei: "Test Horse",
      kishumei_ryakusho: "Jockey",
      tansho_ninkijun: "05",
      tansho_odds: "0250",
    },
  ],
});

const buildSingleHistoryResult = (): FakeQueryResult<QueryResultRow> => ({
  rows: [
    {
      ketto_toroku_bango: "2020001234",
      runs: "10",
      wins: "3",
    },
  ],
});

const buildEmptyResult = (): FakeQueryResult<QueryResultRow> => ({ rows: [] });

interface QuerySequence {
  calls: QueryCall[];
  pool: Pool;
}

type FakeQueryFn = (
  text: string,
  values: readonly unknown[],
) => Promise<FakeQueryResult<QueryResultRow>>;

const buildFakePool = (responses: readonly FakeQueryResult<QueryResultRow>[]): QuerySequence => {
  const calls: QueryCall[] = [];
  const query = vi.fn<FakeQueryFn>(async (text, values) => {
    calls.push({ text, values });
    return responses[calls.length - 1] ?? buildEmptyResult();
  });
  const fakePool = { query } as unknown as Pool;
  return { calls, pool: fakePool };
};

const buildDefaultPool = (): QuerySequence =>
  buildFakePool([
    buildSingleLegMetaResult(),
    buildSingleRunnerResult(),
    buildSingleHistoryResult(),
  ]);

test("padRaceBango pads single digit to two characters", () => {
  expect(padRaceBango("1")).toBe("01");
});

test("padRaceBango leaves two-digit input untouched", () => {
  expect(padRaceBango("11")).toBe("11");
});

test("buildJraRaceId composes the canonical race_id", () => {
  expect(
    buildJraRaceId({
      kaisaiNen: "2026",
      kaisaiTsukihi: "0524",
      keibajoCode: "05",
      raceBango: "01",
    }),
  ).toBe("jra:2026:0524:05:01");
});

test("buildWin5LegInputsWithPool pads raceBango when binding Q1", async () => {
  const { calls, pool } = buildDefaultPool();
  await buildWin5LegInputsWithPool({ pool, schedule: buildSchedule() });
  expect(calls[0]?.values).toStrictEqual(["2026", "0524", "05", "01"]);
});

test("buildWin5LegInputsWithPool binds kaisai_kai and kaisai_nichime to runner fetch", async () => {
  const { calls, pool } = buildDefaultPool();
  await buildWin5LegInputsWithPool({ pool, schedule: buildSchedule() });
  expect(calls[1]?.values).toStrictEqual(["2026", "0524", "05", "01", "01", "01"]);
});

test("buildWin5LegInputsWithPool issues history Q3 with row-tuple comparison", async () => {
  const { calls, pool } = buildDefaultPool();
  await buildWin5LegInputsWithPool({ pool, schedule: buildSchedule() });
  const historyCallText = calls[2]?.text ?? "";
  const expectedClause = "(se.kaisai_nen, se.kaisai_tsukihi) < ($1::text, $2::text)";
  expect(historyCallText.includes(expectedClause)).toBe(true);
});

test("buildWin5LegInputsWithPool applies model score lookup result to runner", async () => {
  const { pool } = buildDefaultPool();
  const lookup: Win5ModelScoreLookup = {
    get: (params) =>
      params.raceId === "jra:2026:0524:05:01" && params.kettoTorokuBango === "2020001234"
        ? 0.875
        : null,
  };
  const result = await buildWin5LegInputsWithPool({
    pool,
    schedule: buildSchedule(),
    modelScoreLookup: lookup,
  });
  expect(result[0]?.runners[0]?.modelScore).toBe(0.875);
});

test("buildWin5LegInputsWithPool returns empty array when runners query is empty", async () => {
  const { pool } = buildFakePool([
    buildSingleLegMetaResult(),
    buildEmptyResult(),
    buildEmptyResult(),
  ]);
  const result = await buildWin5LegInputsWithPool({ pool, schedule: buildSchedule() });
  expect(result).toStrictEqual([]);
});

test("buildWin5LegInputsWithPool maps tansho_odds storage value to 25", async () => {
  const { pool } = buildDefaultPool();
  const result = await buildWin5LegInputsWithPool({ pool, schedule: buildSchedule() });
  expect(result[0]?.runners[0]?.odds).toBe(25);
});

test("buildWin5LegInputsWithPool maps historical wins to score 0.3", async () => {
  const { pool } = buildDefaultPool();
  const result = await buildWin5LegInputsWithPool({ pool, schedule: buildSchedule() });
  expect(result[0]?.runners[0]?.historicalScore).toBe(0.3);
});
