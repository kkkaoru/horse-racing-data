// Run with bun (bunx vitest)

import { afterEach, expect, test, vi } from "vitest";

vi.mock("server-only", () => ({}));

type ExecuteFn = (query: unknown) => Promise<{ rows: unknown[] }>;
type GetDbFn = () => { execute: ExecuteFn };

const { executeMock } = vi.hoisted(() => ({
  executeMock: vi.fn<ExecuteFn>(),
}));

vi.mock("../db/client", () => ({
  getDb: vi.fn<GetDbFn>(() => ({
    execute: executeMock,
  })),
}));

afterEach(() => {
  executeMock.mockReset();
  vi.resetModules();
});

// getAverageWin5PayoutYen is wrapped in React's cache(); re-importing the
// module fresh per test (via vi.resetModules) avoids any cross-test
// memoization so each scenario below is independently verified.
const importGetAverageWin5PayoutYen = async () => {
  const win5Queries = await import("./win5-queries.server");
  return win5Queries.getAverageWin5PayoutYen;
};

test("getAverageWin5PayoutYen returns the real computed average when the query succeeds", async () => {
  executeMock.mockResolvedValue({ rows: [{ average_payout: "22920776.6" }] });
  const getAverageWin5PayoutYen = await importGetAverageWin5PayoutYen();
  const result = await getAverageWin5PayoutYen();
  expect(result).toBe(22_920_776.6);
});

// Regression guard: this used to silently substitute 250_000, measured to
// be ~92x too low against the real historical average (~22.9M yen). The
// fix is to return null and let callers omit the recommendation entirely,
// never fabricate a plausible-looking constant.
test("getAverageWin5PayoutYen returns null (not a fallback constant) when the query yields no average", async () => {
  executeMock.mockResolvedValue({ rows: [{ average_payout: null }] });
  const getAverageWin5PayoutYen = await importGetAverageWin5PayoutYen();
  const result = await getAverageWin5PayoutYen();
  expect(result).toBeNull();
});

test("getAverageWin5PayoutYen returns null when the query returns no rows", async () => {
  executeMock.mockResolvedValue({ rows: [] });
  const getAverageWin5PayoutYen = await importGetAverageWin5PayoutYen();
  const result = await getAverageWin5PayoutYen();
  expect(result).toBeNull();
});

test("getAverageWin5PayoutYen returns null when the query yields a non-finite value", async () => {
  executeMock.mockResolvedValue({ rows: [{ average_payout: "not-a-number" }] });
  const getAverageWin5PayoutYen = await importGetAverageWin5PayoutYen();
  const result = await getAverageWin5PayoutYen();
  expect(result).toBeNull();
});

test("getAverageWin5PayoutYen returns null when the query yields a non-positive value", async () => {
  executeMock.mockResolvedValue({ rows: [{ average_payout: "0" }] });
  const getAverageWin5PayoutYen = await importGetAverageWin5PayoutYen();
  const result = await getAverageWin5PayoutYen();
  expect(result).toBeNull();
});
