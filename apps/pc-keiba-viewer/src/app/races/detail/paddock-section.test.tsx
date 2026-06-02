// Run with: bun run test src/app/races/detail/paddock-section.test.tsx

import { cleanup, render, screen, waitFor } from "@testing-library/react";
import React from "react";
import { afterEach, expect, test, vi } from "vitest";

import type { PaddockHistoryEntry, PaddockState } from "../../../lib/paddock";
import type { Runner } from "../../../lib/race-types";

const fetchWithRetryMock = vi.fn<(input: string, init?: RequestInit) => Promise<Response>>();
const getOrCreateUserIdMock = vi.fn<() => Promise<string>>();

vi.mock("../../../lib/fetch-with-retry", () => ({
  fetchWithRetry: (input: string, init?: RequestInit) => fetchWithRetryMock(input, init),
}));

vi.mock("../../../lib/user-identity-indexeddb", () => ({
  getOrCreateUserId: () => getOrCreateUserIdMock(),
}));

vi.mock("../../../lib/paddock-client-url", () => ({
  getPaddockLiveUrl: (path: string) => `ws://localhost${path}`,
  getPaddockRequestUrl: (path: string) => `http://localhost${path}`,
}));

vi.mock("./realtime-client", () => ({
  useRealtimeRacePayload: () => ({ payload: null }),
}));

vi.mock("next/link", () => ({
  default: ({ children, href }: { children: React.ReactNode; href: string }) =>
    React.createElement("a", { href }, children),
}));

const { PaddockSection, formatUserIdForHistory } = await import("./paddock-section");

const buildRunner = (overrides: Partial<Runner>): Runner => ({
  bamei: "テストホース",
  banushimei: "馬主",
  barei: "03",
  bataiju: "480",
  chokyoshimeiRyakusho: "調教師",
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  futanJuryo: "550",
  kakuteiChakujun: "00",
  kettoTorokuBango: "h1",
  kishumeiRyakusho: "騎手",
  kohan3f: null,
  seibetsuCode: "1",
  sohaTime: null,
  tanshoNinkijun: null,
  tanshoOdds: "0000",
  timeSa: null,
  umaban: "01",
  wakuban: "1",
  zogenFugo: "+",
  zogenSa: "0",
  ...overrides,
});

const buildHistoryEntry = (overrides: Partial<PaddockHistoryEntry>): PaddockHistoryEntry => ({
  at: "2026-06-02T12:00:00.000Z",
  category: "paddock",
  delta: 1,
  horseName: "テストホース",
  horseNumber: "01",
  id: "entry-1",
  scores: {
    attention: 0,
    kaeshi: 0,
    officialRank: null,
    paddock: 1,
    preference: 0,
    total: 1,
  },
  type: "score",
  ...overrides,
});

const buildPaddockState = (history: PaddockHistoryEntry[]): PaddockState => ({
  history,
  horses: {},
  raceKey: "2026:0602:05:01",
  updatedAt: "2026-06-02T12:00:00.000Z",
});

const makeJsonResponse = (body: unknown): Response =>
  new Response(JSON.stringify(body), {
    headers: { "Content-Type": "application/json" },
    status: 200,
  });

const baseProps = {
  day: "02",
  editable: true,
  keibajoCode: "05",
  month: "06",
  raceNumber: "01",
  recentResults: [],
  source: "jra" as const,
  year: "2026",
};

afterEach(() => {
  cleanup();
  fetchWithRetryMock.mockReset();
  getOrCreateUserIdMock.mockReset();
});

test("formatUserIdForHistory truncates to first eight characters", () => {
  expect(formatUserIdForHistory("abcdef1234567890")).toBe("abcdef12");
});

test("formatUserIdForHistory returns dash when user id is undefined", () => {
  expect(formatUserIdForHistory(undefined)).toBe("-");
});

test("formatUserIdForHistory returns dash when user id is empty string", () => {
  expect(formatUserIdForHistory("")).toBe("-");
});

test("formatUserIdForHistory returns the entire id when shorter than eight characters", () => {
  expect(formatUserIdForHistory("abc")).toBe("abc");
});

test("PaddockSection requests user id on mount and POSTs it with score action", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid-1234567890");
  fetchWithRetryMock.mockImplementation((_input: string, init?: RequestInit) => {
    if (init?.method === "POST") {
      return Promise.resolve(makeJsonResponse(buildPaddockState([])));
    }
    return Promise.resolve(makeJsonResponse(buildPaddockState([])));
  });

  render(
    <PaddockSection
      {...baseProps}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  await waitFor(() => {
    expect(getOrCreateUserIdMock).toHaveBeenCalledTimes(1);
  });

  const plusButton = await screen.findByRole("button", { name: "テストホース 気配+" });
  plusButton.click();

  await waitFor(() => {
    const postCall = fetchWithRetryMock.mock.calls.find((call) => call[1]?.method === "POST");
    expect(postCall).toBeTruthy();
  });
  const postCall = fetchWithRetryMock.mock.calls.find((call) => call[1]?.method === "POST");
  const rawBody = postCall?.[1]?.body;
  const postedBody: { category?: string; delta?: number; userId?: string } =
    typeof rawBody === "string" ? JSON.parse(rawBody) : {};
  expect(postedBody.userId).toBe("user-test-uuid-1234567890");
  expect(postedBody.category).toBe("paddock");
  expect(postedBody.delta).toBe(1);
});

test("PaddockSection renders user id column in history when entry has user id", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  const stateWithHistory = buildPaddockState([
    buildHistoryEntry({ id: "h-1", userId: "abcd12345678" }),
  ]);
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(stateWithHistory));

  render(
    <PaddockSection
      {...baseProps}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  await waitFor(() => {
    expect(screen.getByLabelText("操作したユーザー").textContent).toBe("abcd1234");
  });
});

test("PaddockSection renders dash when history entry has no user id", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  const stateWithHistory = buildPaddockState([buildHistoryEntry({ id: "h-2", userId: undefined })]);
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(stateWithHistory));

  render(
    <PaddockSection
      {...baseProps}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  await waitFor(() => {
    expect(screen.getByLabelText("操作したユーザー").textContent).toBe("-");
  });
});
