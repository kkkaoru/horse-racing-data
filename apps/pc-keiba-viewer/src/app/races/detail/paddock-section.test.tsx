// Run with: bun run test src/app/races/detail/paddock-section.test.tsx

import { cleanup, render, screen, waitFor } from "@testing-library/react";
import React from "react";
import { afterEach, expect, test, vi } from "vitest";

import type { PaddockHistoryEntry, PaddockState } from "../../../lib/paddock";
import type { HorseRaceResult, Runner } from "../../../lib/race-types";

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
  damSireName: null,
  futanJuryo: "550",
  kakuteiChakujun: "00",
  kettoTorokuBango: "h1",
  kishumeiRyakusho: "騎手",
  kohan3f: null,
  seibetsuCode: "1",
  sireName: null,
  sireSireName: null,
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

const buildPastResult = (overrides: Partial<HorseRaceResult>): HorseRaceResult => ({
  babajotaiCodeDirt: null,
  babajotaiCodeShiba: null,
  bamei: "テストホース",
  banushimei: null,
  barei: "03",
  bataiju: "480",
  chokyoshimeiRyakusho: null,
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  currentBarei: null,
  currentJockey: null,
  currentSeibetsuCode: null,
  currentUmaban: "01",
  futanJuryo: "550",
  gradeCode: null,
  hassoJikoku: null,
  juryoShubetsuCode: null,
  kaisaiNen: "2026",
  kaisaiTsukihi: "0301",
  kakuteiChakujun: "01",
  keibajoCode: "05",
  kettoTorokuBango: "h1",
  kishumeiRyakusho: "騎手",
  kohan3f: null,
  kyori: "1600",
  kyosoJokenCode: null,
  kyosoJokenMeisho: null,
  kyosoKigoCode: null,
  kyosomeiFukudai: null,
  kyosomeiHondai: "過去レース",
  kyosomeiKakkonai: null,
  kyosoShubetsuCode: null,
  raceBango: "01",
  seibetsuCode: "1",
  sohaTime: null,
  tanshoNinkijun: "01",
  tanshoOdds: "0010",
  tenkoCode: null,
  timeSa: null,
  trackCode: null,
  umaban: "01",
  wakuban: "3",
  zogenFugo: "+",
  zogenSa: "0",
  ...overrides,
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

test("PaddockSection renders trainer name in horse row when chokyoshimeiRyakusho is present", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  render(
    <PaddockSection
      {...baseProps}
      runners={[
        buildRunner({
          bamei: "テストホース",
          chokyoshimeiRyakusho: "佐藤",
          umaban: "01",
        }),
      ]}
    />,
  );

  await waitFor(() => {
    expect(screen.getByLabelText("調教師 佐藤").textContent).toBe("調教師佐藤");
  });
});

test("PaddockSection renders dash for trainer when chokyoshimeiRyakusho is null", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  render(
    <PaddockSection
      {...baseProps}
      runners={[
        buildRunner({
          bamei: "テストホース",
          chokyoshimeiRyakusho: null,
          umaban: "01",
        }),
      ]}
    />,
  );

  await waitFor(() => {
    expect(screen.getByLabelText("調教師 -").textContent).toBe("調教師-");
  });
});

test("PaddockSection renders sire and grandsire names when bloodline fields are present", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  render(
    <PaddockSection
      {...baseProps}
      runners={[
        buildRunner({
          bamei: "テストホース",
          damSireName: "サンデーサイレンス",
          sireName: "ディープインパクト",
          sireSireName: "ステイゴールド",
          umaban: "01",
        }),
      ]}
    />,
  );

  await waitFor(() => {
    expect(screen.getAllByRole("article").length).toBe(1);
  });
  expect(screen.getByText("ディープインパクト").tagName).toBe("DD");
  expect(screen.getByText("ステイゴールド").tagName).toBe("DD");
  expect(screen.getByText("サンデーサイレンス").tagName).toBe("DD");
});

test("PaddockSection recent-results wakuban uses FrameNumberBadge with frame-3 class for parity with race detail", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  render(
    <PaddockSection
      {...baseProps}
      recentResults={[buildPastResult({ currentUmaban: "01", wakuban: "3" })]}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  const wrapper = await screen.findByLabelText("枠番");
  const badge = wrapper.querySelector("span.frame-number-badge");
  expect(badge?.className).toBe("frame-number-badge frame-3");
  expect(badge?.textContent).toBe("3");
});

test("PaddockSection recent-results wakuban renders frame-1 badge for the white frame", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  render(
    <PaddockSection
      {...baseProps}
      recentResults={[buildPastResult({ currentUmaban: "01", wakuban: "1" })]}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  const wrapper = await screen.findByLabelText("枠番");
  const badge = wrapper.querySelector("span.frame-number-badge");
  expect(badge?.className).toBe("frame-number-badge frame-1");
});

test("PaddockSection recent-results wakuban renders frame-8 badge for the pink frame", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  render(
    <PaddockSection
      {...baseProps}
      recentResults={[buildPastResult({ currentUmaban: "01", wakuban: "8" })]}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  const wrapper = await screen.findByLabelText("枠番");
  const badge = wrapper.querySelector("span.frame-number-badge");
  expect(badge?.className).toBe("frame-number-badge frame-8");
});

test("PaddockSection renders dash for bloodline when all fields are null", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  render(
    <PaddockSection
      {...baseProps}
      runners={[
        buildRunner({
          bamei: "テストホース",
          damSireName: null,
          sireName: null,
          sireSireName: null,
          umaban: "01",
        }),
      ]}
    />,
  );

  await waitFor(() => {
    expect(screen.getAllByRole("article").length).toBe(1);
  });
  const sireDt = screen.getByText("父");
  const sireSireDt = screen.getByText("父父");
  const damSireDt = screen.getByText("母父");
  expect(sireDt.nextElementSibling?.textContent).toBe("-");
  expect(sireSireDt.nextElementSibling?.textContent).toBe("-");
  expect(damSireDt.nextElementSibling?.textContent).toBe("-");
});

test("PaddockSection keeps height-stable empty class on official-rank fact when no rank is registered", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  const { container } = render(
    <PaddockSection
      {...baseProps}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  await waitFor(() => {
    expect(screen.getAllByRole("article").length).toBe(1);
  });
  const emptyFact = container.querySelector(".paddock-official-rank-fact-empty");
  expect(emptyFact === null).toBe(false);
  expect(emptyFact?.getAttribute("aria-hidden")).toBe("true");
});

test("PaddockSection renders bloodline value dd with desktop nowrap class", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  const { container } = render(
    <PaddockSection
      {...baseProps}
      runners={[
        buildRunner({
          bamei: "テストホース",
          damSireName: "サンデーサイレンス",
          sireName: "ディープインパクト",
          sireSireName: "ステイゴールド",
          umaban: "01",
        }),
      ]}
    />,
  );

  await waitFor(() => {
    expect(screen.getAllByRole("article").length).toBe(1);
  });
  const bloodlineDdNodes = container.querySelectorAll<HTMLElement>(
    ".paddock-horse-bloodline-value",
  );
  expect(bloodlineDdNodes.length).toBe(3);
  expect(bloodlineDdNodes[0]?.tagName).toBe("DD");
  expect(bloodlineDdNodes[0]?.getAttribute("title")).toBe("ディープインパクト");
  expect(bloodlineDdNodes[1]?.getAttribute("title")).toBe("ステイゴールド");
  expect(bloodlineDdNodes[2]?.getAttribute("title")).toBe("サンデーサイレンス");
});

test("PaddockSection recent runs include 枠番 and 馬番 columns", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  render(
    <PaddockSection
      {...baseProps}
      recentResults={[
        {
          babajotaiCodeDirt: null,
          babajotaiCodeShiba: null,
          bamei: "テストホース",
          banushimei: null,
          barei: "03",
          bataiju: "480",
          chokyoshimeiRyakusho: null,
          corner1: null,
          corner2: null,
          corner3: null,
          corner4: null,
          currentBarei: null,
          currentJockey: null,
          currentSeibetsuCode: null,
          currentUmaban: "01",
          futanJuryo: null,
          gradeCode: null,
          hassoJikoku: null,
          juryoShubetsuCode: null,
          kaisaiNen: "2026",
          kaisaiTsukihi: "0530",
          kakuteiChakujun: "03",
          keibajoCode: "05",
          kettoTorokuBango: "h1",
          kishumeiRyakusho: "騎手",
          kohan3f: null,
          kyori: "1600",
          kyosoJokenCode: null,
          kyosoJokenMeisho: null,
          kyosoKigoCode: null,
          kyosoShubetsuCode: null,
          kyosomeiFukudai: null,
          kyosomeiHondai: "テストレース",
          kyosomeiKakkonai: null,
          raceBango: "05",
          seibetsuCode: null,
          shussoTosu: null,
          sohaTime: null,
          tanshoNinkijun: "02",
          tanshoOdds: "0050",
          tenkoCode: null,
          timeSa: null,
          trackCode: "10",
          umaban: "07",
          wakuban: "4",
          zogenFugo: "+",
          zogenSa: "0",
        },
      ]}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  await waitFor(() => {
    expect(screen.getAllByRole("article").length).toBe(1);
  });
  const frameCell = screen.getByLabelText("枠番");
  expect(frameCell.textContent).toBe("4");
  const umaCell = screen.getByLabelText("馬番");
  expect(umaCell.textContent).toBe("7");
});

test("PaddockSection read-only table includes trainer and bloodline column headers", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  const stateWithScore: PaddockState = {
    history: [],
    horses: {
      "1": {
        attention: 0,
        horseName: "テストホース",
        horseNumber: "1",
        kaeshi: 0,
        officialRank: 1,
        paddock: 0,
        preference: 0,
        total: 0,
      },
    },
    raceKey: "2026:0602:05:01",
    updatedAt: "2026-06-02T12:00:00.000Z",
  };
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(stateWithScore));

  render(
    <PaddockSection
      day="02"
      editable={false}
      keibajoCode="05"
      month="06"
      raceNumber="01"
      recentResults={[]}
      source="jra"
      year="2026"
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  await waitFor(() => {
    expect(screen.getAllByRole("columnheader").length).toBeGreaterThan(0);
  });
  expect(screen.getByRole("columnheader", { name: "調教師" }).tagName).toBe("TH");
  expect(screen.getByRole("columnheader", { name: "父" }).tagName).toBe("TH");
  expect(screen.getByRole("columnheader", { name: "父父" }).tagName).toBe("TH");
  expect(screen.getByRole("columnheader", { name: "母父" }).tagName).toBe("TH");
});

test("PaddockSection lazy recent-results clears skeleton after fetch failure (loading=false on catch)", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockImplementation((input: string, init?: RequestInit) => {
    if (init?.method === "POST") {
      return Promise.resolve(makeJsonResponse(buildPaddockState([])));
    }
    if (input.includes("/recent-results")) {
      return Promise.reject(new Error("recent-results upstream 500"));
    }
    return Promise.resolve(makeJsonResponse(buildPaddockState([])));
  });

  render(
    <PaddockSection
      day="02"
      editable
      keibajoCode="05"
      month="06"
      raceNumber="01"
      source="jra"
      year="2026"
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  await waitFor(() => {
    expect(screen.queryByLabelText("近走成績を読み込み中")).toBeNull();
  });
  expect(screen.getByText("初出走").tagName).toBe("SPAN");
});

test("PaddockSection lazy recent-results clears skeleton via safety timer when fetch never resolves", async () => {
  vi.useFakeTimers();
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  const pendingForever: Promise<Response> = new Promise<Response>(() => undefined);
  fetchWithRetryMock.mockImplementation((input: string, init?: RequestInit) => {
    if (init?.method === "POST") {
      return Promise.resolve(makeJsonResponse(buildPaddockState([])));
    }
    if (input.includes("/recent-results")) {
      return pendingForever;
    }
    return Promise.resolve(makeJsonResponse(buildPaddockState([])));
  });

  render(
    <PaddockSection
      day="02"
      editable
      keibajoCode="05"
      month="06"
      raceNumber="01"
      source="jra"
      year="2026"
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  expect(screen.queryByLabelText("近走成績を読み込み中")).not.toBeNull();
  await vi.advanceTimersByTimeAsync(8001);
  expect(screen.queryByLabelText("近走成績を読み込み中")).toBeNull();
  expect(screen.getByText("初出走").tagName).toBe("SPAN");
  vi.useRealTimers();
});
