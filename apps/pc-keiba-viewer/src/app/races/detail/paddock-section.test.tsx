// Run with: bun run test src/app/races/detail/paddock-section.test.tsx

import { cleanup, render, screen, waitFor } from "@testing-library/react";
import type { RealtimeRacePayload } from "horse-racing-realtime/types";
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

const useRealtimeRacePayloadMock = vi.fn<() => { payload: RealtimeRacePayload | null }>(() => ({
  payload: null,
}));

vi.mock("./realtime-client", () => ({
  useRealtimeRacePayload: () => useRealtimeRacePayloadMock(),
}));

vi.mock("next/link", () => ({
  default: ({ children, href }: { children: React.ReactNode; href: string }) =>
    React.createElement("a", { href }, children),
}));

interface PaddockRecentChartStubProps {
  upcomingPopularity: number | null;
  upcomingRaceDate: string;
  upcomingWeight: number | null;
  upcomingWeightDelta: number | null;
}

vi.mock("./paddock-recent-results-chart", () => ({
  PaddockRecentResultsChart: ({
    upcomingPopularity,
    upcomingRaceDate,
    upcomingWeight,
    upcomingWeightDelta,
  }: PaddockRecentChartStubProps) =>
    React.createElement("div", {
      "data-testid": "paddock-recent-chart-stub",
      "data-upcoming-popularity": String(upcomingPopularity),
      "data-upcoming-race-date": upcomingRaceDate,
      "data-upcoming-weight": String(upcomingWeight),
      "data-upcoming-weight-delta": String(upcomingWeightDelta),
    }),
}));

const { PaddockSection, formatUserIdForHistory, parseUpcomingWeightValues } =
  await import("./paddock-section");

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

// State whose horse "1" (umaban "01" normalized) carries the given counts so the
// editable card renders the metric controls at the requested boundary values.
const buildPaddockStateWithHorseOne = (counts: {
  attention: number;
  kaeshi: number;
  paddock: number;
  preference: number;
}): PaddockState => ({
  history: [],
  horses: {
    "1": {
      attention: counts.attention,
      horseName: "テストホース",
      horseNumber: "1",
      kaeshi: counts.kaeshi,
      officialRank: null,
      paddock: counts.paddock,
      preference: counts.preference,
      total: 0,
    },
  },
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
  showBloodline: true,
  source: "jra" as const,
  year: "2026",
};

afterEach(() => {
  cleanup();
  fetchWithRetryMock.mockReset();
  getOrCreateUserIdMock.mockReset();
  useRealtimeRacePayloadMock.mockReset();
  useRealtimeRacePayloadMock.mockReturnValue({ payload: null });
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

test("PaddockSection recent-results shows the jockey name of the past race row", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  render(
    <PaddockSection
      {...baseProps}
      recentResults={[
        buildPastResult({
          currentUmaban: "01",
          kishumeiRyakusho: "ルメール",
        }),
      ]}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  const jockeyCell = await screen.findByLabelText("騎手");
  expect(jockeyCell.textContent).toBe("騎手 ルメール");
});

test("PaddockSection recent-results falls back to dash when past jockey name is null", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  render(
    <PaddockSection
      {...baseProps}
      recentResults={[
        buildPastResult({
          currentUmaban: "01",
          kishumeiRyakusho: null,
        }),
      ]}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  const jockeyCell = await screen.findByLabelText("騎手");
  expect(jockeyCell.textContent).toBe("騎手 -");
});

test("PaddockSection recent-results shows the ブリンカー token for a past race wearing a blinker", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  const { container } = render(
    <PaddockSection
      {...baseProps}
      recentResults={[
        buildPastResult({
          blinkerShiyoKubun: "1",
          currentUmaban: "01",
        }),
      ]}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  await waitFor(() => {
    expect(screen.getAllByRole("article").length).toBe(1);
  });
  const blinkerMark = container.querySelector(".paddock-recent-blinker");
  expect(blinkerMark?.textContent).toBe("ブリンカー");
});

test("PaddockSection recent-results omits the ブリンカー token for a past race without a blinker", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  const { container } = render(
    <PaddockSection
      {...baseProps}
      recentResults={[
        buildPastResult({
          blinkerShiyoKubun: "0",
          currentUmaban: "01",
        }),
      ]}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  await waitFor(() => {
    expect(screen.getAllByRole("article").length).toBe(1);
  });
  const blinkerMark = container.querySelector(".paddock-recent-blinker");
  expect(blinkerMark?.textContent).toBe("");
});

test("PaddockSection renders the first-attachment blinker pattern badge for a debut wearing horse", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  const { container } = render(
    <PaddockSection
      {...baseProps}
      recentResults={[]}
      runners={[buildRunner({ blinkerShiyoKubun: "1", bamei: "テストホース", umaban: "01" })]}
    />,
  );

  await waitFor(() => {
    expect(screen.getAllByRole("article").length).toBe(1);
  });
  const badge = container.querySelector(".paddock-blinker-pattern-badge");
  expect(badge?.className).toBe("paddock-blinker-pattern-badge pattern-B");
  expect(badge?.textContent).toBe("初ブリンカー(初出走)");
  expect(badge?.getAttribute("aria-label")).toBe("ブリンカー 初ブリンカー(初出走)");
});

test("PaddockSection renders the first-attachment-not-debut blinker pattern badge over past unworn races", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  const { container } = render(
    <PaddockSection
      {...baseProps}
      recentResults={[
        buildPastResult({ blinkerShiyoKubun: "0", currentUmaban: "01", raceBango: "01" }),
      ]}
      runners={[buildRunner({ blinkerShiyoKubun: "1", bamei: "テストホース", umaban: "01" })]}
    />,
  );

  await waitFor(() => {
    expect(screen.getAllByRole("article").length).toBe(1);
  });
  const badge = container.querySelector(".paddock-blinker-pattern-badge");
  expect(badge?.className).toBe("paddock-blinker-pattern-badge pattern-A");
  expect(badge?.textContent).toBe("初ブリンカー");
  expect(badge?.getAttribute("aria-label")).toBe("ブリンカー 初ブリンカー");
});

test("PaddockSection renders no blinker pattern badge when the horse never wears one", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  const { container } = render(
    <PaddockSection
      {...baseProps}
      recentResults={[
        buildPastResult({ blinkerShiyoKubun: "0", currentUmaban: "01", raceBango: "01" }),
      ]}
      runners={[buildRunner({ blinkerShiyoKubun: "0", bamei: "テストホース", umaban: "01" })]}
    />,
  );

  await waitFor(() => {
    expect(screen.getAllByRole("article").length).toBe(1);
  });
  expect(container.querySelector(".paddock-blinker-pattern-badge") === null).toBe(true);
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

test("PaddockSection read-only table includes trainer and bloodline column headers when showBloodline is true", async () => {
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
      showBloodline
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

test("PaddockSection read-only table omits bloodline column headers when showBloodline is false", async () => {
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
      showBloodline={false}
      source="jra"
      year="2026"
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
    expect(screen.getAllByRole("columnheader").length).toBeGreaterThan(0);
  });
  expect(screen.getByRole("columnheader", { name: "調教師" }).tagName).toBe("TH");
  expect(screen.queryByRole("columnheader", { name: "父" })).toBeNull();
  expect(screen.queryByRole("columnheader", { name: "父父" })).toBeNull();
  expect(screen.queryByRole("columnheader", { name: "母父" })).toBeNull();
});

test("PaddockSection read-only table omits bloodline value cells when showBloodline is false", async () => {
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

  const { container } = render(
    <PaddockSection
      day="02"
      editable={false}
      keibajoCode="05"
      month="06"
      raceNumber="01"
      recentResults={[]}
      showBloodline={false}
      source="jra"
      year="2026"
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
    expect(screen.getAllByRole("columnheader").length).toBeGreaterThan(0);
  });
  const bloodlineCells = container.querySelectorAll(".paddock-table-bloodline-cell");
  expect(bloodlineCells.length).toBe(0);
});

test("PaddockSection editable bloodline row wraps three bloodline facts in a flex container", async () => {
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
  const bloodlineRows = container.querySelectorAll(".paddock-horse-bloodline-row");
  expect(bloodlineRows.length).toBe(1);
  const factsInRow = bloodlineRows[0]?.querySelectorAll(".paddock-horse-bloodline-fact");
  expect(factsInRow?.length).toBe(3);
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
      showBloodline
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

test("PaddockSection renders long sire name fully in editable card without truncation", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  render(
    <PaddockSection
      {...baseProps}
      runners={[
        buildRunner({
          bamei: "テストホース",
          damSireName: null,
          sireName: "ノーザンダンサーロングロングネーム　　　　　",
          sireSireName: null,
          umaban: "01",
        }),
      ]}
    />,
  );

  await waitFor(() => {
    expect(screen.getAllByRole("article").length).toBe(1);
  });
  const sireDd = screen.getByText("ノーザンダンサーロングロングネーム", { exact: false });
  expect(sireDd.tagName).toBe("DD");
  expect(sireDd.className).toBe("paddock-horse-bloodline-value");
});

test("PaddockSection renders long sire name in read-only table without truncation", async () => {
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
      showBloodline={true}
      source="jra"
      year="2026"
      runners={[
        buildRunner({
          bamei: "テストホース",
          damSireName: null,
          sireName: "ノーザンダンサーロングロングネーム　　　　　",
          sireSireName: null,
          umaban: "01",
        }),
      ]}
    />,
  );

  await waitFor(() => {
    expect(screen.getAllByRole("columnheader").length).toBeGreaterThan(0);
  });
  const sireTd = screen.getByText("ノーザンダンサーロングロングネーム", { exact: false });
  expect(sireTd.tagName).toBe("TD");
  expect(sireTd.className).toBe("paddock-table-bloodline-cell");
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
      showBloodline
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

test("PaddockSection disables 返し plus and keeps minus enabled at the upper cap of three", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(
    makeJsonResponse(
      buildPaddockStateWithHorseOne({ attention: 0, kaeshi: 3, paddock: 0, preference: 0 }),
    ),
  );

  render(
    <PaddockSection
      {...baseProps}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  const plusButton = await screen.findByRole("button", { name: "テストホース 返し+" });
  const minusButton = await screen.findByRole("button", { name: "テストホース 返し-" });
  expect(plusButton.hasAttribute("disabled")).toStrictEqual(true);
  expect(minusButton.hasAttribute("disabled")).toStrictEqual(false);
});

test("PaddockSection disables 返し minus and keeps plus enabled at the lower cap of minus three", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(
    makeJsonResponse(
      buildPaddockStateWithHorseOne({ attention: 0, kaeshi: -3, paddock: 0, preference: 0 }),
    ),
  );

  render(
    <PaddockSection
      {...baseProps}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  const plusButton = await screen.findByRole("button", { name: "テストホース 返し+" });
  const minusButton = await screen.findByRole("button", { name: "テストホース 返し-" });
  expect(minusButton.hasAttribute("disabled")).toStrictEqual(true);
  expect(plusButton.hasAttribute("disabled")).toStrictEqual(false);
});

test("PaddockSection keeps both 返し buttons enabled when the count is two below the cap of three", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(
    makeJsonResponse(
      buildPaddockStateWithHorseOne({ attention: 0, kaeshi: 2, paddock: 0, preference: 0 }),
    ),
  );

  render(
    <PaddockSection
      {...baseProps}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  const plusButton = await screen.findByRole("button", { name: "テストホース 返し+" });
  const minusButton = await screen.findByRole("button", { name: "テストホース 返し-" });
  expect(plusButton.hasAttribute("disabled")).toStrictEqual(false);
  expect(minusButton.hasAttribute("disabled")).toStrictEqual(false);
});

test("PaddockSection disables 好き and keeps 嫌い enabled at the upper cap of ten", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(
    makeJsonResponse(
      buildPaddockStateWithHorseOne({ attention: 0, kaeshi: 0, paddock: 0, preference: 10 }),
    ),
  );

  render(
    <PaddockSection
      {...baseProps}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  const plusButton = await screen.findByRole("button", { name: "テストホース 好き" });
  const minusButton = await screen.findByRole("button", { name: "テストホース 嫌い" });
  expect(plusButton.hasAttribute("disabled")).toStrictEqual(true);
  expect(minusButton.hasAttribute("disabled")).toStrictEqual(false);
});

test("PaddockSection disables 嫌い and keeps 好き enabled at the lower cap of minus ten", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(
    makeJsonResponse(
      buildPaddockStateWithHorseOne({ attention: 0, kaeshi: 0, paddock: 0, preference: -10 }),
    ),
  );

  render(
    <PaddockSection
      {...baseProps}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  const plusButton = await screen.findByRole("button", { name: "テストホース 好き" });
  const minusButton = await screen.findByRole("button", { name: "テストホース 嫌い" });
  expect(minusButton.hasAttribute("disabled")).toStrictEqual(true);
  expect(plusButton.hasAttribute("disabled")).toStrictEqual(false);
});

test("PaddockSection keeps both 好み buttons enabled when the count is nine below the cap of ten", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(
    makeJsonResponse(
      buildPaddockStateWithHorseOne({ attention: 0, kaeshi: 0, paddock: 0, preference: 9 }),
    ),
  );

  render(
    <PaddockSection
      {...baseProps}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  const plusButton = await screen.findByRole("button", { name: "テストホース 好き" });
  const minusButton = await screen.findByRole("button", { name: "テストホース 嫌い" });
  expect(plusButton.hasAttribute("disabled")).toStrictEqual(false);
  expect(minusButton.hasAttribute("disabled")).toStrictEqual(false);
});

test("PaddockSection disables 気配+ and keeps 気配- enabled at the upper cap of five", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(
    makeJsonResponse(
      buildPaddockStateWithHorseOne({ attention: 0, kaeshi: 0, paddock: 5, preference: 0 }),
    ),
  );

  render(
    <PaddockSection
      {...baseProps}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  const plusButton = await screen.findByRole("button", { name: "テストホース 気配+" });
  const minusButton = await screen.findByRole("button", { name: "テストホース 気配-" });
  expect(plusButton.hasAttribute("disabled")).toStrictEqual(true);
  expect(minusButton.hasAttribute("disabled")).toStrictEqual(false);
});

test("PaddockSection disables 注目- and keeps 注目+ enabled at the lower cap of minus five", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(
    makeJsonResponse(
      buildPaddockStateWithHorseOne({ attention: -5, kaeshi: 0, paddock: 0, preference: 0 }),
    ),
  );

  render(
    <PaddockSection
      {...baseProps}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  const plusButton = await screen.findByRole("button", { name: "テストホース 注目+" });
  const minusButton = await screen.findByRole("button", { name: "テストホース 注目-" });
  expect(minusButton.hasAttribute("disabled")).toStrictEqual(true);
  expect(plusButton.hasAttribute("disabled")).toStrictEqual(false);
});

test("PaddockSection 近走 defaults to text mode showing the past race row and no chart", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  render(
    <PaddockSection
      {...baseProps}
      recentResults={[buildPastResult({ currentUmaban: "01", kyosomeiHondai: "過去レース" })]}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  const textButton = await screen.findByRole("button", { name: "テキスト" });
  const graphButton = await screen.findByRole("button", { name: "グラフ" });
  expect(textButton.getAttribute("aria-pressed")).toBe("true");
  expect(graphButton.getAttribute("aria-pressed")).toBe("false");
  expect(screen.getByText("過去レース").tagName).toBe("STRONG");
  expect(screen.queryByTestId("paddock-recent-chart-stub")).toBeNull();
});

test("PaddockSection 近走 graph button switches to chart and hides the past race text", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  render(
    <PaddockSection
      {...baseProps}
      recentResults={[buildPastResult({ currentUmaban: "01", kyosomeiHondai: "過去レース" })]}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  const graphButton = await screen.findByRole("button", { name: "グラフ" });
  graphButton.click();

  await waitFor(() => {
    expect(screen.queryByTestId("paddock-recent-chart-stub")).not.toBeNull();
  });
  const textButton = await screen.findByRole("button", { name: "テキスト" });
  expect(graphButton.getAttribute("aria-pressed")).toBe("true");
  expect(textButton.getAttribute("aria-pressed")).toBe("false");
  expect(screen.queryByText("過去レース")).toBeNull();
});

test("PaddockSection 近走 text button switches back from chart to the past race text", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  render(
    <PaddockSection
      {...baseProps}
      recentResults={[buildPastResult({ currentUmaban: "01", kyosomeiHondai: "過去レース" })]}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  const graphButton = await screen.findByRole("button", { name: "グラフ" });
  graphButton.click();

  await waitFor(() => {
    expect(screen.queryByTestId("paddock-recent-chart-stub")).not.toBeNull();
  });

  const textButton = await screen.findByRole("button", { name: "テキスト" });
  textButton.click();

  await waitFor(() => {
    expect(screen.queryByTestId("paddock-recent-chart-stub")).toBeNull();
  });
  expect(textButton.getAttribute("aria-pressed")).toBe("true");
  expect(graphButton.getAttribute("aria-pressed")).toBe("false");
  expect(screen.getByText("過去レース").tagName).toBe("STRONG");
});

test("PaddockSection 近走 toggle container has a comfortable inline gap between buttons", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  render(
    <PaddockSection
      {...baseProps}
      recentResults={[buildPastResult({ currentUmaban: "01", kyosomeiHondai: "過去レース" })]}
      runners={[buildRunner({ bamei: "テストホース", umaban: "01" })]}
    />,
  );

  const controls = await screen.findByLabelText("近走の表示切替");
  expect(controls.style.display).toBe("flex");
  expect(controls.style.gap).toBe("8px");
});

test("PaddockSection 近走 chart receives the upcoming weight, delta and race date props", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  render(
    <PaddockSection
      {...baseProps}
      recentResults={[buildPastResult({ currentUmaban: "01", kyosomeiHondai: "過去レース" })]}
      runners={[
        buildRunner({
          bamei: "テストホース",
          bataiju: "486",
          umaban: "01",
          zogenFugo: "+",
          zogenSa: "4",
        }),
      ]}
    />,
  );

  const graphButton = await screen.findByRole("button", { name: "グラフ" });
  graphButton.click();

  const chart = await screen.findByTestId("paddock-recent-chart-stub");
  expect(chart.getAttribute("data-upcoming-race-date")).toBe("20260602");
  expect(chart.getAttribute("data-upcoming-weight")).toBe("486");
  expect(chart.getAttribute("data-upcoming-weight-delta")).toBe("4");
  expect(chart.getAttribute("data-upcoming-popularity")).toBe("null");
});

test("PaddockSection 近走 chart receives the realtime tansho popularity by umaban", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));
  useRealtimeRacePayloadMock.mockReturnValue({
    payload: {
      horseWeights: null,
      odds: {
        fetchedAt: "2026-06-02T12:00:00Z",
        horseTrends: [],
        history: [],
        latest: { tansho: [{ combination: "1", odds: 3.2, rank: 2 }] },
      },
      raceEntries: null,
      raceKey: "jra:20260602:05:01",
      raceResults: null,
      source: null,
    },
  });

  render(
    <PaddockSection
      {...baseProps}
      recentResults={[buildPastResult({ currentUmaban: "01", kyosomeiHondai: "過去レース" })]}
      runners={[
        buildRunner({
          bamei: "テストホース",
          bataiju: "486",
          umaban: "01",
          zogenFugo: "+",
          zogenSa: "4",
        }),
      ]}
    />,
  );

  const graphButton = await screen.findByRole("button", { name: "グラフ" });
  graphButton.click();

  const chart = await screen.findByTestId("paddock-recent-chart-stub");
  expect(chart.getAttribute("data-upcoming-popularity")).toBe("2");
});

test("PaddockSection 近走 chart receives null upcoming weight when the runner has no weight", async () => {
  getOrCreateUserIdMock.mockResolvedValue("user-test-uuid");
  fetchWithRetryMock.mockResolvedValue(makeJsonResponse(buildPaddockState([])));

  render(
    <PaddockSection
      {...baseProps}
      recentResults={[buildPastResult({ currentUmaban: "01", kyosomeiHondai: "過去レース" })]}
      runners={[
        buildRunner({
          bamei: "テストホース",
          bataiju: "000",
          umaban: "01",
          zogenFugo: "+",
          zogenSa: "0",
        }),
      ]}
    />,
  );

  const graphButton = await screen.findByRole("button", { name: "グラフ" });
  graphButton.click();

  const chart = await screen.findByTestId("paddock-recent-chart-stub");
  expect(chart.getAttribute("data-upcoming-weight")).toBe("null");
  expect(chart.getAttribute("data-upcoming-weight-delta")).toBe("null");
});

test("parseUpcomingWeightValues parses weight and signed change from a kg label", () => {
  expect(parseUpcomingWeightValues("486kg (+4)")).toStrictEqual({ weight: 486, weightDelta: 4 });
});

test("parseUpcomingWeightValues parses a negative change", () => {
  expect(parseUpcomingWeightValues("452kg (-6)")).toStrictEqual({ weight: 452, weightDelta: -6 });
});

test("parseUpcomingWeightValues parses a zero change", () => {
  expect(parseUpcomingWeightValues("480kg (+0)")).toStrictEqual({ weight: 480, weightDelta: 0 });
});

test("parseUpcomingWeightValues returns null delta when the label has no parentheses", () => {
  expect(parseUpcomingWeightValues("480kg")).toStrictEqual({ weight: 480, weightDelta: null });
});

test("parseUpcomingWeightValues returns null weight when the label is a dash", () => {
  expect(parseUpcomingWeightValues("-")).toStrictEqual({ weight: null, weightDelta: null });
});

test("parseUpcomingWeightValues parses a bare numeric weight without a kg suffix", () => {
  expect(parseUpcomingWeightValues("456(+4)")).toStrictEqual({ weight: 456, weightDelta: 4 });
});
