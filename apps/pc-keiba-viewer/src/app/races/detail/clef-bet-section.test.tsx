// Runs with Bun through Vitest; the Worker request is mocked.
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { ClefBetSection } from "./clef-bet-section";

const payload = {
  oddsFetchedAt: "2026-10-18T06:55:31+09:00",
  runners: [
    {
      horseNumber: 4,
      frameNumber: 3,
      horseName: "フォー",
      jockey: "騎手B",
      trainer: "調教師B",
      owner: "馬主B",
    },
    {
      horseNumber: 11,
      frameNumber: 8,
      horseName: "イレブン",
      jockey: "騎手A",
      trainer: "調教師A",
      owner: "馬主A",
    },
  ],
  suggestion: {
    betType: "wide",
    style: "nagashi",
    axis: 11,
    legs: [[11], [4, 9]],
    budget: 2000,
    totalStake: 2000,
    expectedReturn: 2100,
    tickets: [
      { horses: [4, 11], odds: 2.4, maxOdds: 2.7, stake: 1200, payout: 2880, payoutMax: 3240 },
      { horses: [9, 11], odds: 3.1, maxOdds: 3.6, stake: 800, payout: 2480, payoutMax: 2880 },
    ],
  },
};

const race = { year: "2026", month: "10", day: "18", keibajoCode: "08", raceNumber: "03" };

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

it("renders the suggested tickets with runner details after the debounce", async () => {
  const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json(payload));
  vi.stubGlobal("fetch", fetcher);
  render(<ClefBetSection {...race} />);
  expect(screen.getByText("推奨馬券を計算しています…")).toBeTruthy();
  expect(await screen.findByText("フォー", {}, { timeout: 2000 })).toBeTruthy();
  expect(
    screen.getByText(
      "ワイド 軸流し ／ 軸 11 ／ 1列目: 11 ／ 2列目: 4, 9 ／ 2点 ／ 合計 2,000円 ／ 期待払戻 2,100円",
    ),
  ).toBeTruthy();
  expect(screen.getAllByRole("row").map((row) => row.textContent)).toStrictEqual([
    "枠番馬番馬名騎手調教師馬主オッズ購入金額払戻額",
    "38411フォーイレブン騎手B騎手A調教師B調教師A馬主B馬主A2.4〜2.71,200円2,880円〜3,240円",
    "-8-11-イレブン-騎手A-調教師A-馬主A3.1〜3.6800円2,480円〜2,880円",
  ]);
  expect(fetcher).toHaveBeenCalledWith(
    "/api/clef-rerank/bets/20261018/08/03?budget=2000",
    expect.objectContaining({ credentials: "same-origin", cache: "no-store" }),
  );
});

it("refetches with the slider budget and the typed budget once it is on the grid", async () => {
  const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json(payload));
  vi.stubGlobal("fetch", fetcher);
  render(<ClefBetSection {...race} />);
  await screen.findByText("フォー", {}, { timeout: 2000 });
  fireEvent.change(screen.getByLabelText("予算スライダー"), { target: { value: "3049" } });
  await waitFor(
    () =>
      expect(fetcher).toHaveBeenLastCalledWith(
        "/api/clef-rerank/bets/20261018/08/03?budget=3000",
        expect.objectContaining({ cache: "no-store" }),
      ),
    { timeout: 2000 },
  );
  expect(screen.getByLabelText<HTMLInputElement>("予算（円）").value).toBe("3000");
  fireEvent.change(screen.getByLabelText("予算（円）"), { target: { value: "45" } });
  expect(screen.getByLabelText<HTMLInputElement>("予算（円）").value).toBe("45");
  fireEvent.change(screen.getByLabelText("予算（円）"), { target: { value: "" } });
  fireEvent.change(screen.getByLabelText("予算（円）"), { target: { value: "60000" } });
  await waitFor(
    () =>
      expect(fetcher).toHaveBeenLastCalledWith(
        "/api/clef-rerank/bets/20261018/08/03?budget=60000",
        expect.objectContaining({ cache: "no-store" }),
      ),
    { timeout: 2000 },
  );
  expect(screen.getByLabelText<HTMLInputElement>("予算スライダー").value).toBe("50000");
});

it("asks to wait for odds when the Worker has no suggestion", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn<typeof fetch>().mockResolvedValue(Response.json({ error: "missing" }, { status: 404 })),
  );
  render(<ClefBetSection {...race} />);
  expect(
    await screen.findByText("オッズ発表後に推奨馬券を表示します。", {}, { timeout: 2000 }),
  ).toBeTruthy();
});

it("asks to wait for odds when the request fails", async () => {
  vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockRejectedValue(new Error("offline")));
  render(<ClefBetSection {...race} />);
  expect(
    await screen.findByText("オッズ発表後に推奨馬券を表示します。", {}, { timeout: 2000 }),
  ).toBeTruthy();
});

it("ignores a response that arrives after unmount", async () => {
  vi.useFakeTimers();
  const resolvers: ((value: Response) => void)[] = [];
  const fetcher = vi.fn<typeof fetch>().mockImplementation(
    async () =>
      new Promise<Response>((resolve) => {
        resolvers.push(resolve);
      }),
  );
  vi.stubGlobal("fetch", fetcher);
  const { unmount } = render(<ClefBetSection {...race} />);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(500);
  });
  expect(fetcher).toHaveBeenCalledTimes(1);
  unmount();
  resolvers[0]?.(Response.json(payload));
  await act(async () => {
    await vi.advanceTimersByTimeAsync(10);
  });
  expect(screen.queryByText("フォー")).toBeNull();
});
