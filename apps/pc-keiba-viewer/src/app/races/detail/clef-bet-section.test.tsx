// Runs with Bun through Vitest; the Worker request and realtime store are mocked.
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { ClefBetSection } from "./clef-bet-section";

const realtime = vi.hoisted(() => ({ fetchedAt: "T1" }));

vi.mock("./realtime-client", () => ({
  useRealtimeRaceSelector: <T,>(
    selector: (state: { payload: { odds: { fetchedAt: string } | null } | null }) => T,
  ): T =>
    selector({
      payload: realtime.fetchedAt === "" ? null : { odds: { fetchedAt: realtime.fetchedAt } },
    }),
}));

const payload = {
  oddsFetchedAt: "2026-10-18T06:55:31+09:00",
  runners: [
    {
      horseNumber: 4,
      frameNumber: 3,
      horseName: "フォー",
      jockey: "騎手フォー",
      trainer: "調教師フォー",
      owner: "馬主フォー",
    },
    {
      horseNumber: 11,
      frameNumber: 8,
      horseName: "イレブン",
      jockey: "騎手イレブン",
      trainer: "調教師イレブン",
      owner: "馬主イレブン",
    },
  ],
  plans: {
    budget: 2000,
    totalStake: 2000,
    expectedReturn: 2100,
    plans: [
      {
        betType: "wide",
        style: "nagashi",
        axis: 11,
        legs: [[11], [4, 9]],
        budget: 1200,
        totalStake: 1200,
        expectedReturn: 1300,
        tickets: [
          { horses: [4, 11], odds: 2.4, maxOdds: 2.7, stake: 600, payout: 1440, payoutMax: 1620 },
          { horses: [9, 11], odds: 3.1, maxOdds: 3.6, stake: 600, payout: 1860, payoutMax: 2160 },
        ],
      },
      {
        betType: "tansho",
        style: "single",
        axis: 11,
        legs: [[11]],
        budget: 800,
        totalStake: 800,
        expectedReturn: 800,
        tickets: [
          { horses: [11], odds: 4.3, maxOdds: null, stake: 800, payout: 3440, payoutMax: null },
        ],
      },
    ],
  },
};

const race = { year: "2026", month: "10", day: "18", keibajoCode: "08", raceNumber: "03" };

afterEach(() => {
  realtime.fetchedAt = "T1";
  cleanup();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

it("renders one row per bet plan with legs grouped inside each column", async () => {
  const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json(payload));
  vi.stubGlobal("fetch", fetcher);
  render(<ClefBetSection {...race} />);
  expect(screen.getByRole("heading", { name: "推奨馬券" })).toBeTruthy();
  expect(screen.getByText("推奨馬券を計算しています…")).toBeTruthy();
  expect(await screen.findByText("馬主フォー", {}, { timeout: 2000 })).toBeTruthy();
  expect(screen.getByText("2種類 ／ 3点 ／ 合計 2,000円 ／ 期待払戻 2,100円")).toBeTruthy();
  expect(screen.getAllByRole("row").map((row) => row.textContent)).toStrictEqual([
    "馬券枠番馬番馬名騎手調教師馬主オッズ購入金額払戻額",
    "ワイド軸流し軸 11 → 相手 4・9軸8相手3-軸11相手4-軸イレブン相手フォー-軸騎手イレブン相手騎手フォー-軸調教師イレブン相手調教師フォー-軸馬主イレブン相手馬主フォー-2.4〜3.6各600円 × 2点 = 1,200円1,440円〜2,160円",
    "単勝通常11番811イレブン騎手イレブン調教師イレブン馬主イレブン4.3800円3,440円",
  ]);
  expect(fetcher).toHaveBeenCalledWith(
    "/api/clef-rerank/bets/20261018/08/03?budget=2000&odds=T1",
    expect.objectContaining({ credentials: "same-origin", cache: "no-store" }),
  );
});

it("changes the budget with the slider, step buttons and the typed amount", async () => {
  const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json(payload));
  vi.stubGlobal("fetch", fetcher);
  render(<ClefBetSection {...race} />);
  await screen.findByText("馬主フォー", {}, { timeout: 2000 });
  fireEvent.change(screen.getByLabelText("予算スライダー"), { target: { value: "3049" } });
  expect(screen.getByRole("status").textContent).toBe("3,000円");
  expect(screen.getByLabelText("予算スライダー").getAttribute("aria-valuetext")).toBe("3,000円");
  fireEvent.click(screen.getByRole("button", { name: "予算を100円増やす" }));
  fireEvent.click(screen.getByRole("button", { name: "予算を1,000円増やす" }));
  fireEvent.click(screen.getByRole("button", { name: "予算を100円減らす" }));
  expect(screen.getByRole("status").textContent).toBe("4,000円");
  fireEvent.click(screen.getByRole("button", { name: "予算を1,000円減らす" }));
  expect(screen.getByRole("status").textContent).toBe("3,000円");
  const typed = screen.getByLabelText<HTMLInputElement>("金額を入力（100円単位）");
  fireEvent.change(typed, { target: { value: "45" } });
  expect(typed.value).toBe("45");
  expect(screen.getByRole("status").textContent).toBe("3,000円");
  fireEvent.keyDown(typed, { key: "a" });
  fireEvent.keyDown(typed, { key: "Enter" });
  expect(typed.value).toBe("200");
  expect(screen.getByRole("button", { name: "予算を100円減らす" }).hasAttribute("disabled")).toBe(
    true,
  );
  fireEvent.change(typed, { target: { value: "60049" } });
  fireEvent.blur(typed);
  expect(screen.getByRole("status").textContent).toBe("60,000円");
  expect(screen.getByLabelText<HTMLInputElement>("予算スライダー").value).toBe("20000");
  await waitFor(
    () =>
      expect(fetcher).toHaveBeenLastCalledWith(
        "/api/clef-rerank/bets/20261018/08/03?budget=60000&odds=T1",
        expect.objectContaining({ cache: "no-store" }),
      ),
    { timeout: 2000 },
  );
  fireEvent.change(typed, { target: { value: "2000000" } });
  fireEvent.blur(typed);
  expect(screen.getByRole("button", { name: "予算を100円増やす" }).hasAttribute("disabled")).toBe(
    true,
  );
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
  expect(screen.queryByText("馬主フォー")).toBeNull();
});

it("refetches for each new odds snapshot and keeps the last suggestion visible meanwhile", async () => {
  const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json(payload));
  vi.stubGlobal("fetch", fetcher);
  const { rerender } = render(<ClefBetSection {...race} />);
  await screen.findByText("馬主フォー", {}, { timeout: 2000 });
  realtime.fetchedAt = "T2";
  rerender(<ClefBetSection {...race} />);
  expect(screen.getByText("馬主フォー")).toBeTruthy();
  await waitFor(
    () =>
      expect(fetcher).toHaveBeenLastCalledWith(
        "/api/clef-rerank/bets/20261018/08/03?budget=2000&odds=T2",
        expect.objectContaining({ cache: "no-store" }),
      ),
    { timeout: 2000 },
  );
});

it("requests without an odds snapshot before realtime data arrives", async () => {
  realtime.fetchedAt = "";
  const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json(payload));
  vi.stubGlobal("fetch", fetcher);
  render(<ClefBetSection {...race} />);
  await waitFor(
    () =>
      expect(fetcher).toHaveBeenCalledWith(
        "/api/clef-rerank/bets/20261018/08/03?budget=2000&odds=",
        expect.objectContaining({ cache: "no-store" }),
      ),
    { timeout: 2000 },
  );
});
