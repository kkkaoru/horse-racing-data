// Runs with Bun through Vitest; the Worker request and realtime store are mocked.
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { ClefBetSection } from "./clef-bet-section";

type LatestOdds = Partial<Record<string, unknown[]>> | undefined;

const realtime = vi.hoisted(() => ({ fetchedAt: "T1", latest: undefined as LatestOdds }));

vi.mock("./realtime-client", () => ({
  useRealtimeRaceSelector: <T,>(
    selector: (state: {
      payload: { odds: { fetchedAt: string; latest: LatestOdds } | null } | null;
    }) => T,
  ): T =>
    selector({
      payload:
        realtime.fetchedAt === ""
          ? null
          : { odds: { fetchedAt: realtime.fetchedAt, latest: realtime.latest } },
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

const blocked = (): never => {
  throw new Error("blocked");
};

const budgetValue = (): string | null | undefined =>
  document.querySelector(".clef-bet-budget-value")?.textContent;

const race = { year: "2026", month: "10", day: "18", keibajoCode: "08", raceNumber: "03" };

afterEach(() => {
  realtime.fetchedAt = "T1";
  realtime.latest = undefined;
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
    "ワイド軸流し軸 11 → 相手 4・9軸11相手4-軸イレブン相手フォー-軸騎手イレブン相手騎手フォー-軸調教師イレブン相手調教師フォー-軸馬主イレブン相手馬主フォー-2.4〜3.6各600円 × 2点 = 1,200円1,440円〜2,160円",
    "単勝通常11番811イレブン騎手イレブン調教師イレブン馬主イレブン4.3800円3,440円",
    "損益（払戻額 − 購入金額）−560円〜+5,220円",
  ]);
  expect(
    screen.getByRole("rowheader", { name: "損益（払戻額 − 購入金額）" }).getAttribute("colSpan"),
  ).toBe("9");
  expect(fetcher).toHaveBeenCalledWith(
    "/api/clef-rerank/bets/20261018/08/03?budget=2000&target=10000&odds=T1",
    expect.objectContaining({ credentials: "same-origin", cache: "no-store" }),
  );
});

it("changes the budget with the slider, step buttons and the typed amount", async () => {
  const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json(payload));
  vi.stubGlobal("fetch", fetcher);
  render(<ClefBetSection {...race} />);
  await screen.findByText("馬主フォー", {}, { timeout: 2000 });
  fireEvent.change(screen.getByLabelText("予算スライダー"), { target: { value: "3049" } });
  expect(budgetValue()).toBe("3,000円");
  expect(screen.getByLabelText("予算スライダー").getAttribute("aria-valuetext")).toBe("3,000円");
  fireEvent.click(screen.getByRole("button", { name: "予算を100円増やす" }));
  fireEvent.click(screen.getByRole("button", { name: "予算を1,000円増やす" }));
  fireEvent.click(screen.getByRole("button", { name: "予算を100円減らす" }));
  expect(budgetValue()).toBe("4,000円");
  fireEvent.click(screen.getByRole("button", { name: "予算を1,000円減らす" }));
  expect(budgetValue()).toBe("3,000円");
  const typed = screen.getByLabelText<HTMLInputElement>("金額を入力（100円単位）");
  fireEvent.change(typed, { target: { value: "45" } });
  expect(typed.value).toBe("45");
  expect(budgetValue()).toBe("3,000円");
  fireEvent.keyDown(typed, { key: "a" });
  fireEvent.keyDown(typed, { key: "Enter" });
  expect(typed.value).toBe("200");
  expect(screen.getByRole("button", { name: "予算を100円減らす" }).hasAttribute("disabled")).toBe(
    true,
  );
  fireEvent.change(typed, { target: { value: "60049" } });
  fireEvent.blur(typed);
  expect(budgetValue()).toBe("60,000円");
  expect(screen.getByLabelText<HTMLInputElement>("予算スライダー").value).toBe("20000");
  await waitFor(
    () =>
      expect(fetcher).toHaveBeenLastCalledWith(
        "/api/clef-rerank/bets/20261018/08/03?budget=60000&target=10000&odds=T1",
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
        "/api/clef-rerank/bets/20261018/08/03?budget=2000&target=10000&odds=T2",
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
        "/api/clef-rerank/bets/20261018/08/03?budget=2000&target=10000&odds=",
        expect.objectContaining({ cache: "no-store" }),
      ),
    { timeout: 2000 },
  );
});

it("hides 調教師 and 馬主 on phones by default and remembers toggles on the device", async () => {
  const store = new Map<string, string>();
  vi.stubGlobal("localStorage", {
    getItem: (key: string) => store.get(key) ?? null,
    setItem: (key: string, value: string) => store.set(key, value),
  });
  vi.stubGlobal("matchMedia", (query: string) => ({ matches: query === "(max-width: 760px)" }));
  vi.stubGlobal(
    "fetch",
    vi.fn<typeof fetch>().mockImplementation(async () => Response.json(payload)),
  );
  render(<ClefBetSection {...race} />);
  await screen.findByText("騎手フォー", {}, { timeout: 2000 });
  expect(screen.getAllByRole("columnheader").map((cell) => cell.textContent)).toStrictEqual([
    "馬券",
    "枠番",
    "馬番",
    "馬名",
    "騎手",
    "オッズ",
    "購入金額",
    "払戻額",
  ]);
  fireEvent.click(screen.getByRole("checkbox", { name: "馬主" }));
  fireEvent.click(screen.getByRole("checkbox", { name: "馬名" }));
  expect(store.get("pc-keiba:bet-hidden-columns")).toBe('["調教師","馬名"]');
  cleanup();
  render(<ClefBetSection {...race} />);
  await screen.findByText("馬主フォー", {}, { timeout: 2000 });
  expect(screen.getByRole("checkbox", { name: "馬名" })).toHaveProperty("checked", false);
  expect(screen.getByRole("checkbox", { name: "馬主" })).toHaveProperty("checked", true);
});

it("shows every column on desktop and survives blocked storage", async () => {
  vi.stubGlobal("matchMedia", () => ({ matches: false }));
  vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockResolvedValue(Response.json(payload)));
  vi.stubGlobal("localStorage", { getItem: blocked, setItem: blocked });
  render(<ClefBetSection {...race} />);
  await screen.findByText("調教師フォー", {}, { timeout: 2000 });
  expect(screen.getAllByRole("columnheader").length).toBe(10);
  fireEvent.click(screen.getByRole("checkbox", { name: "騎手" }));
  expect(screen.getAllByRole("columnheader").length).toBe(9);
});

it("trims half- and full-width spaces from jockey, trainer and owner names", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn<typeof fetch>().mockResolvedValue(
      Response.json({
        ...payload,
        runners: [
          {
            ...payload.runners[1],
            jockey: "\u3000騎手イレブン ",
            trainer: " 調教師イレブン\u3000\u3000",
            owner: "馬主イレブン\u3000",
          },
          payload.runners[0],
        ],
      }),
    ),
  );
  render(<ClefBetSection {...race} />);
  expect(await screen.findAllByText("騎手イレブン", {}, { timeout: 2000 })).toHaveLength(2);
  expect(screen.getAllByText("調教師イレブン")).toHaveLength(2);
  expect(screen.getAllByText("馬主イレブン")).toHaveLength(2);
});

it("keeps frame numbers for frame bets with a multi-horse structure", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn<typeof fetch>().mockResolvedValue(
      Response.json({
        ...payload,
        plans: {
          ...payload.plans,
          plans: [{ ...payload.plans.plans[0], betType: "wakuren" }, payload.plans.plans[1]],
        },
      }),
    ),
  );
  render(<ClefBetSection {...race} />);
  await screen.findByText("馬主フォー", {}, { timeout: 2000 });
  expect(screen.getAllByRole("row")[1]?.textContent).toBe(
    "枠連軸流し軸 11 → 相手 4・9軸8相手3-軸11相手4-軸イレブン相手フォー-軸騎手イレブン相手騎手フォー-軸調教師イレブン相手調教師フォー-軸馬主イレブン相手馬主フォー-2.4〜3.6各600円 × 2点 = 1,200円1,440円〜2,160円",
  );
});

it("shows frame and horse numbers in the shared colored badges", async () => {
  vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockResolvedValue(Response.json(payload)));
  const { container } = render(<ClefBetSection {...race} />);
  await screen.findByText("馬主フォー", {}, { timeout: 2000 });
  expect(
    [...container.querySelectorAll(".frame-number-badge")].map((node) => node.className),
  ).toStrictEqual(["frame-number-badge frame-8"]);
  expect(
    [...container.querySelectorAll(".horse-number-badge")].map(
      (node) => `${node.className}:${node.textContent ?? ""}:${node.getAttribute("style") ?? ""}`,
    ),
  ).toStrictEqual([
    "horse-number-badge plain:11:",
    "horse-number-badge plain:4:",
    "horse-number-badge plain:11:",
  ]);
});

const memoryStorage = (initial: Record<string, string>): Map<string, string> => {
  const store = new Map(Object.entries(initial));
  vi.stubGlobal("localStorage", {
    getItem: (key: string) => store.get(key) ?? null,
    setItem: (key: string, value: string) => store.set(key, value),
  });
  return store;
};

it("disables bet types without odds and requests only checked types with odds", async () => {
  realtime.latest = { tansho: [{}], wide: [{}], umaren: [{}], wakuren: [] };
  const store = memoryStorage({});
  const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json(payload));
  vi.stubGlobal("fetch", fetcher);
  render(<ClefBetSection {...race} />);
  await screen.findByText("馬主フォー", {}, { timeout: 2000 });
  const types = screen.getByRole("group", { name: "馬券の種類" });
  expect(
    [...types.querySelectorAll("input")].map(
      (input) =>
        `${input.getAttribute("aria-label") ?? ""}:${String(input.checked)}:${String(input.disabled)}`,
    ),
  ).toStrictEqual([
    "単勝:true:false",
    "複勝（オッズなし）:false:true",
    "ワイド:true:false",
    "枠連（オッズなし）:false:true",
    "馬連:true:false",
    "馬単（オッズなし）:false:true",
    "3連複（オッズなし）:false:true",
    "3連単（オッズなし）:false:true",
    "チェックした馬券の種類をすべて含める:false:false",
  ]);
  expect(fetcher).toHaveBeenLastCalledWith(
    "/api/clef-rerank/bets/20261018/08/03?budget=2000&target=10000&odds=T1",
    expect.objectContaining({ cache: "no-store" }),
  );
  fireEvent.click(screen.getByRole("checkbox", { name: "ワイド" }));
  expect(store.get("pc-keiba:bet-types")).toBe(
    '["tansho","fukusho","wakuren","umaren","umatan","3renpuku","3rentan"]',
  );
  await waitFor(
    () =>
      expect(fetcher).toHaveBeenLastCalledWith(
        "/api/clef-rerank/bets/20261018/08/03?budget=2000&target=10000&types=tansho,umaren&odds=T1",
        expect.objectContaining({ cache: "no-store" }),
      ),
    { timeout: 2000 },
  );
  fireEvent.click(screen.getByRole("checkbox", { name: "ワイド" }));
  expect(store.get("pc-keiba:bet-types")).toBe(
    '["tansho","fukusho","wakuren","umaren","umatan","3renpuku","3rentan","wide"]',
  );
});

it("asks for a bet type with odds instead of fetching when none is checked", async () => {
  realtime.latest = { tansho: [{}] };
  memoryStorage({ "pc-keiba:bet-types": '["wide"]' });
  const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json(payload));
  vi.stubGlobal("fetch", fetcher);
  render(<ClefBetSection {...race} />);
  expect(
    await screen.findByText("オッズが発表されている馬券の種類を1つ以上選んでください。"),
  ).toBeTruthy();
  await act(async () => {
    await new Promise((resolve) => {
      setTimeout(resolve, 500);
    });
  });
  expect(fetcher).toHaveBeenCalledTimes(0);
  fireEvent.click(screen.getByRole("checkbox", { name: "単勝" }));
  expect(await screen.findByText("馬主フォー", {}, { timeout: 2000 })).toBeTruthy();
  expect(fetcher).toHaveBeenLastCalledWith(
    "/api/clef-rerank/bets/20261018/08/03?budget=2000&target=10000&odds=T1",
    expect.objectContaining({ cache: "no-store" }),
  );
});

it("requests the remembered types before any odds are published", async () => {
  memoryStorage({ "pc-keiba:bet-types": '["umaren","tansho"]' });
  const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json(payload));
  vi.stubGlobal("fetch", fetcher);
  render(<ClefBetSection {...race} />);
  await waitFor(
    () =>
      expect(fetcher).toHaveBeenCalledWith(
        "/api/clef-rerank/bets/20261018/08/03?budget=2000&target=10000&types=tansho,umaren&odds=T1",
        expect.objectContaining({ cache: "no-store" }),
      ),
    { timeout: 2000 },
  );
});

it("restores the target profit, saves edits and refetches with the new target", async () => {
  const store = memoryStorage({ "pc-keiba:bet-target": "3000" });
  const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json(payload));
  vi.stubGlobal("fetch", fetcher);
  render(<ClefBetSection {...race} />);
  await screen.findByText("馬主フォー", {}, { timeout: 2000 });
  expect(fetcher).toHaveBeenLastCalledWith(
    "/api/clef-rerank/bets/20261018/08/03?budget=2000&target=3000&odds=T1",
    expect.objectContaining({ cache: "no-store" }),
  );
  const typed = screen.getByLabelText<HTMLInputElement>("目標損益額を入力（円単位）");
  expect(typed.value).toBe("3000");
  fireEvent.change(typed, { target: { value: "4500.4" } });
  expect(store.get("pc-keiba:bet-target")).toBe("3000");
  fireEvent.keyDown(typed, { key: "a" });
  fireEvent.keyDown(typed, { key: "Enter" });
  expect(typed.value).toBe("4500");
  expect(store.get("pc-keiba:bet-target")).toBe("4500");
  await waitFor(
    () =>
      expect(fetcher).toHaveBeenLastCalledWith(
        "/api/clef-rerank/bets/20261018/08/03?budget=2000&target=4500&odds=T1",
        expect.objectContaining({ cache: "no-store" }),
      ),
    { timeout: 2000 },
  );
  fireEvent.change(typed, { target: { value: "-5" } });
  fireEvent.blur(typed);
  expect(typed.value).toBe("0");
  expect(store.get("pc-keiba:bet-target")).toBe("0");
});

it("explains when no ticket reaches the target profit within the budget", async () => {
  vi.stubGlobal(
    "fetch",
    vi
      .fn<typeof fetch>()
      .mockResolvedValue(
        Response.json({ error: "unreachable", reason: "target-unreachable" }, { status: 404 }),
      ),
  );
  render(<ClefBetSection {...race} />);
  expect(
    await screen.findByText(
      "この予算では目標損益額に届く買い目がありません。予算を増やすか目標損益額を下げてください。",
      {},
      { timeout: 2000 },
    ),
  ).toBeTruthy();
});

it("asks to wait for odds when an error body is not an object", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn<typeof fetch>().mockResolvedValue(Response.json(null, { status: 500 })),
  );
  render(<ClefBetSection {...race} />);
  expect(
    await screen.findByText("オッズ発表後に推奨馬券を表示します。", {}, { timeout: 2000 }),
  ).toBeTruthy();
});

it("remembers the require-all switch and adds requireAll after the bet types", async () => {
  realtime.latest = { tansho: [{}], wide: [{}] };
  const store = memoryStorage({});
  const fetcher = vi.fn<typeof fetch>().mockImplementation(async () => Response.json(payload));
  vi.stubGlobal("fetch", fetcher);
  render(<ClefBetSection {...race} />);
  await screen.findByText("馬主フォー", {}, { timeout: 2000 });
  const toggle = screen.getByRole("checkbox", { name: "チェックした馬券の種類をすべて含める" });
  expect(toggle).toHaveProperty("checked", false);
  expect(toggle.closest("fieldset")?.querySelector("legend")?.textContent).toBe("馬券の種類");
  fireEvent.click(toggle);
  expect(store.get("pc-keiba:bet-require-all")).toBe("true");
  await waitFor(
    () =>
      expect(fetcher).toHaveBeenLastCalledWith(
        "/api/clef-rerank/bets/20261018/08/03?budget=2000&target=10000&requireAll=1&odds=T1",
        expect.objectContaining({ cache: "no-store" }),
      ),
    { timeout: 2000 },
  );
  fireEvent.click(screen.getByRole("checkbox", { name: "ワイド" }));
  await waitFor(
    () =>
      expect(fetcher).toHaveBeenLastCalledWith(
        "/api/clef-rerank/bets/20261018/08/03?budget=2000&target=10000&types=tansho&requireAll=1&odds=T1",
        expect.objectContaining({ cache: "no-store" }),
      ),
    { timeout: 2000 },
  );
  cleanup();
  render(<ClefBetSection {...race} />);
  await screen.findByText("馬主フォー", {}, { timeout: 2000 });
  const restored = screen.getByRole("checkbox", { name: "チェックした馬券の種類をすべて含める" });
  expect(restored).toHaveProperty("checked", true);
  fireEvent.click(restored);
  expect(store.get("pc-keiba:bet-require-all")).toBe("false");
  await waitFor(
    () =>
      expect(fetcher).toHaveBeenLastCalledWith(
        "/api/clef-rerank/bets/20261018/08/03?budget=2000&target=10000&types=tansho&odds=T1",
        expect.objectContaining({ cache: "no-store" }),
      ),
    { timeout: 2000 },
  );
});

it("explains when the budget and target cannot fit every checked bet type", async () => {
  vi.stubGlobal(
    "fetch",
    vi
      .fn<typeof fetch>()
      .mockResolvedValue(
        Response.json({ error: "unfit", reason: "required-types-unfit" }, { status: 404 }),
      ),
  );
  render(<ClefBetSection {...race} />);
  expect(
    await screen.findByText(
      "この予算と目標損益額では、チェックした馬券の種類をすべて含める提案ができません。予算を増やすか、目標損益額を下げるか、「すべて含める」をオフにしてください。",
      {},
      { timeout: 2000 },
    ),
  ).toBeTruthy();
});

it("shows a skeleton while a changed request is pending and clears it on the answer", async () => {
  const resolvers: ((value: Response) => void)[] = [];
  const fetcher = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(Response.json(payload))
    .mockImplementation(
      async () =>
        new Promise<Response>((resolve) => {
          resolvers.push(resolve);
        }),
    );
  vi.stubGlobal("fetch", fetcher);
  const { container } = render(<ClefBetSection {...race} />);
  expect(screen.getByText("推奨馬券を計算しています…")).toBeTruthy();
  expect(container.querySelector(".clef-bet-skeleton")?.getAttribute("aria-busy")).toBe("true");
  await screen.findByText("馬主フォー", {}, { timeout: 2000 });
  expect(screen.queryByText("更新中…")).toBeNull();
  expect(container.querySelector(".clef-bet-skeleton")).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "予算を100円増やす" }));
  const status = screen.getByText("更新中…");
  expect(status.tagName).toBe("OUTPUT");
  expect(status.getAttribute("aria-live")).toBe("polite");
  expect(container.querySelector(".clef-bet-skeleton")?.getAttribute("aria-busy")).toBe("true");
  expect(
    container.querySelectorAll(".clef-bet-skeleton .detail-section-skeleton span"),
  ).toHaveLength(5);
  expect(screen.queryByText("馬主フォー")).toBeNull();
  await waitFor(() => expect(resolvers).toHaveLength(1), { timeout: 2000 });
  expect(screen.getByText("更新中…")).toBeTruthy();
  await act(async () => {
    resolvers[0]?.(Response.json({ error: "missing" }, { status: 404 }));
    await Promise.resolve();
  });
  expect(await screen.findByText("オッズ発表後に推奨馬券を表示します。")).toBeTruthy();
  expect(screen.queryByText("更新中…")).toBeNull();
  expect(container.querySelector(".clef-bet-skeleton")).toBeNull();
});

it("marks the 枠番 and 馬番 columns as badge cells", async () => {
  vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockResolvedValue(Response.json(payload)));
  const { container } = render(<ClefBetSection {...race} />);
  await screen.findByText("馬主フォー", {}, { timeout: 2000 });
  expect(
    [...container.querySelectorAll(".clef-bet-table thead th.clef-bet-badge-cell")].map(
      (cell) => cell.textContent,
    ),
  ).toStrictEqual(["枠番", "馬番"]);
  expect(
    container.querySelectorAll(".clef-bet-table tbody td.clef-bet-badge-cell").length > 0,
  ).toBe(true);
});
