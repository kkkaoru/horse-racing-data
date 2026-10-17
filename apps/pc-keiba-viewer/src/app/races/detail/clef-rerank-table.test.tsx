// Runs with Bun through Vitest; the Worker request is mocked.
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { ClefRerankTable } from "./clef-rerank-table";

const runners = [
  { umaban: "1", bamei: "アルファ" },
  { umaban: "3", bamei: "ガンマ" },
];

const payload = {
  modelVersion: "clef-rerank-jra-v1",
  label: "clef-flash 補正",
  baseModelVersion: "jra-cb-v9-sim-2013-clean-nowin1",
  baseGeneratedAt: "2026-10-18T00:00:00Z",
  predictions: [
    { horseNumber: 3, rank: 1, baseRank: 2, clefWin: 0.3, clefTop3: 0.7 },
    { horseNumber: 1, rank: 2, baseRank: 1, clefWin: 0.2, clefTop3: 0.5 },
    { horseNumber: 9, rank: 3, baseRank: 3, clefWin: 0.1, clefTop3: 0.25 },
  ],
};

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

it("renders the reranked order with movement and clef estimates", async () => {
  const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json(payload));
  vi.stubGlobal("fetch", fetcher);
  render(
    <ClefRerankTable
      day="18"
      keibajoCode="08"
      month="10"
      raceNumber="11"
      runners={runners}
      year="2026"
    />,
  );
  expect(await screen.findByText("ガンマ")).toBeTruthy();
  expect(screen.getAllByRole("row").map((row) => row.textContent)).toStrictEqual([
    "補正順位馬番馬名元の順位変動clef 勝率clef 3着内",
    "13ガンマ2↑130.0%70.0%",
    "21アルファ1↓120.0%50.0%",
    "39-3→10.0%25.0%",
  ]);
  expect(screen.getByRole("heading", { name: "clef-flash 補正 着順予測" })).toBeTruthy();
  expect(fetcher).toHaveBeenCalledWith(
    "/api/clef-rerank/20261018/08/11",
    expect.objectContaining({ credentials: "same-origin", cache: "no-store" }),
  );
});

it("renders nothing when the Worker has no bundle for the race", async () => {
  const fetcher = vi
    .fn<typeof fetch>()
    .mockResolvedValue(Response.json({ error: "missing" }, { status: 404 }));
  vi.stubGlobal("fetch", fetcher);
  const { container } = render(
    <ClefRerankTable
      day="18"
      keibajoCode="08"
      month="10"
      raceNumber="01"
      runners={runners}
      year="2026"
    />,
  );
  await waitFor(() => expect(fetcher).toHaveBeenCalledTimes(1));
  expect(container.innerHTML).toBe("");
});

it("renders nothing when the request fails", async () => {
  const fetcher = vi.fn<typeof fetch>().mockRejectedValue(new Error("offline"));
  vi.stubGlobal("fetch", fetcher);
  const { container } = render(
    <ClefRerankTable
      day="18"
      keibajoCode="08"
      month="10"
      raceNumber="02"
      runners={runners}
      year="2026"
    />,
  );
  await waitFor(() => expect(fetcher).toHaveBeenCalledTimes(1));
  expect(container.innerHTML).toBe("");
});

it("ignores a response that arrives after unmount", async () => {
  const resolvers: ((value: Response) => void)[] = [];
  const fetcher = vi.fn<typeof fetch>().mockImplementation(
    async () =>
      new Promise<Response>((resolve) => {
        resolvers.push(resolve);
      }),
  );
  vi.stubGlobal("fetch", fetcher);
  const { unmount } = render(
    <ClefRerankTable
      day="18"
      keibajoCode="08"
      month="10"
      raceNumber="03"
      runners={runners}
      year="2026"
    />,
  );
  await waitFor(() => expect(fetcher).toHaveBeenCalledTimes(1));
  unmount();
  resolvers[0]?.(Response.json(payload));
  await Promise.resolve();
  expect(screen.queryByText("ガンマ")).toBeNull();
});
