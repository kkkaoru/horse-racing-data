// Runs with Bun through Vitest; network and the independently tested decoder are mocked.
import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { readArcPrediction, type ArcRunner } from "../../../lib/arc-prediction";
import { ArcPredictionTable } from "./arc-prediction-table";

vi.mock("../../../lib/arc-prediction", () => ({
  readArcPrediction: vi.fn<typeof readArcPrediction>(),
}));
const runners: ArcRunner[] = [
  { umaban: "16", bamei: "ダイヤモンドネックレス", kettoTorokuBango: "0000000000" },
];
const card = {
  modelVersion: "arc-xgb-top1-v2-20261004",
  modelSha256: "a".repeat(64),
  featureSnapshotAt: "2026-10-03T17:46:11.825Z",
  trainingMaxDate: "2025-10-05",
  predictions: [
    {
      horseId: "000a02ca5a",
      umaban: 16,
      name: "ダイヤモンドネックレス",
      baseScore: 0.2,
      winScore: 0.3,
      predictedRank: 1,
    },
  ],
};
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.resetAllMocks();
});

it("displays dedicated ranks and honest snapshot/probability/evaluation disclosures", async () => {
  vi.mocked(readArcPrediction).mockReturnValue(card);
  const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json({ model: "test" }));
  vi.stubGlobal("fetch", fetcher);
  render(<ArcPredictionTable runners={runners} />);
  expect(screen.getByRole("status").textContent).toBe("凱旋門賞専用予測を読み込み中…");
  expect(await screen.findByText("ダイヤモンドネックレス")).toBeTruthy();
  expect(screen.getAllByRole("row")).toHaveLength(2);
  expect(screen.getByRole("columnheader", { name: "単勝" })).toBeTruthy();
  expect(screen.getByRole("columnheader", { name: "人気" })).toBeTruthy();
  expect(screen.getByText(/勝率・複勝率は未提供/u)).toBeTruthy();
  expect(screen.getByText(/独立した検証精度/u)).toBeTruthy();
  expect(screen.getByText(/2026-10-03T17:46:11.825Z/u)).toBeTruthy();
  expect(fetcher).toHaveBeenCalledWith(
    "/api/arc-prediction/2026-10-04",
    expect.objectContaining({
      credentials: "same-origin",
      cache: "no-store",
      signal: expect.any(AbortSignal),
    }),
  );
  expect(readArcPrediction).toHaveBeenCalledWith({ model: "test" }, runners);
});
it("fails closed on an HTTP error", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 503 })),
  );
  render(<ArcPredictionTable runners={runners} />);
  expect(await screen.findByRole("alert")).toBeTruthy();
  expect(readArcPrediction).not.toHaveBeenCalled();
});
it("fails closed on invalid roster or prediction data", async () => {
  vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockResolvedValue(Response.json({})));
  vi.mocked(readArcPrediction).mockImplementation(() => {
    throw new Error("Roster changed");
  });
  render(<ArcPredictionTable runners={runners} />);
  expect(await screen.findByRole("alert")).toBeTruthy();
  expect(screen.queryByRole("table")).toBeNull();
});
it("hides previous predictions immediately when the current roster changes", async () => {
  vi.mocked(readArcPrediction).mockReturnValue(card);
  const pending = Promise.withResolvers<Response>();
  vi.stubGlobal(
    "fetch",
    vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json({}))
      .mockReturnValueOnce(pending.promise),
  );
  const view = render(<ArcPredictionTable runners={runners} />);
  await screen.findByRole("table");
  view.rerender(<ArcPredictionTable runners={[]} />);
  expect(screen.queryByRole("table")).toBeNull();
  expect(screen.getByRole("status")).toBeTruthy();
  await act(async () => {
    pending.reject(new Error("network"));
  });
  expect(await screen.findByRole("alert")).toBeTruthy();
});
it("ignores a late response from the old roster after its request was aborted", async () => {
  vi.mocked(readArcPrediction).mockReturnValue(card);
  const pending = Promise.withResolvers<Response>();
  const fetcher = vi
    .fn<typeof fetch>()
    .mockReturnValueOnce(pending.promise)
    .mockResolvedValueOnce(new Response(null, { status: 503 }));
  vi.stubGlobal("fetch", fetcher);
  const view = render(<ArcPredictionTable runners={runners} />);
  view.rerender(<ArcPredictionTable runners={[]} />);
  await screen.findByRole("alert");
  await act(async () => {
    pending.resolve(Response.json({}));
  });
  expect(screen.queryByRole("table")).toBeNull();
  expect(screen.getByRole("alert")).toBeTruthy();
  expect(fetcher.mock.calls[0]?.[1]?.signal?.aborted).toBe(true);
});
it("aborts unmounted requests without displaying their rejection", async () => {
  const pending = Promise.withResolvers<Response>();
  const fetcher = vi.fn<typeof fetch>().mockReturnValue(pending.promise);
  vi.stubGlobal("fetch", fetcher);
  const view = render(<ArcPredictionTable runners={runners} />);
  await waitFor(() => expect(fetcher).toHaveBeenCalledTimes(1));
  view.unmount();
  await act(async () => {
    pending.reject(new Error("aborted"));
  });
  expect(fetcher.mock.calls[0]?.[1]?.signal?.aborted).toBe(true);
  expect(screen.queryByRole("alert")).toBeNull();
});
