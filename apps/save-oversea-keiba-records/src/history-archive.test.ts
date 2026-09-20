// This file runs with Bun.
import { beforeEach, expect, it, vi } from "vitest";
import {
  collectHistoryArchive,
  verifyHistoryArchive,
  type HistoryArchivePlan,
  type HistoryArchivePorts,
} from "./history-archive";

const plan: HistoryArchivePlan = {
  kind: "jockey",
  sourceId: "rider1",
  initialUrl: "https://example.test/results/",
  encoding: "utf-8",
  initialHtmlPath: null,
  profile: {
    populationPattern: "Published \\(([0-9]+)\\)",
    nextLabels: ["Next"],
    emptyMarker: "No results",
    markup: {
      tableMarker: '<table data-demo="results">',
      racePathPrefix: "/event/",
      horsePathPrefix: "/animal/",
      jockeyPathPrefix: "/rider/",
      raceUrlTemplate: "https://example.test/event/{RACE_ID}",
      horseFields: {
        date: 0,
        venue: 1,
        raceNumber: 2,
        raceName: 3,
        finishPosition: 4,
        distance: 5,
        going: 6,
        relatedEntity: 7,
      },
      personFields: {
        date: 0,
        venue: 1,
        raceNumber: 2,
        raceName: 3,
        finishPosition: 4,
        distance: 5,
        going: 6,
        relatedEntity: 7,
      },
    },
  },
};
const table: string =
  '<table data-demo="results"><tr><td>2026/09/01</td><td>Venue</td><td>1</td><td><a href="/event/race1">Race</a></td><td>2</td><td>Dirt1800</td><td>Good</td><td><a href="/animal/horse1">Horse</a></td></tr></table>';
const readPage = vi.fn<HistoryArchivePorts["readPage"]>();
const fetchPage = vi.fn<HistoryArchivePorts["fetchPage"]>();
const archivePage = vi.fn<HistoryArchivePorts["archivePage"]>();
const waitBeforeFetch = vi.fn<HistoryArchivePorts["waitBeforeFetch"]>();
const saveState = vi.fn<HistoryArchivePorts["saveState"]>();
const ports: HistoryArchivePorts = { readPage, fetchPage, archivePage, waitBeforeFetch, saveState };

beforeEach(() => {
  vi.resetAllMocks();
  readPage.mockResolvedValue(null);
  fetchPage.mockResolvedValue(`Published (1) ${table}</html>`);
  archivePage.mockResolvedValue();
  waitBeforeFetch.mockResolvedValue();
  saveState.mockResolvedValue();
});

it("archives every parsed row and counts completion only against the source total", async () => {
  const result = await collectHistoryArchive({ plan, state: null, pageBudget: 1, ports });
  expect(result.collection.status).toBe("complete");
  expect(result.state.rows).toHaveLength(1);
  expect(result.state.publishedCount).toBe(1);
  expect(result.state.checkpoint).toStrictEqual({
    pendingUrl: null,
    completedUrls: ["https://example.test/results/"],
    processedRows: 1,
  });
  expect(saveState).toHaveBeenCalledTimes(1);
});

it("resumes a bounded first page and finishes a second without refetching the first", async () => {
  fetchPage
    .mockResolvedValueOnce(`Published (2) ${table}<a href="?page=2">Next</a></html>`)
    .mockResolvedValueOnce(`Published (2) ${table.replaceAll("race1", "race2")}</html>`);
  const first = await collectHistoryArchive({ plan, state: null, pageBudget: 1, ports });
  expect(first.collection.status).toBe("paused");
  const second = await collectHistoryArchive({ plan, state: first.state, pageBudget: 1, ports });
  expect(second.collection.status).toBe("complete");
  expect(second.state.rows).toHaveLength(2);
  expect(fetchPage).toHaveBeenCalledTimes(2);
  expect(fetchPage).toHaveBeenLastCalledWith("https://example.test/results/?page=2");
});

it("rejects a terminal page that silently truncates the declared population", async () => {
  fetchPage.mockResolvedValue(`Published (2) ${table}</html>`);
  const result = await collectHistoryArchive({ plan, state: null, pageBudget: 2, ports });
  expect(result.collection.status).toBe("blocked");
  expect(result.state.rows).toHaveLength(0);
  expect(saveState).not.toHaveBeenCalled();
});

it("blocks population changes midway instead of mixing snapshots", async () => {
  fetchPage
    .mockResolvedValueOnce(`Published (2) ${table}<a href="?page=2">Next</a></html>`)
    .mockResolvedValueOnce(`Published (3) ${table}</html>`);
  const result = await collectHistoryArchive({ plan, state: null, pageBudget: 2, ports });
  expect(result.collection.status).toBe("blocked");
  expect(result.state.publishedCount).toBe(2);
  expect(result.state.rows).toHaveLength(1);
});

it("does not advance progress after failed atomic state persistence", async () => {
  saveState.mockRejectedValue(new Error("Disk write failed"));
  const result = await collectHistoryArchive({ plan, state: null, pageBudget: 1, ports });
  expect(result.collection.status).toBe("blocked");
  expect(result.state.rows).toHaveLength(0);
  expect(result.state.checkpoint.pendingUrl).toBe("https://example.test/results/");
});

it("rejects a resume under a different source plan", async () => {
  const first = await collectHistoryArchive({ plan, state: null, pageBudget: 1, ports });
  await expect(
    collectHistoryArchive({
      plan: { ...plan, sourceId: "other" },
      state: first.state,
      pageBudget: 1,
      ports,
    }),
  ).rejects.toThrow("History archive belongs to a different plan or has inconsistent progress.");
});

it("rejects inconsistent row counts in a stored checkpoint", async () => {
  const first = await collectHistoryArchive({ plan, state: null, pageBudget: 1, ports });
  expect(() => verifyHistoryArchive({ ...first.state, rows: [] }, plan)).toThrow(
    "History archive belongs to a different plan or has inconsistent progress.",
  );
});

it("does not trust a completed checkpoint when its source population differs", async () => {
  const first = await collectHistoryArchive({ plan, state: null, pageBudget: 1, ports });
  expect(() => verifyHistoryArchive({ ...first.state, publishedCount: 2 }, plan)).toThrow(
    "History archive completion does not match the published population.",
  );
});

it("preserves explicitly declared zero-result coverage", async () => {
  fetchPage.mockResolvedValue("Published (0) No results</html>");
  const result = await collectHistoryArchive({ plan, state: null, pageBudget: 1, ports });
  expect(result.collection.status).toBe("complete");
  expect(result.state.publishedCount).toBe(0);
  expect(result.state.rows).toStrictEqual([]);
});
