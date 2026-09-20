// This file runs with Bun.
import { beforeEach, expect, it, vi } from "vitest";
import {
  collectHistory,
  type HistoryCollectionPorts,
  type HistoryCollectionCheckpoint,
  type HistoryPage,
} from "./history-collection";

const readArchive = vi.fn<(url: string) => Promise<string | null>>();
const fetchPage = vi.fn<(url: string) => Promise<string>>();
const archivePage = vi.fn<(url: string, html: string) => Promise<void>>();
const parsePage = vi.fn<(html: string) => HistoryPage<number>>();
const commitPage = vi.fn<HistoryCollectionPorts<number>["commitPage"]>();
const waitBeforeFetch = vi.fn<() => Promise<void>>();
const ports: HistoryCollectionPorts<number> = {
  readArchive,
  fetchPage,
  archivePage,
  parsePage,
  commitPage,
  waitBeforeFetch,
};
const checkpoint: HistoryCollectionCheckpoint = {
  pendingUrl: "https://example.test/page/1",
  completedUrls: [],
  processedRows: 0,
};

beforeEach(() => {
  vi.resetAllMocks();
  readArchive.mockResolvedValue(null);
  fetchPage.mockResolvedValue("published page");
  archivePage.mockResolvedValue();
  commitPage.mockResolvedValue();
  waitBeforeFetch.mockResolvedValue();
  parsePage.mockReturnValue({ rows: [1], nextUrl: null, terminal: true });
});

it("archives before parsing and commits a verified terminal page", async () => {
  const result = await collectHistory({
    checkpoint,
    allowedOrigin: "https://example.test",
    pageBudget: 2,
    ports,
  });
  expect(result).toStrictEqual({
    status: "complete",
    checkpoint: {
      pendingUrl: null,
      completedUrls: ["https://example.test/page/1"],
      processedRows: 1,
    },
    error: null,
  });
  expect(waitBeforeFetch.mock.invocationCallOrder[0]).toBeLessThan(
    fetchPage.mock.invocationCallOrder[0]!,
  );
  expect(archivePage.mock.invocationCallOrder[0]).toBeLessThan(
    parsePage.mock.invocationCallOrder[0]!,
  );
  expect(commitPage).toHaveBeenCalledWith({
    url: "https://example.test/page/1",
    rows: [1],
    checkpoint: {
      pendingUrl: null,
      completedUrls: ["https://example.test/page/1"],
      processedRows: 1,
    },
  });
});

it("resumes archived HTML without another network request", async () => {
  readArchive.mockResolvedValue("cached page");
  const result = await collectHistory({
    checkpoint,
    allowedOrigin: "https://example.test",
    pageBudget: 1,
    ports,
  });
  expect(result.status).toBe("complete");
  expect(parsePage).toHaveBeenCalledWith("cached page");
  expect(fetchPage).not.toHaveBeenCalled();
  expect(archivePage).not.toHaveBeenCalled();
  expect(waitBeforeFetch).not.toHaveBeenCalled();
});

it("pauses at the budget rather than reporting a partial traversal as complete", async () => {
  parsePage.mockReturnValue({
    rows: [1, 2],
    nextUrl: "https://example.test/page/2",
    terminal: false,
  });
  const result = await collectHistory({
    checkpoint,
    allowedOrigin: "https://example.test",
    pageBudget: 1,
    ports,
  });
  expect(result).toStrictEqual({
    status: "paused",
    checkpoint: {
      pendingUrl: "https://example.test/page/2",
      completedUrls: ["https://example.test/page/1"],
      processedRows: 2,
    },
    error: null,
  });
});

it("keeps the last committed page when a later page fails", async () => {
  parsePage
    .mockReturnValueOnce({ rows: [1], nextUrl: "https://example.test/page/2", terminal: false })
    .mockImplementationOnce(() => {
      throw new Error("private source markup");
    });
  const result = await collectHistory({
    checkpoint,
    allowedOrigin: "https://example.test",
    pageBudget: 3,
    ports,
  });
  expect(result).toStrictEqual({
    status: "blocked",
    checkpoint: {
      pendingUrl: "https://example.test/page/2",
      completedUrls: ["https://example.test/page/1"],
      processedRows: 1,
    },
    error:
      "History page could not be archived, validated or committed; resume from the pending page after inspection.",
  });
  expect(archivePage).toHaveBeenCalledTimes(2);
  expect(commitPage).toHaveBeenCalledTimes(1);
});

it("does not advance when storage fails", async () => {
  commitPage.mockRejectedValue(new Error("private connection detail"));
  const result = await collectHistory({
    checkpoint,
    allowedOrigin: "https://example.test",
    pageBudget: 2,
    ports,
  });
  expect(result.status).toBe("blocked");
  expect(result.checkpoint).toStrictEqual({
    pendingUrl: "https://example.test/page/1",
    completedUrls: [],
    processedRows: 0,
  });
});

it("does not parse or commit a page that could not be archived", async () => {
  archivePage.mockRejectedValue(new Error("disk full"));
  expect(
    (
      await collectHistory({
        checkpoint,
        allowedOrigin: "https://example.test",
        pageBudget: 1,
        ports,
      })
    ).status,
  ).toBe("blocked");
  expect(parsePage).not.toHaveBeenCalled();
  expect(commitPage).not.toHaveBeenCalled();
});

it.each([
  "http://example.test/page",
  "https://other.test/page",
  "https://user@example.test/page",
  "https://:password@example.test/page",
  "https://example.test/page#fragment",
  "not a URL",
])("blocks invalid pending URL %s before I/O", async (url) => {
  const result = await collectHistory({
    checkpoint: { ...checkpoint, pendingUrl: url },
    allowedOrigin: "https://example.test",
    pageBudget: 1,
    ports,
  });
  expect(result.status).toBe("blocked");
  expect(readArchive).not.toHaveBeenCalled();
});

it("blocks replay of a committed page", async () => {
  const result = await collectHistory({
    checkpoint: { ...checkpoint, completedUrls: ["https://example.test/page/1"] },
    allowedOrigin: "https://example.test",
    pageBudget: 1,
    ports,
  });
  expect(result.status).toBe("blocked");
  expect(readArchive).not.toHaveBeenCalled();
});

it.each(["https://example.test/page/1", "https://other.test/page/2"])(
  "blocks circular or cross-origin pagination %s",
  async (nextUrl) => {
    parsePage.mockReturnValue({ rows: [1], nextUrl, terminal: false });
    expect(
      (
        await collectHistory({
          checkpoint,
          allowedOrigin: "https://example.test",
          pageBudget: 1,
          ports,
        })
      ).status,
    ).toBe("blocked");
    expect(commitPage).not.toHaveBeenCalled();
  },
);

it("blocks an unexplained missing next link", async () => {
  parsePage.mockReturnValue({ rows: [1], nextUrl: null, terminal: false });
  expect(
    (
      await collectHistory({
        checkpoint,
        allowedOrigin: "https://example.test",
        pageBudget: 1,
        ports,
      })
    ).status,
  ).toBe("blocked");
  expect(commitPage).not.toHaveBeenCalled();
});

it("blocks empty nonterminal pages rather than assuming successful AJAX loading", async () => {
  parsePage.mockReturnValue({ rows: [], nextUrl: "https://example.test/page/2", terminal: false });
  expect(
    (
      await collectHistory({
        checkpoint,
        allowedOrigin: "https://example.test",
        pageBudget: 1,
        ports,
      })
    ).status,
  ).toBe("blocked");
});

it("accepts explicitly verified no-history terminal pages", async () => {
  parsePage.mockReturnValue({ rows: [], nextUrl: null, terminal: true });
  const result = await collectHistory({
    checkpoint,
    allowedOrigin: "https://example.test",
    pageBudget: 1,
    ports,
  });
  expect(result).toStrictEqual({
    status: "complete",
    checkpoint: {
      pendingUrl: null,
      completedUrls: ["https://example.test/page/1"],
      processedRows: 0,
    },
    error: null,
  });
});

it.each([-1, 0.5, Number.NaN, Number.POSITIVE_INFINITY])(
  "rejects invalid budget %s",
  async (pageBudget) => {
    await expect(
      collectHistory({ checkpoint, allowedOrigin: "https://example.test", pageBudget, ports }),
    ).rejects.toThrow("History page budget must be a non-negative integer.");
  },
);

it("rejects a completion checkpoint with no committed page", async () => {
  await expect(
    collectHistory({
      checkpoint: { ...checkpoint, pendingUrl: null },
      allowedOrigin: "https://example.test",
      pageBudget: 1,
      ports,
    }),
  ).rejects.toThrow("History completion requires a committed terminal page.");
});
