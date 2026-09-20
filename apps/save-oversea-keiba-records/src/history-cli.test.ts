// This file runs with Bun. All filesystem and network operations are mocked.
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { createHistoryCliRuntime, runHistoryCli, type HistoryCliRuntime } from "./history-cli";
import { historyPlanDigest, type HistoryArchivePlan } from "./history-archive";

const fs = vi.hoisted(() => ({
  readFile: vi.fn(),
  mkdir: vi.fn(),
  writeFile: vi.fn(),
  rename: vi.fn(),
}));
vi.mock("node:fs/promises", () => fs);
const plan: HistoryArchivePlan = {
  kind: "owner",
  sourceId: "owner1",
  initialUrl: "https://example.test/results/",
  encoding: "utf-8",
  initialHtmlPath: null,
  profile: {
    populationPattern: "Count ([0-9]+)",
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
const files: Map<string, string> = new Map();
const read = vi.fn<HistoryCliRuntime["read"]>();
const writeAtomic = vi.fn<HistoryCliRuntime["writeAtomic"]>();
const fetchPage = vi.fn<HistoryCliRuntime["fetchPage"]>();
const wait = vi.fn<HistoryCliRuntime["wait"]>();
const runtime: HistoryCliRuntime = { read, writeAtomic, fetchPage, wait };

beforeEach(() => {
  vi.resetAllMocks();
  files.clear();
  files.set("/private/input.json", JSON.stringify(plan));
  read.mockImplementation(async (path) => files.get(path) ?? null);
  writeAtomic.mockImplementation(async (path, content) => {
    files.set(path, content);
  });
  fetchPage.mockResolvedValue("Count 0 No results</html>");
  wait.mockResolvedValue();
  fs.mkdir.mockResolvedValue(undefined);
  fs.writeFile.mockResolvedValue(undefined);
  fs.rename.mockResolvedValue(undefined);
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

it("collects and persists a verified zero-result archive without database claims", async () => {
  expect(
    await runHistoryCli({ argv: ["collect", "/private/input.json", "/private/run", "1"], runtime }),
  ).toStrictEqual({
    status: "complete",
    archivedRows: 0,
    publishedCount: 0,
    archivedPages: 1,
    databasePublished: false,
    error: null,
  });
  expect(fetchPage).toHaveBeenCalledTimes(1);
  expect(wait).toHaveBeenCalledTimes(1);
  expect(files.get("/private/run/archive.json")).toMatch(/"publishedCount":0/);
});

it("reports existing archive completion without fetching or writing again", async () => {
  files.set("/private/run/plan.json", JSON.stringify(plan));
  files.set(
    "/private/run/archive.json",
    JSON.stringify({
      planDigest: historyPlanDigest(plan),
      publishedCount: 0,
      checkpoint: { pendingUrl: null, completedUrls: [plan.initialUrl], processedRows: 0 },
      rows: [],
    }),
  );
  expect(await runHistoryCli({ argv: ["status", "/private/run"], runtime })).toStrictEqual({
    status: "complete",
    archivedRows: 0,
    publishedCount: 0,
    archivedPages: 1,
    databasePublished: false,
    error: null,
  });
  expect(fetchPage).not.toHaveBeenCalled();
  expect(writeAtomic).not.toHaveBeenCalled();
});

it("status distinguishes an unfinished archive from complete coverage", async () => {
  files.set("/private/run/plan.json", JSON.stringify(plan));
  files.set(
    "/private/run/archive.json",
    JSON.stringify({
      planDigest: historyPlanDigest(plan),
      publishedCount: null,
      checkpoint: { pendingUrl: plan.initialUrl, completedUrls: [], processedRows: 0 },
      rows: [],
    }),
  );
  expect((await runHistoryCli({ argv: ["status", "/private/run"], runtime })).status).toBe(
    "paused",
  );
});

it("resumes an already completed snapshot without network calls", async () => {
  await runHistoryCli({ argv: ["collect", "/private/input.json", "/private/run", "1"], runtime });
  fetchPage.mockClear();
  writeAtomic.mockClear();
  expect(
    (
      await runHistoryCli({
        argv: ["collect", "/private/input.json", "/private/run", "1"],
        runtime,
      })
    ).status,
  ).toBe("complete");
  expect(fetchPage).not.toHaveBeenCalled();
  expect(writeAtomic).not.toHaveBeenCalled();
});

it("refuses changed plans instead of mixing snapshots", async () => {
  files.set("/private/run/plan.json", JSON.stringify({ ...plan, sourceId: "other" }));
  await expect(
    runHistoryCli({ argv: ["collect", "/private/input.json", "/private/run", "1"], runtime }),
  ).rejects.toThrow("History source plan changed; use a new snapshot directory.");
  expect(fetchPage).not.toHaveBeenCalled();
});

it("uses an explicitly supplied normal-browser HTML export before HTTP", async () => {
  files.set(
    "/private/input.json",
    JSON.stringify({ ...plan, initialHtmlPath: "/private/rendered.html" }),
  );
  files.set("/private/rendered.html", "Count 0 No results</html>");
  expect(
    (
      await runHistoryCli({
        argv: ["collect", "/private/input.json", "/private/run", "1"],
        runtime,
      })
    ).status,
  ).toBe("complete");
  expect(fetchPage).not.toHaveBeenCalled();
});

it("uses the normal HTTP path when the configured browser export is not present", async () => {
  files.set(
    "/private/input.json",
    JSON.stringify({ ...plan, initialHtmlPath: "/private/rendered.html" }),
  );
  await runHistoryCli({ argv: ["collect", "/private/input.json", "/private/run", "1"], runtime });
  expect(fetchPage).toHaveBeenCalledTimes(1);
});

it("reuses a raw page archive after a parse failure rather than repeating HTTP", async () => {
  fetchPage.mockResolvedValue("Count 0 Sign in</html>");
  expect(
    (
      await runHistoryCli({
        argv: ["collect", "/private/input.json", "/private/run", "1"],
        runtime,
      })
    ).status,
  ).toBe("blocked");
  fetchPage.mockClear();
  expect(
    (
      await runHistoryCli({
        argv: ["collect", "/private/input.json", "/private/run", "1"],
        runtime,
      })
    ).status,
  ).toBe("blocked");
  expect(fetchPage).not.toHaveBeenCalled();
});

it("does not invent a plan when a required file is missing", async () => {
  files.clear();
  await expect(
    runHistoryCli({ argv: ["collect", "/private/input.json", "/private/run", "1"], runtime }),
  ).rejects.toThrow("Required private history file is missing.");
});

it.each([
  { argv: [] },
  { argv: ["unknown"] },
  { argv: ["collect", "/private/input.json", "/private/run", "-1"] },
  { argv: ["status"] },
])("rejects invalid arguments %j", async ({ argv }) => {
  await expect(runHistoryCli({ argv, runtime })).rejects.toThrow(
    "Use collect PLAN_JSON DIRECTORY PAGE_BUDGET or status DIRECTORY.",
  );
});

it("reads existing private files and treats only ENOENT as absent", async () => {
  fs.readFile
    .mockResolvedValueOnce("contents")
    .mockRejectedValueOnce({ code: "ENOENT" })
    .mockRejectedValueOnce(new Error("Denied"));
  const native = createHistoryCliRuntime();
  expect(await native.read("/private/file")).toBe("contents");
  expect(await native.read("/private/missing")).toBeNull();
  await expect(native.read("/private/denied")).rejects.toThrow("Denied");
});

it("does not hide other filesystem error codes", async () => {
  fs.readFile.mockRejectedValue({ code: "EACCES" });
  await expect(createHistoryCliRuntime().read("/private/denied")).rejects.toStrictEqual({
    code: "EACCES",
  });
});

it("writes a private temporary file before atomically replacing the checkpoint", async () => {
  await createHistoryCliRuntime().writeAtomic("/private/run/archive.json", "contents");
  expect(fs.mkdir).toHaveBeenCalledWith("/private/run", { recursive: true, mode: 0o700 });
  expect(fs.writeFile).toHaveBeenCalledWith(
    expect.stringMatching(/^\/private\/run\/archive\.json\..+\.pending$/u),
    "contents",
    { mode: 0o600, flag: "wx" },
  );
  expect(fs.rename).toHaveBeenCalledWith(
    expect.stringMatching(/\.pending$/u),
    "/private/run/archive.json",
  );
});

it("fetches once, archives raw bytes and provenance, and decodes explicit encoding", async () => {
  const network = vi.fn().mockResolvedValue(
    new Response("published text", {
      status: 200,
      headers: { "content-type": "text/html;charset=utf-8" },
    }),
  );
  vi.stubGlobal("fetch", network);
  expect(
    await createHistoryCliRuntime().fetchPage({
      url: "https://example.test/results/",
      encoding: "utf-8",
      rawPath: "/private/page.raw",
    }),
  ).toBe("published text");
  expect(network).toHaveBeenCalledWith("https://example.test/results/", {
    redirect: "manual",
    signal: expect.any(AbortSignal),
  });
  expect(fs.rename).toHaveBeenCalledTimes(2);
  expect(fs.writeFile).toHaveBeenLastCalledWith(
    expect.stringMatching(/\.pending$/u),
    expect.stringMatching(/"status":200/),
    { mode: 0o600, flag: "wx" },
  );
});

it("does not fetch with an unknown encoding", async () => {
  const network = vi.fn();
  vi.stubGlobal("fetch", network);
  await expect(
    createHistoryCliRuntime().fetchPage({
      url: "https://example.test/results/",
      encoding: "unknown-encoding",
      rawPath: "/private/page.raw",
    }),
  ).rejects.toThrow("History source encoding is unsupported.");
  expect(network).not.toHaveBeenCalled();
});

it.each([302, 403, 429])("never follows redirects or bypasses HTTP status %s", async (status) => {
  const network = vi.fn().mockResolvedValue(new Response("blocked", { status }));
  vi.stubGlobal("fetch", network);
  await expect(
    createHistoryCliRuntime().fetchPage({
      url: "https://example.test/results/",
      encoding: "utf-8",
      rawPath: "/private/page.raw",
    }),
  ).rejects.toThrow(
    "History source request failed; no redirect or access-control bypass is allowed.",
  );
  expect(network).toHaveBeenCalledTimes(1);
});

it("paces network requests without real waits during tests", async () => {
  vi.useFakeTimers();
  const pending = createHistoryCliRuntime().wait();
  await vi.advanceTimersByTimeAsync(1500);
  await expect(pending).resolves.toBeUndefined();
});
