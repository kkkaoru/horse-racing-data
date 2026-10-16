// This file runs with Bun. Audits use mocked files only, never network or databases.
import { createHash } from "node:crypto";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { auditHistorySnapshot } from "./history-audit";
import {
  historyPlanDigest,
  type HistoryArchivePlan,
  type HistoryArchiveState,
} from "./history-archive";
import { historyProxyUrl } from "./history-proxy";
import { parseHistorySourcePage } from "./sources/history-source-page";
const fs = vi.hoisted(() => ({ readFile: vi.fn() }));
vi.mock("node:fs/promises", () => fs);
const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
const encode = (value: string): Uint8Array => new TextEncoder().encode(value);
const plan: HistoryArchivePlan = {
  kind: "jockey",
  sourceId: "rider1",
  initialUrl: "https://db.netkeiba.com/history/",
  encoding: "euc-jp",
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
const secondUrl: string = "https://db.netkeiba.com/history/?page=2";
const table: string =
  '<table data-demo="results"><tr><td>2026/09/01</td><td>Venue</td><td>1</td><td><a href="/event/race1">Race</a></td><td>2</td><td>Dirt1800</td><td>Good</td><td><a href="/animal/horse1">Horse</a></td></tr></table>';
const firstHtml: string = `Count 2 ${table}<a href="?page=2">Next</a></html>`;
const secondHtml: string = `Count 2 ${table.replaceAll("race1", "race2")}</html>`;
const firstPath: string = `/snapshot/pages/${hash(plan.initialUrl)}`;
const secondPath: string = `/snapshot/pages/${hash(secondUrl)}`;
const firstMeta = {
  url: plan.initialUrl,
  status: 200,
  encoding: "utf-8",
  transport: "reverse-proxy-browser-get",
  transportUrl: historyProxyUrl(plan.initialUrl),
  sha256: hash(firstHtml),
  bytes: encode(firstHtml).byteLength,
};
const secondMeta = {
  url: secondUrl,
  status: 200,
  encoding: "utf-8",
  transport: "reverse-proxy-browser-get",
  transportUrl: historyProxyUrl(secondUrl),
  sha256: hash(secondHtml),
  bytes: encode(secondHtml).byteLength,
};
const state: HistoryArchiveState = {
  planDigest: historyPlanDigest(plan),
  publishedCount: 2,
  checkpoint: { pendingUrl: null, completedUrls: [plan.initialUrl, secondUrl], processedRows: 2 },
  rows: [
    ...parseHistorySourcePage({
      html: firstHtml,
      url: plan.initialUrl,
      kind: plan.kind,
      sourceId: plan.sourceId,
      profile: plan.profile,
    }).rows,
    ...parseHistorySourcePage({
      html: secondHtml,
      url: secondUrl,
      kind: plan.kind,
      sourceId: plan.sourceId,
      profile: plan.profile,
    }).rows,
  ],
};
const batchText: string = JSON.stringify([
  { url: plan.initialUrl, httpStatus: 200, result: "success", body: firstHtml },
]);
const batchMeta = {
  url: plan.initialUrl,
  status: 200,
  encoding: "utf-8",
  transport: "reverse-proxy-batch",
  batchResponsePath: "/original/batch.json",
  batchResponseSha256: hash(batchText),
  sourceHtmlSha256: hash(firstHtml),
  result: "success",
};
const files: Map<string, Uint8Array> = new Map();
const network = vi.fn();
beforeEach(() => {
  vi.resetAllMocks();
  files.clear();
  vi.stubGlobal("fetch", network);
  files.set("/snapshot/plan.json", encode(JSON.stringify(plan)));
  files.set("/snapshot/archive.json", encode(JSON.stringify(state)));
  files.set(`${firstPath}.raw`, encode(firstHtml));
  files.set(`${firstPath}.html`, encode(firstHtml));
  files.set(`${firstPath}.raw.meta.json`, encode(JSON.stringify(firstMeta)));
  files.set(`${secondPath}.raw`, encode(secondHtml));
  files.set(`${secondPath}.html`, encode(secondHtml));
  files.set(`${secondPath}.raw.meta.json`, encode(JSON.stringify(secondMeta)));
  fs.readFile.mockImplementation(async (path: string, encoding?: string) => {
    const value: Uint8Array | undefined = files.get(path);
    if (value === undefined) throw new Error("Missing source fixture");
    return encoding === undefined ? Buffer.from(value) : new TextDecoder().decode(value);
  });
});
afterEach(() => {
  vi.unstubAllGlobals();
});
it("reparses all original bytes, checks pagination and hashes, without network access", async () => {
  const report = await auditHistorySnapshot("/snapshot");
  expect(report.status).toBe("complete");
  expect(report.archivedRows).toBe(2);
  expect(report.verifiedRawPages).toBe(2);
  expect(report.allArchivedRowsReparsed).toBe(true);
  expect(report.databasePublished).toBe(false);
  expect(report.archiveSha256).toMatch(/^[a-f0-9]{64}$/u);
  expect(network).not.toHaveBeenCalled();
});
it("audits partial evidence without labeling it complete", async () => {
  files.set(
    "/snapshot/archive.json",
    encode(
      JSON.stringify({
        ...state,
        rows: state.rows.slice(0, 1),
        checkpoint: { pendingUrl: secondUrl, completedUrls: [plan.initialUrl], processedRows: 1 },
      }),
    ),
  );
  const report = await auditHistorySnapshot("/snapshot");
  expect(report.status).toBe("paused");
  expect(report.verifiedRawPages).toBe(1);
});
it("supports mixed genuine POST originals and GET originals", async () => {
  files.set(`${firstPath}.raw.meta.json`, encode(JSON.stringify(batchMeta)));
  files.set("/original/batch.json", encode(batchText));
  expect((await auditHistorySnapshot("/snapshot")).verifiedRawPages).toBe(2);
});
it("accepts a genuine cache-hit batch result", async () => {
  const response: string = batchText.replace('"success"', '"cache_hit"');
  files.set(
    `${firstPath}.raw.meta.json`,
    encode(
      JSON.stringify({ ...batchMeta, batchResponseSha256: hash(response), result: "cache_hit" }),
    ),
  );
  files.set("/original/batch.json", encode(response));
  expect((await auditHistorySnapshot("/snapshot")).status).toBe("complete");
});
it.each([
  { url: "https://example.test/" },
  { status: 400 },
  { encoding: "euc-jp" },
  { transport: "direct" },
  { transportUrl: "https://db.netkeiba.com/history/" },
  { sha256: "wrong" },
  { bytes: 0 },
])("rejects incorrect raw provenance %j", async (changed) => {
  files.set(`${firstPath}.raw.meta.json`, encode(JSON.stringify({ ...firstMeta, ...changed })));
  await expect(auditHistorySnapshot("/snapshot")).rejects.toThrow();
});
it("rejects non-object metadata", async () => {
  files.set(`${firstPath}.raw.meta.json`, encode("null"));
  await expect(auditHistorySnapshot("/snapshot")).rejects.toThrow("metadata must be an object");
});
it("rejects changed HTML even when the original raw bytes remain valid", async () => {
  files.set(`${firstPath}.html`, encode("edited"));
  await expect(auditHistorySnapshot("/snapshot")).rejects.toThrow(
    "Raw bytes and parsed HTML differ",
  );
});
it.each([
  null,
  "not an array",
  [],
  [null],
  [{ url: "https://example.test/" }],
  [{ url: plan.initialUrl, httpStatus: 400, body: firstHtml }],
  [{ url: plan.initialUrl, httpStatus: 200, result: "error", body: firstHtml }],
])("rejects malformed genuine batch envelopes %j", async (value) => {
  const response: string = JSON.stringify(value);
  files.set("/original/batch.json", encode(response));
  files.set(
    `${firstPath}.raw.meta.json`,
    encode(JSON.stringify({ ...batchMeta, batchResponseSha256: hash(response) })),
  );
  await expect(auditHistorySnapshot("/snapshot")).rejects.toThrow();
});
it("rejects batch metadata without its original response path", async () => {
  files.set(
    `${firstPath}.raw.meta.json`,
    encode(JSON.stringify({ ...batchMeta, batchResponsePath: null })),
  );
  await expect(auditHistorySnapshot("/snapshot")).rejects.toThrow(
    "Missing original batch response path",
  );
});
it("rejects a corrupt original batch SHA", async () => {
  files.set(`${firstPath}.raw.meta.json`, encode(JSON.stringify(batchMeta)));
  files.set("/original/batch.json", encode("[]"));
  await expect(auditHistorySnapshot("/snapshot")).rejects.toThrow("Original batch SHA differs");
});
it("rejects duplicate URLs in the checkpoint", async () => {
  files.set(
    "/snapshot/archive.json",
    encode(
      JSON.stringify({
        ...state,
        checkpoint: { ...state.checkpoint, completedUrls: [plan.initialUrl, plan.initialUrl] },
      }),
    ),
  );
  await expect(auditHistorySnapshot("/snapshot")).rejects.toThrow("Repeated archived source URL");
});
it("rejects archive/plan mutation during audit", async () => {
  fs.readFile.mockImplementation(async (path: string, encoding?: string) => {
    const value: Uint8Array | undefined = files.get(path);
    if (value === undefined) throw new Error("Missing fixture");
    if (path === `${secondPath}.raw`) files.set("/snapshot/archive.json", encode("changed"));
    return encoding === undefined ? Buffer.from(value) : new TextDecoder().decode(value);
  });
  await expect(auditHistorySnapshot("/snapshot")).rejects.toThrow("Archive changed during audit");
});
it("rejects substituted rows with unchanged counts", async () => {
  files.set(
    "/snapshot/archive.json",
    encode(JSON.stringify({ ...state, rows: [...state.rows].reverse() })),
  );
  await expect(auditHistorySnapshot("/snapshot")).rejects.toThrow("Reparsed originals differ");
});
