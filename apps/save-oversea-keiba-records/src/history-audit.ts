// This file runs with Bun. Reparse immutable source evidence; never fetch or publish while auditing.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import {
  verifyHistoryArchive,
  type HistoryArchivePlan,
  type HistoryArchiveState,
} from "./history-archive";
import type { HistoryCliReport } from "./history-cli";
import { decodeHistoryArchive, decodeHistoryPlan, isHistoryRecord } from "./history-json";
import { historyProxyUrl } from "./history-proxy";
import {
  parseHistorySourcePage,
  type HistorySourceRow,
  type ParsedHistorySourcePage,
} from "./sources/history-source-page";

export interface HistoryAuditReport extends HistoryCliReport {
  readonly archiveSha256: string;
  readonly planSha256: string;
  readonly verifiedRawPages: number;
  readonly allArchivedRowsReparsed: true;
}
interface PageAuditInput {
  readonly directory: string;
  readonly url: string;
  readonly plan: HistoryArchivePlan;
}
interface BatchEvidenceInput {
  readonly meta: Record<string, unknown>;
  readonly url: string;
  readonly html: string;
}

const SUCCESS_STATUS: number = 200;
const digest = (data: string | Uint8Array): string =>
  createHash("sha256").update(data).digest("hex");

const verifyBatchEvidence = async ({ meta, url, html }: BatchEvidenceInput): Promise<void> => {
  assert(typeof meta.batchResponsePath === "string", "Missing original batch response path.");
  const bytes: Uint8Array = await readFile(meta.batchResponsePath);
  assert.equal(digest(bytes), meta.batchResponseSha256, "Original batch SHA differs.");
  const response: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
  assert(Array.isArray(response), "Original batch is not an array.");
  const matching: unknown[] = response.filter(
    (item: unknown) => isHistoryRecord(item) && item.url === url,
  );
  assert.equal(matching.length, 1, "Original batch has missing or duplicate source URLs.");
  const result: unknown = matching[0];
  assert(isHistoryRecord(result));
  assert.equal(result.httpStatus, SUCCESS_STATUS);
  assert.equal(result.body, html);
  assert(result.result === "success" || result.result === "cache_hit");
  assert.equal(result.result, meta.result);
  assert.equal(digest(html), meta.sourceHtmlSha256);
};

const auditPage = async ({
  directory,
  url,
  plan,
}: PageAuditInput): Promise<ParsedHistorySourcePage> => {
  const prefix: string = join(directory, "pages", digest(url));
  const [raw, html, metaText]: [Buffer, string, string] = await Promise.all([
    readFile(`${prefix}.raw`),
    readFile(`${prefix}.html`, "utf8"),
    readFile(`${prefix}.raw.meta.json`, "utf8"),
  ]);
  const meta: unknown = JSON.parse(metaText);
  assert(isHistoryRecord(meta), "Source metadata must be an object.");
  assert.equal(meta.url, url);
  assert.equal(meta.status, SUCCESS_STATUS);
  assert.equal(meta.encoding, "utf-8");
  assert.equal(
    new TextDecoder("utf-8", { fatal: true }).decode(raw),
    html,
    "Raw bytes and parsed HTML differ.",
  );
  if (meta.transport === "reverse-proxy-batch") {
    await verifyBatchEvidence({ meta, url, html });
  } else {
    assert.equal(
      meta.transport,
      "reverse-proxy-browser-get",
      "Unsupported raw evidence transport.",
    );
    assert.equal(meta.transportUrl, historyProxyUrl(url));
    assert.equal(meta.bytes, raw.byteLength);
    assert.equal(meta.sha256, digest(raw));
  }
  return parseHistorySourcePage({
    html,
    url,
    kind: plan.kind,
    sourceId: plan.sourceId,
    profile: plan.profile,
  });
};

export const auditHistorySnapshot = async (directory: string): Promise<HistoryAuditReport> => {
  const planPath: string = join(directory, "plan.json"),
    archivePath: string = join(directory, "archive.json");
  const [planText, archiveText]: [string, string] = await Promise.all([
    readFile(planPath, "utf8"),
    readFile(archivePath, "utf8"),
  ]);
  const plan: HistoryArchivePlan = decodeHistoryPlan(planText),
    state: HistoryArchiveState = decodeHistoryArchive(archiveText);
  verifyHistoryArchive(state, plan);
  assert.equal(
    new Set(state.checkpoint.completedUrls).size,
    state.checkpoint.completedUrls.length,
    "Repeated archived source URL.",
  );
  const rows: HistorySourceRow[] = [];
  for (const [position, url] of state.checkpoint.completedUrls.entries()) {
    if (position === 0) assert.equal(url, plan.initialUrl);
    const page: ParsedHistorySourcePage = await auditPage({ directory, url, plan });
    assert.equal(page.publishedCount, state.publishedCount);
    assert.equal(
      page.nextUrl,
      state.checkpoint.completedUrls[position + 1] ?? state.checkpoint.pendingUrl,
    );
    rows.push(...page.rows);
  }
  assert.deepEqual(rows, state.rows, "Reparsed originals differ from archived rows.");
  assert.equal(await readFile(archivePath, "utf8"), archiveText, "Archive changed during audit.");
  assert.equal(await readFile(planPath, "utf8"), planText, "Plan changed during audit.");
  return {
    status: state.checkpoint.pendingUrl === null ? "complete" : "paused",
    archivedRows: state.rows.length,
    publishedCount: state.publishedCount,
    archivedPages: state.checkpoint.completedUrls.length,
    databasePublished: false,
    error: null,
    archiveSha256: digest(archiveText),
    planSha256: digest(planText),
    verifiedRawPages: state.checkpoint.completedUrls.length,
    allArchivedRowsReparsed: true,
  };
};
