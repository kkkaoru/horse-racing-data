// This file runs with Bun. Source-specific profiles and archives stay private.
import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import iconv from "iconv-lite";
import {
  collectHistoryArchive,
  historyPlanDigest,
  verifyHistoryArchive,
  type HistoryArchivePlan,
  type HistoryArchiveResult,
  type HistoryArchiveState,
} from "./history-archive";
import { decodeHistoryArchive, decodeHistoryPlan, isHistoryRecord } from "./history-json";

export interface HistoryCliRuntime {
  readonly read: (path: string) => Promise<string | null>;
  readonly writeAtomic: (path: string, content: string) => Promise<void>;
  readonly fetchPage: (input: HistoryFetchInput) => Promise<string>;
  readonly wait: () => Promise<void>;
}

export interface HistoryFetchInput {
  readonly url: string;
  readonly encoding: string;
  readonly rawPath: string;
}

export interface HistoryCliInput {
  readonly argv: readonly string[];
  readonly runtime: HistoryCliRuntime;
}

export interface HistoryCliReport {
  readonly status: "complete" | "paused" | "blocked";
  readonly archivedRows: number;
  readonly publishedCount: number | null;
  readonly archivedPages: number;
  readonly databasePublished: false;
  readonly error: string | null;
}

interface CollectInput {
  readonly directory: string;
  readonly planPath: string;
  readonly pageBudget: number;
  readonly runtime: HistoryCliRuntime;
}

const PLAN_FILE: string = "plan.json";
const STATE_FILE: string = "archive.json";
const FETCH_TIMEOUT_MS: number = 30000;
const REQUEST_INTERVAL_MS: number = 1500;
const OWNER_DIRECTORY_MODE: number = 0o700;
const OWNER_FILE_MODE: number = 0o600;
const digest = (text: string): string => createHash("sha256").update(text).digest("hex");

const requiredFile = async (runtime: HistoryCliRuntime, path: string): Promise<string> => {
  const text: string | null = await runtime.read(path);
  if (text === null) throw new Error("Required private history file is missing.");
  return text;
};

const report = (result: HistoryArchiveResult): HistoryCliReport => ({
  status: result.collection.status,
  archivedRows: result.state.rows.length,
  publishedCount: result.state.publishedCount,
  archivedPages: result.state.checkpoint.completedUrls.length,
  databasePublished: false,
  error: result.collection.error,
});

const collect = async ({
  directory,
  planPath,
  pageBudget,
  runtime,
}: CollectInput): Promise<HistoryCliReport> => {
  const plan: HistoryArchivePlan = decodeHistoryPlan(await requiredFile(runtime, planPath));
  const frozen: string | null = await runtime.read(join(directory, PLAN_FILE));
  if (frozen !== null && historyPlanDigest(decodeHistoryPlan(frozen)) !== historyPlanDigest(plan)) {
    throw new Error("History source plan changed; use a new snapshot directory.");
  }
  if (frozen === null) await runtime.writeAtomic(join(directory, PLAN_FILE), JSON.stringify(plan));
  const saved: string | null = await runtime.read(join(directory, STATE_FILE));
  const state: HistoryArchiveState | null = saved === null ? null : decodeHistoryArchive(saved);
  const pagePath = (url: string): string => join(directory, "pages", digest(url));
  return report(
    await collectHistoryArchive({
      plan,
      state,
      pageBudget,
      ports: {
        readPage: async (url: string): Promise<string | null> => {
          if (url === plan.initialUrl && plan.initialHtmlPath !== null) {
            const rendered: string | null = await runtime.read(plan.initialHtmlPath);
            if (rendered !== null) return rendered;
          }
          return runtime.read(`${pagePath(url)}.html`);
        },
        fetchPage: (url: string): Promise<string> =>
          runtime.fetchPage({ url, encoding: plan.encoding, rawPath: `${pagePath(url)}.raw` }),
        archivePage: (url: string, html: string): Promise<void> =>
          runtime.writeAtomic(`${pagePath(url)}.html`, html),
        waitBeforeFetch: runtime.wait,
        saveState: (value: HistoryArchiveState): Promise<void> =>
          runtime.writeAtomic(join(directory, STATE_FILE), JSON.stringify(value)),
      },
    }),
  );
};

export const runHistoryCli = async ({
  argv,
  runtime,
}: HistoryCliInput): Promise<HistoryCliReport> => {
  const [action, first, second, budget]: readonly (string | undefined)[] = argv;
  if (
    action === "collect" &&
    argv.length === 4 &&
    first &&
    second &&
    budget &&
    /^\d+$/u.test(budget)
  ) {
    return collect({ directory: second, planPath: first, pageBudget: Number(budget), runtime });
  }
  if (action === "status" && argv.length === 2 && first) {
    const plan: HistoryArchivePlan = decodeHistoryPlan(
      await requiredFile(runtime, join(first, PLAN_FILE)),
    );
    const state: HistoryArchiveState = decodeHistoryArchive(
      await requiredFile(runtime, join(first, STATE_FILE)),
    );
    verifyHistoryArchive(state, plan);
    return {
      status: state.checkpoint.pendingUrl === null ? "complete" : "paused",
      archivedRows: state.rows.length,
      publishedCount: state.publishedCount,
      archivedPages: state.checkpoint.completedUrls.length,
      databasePublished: false,
      error: null,
    };
  }
  throw new Error("Use collect PLAN_JSON DIRECTORY PAGE_BUDGET or status DIRECTORY.");
};

const readPrivate = async (path: string): Promise<string | null> => {
  try {
    return await readFile(path, "utf8");
  } catch (error: unknown) {
    if (isHistoryRecord(error) && error.code === "ENOENT") return null;
    throw error;
  }
};

const writePrivateAtomic = async (path: string, content: string | Uint8Array): Promise<void> => {
  await mkdir(dirname(path), { recursive: true, mode: OWNER_DIRECTORY_MODE });
  const temporary: string = `${path}.${randomUUID()}.pending`;
  await writeFile(temporary, content, { mode: OWNER_FILE_MODE, flag: "wx" });
  await rename(temporary, path);
};

const fetchHistoryPage = async ({ url, encoding, rawPath }: HistoryFetchInput): Promise<string> => {
  if (!iconv.encodingExists(encoding)) throw new Error("History source encoding is unsupported.");
  const response: Response = await fetch(url, {
    redirect: "manual",
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
  });
  if (!response.ok)
    throw new Error(
      "History source request failed; no redirect or access-control bypass is allowed.",
    );
  const bytes: Uint8Array = new Uint8Array(await response.arrayBuffer());
  await writePrivateAtomic(rawPath, bytes);
  await writePrivateAtomic(
    `${rawPath}.meta.json`,
    JSON.stringify({
      url,
      fetchedAt: new Date().toISOString(),
      status: response.status,
      contentType: response.headers.get("content-type"),
      encoding,
    }),
  );
  return iconv.decode(Buffer.from(bytes), encoding);
};

export const createHistoryCliRuntime = (): HistoryCliRuntime => ({
  read: readPrivate,
  writeAtomic: writePrivateAtomic,
  fetchPage: fetchHistoryPage,
  wait: async (): Promise<void> => {
    await new Promise<void>((resolve) => setTimeout(resolve, REQUEST_INTERVAL_MS));
  },
});
