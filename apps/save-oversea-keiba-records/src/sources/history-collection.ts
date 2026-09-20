// This file runs with Bun.
// Source markup stays in the caller's private parser, never in this workflow.

export interface HistoryPage<Row> {
  readonly rows: readonly Row[];
  readonly nextUrl: string | null;
  readonly terminal: boolean;
}

export interface HistoryCollectionCheckpoint {
  readonly pendingUrl: string | null;
  readonly completedUrls: readonly string[];
  readonly processedRows: number;
}

export interface HistoryCollectionPorts<Row> {
  readonly readArchive: (url: string) => Promise<string | null>;
  readonly fetchPage: (url: string) => Promise<string>;
  readonly archivePage: (url: string, html: string) => Promise<void>;
  readonly parsePage: (html: string) => HistoryPage<Row>;
  // The row write and checkpoint must commit together, with idempotent row keys.
  readonly commitPage: (input: HistoryPageCommit<Row>) => Promise<void>;
  readonly waitBeforeFetch: () => Promise<void>;
}

export interface HistoryPageCommit<Row> {
  readonly url: string;
  readonly rows: readonly Row[];
  readonly checkpoint: HistoryCollectionCheckpoint;
}

export interface HistoryCollectionInput<Row> {
  readonly checkpoint: HistoryCollectionCheckpoint;
  readonly allowedOrigin: string;
  readonly pageBudget: number;
  readonly ports: HistoryCollectionPorts<Row>;
}

export interface HistoryCollectionResult {
  readonly status: "complete" | "paused" | "blocked";
  readonly checkpoint: HistoryCollectionCheckpoint;
  readonly error: string | null;
}

const validUrl = (raw: string, origin: string): boolean => {
  const url: URL = new URL(raw);
  return (
    url.protocol === "https:" &&
    url.origin === origin &&
    url.username === "" &&
    url.password === "" &&
    url.hash === ""
  );
};

const loadPage = async <Row>(input: HistoryCollectionInput<Row>, url: string): Promise<string> => {
  const cached: string | null = await input.ports.readArchive(url);
  if (cached !== null) return cached;
  await input.ports.waitBeforeFetch();
  const html: string = await input.ports.fetchPage(url);
  await input.ports.archivePage(url, html);
  return html;
};

const advancePage = async <Row>(
  input: HistoryCollectionInput<Row>,
  url: string,
): Promise<HistoryCollectionCheckpoint> => {
  if (!validUrl(url, input.allowedOrigin) || input.checkpoint.completedUrls.includes(url)) {
    throw new Error("History page URL is invalid or pagination repeats a completed page.");
  }
  const html: string = await loadPage(input, url);
  const page: HistoryPage<Row> = input.ports.parsePage(html);
  if (page.terminal !== (page.nextUrl === null) || (!page.terminal && page.rows.length === 0)) {
    throw new Error("History page has no verified terminal state or contains empty pagination.");
  }
  const visited: readonly string[] = [...input.checkpoint.completedUrls, url];
  if (
    page.nextUrl !== null &&
    (!validUrl(page.nextUrl, input.allowedOrigin) || visited.includes(page.nextUrl))
  ) {
    throw new Error("History next-page URL is invalid or repeats a completed page.");
  }
  const checkpoint: HistoryCollectionCheckpoint = {
    pendingUrl: page.nextUrl,
    completedUrls: visited,
    processedRows: input.checkpoint.processedRows + page.rows.length,
  };
  await input.ports.commitPage({ url, rows: page.rows, checkpoint });
  return checkpoint;
};

export const collectHistory = async <Row>(
  input: HistoryCollectionInput<Row>,
): Promise<HistoryCollectionResult> => {
  if (!Number.isSafeInteger(input.pageBudget) || input.pageBudget < 0) {
    throw new Error("History page budget must be a non-negative integer.");
  }
  if (input.checkpoint.pendingUrl === null) {
    if (input.checkpoint.completedUrls.length === 0) {
      throw new Error("History completion requires a committed terminal page.");
    }
    return { status: "complete", checkpoint: input.checkpoint, error: null };
  }
  if (input.pageBudget === 0) {
    return { status: "paused", checkpoint: input.checkpoint, error: null };
  }
  try {
    const checkpoint: HistoryCollectionCheckpoint = await advancePage(
      input,
      input.checkpoint.pendingUrl,
    );
    return collectHistory({ ...input, checkpoint, pageBudget: input.pageBudget - 1 });
  } catch {
    // Do not leak private URLs, source markup or connection details from ports.
    return {
      status: "blocked",
      checkpoint: input.checkpoint,
      error:
        "History page could not be archived, validated or committed; resume from the pending page after inspection.",
    };
  }
};
