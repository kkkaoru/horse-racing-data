// This file runs with Bun. Archival completion is separate from database publication.
import { createHash } from "node:crypto";
import {
  collectHistory,
  type HistoryCollectionCheckpoint,
  type HistoryCollectionResult,
} from "./sources/history-collection";
import {
  parseHistorySourcePage,
  type HistorySourceKind,
  type HistorySourcePageProfile,
  type HistorySourceRow,
  type ParsedHistorySourcePage,
} from "./sources/history-source-page";

export interface HistoryArchivePlan {
  readonly kind: HistorySourceKind;
  readonly sourceId: string;
  readonly initialUrl: string;
  readonly encoding: string;
  readonly initialHtmlPath: string | null;
  readonly profile: HistorySourcePageProfile;
}

export interface HistoryArchiveState {
  readonly planDigest: string;
  readonly publishedCount: number | null;
  readonly checkpoint: HistoryCollectionCheckpoint;
  readonly rows: readonly HistorySourceRow[];
}

export interface HistoryArchivePorts {
  readonly readPage: (url: string) => Promise<string | null>;
  readonly fetchPage: (url: string) => Promise<string>;
  readonly archivePage: (url: string, html: string) => Promise<void>;
  readonly waitBeforeFetch: () => Promise<void>;
  readonly saveState: (state: HistoryArchiveState) => Promise<void>;
}

export interface HistoryArchiveInput {
  readonly plan: HistoryArchivePlan;
  readonly state: HistoryArchiveState | null;
  readonly pageBudget: number;
  readonly ports: HistoryArchivePorts;
}

export interface HistoryArchiveResult {
  readonly collection: HistoryCollectionResult;
  readonly state: HistoryArchiveState;
}

interface ArchiveProgress {
  state: HistoryArchiveState;
  pendingPopulation: number | null;
}

export const historyPlanDigest = (plan: HistoryArchivePlan): string =>
  createHash("sha256").update(JSON.stringify(plan)).digest("hex");

const initialState = (plan: HistoryArchivePlan): HistoryArchiveState => ({
  planDigest: historyPlanDigest(plan),
  publishedCount: null,
  checkpoint: { pendingUrl: plan.initialUrl, completedUrls: [], processedRows: 0 },
  rows: [],
});

export const verifyHistoryArchive = (
  state: HistoryArchiveState,
  plan: HistoryArchivePlan,
): void => {
  if (
    state.planDigest !== historyPlanDigest(plan) ||
    state.rows.length !== state.checkpoint.processedRows
  ) {
    throw new Error("History archive belongs to a different plan or has inconsistent progress.");
  }
  if (state.checkpoint.pendingUrl === null && state.rows.length !== state.publishedCount) {
    throw new Error("History archive completion does not match the published population.");
  }
};

export const collectHistoryArchive = async (
  input: HistoryArchiveInput,
): Promise<HistoryArchiveResult> => {
  const progress: ArchiveProgress = {
    state: input.state ?? initialState(input.plan),
    pendingPopulation: null,
  };
  verifyHistoryArchive(progress.state, input.plan);
  const collection: HistoryCollectionResult = await collectHistory({
    checkpoint: progress.state.checkpoint,
    allowedOrigin: new URL(input.plan.initialUrl).origin,
    pageBudget: input.pageBudget,
    ports: {
      readArchive: input.ports.readPage,
      fetchPage: input.ports.fetchPage,
      archivePage: input.ports.archivePage,
      waitBeforeFetch: input.ports.waitBeforeFetch,
      parsePage: (html: string) => {
        const url: string | null = progress.state.checkpoint.pendingUrl;
        if (url === null) throw new Error("History archive has no pending page.");
        const page: ParsedHistorySourcePage = parseHistorySourcePage({
          html,
          url,
          kind: input.plan.kind,
          sourceId: input.plan.sourceId,
          profile: input.plan.profile,
        });
        if (
          progress.state.publishedCount !== null &&
          progress.state.publishedCount !== page.publishedCount
        ) {
          throw new Error("Published history population changed; start a new snapshot.");
        }
        if (
          page.nextUrl === null &&
          progress.state.rows.length + page.rows.length !== page.publishedCount
        ) {
          throw new Error("History terminal row count differs from the published population.");
        }
        progress.pendingPopulation = page.publishedCount;
        return { rows: page.rows, nextUrl: page.nextUrl, terminal: page.nextUrl === null };
      },
      commitPage: async ({ rows, checkpoint }): Promise<void> => {
        const state: HistoryArchiveState = {
          planDigest: progress.state.planDigest,
          publishedCount: progress.pendingPopulation,
          rows: [...progress.state.rows, ...rows],
          checkpoint,
        };
        // Rows and checkpoint share a single atomic local archive file. This is
        // not a claim that the canonical database has received these records.
        await input.ports.saveState(state);
        progress.state = state;
      },
    },
  });
  return { collection, state: progress.state };
};
