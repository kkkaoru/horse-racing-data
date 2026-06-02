// Run with bun (Next.js route). One-shot admin endpoint that backfills the
// PaddockState history entry `userId` for entries created before the field was
// introduced. Score-type history entries dated 2026-06-02 receive
// `legacyIdFor602`, entries dated before that day receive `legacyIdForBefore`,
// and entries after that day are left untouched. Authenticated via the
// `x-pc-keiba-internal-token` header, matching the existing internal token
// scheme used by `api/internal/trend-cache-bust`.
import { NextResponse } from "next/server";

import { safeGetCloudflareEnv } from "../../../../lib/cloudflare-context.server";
import {
  isPaddockState,
  type PaddockHistoryEntry,
  type PaddockState,
} from "../../../../lib/paddock";

export const dynamic = "force-dynamic";

const AUTH_HEADER = "x-pc-keiba-internal-token";
const KV_PREFIX = "paddock:";
const KV_LIST_LIMIT = 1000;
const KV_TTL_SECONDS = 30 * 24 * 60 * 60;
const CUTOFF_DATE = "2026-06-02";
const ISO_DATE_LENGTH = 10;

interface BackfillRequestBody {
  legacyIdFor602: string;
  legacyIdForBefore: string;
}

interface BackfillCounts {
  entriesUpdated: number;
  entriesUpdatedFor602: number;
  entriesUpdatedForBefore: number;
  keysScanned: number;
  keysUpdated: number;
}

interface EntryClassification {
  entry: PaddockHistoryEntry;
  mutated: boolean;
  updatedFor602: boolean;
  updatedForBefore: boolean;
}

interface StateCounts {
  entriesUpdated: number;
  entriesUpdatedFor602: number;
  entriesUpdatedForBefore: number;
}

interface StateClassification {
  counts: StateCounts;
  history: PaddockHistoryEntry[];
  mutated: boolean;
}

const EMPTY_COUNTS: BackfillCounts = {
  entriesUpdated: 0,
  entriesUpdatedFor602: 0,
  entriesUpdatedForBefore: 0,
  keysScanned: 0,
  keysUpdated: 0,
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isAuthorized = (request: Request): boolean => {
  const expected = process.env.PC_KEIBA_INTERNAL_TOKEN;
  if (!expected) return false;
  return request.headers.get(AUTH_HEADER) === expected;
};

const parseBody = (raw: unknown): BackfillRequestBody | null => {
  if (!isRecord(raw)) return null;
  if (typeof raw.legacyIdFor602 !== "string" || raw.legacyIdFor602.length === 0) return null;
  if (typeof raw.legacyIdForBefore !== "string" || raw.legacyIdForBefore.length === 0) return null;
  return { legacyIdFor602: raw.legacyIdFor602, legacyIdForBefore: raw.legacyIdForBefore };
};

const getEntryDate = (entry: PaddockHistoryEntry): string => entry.at.slice(0, ISO_DATE_LENGTH);

const isBackfillTarget = (entry: PaddockHistoryEntry): boolean =>
  entry.type === "score" && entry.userId === undefined;

const classifyEntry = (
  entry: PaddockHistoryEntry,
  body: BackfillRequestBody,
): EntryClassification => {
  if (!isBackfillTarget(entry)) {
    return { entry, mutated: false, updatedFor602: false, updatedForBefore: false };
  }
  const date = getEntryDate(entry);
  if (date === CUTOFF_DATE) {
    return {
      entry: { ...entry, userId: body.legacyIdFor602 },
      mutated: true,
      updatedFor602: true,
      updatedForBefore: false,
    };
  }
  if (date < CUTOFF_DATE) {
    return {
      entry: { ...entry, userId: body.legacyIdForBefore },
      mutated: true,
      updatedFor602: false,
      updatedForBefore: true,
    };
  }
  return { entry, mutated: false, updatedFor602: false, updatedForBefore: false };
};

const classifyState = (state: PaddockState, body: BackfillRequestBody): StateClassification => {
  const classified = state.history.map((entry) => classifyEntry(entry, body));
  const history = classified.map((item) => item.entry);
  const entriesUpdated = classified.filter((item) => item.mutated).length;
  const entriesUpdatedFor602 = classified.filter((item) => item.updatedFor602).length;
  const entriesUpdatedForBefore = classified.filter((item) => item.updatedForBefore).length;
  return {
    counts: { entriesUpdated, entriesUpdatedFor602, entriesUpdatedForBefore },
    history,
    mutated: entriesUpdated > 0,
  };
};

const listAllPaddockKeys = async (kv: PcKeibaKvNamespace): Promise<string[]> => {
  const collected: string[] = [];
  const seed = await kv.list({ limit: KV_LIST_LIMIT, prefix: KV_PREFIX });
  collected.push(...seed.keys.map((key) => key.name));
  const drainPage = async (cursor: string | undefined): Promise<void> => {
    if (!cursor) return;
    const next = await kv.list({ cursor, limit: KV_LIST_LIMIT, prefix: KV_PREFIX });
    collected.push(...next.keys.map((key) => key.name));
    if (!next.list_complete) {
      await drainPage(next.cursor);
    }
  };
  if (!seed.list_complete) {
    await drainPage(seed.cursor);
  }
  return collected;
};

interface ProcessKeyResult {
  classification: StateClassification;
  state: PaddockState;
}

const processKey = async (
  kv: PcKeibaKvNamespace,
  key: string,
  body: BackfillRequestBody,
): Promise<ProcessKeyResult | null> => {
  const state = await kv.get(key, { type: "json" });
  if (!isPaddockState(state)) return null;
  return { classification: classifyState(state, body), state };
};

const writeBackfilledState = async (
  kv: PcKeibaKvNamespace,
  key: string,
  state: PaddockState,
  history: PaddockHistoryEntry[],
  now: string,
): Promise<void> => {
  const nextState: PaddockState = {
    history,
    horses: state.horses,
    raceKey: state.raceKey,
    updatedAt: now,
  };
  await kv.put(key, JSON.stringify(nextState), { expirationTtl: KV_TTL_SECONDS });
};

interface BackfillRunInput {
  body: BackfillRequestBody;
  commit: boolean;
  kv: PcKeibaKvNamespace;
  now: string;
}

interface ProcessedItem {
  key: string;
  result: ProcessKeyResult | null;
}

const sumCounts = (items: ReadonlyArray<ProcessedItem>, keysScanned: number): BackfillCounts =>
  items.reduce<BackfillCounts>(
    (acc, item) => {
      if (!item.result) return acc;
      const { counts, mutated } = item.result.classification;
      return {
        entriesUpdated: acc.entriesUpdated + counts.entriesUpdated,
        entriesUpdatedFor602: acc.entriesUpdatedFor602 + counts.entriesUpdatedFor602,
        entriesUpdatedForBefore: acc.entriesUpdatedForBefore + counts.entriesUpdatedForBefore,
        keysScanned: acc.keysScanned,
        keysUpdated: acc.keysUpdated + (mutated ? 1 : 0),
      };
    },
    { ...EMPTY_COUNTS, keysScanned },
  );

const writeMutatedItems = async (
  kv: PcKeibaKvNamespace,
  items: ReadonlyArray<ProcessedItem>,
  now: string,
): Promise<void> => {
  const targets = items.filter(
    (item): item is { key: string; result: ProcessKeyResult } =>
      item.result !== null && item.result.classification.mutated,
  );
  await Promise.all(
    targets.map((item) =>
      writeBackfilledState(
        kv,
        item.key,
        item.result.state,
        item.result.classification.history,
        now,
      ),
    ),
  );
};

const runBackfill = async (input: BackfillRunInput): Promise<BackfillCounts> => {
  const keys = await listAllPaddockKeys(input.kv);
  const results = await Promise.all(keys.map((key) => processKey(input.kv, key, input.body)));
  const items: ProcessedItem[] = keys.map((key, index) => ({
    key,
    result: results[index] ?? null,
  }));
  if (input.commit) {
    await writeMutatedItems(input.kv, items, input.now);
  }
  return sumCounts(items, keys.length);
};

const getKv = async (): Promise<PcKeibaKvNamespace | null> => {
  const env = await safeGetCloudflareEnv();
  return env?.PADDOCK_STATE_KV ?? null;
};

const buildResponse = (counts: BackfillCounts, commit: boolean): Response =>
  NextResponse.json({ commit, counts, ok: true });

const forbiddenResponse = (): Response =>
  NextResponse.json({ error: "forbidden" }, { status: 403 });

const kvUnavailableResponse = (): Response =>
  NextResponse.json({ error: "kv_unavailable" }, { status: 503 });

const invalidBodyResponse = (): Response =>
  NextResponse.json({ error: "invalid_body" }, { status: 400 });

const DRY_RUN_BODY: BackfillRequestBody = {
  legacyIdFor602: "DRY_RUN_602",
  legacyIdForBefore: "DRY_RUN_BEFORE",
};

export async function GET(request: Request): Promise<Response> {
  if (!isAuthorized(request)) return forbiddenResponse();
  const kv = await getKv();
  if (!kv) return kvUnavailableResponse();
  const counts = await runBackfill({
    body: DRY_RUN_BODY,
    commit: false,
    kv,
    now: new Date().toISOString(),
  });
  return buildResponse(counts, false);
}

export async function POST(request: Request): Promise<Response> {
  if (!isAuthorized(request)) return forbiddenResponse();
  const rawBody = (await request.json().catch(() => null)) as unknown;
  const body = parseBody(rawBody);
  if (!body) return invalidBodyResponse();
  const kv = await getKv();
  if (!kv) return kvUnavailableResponse();
  const counts = await runBackfill({
    body,
    commit: true,
    kv,
    now: new Date().toISOString(),
  });
  return buildResponse(counts, true);
}
