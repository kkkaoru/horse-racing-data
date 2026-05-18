import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

type DebugLogLevel = "debug" | "error" | "info" | "warn";

interface AiPlaygroundDebugLog {
  at: string;
  details: Record<string, boolean | null | number | string> | null;
  event: string;
  level: DebugLogLevel;
  sequence: number;
}

interface AiPlaygroundClientSnapshot {
  chatStatus: string;
  error: string | null;
  lastAssistantTextLength: number;
  lastUserTextLength: number;
  mockMode: boolean;
  modelCacheStatus: string;
  modelPartialLength: number;
  modelStatus: string;
  statusMessage: string;
  supportState: string;
}

interface AiPlaygroundHeartbeat {
  at: string;
  client: AiPlaygroundClientSnapshot;
  reason: string;
  serverSequence: number;
}

interface AiPlaygroundDebugSnapshot {
  client: AiPlaygroundClientSnapshot;
  createdAt: string;
  heartbeats: AiPlaygroundHeartbeat[];
  logs: AiPlaygroundDebugLog[];
  route: string;
  serverSequence: number;
  sessionId: string;
  updatedAt: string;
  userAgent: string;
}

type AiPlaygroundDebugStore = Map<string, AiPlaygroundDebugSnapshot>;

declare global {
  var pcKeibaAiPlaygroundDebugStore: AiPlaygroundDebugStore | undefined;
}

const LOG_LIMIT = 500;
const HEARTBEAT_LIMIT = 120;

const emptyClientSnapshot = (): AiPlaygroundClientSnapshot => ({
  chatStatus: "",
  error: null,
  lastAssistantTextLength: 0,
  lastUserTextLength: 0,
  mockMode: false,
  modelCacheStatus: "",
  modelPartialLength: 0,
  modelStatus: "",
  statusMessage: "",
  supportState: "",
});

const getStore = (): AiPlaygroundDebugStore => {
  globalThis.pcKeibaAiPlaygroundDebugStore ??= new Map<string, AiPlaygroundDebugSnapshot>();
  return globalThis.pcKeibaAiPlaygroundDebugStore;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const toStringValue = (value: unknown): string | null =>
  typeof value === "string" && value.trim() ? value : null;

const toNumberValue = (value: unknown): number =>
  typeof value === "number" && Number.isFinite(value) ? value : 0;

const toBooleanValue = (value: unknown): boolean => value === true;

const parseLogDetails = (
  value: unknown,
): Record<string, boolean | null | number | string> | null => {
  if (!isRecord(value)) {
    return null;
  }
  const entries: Array<[string, boolean | null | number | string]> = [];
  for (const [key, item] of Object.entries(value)) {
    if (typeof item === "string" || typeof item === "number" || typeof item === "boolean") {
      entries.push([key, item]);
    } else if (item === null) {
      entries.push([key, null]);
    }
  }
  return entries.length > 0 ? Object.fromEntries(entries) : null;
};

const parseLog = (value: unknown): AiPlaygroundDebugLog | null => {
  if (!isRecord(value)) {
    return null;
  }
  const event = toStringValue(value.event);
  if (!event) {
    return null;
  }
  const level = value.level;
  return {
    at: toStringValue(value.at) ?? new Date().toISOString(),
    details: parseLogDetails(value.details),
    event,
    level:
      level === "debug" || level === "error" || level === "info" || level === "warn"
        ? level
        : "info",
    sequence: toNumberValue(value.sequence),
  };
};

const parseLogs = (value: unknown): AiPlaygroundDebugLog[] =>
  Array.isArray(value)
    ? value
        .map(parseLog)
        .filter((log): log is AiPlaygroundDebugLog => log !== null)
        .slice(-LOG_LIMIT)
    : [];

const parseClientSnapshot = (value: unknown): AiPlaygroundClientSnapshot => {
  if (!isRecord(value)) {
    return emptyClientSnapshot();
  }
  return {
    chatStatus: toStringValue(value.chatStatus) ?? "",
    error: toStringValue(value.error),
    lastAssistantTextLength: toNumberValue(value.lastAssistantTextLength),
    lastUserTextLength: toNumberValue(value.lastUserTextLength),
    mockMode: toBooleanValue(value.mockMode),
    modelCacheStatus: toStringValue(value.modelCacheStatus) ?? "",
    modelPartialLength: toNumberValue(value.modelPartialLength),
    modelStatus: toStringValue(value.modelStatus) ?? "",
    statusMessage: toStringValue(value.statusMessage) ?? "",
    supportState: toStringValue(value.supportState) ?? "",
  };
};

const summarizeSnapshot = (snapshot: AiPlaygroundDebugSnapshot) => ({
  client: snapshot.client,
  createdAt: snapshot.createdAt,
  lastLog: snapshot.logs.at(-1) ?? null,
  logCount: snapshot.logs.length,
  route: snapshot.route,
  serverSequence: snapshot.serverSequence,
  sessionId: snapshot.sessionId,
  updatedAt: snapshot.updatedAt,
  userAgent: snapshot.userAgent,
});

export async function GET(request: Request) {
  const url = new URL(request.url);
  const sessionId = url.searchParams.get("sessionId");
  const store = getStore();
  if (sessionId) {
    return NextResponse.json(store.get(sessionId) ?? null);
  }
  return NextResponse.json({
    sessions: Array.from(store.values())
      .toSorted((a, b) => b.updatedAt.localeCompare(a.updatedAt))
      .slice(0, 20)
      .map(summarizeSnapshot),
  });
}

export async function POST(request: Request) {
  const payload: unknown = await request.json();
  if (!isRecord(payload)) {
    return NextResponse.json({ error: "invalid payload" }, { status: 400 });
  }
  const sessionId = toStringValue(payload.sessionId);
  if (!sessionId) {
    return NextResponse.json({ error: "sessionId is required" }, { status: 400 });
  }
  if (payload.action !== "clear") {
    return NextResponse.json({ error: "invalid action" }, { status: 400 });
  }
  getStore().delete(sessionId);
  return NextResponse.json({ ok: true, sessionId });
}

export async function PUT(request: Request) {
  const payload: unknown = await request.json();
  if (!isRecord(payload)) {
    return NextResponse.json({ error: "invalid payload" }, { status: 400 });
  }
  const sessionId = toStringValue(payload.sessionId);
  if (!sessionId) {
    return NextResponse.json({ error: "sessionId is required" }, { status: 400 });
  }
  const store = getStore();
  const previous = store.get(sessionId) ?? null;
  const now = new Date().toISOString();
  const nextServerSequence = (previous?.serverSequence ?? 0) + 1;
  const client = parseClientSnapshot(payload.client);
  const logs = parseLogs(payload.logs);
  const mergedLogs = previous
    ? [...previous.logs, ...logs]
        .filter(
          (log, index, rows) =>
            rows.findIndex(
              (item) =>
                item.sequence === log.sequence && item.at === log.at && item.event === log.event,
            ) === index,
        )
        .slice(-LOG_LIMIT)
    : logs;
  const heartbeat = {
    at: now,
    client,
    reason: toStringValue(payload.reason) ?? "heartbeat",
    serverSequence: nextServerSequence,
  } satisfies AiPlaygroundHeartbeat;
  const snapshot = {
    client,
    createdAt: previous?.createdAt ?? now,
    heartbeats: [...(previous?.heartbeats ?? []), heartbeat].slice(-HEARTBEAT_LIMIT),
    logs: mergedLogs,
    route: toStringValue(payload.route) ?? previous?.route ?? "",
    serverSequence: nextServerSequence,
    sessionId,
    updatedAt: now,
    userAgent: toStringValue(payload.userAgent) ?? previous?.userAgent ?? "",
  } satisfies AiPlaygroundDebugSnapshot;
  store.set(sessionId, snapshot);
  console.info(
    "[ai-playground-debug]",
    JSON.stringify({
      client: {
        chatStatus: client.chatStatus,
        error: client.error,
        modelCacheStatus: client.modelCacheStatus,
        modelPartialLength: client.modelPartialLength,
        modelStatus: client.modelStatus,
        statusMessage: client.statusMessage,
        supportState: client.supportState,
      },
      heartbeatCount: snapshot.heartbeats.length,
      lastLog: snapshot.logs.at(-1) ?? null,
      logCount: snapshot.logs.length,
      reason: heartbeat.reason,
      route: snapshot.route,
      serverSequence: nextServerSequence,
      sessionId,
    }),
  );
  return NextResponse.json({
    ok: true,
    logCount: snapshot.logs.length,
    serverNow: now,
    serverSequence: nextServerSequence,
    sessionId,
  });
}
