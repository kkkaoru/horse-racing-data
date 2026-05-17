import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

type DebugAiChatRole = "assistant" | "user";
type DebugAiCommandType = "replace-messages" | "reset" | "send-message";

interface DebugAiChatMessage {
  content: string;
  createdAt: string;
  id: string;
  role: DebugAiChatRole;
}

interface DebugAiThoughtLog {
  content: string;
  createdAt: string;
  id: string;
  modelVersion: string;
  trigger: string;
}

interface DebugAiCommand {
  createdAt: string;
  id: string;
  messages?: DebugAiChatMessage[];
  text?: string;
  type: DebugAiCommandType;
}

interface DebugAiChatSnapshot {
  answer: string;
  chatMessages: DebugAiChatMessage[];
  command: DebugAiCommand | null;
  dataStatus: {
    availableSummary: string;
    dataReadiness: unknown;
    modelStatus: string;
    runtimeStatus: string;
  };
  error: string | null;
  messages: DebugAiChatMessage[];
  prediction: unknown[];
  raceKey: string;
  route: {
    day: string;
    keibajoCode: string;
    month: string;
    raceNumber: string;
    source: string;
    year: string;
  } | null;
  status: string;
  thoughtLogs: DebugAiThoughtLog[];
  updatedAt: string;
}

type DebugAiChatStore = Map<string, DebugAiChatSnapshot>;

declare global {
  var pcKeibaAiChatDebugStore: DebugAiChatStore | undefined;
}

const getStore = (): DebugAiChatStore => {
  globalThis.pcKeibaAiChatDebugStore ??= new Map<string, DebugAiChatSnapshot>();
  return globalThis.pcKeibaAiChatDebugStore;
};

const isLocalhostRequest = (request: Request): boolean => {
  const url = new URL(request.url);
  const host = request.headers.get("host") ?? url.host;
  const hostname = host.split(":")[0] ?? url.hostname;
  return ["localhost", "127.0.0.1", "0.0.0.0", "::1", "[::1]"].includes(hostname);
};

const forbidden = () => NextResponse.json({ error: "not_found" }, { status: 404 });

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const toStringValue = (value: unknown): string | null =>
  typeof value === "string" && value.trim() ? value : null;

const parseMessage = (value: unknown): DebugAiChatMessage | null => {
  if (!isRecord(value)) {
    return null;
  }
  const content = toStringValue(value.content);
  const id = toStringValue(value.id);
  const role = value.role === "assistant" || value.role === "user" ? value.role : null;
  if (!content || !id || !role) {
    return null;
  }
  return {
    content,
    createdAt: toStringValue(value.createdAt) ?? new Date().toISOString(),
    id,
    role,
  };
};

const parseMessages = (value: unknown): DebugAiChatMessage[] =>
  Array.isArray(value)
    ? value
        .map(parseMessage)
        .filter((message): message is DebugAiChatMessage => message !== null)
        .slice(-50)
    : [];

const parseThoughtLog = (value: unknown): DebugAiThoughtLog | null => {
  if (!isRecord(value)) {
    return null;
  }
  const content = toStringValue(value.content);
  const id = toStringValue(value.id);
  if (!content || !id) {
    return null;
  }
  return {
    content,
    createdAt: toStringValue(value.createdAt) ?? new Date().toISOString(),
    id,
    modelVersion: toStringValue(value.modelVersion) ?? "-",
    trigger: toStringValue(value.trigger) ?? "-",
  };
};

const parseThoughtLogs = (value: unknown): DebugAiThoughtLog[] =>
  Array.isArray(value)
    ? value
        .map(parseThoughtLog)
        .filter((log): log is DebugAiThoughtLog => log !== null)
        .slice(-50)
    : [];

const parseSnapshot = (value: unknown, previous: DebugAiChatSnapshot | null) => {
  if (!isRecord(value)) {
    return null;
  }
  const raceKey = toStringValue(value.raceKey);
  if (!raceKey) {
    return null;
  }
  return {
    answer: toStringValue(value.answer) ?? "",
    chatMessages: parseMessages(value.chatMessages),
    command: previous?.command ?? null,
    dataStatus: isRecord(value.dataStatus)
      ? {
          availableSummary: toStringValue(value.dataStatus.availableSummary) ?? "",
          dataReadiness: value.dataStatus.dataReadiness ?? null,
          modelStatus: toStringValue(value.dataStatus.modelStatus) ?? "",
          runtimeStatus: toStringValue(value.dataStatus.runtimeStatus) ?? "",
        }
      : {
          availableSummary: "",
          dataReadiness: null,
          modelStatus: "",
          runtimeStatus: "",
        },
    error: toStringValue(value.error),
    messages: parseMessages(value.messages),
    prediction: Array.isArray(value.prediction) ? value.prediction.slice(0, 30) : [],
    raceKey,
    route: isRecord(value.route)
      ? {
          day: toStringValue(value.route.day) ?? "",
          keibajoCode: toStringValue(value.route.keibajoCode) ?? "",
          month: toStringValue(value.route.month) ?? "",
          raceNumber: toStringValue(value.route.raceNumber) ?? "",
          source: toStringValue(value.route.source) ?? "",
          year: toStringValue(value.route.year) ?? "",
        }
      : null,
    status: toStringValue(value.status) ?? "idle",
    thoughtLogs: parseThoughtLogs(value.thoughtLogs),
    updatedAt: new Date().toISOString(),
  } satisfies DebugAiChatSnapshot;
};

const createCommand = (value: unknown): DebugAiCommand | null => {
  if (!isRecord(value)) {
    return null;
  }
  const type = value.type;
  if (type !== "replace-messages" && type !== "reset" && type !== "send-message") {
    return null;
  }
  return {
    createdAt: new Date().toISOString(),
    id: crypto.randomUUID(),
    messages: type === "replace-messages" ? parseMessages(value.messages) : undefined,
    text: type === "send-message" ? (toStringValue(value.text) ?? "") : undefined,
    type,
  };
};

export async function GET(request: Request) {
  if (!isLocalhostRequest(request)) {
    return forbidden();
  }
  const url = new URL(request.url);
  const raceKey = url.searchParams.get("raceKey");
  const store = getStore();
  if (raceKey) {
    return NextResponse.json(store.get(raceKey) ?? null);
  }
  return NextResponse.json({
    races: Array.from(store.values()).toSorted((a, b) => b.updatedAt.localeCompare(a.updatedAt)),
  });
}

export async function PUT(request: Request) {
  if (!isLocalhostRequest(request)) {
    return forbidden();
  }
  const payload: unknown = await request.json();
  const store = getStore();
  const raceKey = isRecord(payload) ? toStringValue(payload.raceKey) : null;
  if (!raceKey) {
    return NextResponse.json({ error: "raceKey is required" }, { status: 400 });
  }
  const previous = store.get(raceKey) ?? null;
  const ackCommandId = isRecord(payload) ? toStringValue(payload.ackCommandId) : null;
  if (ackCommandId && previous) {
    const snapshot = {
      ...previous,
      command: previous.command?.id === ackCommandId ? null : previous.command,
      updatedAt: new Date().toISOString(),
    } satisfies DebugAiChatSnapshot;
    store.set(raceKey, snapshot);
    return NextResponse.json(snapshot);
  }
  const snapshot = parseSnapshot(payload, previous);
  if (!snapshot) {
    return NextResponse.json({ error: "invalid snapshot" }, { status: 400 });
  }
  store.set(raceKey, snapshot);
  return NextResponse.json(snapshot);
}

export async function POST(request: Request) {
  if (!isLocalhostRequest(request)) {
    return forbidden();
  }
  const payload: unknown = await request.json();
  if (!isRecord(payload)) {
    return NextResponse.json({ error: "invalid payload" }, { status: 400 });
  }
  const raceKey = toStringValue(payload.raceKey);
  if (!raceKey) {
    return NextResponse.json({ error: "raceKey is required" }, { status: 400 });
  }
  const store = getStore();
  const previous = store.get(raceKey) ?? null;
  if (payload.action === "clear") {
    store.delete(raceKey);
    return NextResponse.json({ ok: true });
  }
  const command = createCommand(payload.command ?? payload);
  if (!command) {
    return NextResponse.json({ error: "invalid command" }, { status: 400 });
  }
  const now = new Date().toISOString();
  const snapshot =
    previous ??
    ({
      answer: "",
      chatMessages: [],
      command: null,
      dataStatus: {
        availableSummary: "",
        dataReadiness: null,
        modelStatus: "",
        runtimeStatus: "",
      },
      error: null,
      messages: [],
      prediction: [],
      raceKey,
      route: null,
      status: "idle",
      thoughtLogs: [],
      updatedAt: now,
    } satisfies DebugAiChatSnapshot);
  const nextSnapshot = { ...snapshot, command, updatedAt: now };
  store.set(raceKey, nextSnapshot);
  return NextResponse.json(nextSnapshot);
}
