// Run with: imported from evaluate-bucket-21y.ts (bun runtime)
// Stdin/stdout JSONL RPC client. Wraps a Python child process (Bun.spawn or
// node:child_process) and exposes BucketQueryRunner-compatible query()/exec()
// plus close() so the driver can route bucket aggregate / upsert SQL into the
// same PG session that loaded the temp table.

import type {
  RpcClosedResponse,
  RpcErrorResponse,
  RpcOkResponse,
  RpcReadyResponse,
  RpcResponse,
  RpcRowsResponse,
  RpcSqlRequest,
} from "./bucket-eval-rpc-protocol";
import {
  RPC_REQUEST_TYPE_EXIT,
  RPC_REQUEST_TYPE_SQL,
  RPC_RESPONSE_TYPE_CLOSED,
  RPC_RESPONSE_TYPE_ERROR,
  RPC_RESPONSE_TYPE_OK,
  RPC_RESPONSE_TYPE_READY,
  RPC_RESPONSE_TYPE_ROWS,
} from "./bucket-eval-rpc-protocol";

export interface BucketRpcReadable {
  on: (event: "data", listener: (chunk: string | Uint8Array) => void) => void;
  setEncoding?: (encoding: "utf8") => void;
}

export interface BucketRpcWritable {
  write: (chunk: string) => boolean | void;
  end?: () => void;
}

export interface BucketEvalRpcChildLike {
  stdin: BucketRpcWritable;
  stdout: BucketRpcReadable;
}

export interface BucketRpcQueryResult<Row> {
  rows: Row[];
}

export interface BucketEvalRpcClient {
  ready: Promise<RpcReadyResponse>;
  query: <Row>(sql: string, params?: unknown[]) => Promise<BucketRpcQueryResult<Row>>;
  exec: (sql: string, params?: unknown[]) => Promise<number>;
  close: () => Promise<void>;
}

export interface CreateBucketEvalRpcClientArgs {
  child: BucketEvalRpcChildLike;
  generateId?: () => string;
}

interface RowsChunk {
  seq: number;
  rows: unknown[];
}

interface PendingQuery {
  resolveRows: (rows: unknown[]) => void;
  resolveOk: (rowcount: number) => void;
  reject: (error: Error) => void;
  chunks: RowsChunk[];
  expectedSeq: number;
}

const NEWLINE: string = "\n";

const decodeChunk = (chunk: string | Uint8Array): string =>
  typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk);

const buildRandomId = (): string =>
  `rpc-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;

const sortRowsChunks = (a: RowsChunk, b: RowsChunk): number => a.seq - b.seq;

const flattenChunks = (chunks: RowsChunk[]): unknown[] =>
  chunks.toSorted(sortRowsChunks).flatMap((chunk) => chunk.rows);

const isRowsResponse = (response: RpcResponse): response is RpcRowsResponse =>
  response.type === RPC_RESPONSE_TYPE_ROWS;

const isOkResponse = (response: RpcResponse): response is RpcOkResponse =>
  response.type === RPC_RESPONSE_TYPE_OK;

const isErrorResponse = (response: RpcResponse): response is RpcErrorResponse =>
  response.type === RPC_RESPONSE_TYPE_ERROR;

const isReadyResponse = (response: RpcResponse): response is RpcReadyResponse =>
  response.type === RPC_RESPONSE_TYPE_READY;

const isClosedResponse = (response: RpcResponse): response is RpcClosedResponse =>
  response.type === RPC_RESPONSE_TYPE_CLOSED;

const buildReadyFromRecord = (record: Record<string, unknown>): RpcReadyResponse => ({
  type: "ready",
  loadedRows: typeof record["loadedRows"] === "number" ? record["loadedRows"] : 0,
});

const buildClosedFromRecord = (): RpcClosedResponse => ({ type: "closed" });

const buildRowsFromRecord = (record: Record<string, unknown>): RpcRowsResponse => ({
  type: "rows",
  id: typeof record["id"] === "string" ? record["id"] : "",
  seq: typeof record["seq"] === "number" ? record["seq"] : 0,
  rows: Array.isArray(record["rows"]) ? record["rows"] : [],
  done: record["done"] === true,
});

const buildOkFromRecord = (record: Record<string, unknown>): RpcOkResponse => ({
  type: "ok",
  id: typeof record["id"] === "string" ? record["id"] : "",
  rowcount: typeof record["rowcount"] === "number" ? record["rowcount"] : 0,
});

const buildErrorFromRecord = (record: Record<string, unknown>): RpcErrorResponse => ({
  type: "error",
  id: typeof record["id"] === "string" ? record["id"] : null,
  message: typeof record["message"] === "string" ? record["message"] : "",
  details: typeof record["details"] === "string" ? record["details"] : "",
});

const buildResponseFromRecord = (record: Record<string, unknown>): RpcResponse => {
  const type = record["type"];
  if (type === RPC_RESPONSE_TYPE_READY) return buildReadyFromRecord(record);
  if (type === RPC_RESPONSE_TYPE_CLOSED) return buildClosedFromRecord();
  if (type === RPC_RESPONSE_TYPE_ROWS) return buildRowsFromRecord(record);
  if (type === RPC_RESPONSE_TYPE_OK) return buildOkFromRecord(record);
  if (type === RPC_RESPONSE_TYPE_ERROR) return buildErrorFromRecord(record);
  throw new Error(`Unknown RPC response type: ${String(type)}`);
};

const toRecord = (value: unknown): Record<string, unknown> | null => {
  if (value === null || typeof value !== "object") return null;
  const record: Record<string, unknown> = {};
  Object.assign(record, value);
  return record;
};

const parseLine = (line: string): RpcResponse | null => {
  if (line === "") return null;
  const parsed: unknown = JSON.parse(line);
  const record = toRecord(parsed);
  if (record === null) {
    throw new Error(`RPC response is not a JSON object: ${line}`);
  }
  return buildResponseFromRecord(record);
};

interface BufferState {
  buffer: string;
}

const splitBuffered = (state: BufferState, incoming: string): string[] => {
  state.buffer = state.buffer + incoming;
  const lines = state.buffer.split(NEWLINE);
  state.buffer = lines.pop() ?? "";
  return lines;
};

interface RouteResponseContext {
  pending: Map<string, PendingQuery>;
  readyResolve: (response: RpcReadyResponse) => void;
  readyReject: (error: Error) => void;
  closedResolve: () => void;
  pendingErrorHandler: (error: Error) => void;
}

const handleRowsResponse = (context: RouteResponseContext, response: RpcRowsResponse): void => {
  const entry = context.pending.get(response.id);
  if (entry === undefined) return;
  entry.chunks.push({ seq: response.seq, rows: response.rows });
  entry.expectedSeq += 1;
  if (response.done) {
    context.pending.delete(response.id);
    entry.resolveRows(flattenChunks(entry.chunks));
  }
};

const handleOkResponse = (context: RouteResponseContext, response: RpcOkResponse): void => {
  const entry = context.pending.get(response.id);
  if (entry === undefined) return;
  context.pending.delete(response.id);
  entry.resolveOk(response.rowcount);
};

const handleErrorResponse = (context: RouteResponseContext, response: RpcErrorResponse): void => {
  if (response.id === null) {
    context.pendingErrorHandler(new Error(`RPC server error: ${response.message}`));
    return;
  }
  const entry = context.pending.get(response.id);
  if (entry === undefined) return;
  context.pending.delete(response.id);
  entry.reject(new Error(`RPC error: ${response.message}`));
};

const routeResponse = (context: RouteResponseContext, response: RpcResponse): void => {
  if (isReadyResponse(response)) {
    context.readyResolve(response);
    return;
  }
  if (isClosedResponse(response)) {
    context.closedResolve();
    return;
  }
  if (isRowsResponse(response)) {
    handleRowsResponse(context, response);
    return;
  }
  if (isOkResponse(response)) {
    handleOkResponse(context, response);
    return;
  }
  if (isErrorResponse(response)) {
    handleErrorResponse(context, response);
  }
};

interface SendSqlArgs {
  child: BucketEvalRpcChildLike;
  pending: Map<string, PendingQuery>;
  id: string;
  sql: string;
  params: unknown[];
}

const registerPendingEntry = (args: SendSqlArgs, entry: PendingQuery): void => {
  args.pending.set(args.id, entry);
  const request: RpcSqlRequest = {
    type: RPC_REQUEST_TYPE_SQL,
    id: args.id,
    query: args.sql,
    params: args.params,
  };
  args.child.stdin.write(`${JSON.stringify(request)}${NEWLINE}`);
};

function isRowArray<Row>(rows: unknown[]): rows is Row[] {
  void rows;
  return true;
}

function coerceRowsForCaller<Row>(rows: unknown[]): Row[] {
  if (isRowArray<Row>(rows)) return rows;
  return [];
}

const queryViaRpc = <Row>(args: SendSqlArgs): Promise<BucketRpcQueryResult<Row>> =>
  new Promise<BucketRpcQueryResult<Row>>((resolve, reject) => {
    const entry: PendingQuery = {
      resolveRows: (rows) => {
        resolve({ rows: coerceRowsForCaller<Row>(rows) });
      },
      resolveOk: (rowcount) => {
        resolve({ rows: coerceRowsForCaller<Row>([]) });
        void rowcount;
      },
      reject,
      chunks: [],
      expectedSeq: 0,
    };
    registerPendingEntry(args, entry);
  });

const execViaRpc = (args: SendSqlArgs): Promise<number> =>
  new Promise<number>((resolve, reject) => {
    const entry: PendingQuery = {
      resolveRows: (rows) => {
        resolve(rows.length);
      },
      resolveOk: resolve,
      reject,
      chunks: [],
      expectedSeq: 0,
    };
    registerPendingEntry(args, entry);
  });

const installStdoutListener = (
  child: BucketEvalRpcChildLike,
  context: RouteResponseContext,
): void => {
  if (child.stdout.setEncoding !== undefined) {
    child.stdout.setEncoding("utf8");
  }
  const state: BufferState = { buffer: "" };
  child.stdout.on("data", (chunk) => {
    const incoming = decodeChunk(chunk);
    const lines = splitBuffered(state, incoming);
    lines.forEach((line) => {
      const response = parseLine(line);
      if (response !== null) routeResponse(context, response);
    });
  });
};

const sendExit = (child: BucketEvalRpcChildLike): void => {
  child.stdin.write(`${JSON.stringify({ type: RPC_REQUEST_TYPE_EXIT })}${NEWLINE}`);
  if (child.stdin.end !== undefined) child.stdin.end();
};

export const createBucketEvalRpcClient = (
  args: CreateBucketEvalRpcClientArgs,
): BucketEvalRpcClient => {
  const child = args.child;
  const generateId = args.generateId ?? buildRandomId;
  const pending: Map<string, PendingQuery> = new Map();
  const readyState: {
    resolve: (response: RpcReadyResponse) => void;
    reject: (error: Error) => void;
  } = {
    resolve: () => {},
    reject: () => {},
  };
  const closedState: { resolve: () => void } = { resolve: () => {} };
  const ready = new Promise<RpcReadyResponse>((resolve, reject) => {
    readyState.resolve = resolve;
    readyState.reject = reject;
  });
  const closedPromise = new Promise<void>((resolve) => {
    closedState.resolve = resolve;
  });
  const failPendingForServerError = (error: Error): void => {
    pending.forEach((entry) => entry.reject(error));
    pending.clear();
    readyState.reject(error);
  };
  const context: RouteResponseContext = {
    pending,
    readyResolve: readyState.resolve,
    readyReject: readyState.reject,
    closedResolve: closedState.resolve,
    pendingErrorHandler: failPendingForServerError,
  };
  installStdoutListener(child, context);
  return {
    ready,
    query: <Row>(sql: string, params?: unknown[]) =>
      queryViaRpc<Row>({ child, pending, id: generateId(), sql, params: params ?? [] }),
    exec: (sql: string, params?: unknown[]) =>
      execViaRpc({ child, pending, id: generateId(), sql, params: params ?? [] }),
    close: async () => {
      sendExit(child);
      await closedPromise;
    },
  };
};
