// Run with: bunx vitest run src/scripts/finish-position-features/bucket-eval-rpc-client.test.ts
import { expect, test, vi } from "vitest";

import type {
  BucketEvalRpcChildLike,
  BucketRpcReadable,
  BucketRpcWritable,
} from "./bucket-eval-rpc-client";
import { createBucketEvalRpcClient } from "./bucket-eval-rpc-client";

interface MockChildState {
  writes: string[];
  emit: (data: string) => void;
  ended: boolean;
  encoding: string | undefined;
}

interface MockChild {
  child: BucketEvalRpcChildLike;
  state: MockChildState;
}

const buildMockChild = (): MockChild => {
  const state: MockChildState = {
    writes: [],
    emit: () => {},
    ended: false,
    encoding: undefined,
  };
  const listeners: ((chunk: string) => void)[] = [];
  const stdout: BucketRpcReadable = {
    on: (_event, listener) => {
      listeners.push(listener as (chunk: string) => void);
    },
    setEncoding: (encoding) => {
      state.encoding = encoding;
    },
  };
  state.emit = (data: string) => {
    listeners.forEach((listener) => listener(data));
  };
  const stdin: BucketRpcWritable = {
    write: (chunk) => {
      state.writes.push(chunk);
      return true;
    },
    end: () => {
      state.ended = true;
    },
  };
  return { child: { stdin, stdout }, state };
};

const sequentialId = (): (() => string) => {
  let counter = 0;
  return () => {
    counter += 1;
    return `id-${counter}`;
  };
};

test("createBucketEvalRpcClient resolves ready when server emits ready", async () => {
  const mock = buildMockChild();
  const client = createBucketEvalRpcClient({ child: mock.child, generateId: sequentialId() });
  mock.state.emit('{"type":"ready","loadedRows":42}\n');
  await expect(client.ready).resolves.toStrictEqual({ type: "ready", loadedRows: 42 });
  expect(mock.state.encoding).toBe("utf8");
});

test("createBucketEvalRpcClient query resolves with chunked rows in seq order", async () => {
  const mock = buildMockChild();
  const client = createBucketEvalRpcClient({ child: mock.child, generateId: sequentialId() });
  mock.state.emit('{"type":"ready","loadedRows":0}\n');
  const promise = client.query<{ value: number }>("select * from x");
  mock.state.emit('{"type":"rows","id":"id-1","seq":1,"rows":[{"value":2}],"done":false}\n');
  mock.state.emit('{"type":"rows","id":"id-1","seq":0,"rows":[{"value":1}],"done":false}\n');
  mock.state.emit('{"type":"rows","id":"id-1","seq":2,"rows":[{"value":3}],"done":true}\n');
  await expect(promise).resolves.toStrictEqual({
    rows: [{ value: 1 }, { value: 2 }, { value: 3 }],
  });
});

test("createBucketEvalRpcClient query writes JSONL sql request to stdin", async () => {
  const mock = buildMockChild();
  const client = createBucketEvalRpcClient({ child: mock.child, generateId: sequentialId() });
  mock.state.emit('{"type":"ready","loadedRows":0}\n');
  const promise = client.query("select 1", [1, "a"]);
  expect(mock.state.writes[0]).toBe(
    '{"type":"sql","id":"id-1","query":"select 1","params":[1,"a"]}\n',
  );
  mock.state.emit('{"type":"rows","id":"id-1","seq":0,"rows":[],"done":true}\n');
  await expect(promise).resolves.toStrictEqual({ rows: [] });
});

test("createBucketEvalRpcClient exec resolves with rowcount from ok response", async () => {
  const mock = buildMockChild();
  const client = createBucketEvalRpcClient({ child: mock.child, generateId: sequentialId() });
  mock.state.emit('{"type":"ready","loadedRows":0}\n');
  const promise = client.exec("update x set v = 1");
  mock.state.emit('{"type":"ok","id":"id-1","rowcount":7}\n');
  await expect(promise).resolves.toBe(7);
});

test("createBucketEvalRpcClient exec falls back to rows length when server emits rows", async () => {
  const mock = buildMockChild();
  const client = createBucketEvalRpcClient({ child: mock.child, generateId: sequentialId() });
  mock.state.emit('{"type":"ready","loadedRows":0}\n');
  const promise = client.exec("select 1");
  mock.state.emit('{"type":"rows","id":"id-1","seq":0,"rows":[{"a":1},{"a":2}],"done":true}\n');
  await expect(promise).resolves.toBe(2);
});

test("createBucketEvalRpcClient query rejects when server emits error with id", async () => {
  const mock = buildMockChild();
  const client = createBucketEvalRpcClient({ child: mock.child, generateId: sequentialId() });
  mock.state.emit('{"type":"ready","loadedRows":0}\n');
  const promise = client.query("select bad");
  mock.state.emit('{"type":"error","id":"id-1","message":"syntax","details":"d"}\n');
  await expect(promise).rejects.toThrowError("RPC error: syntax");
});

test("createBucketEvalRpcClient close sends exit request and resolves on closed", async () => {
  const mock = buildMockChild();
  const client = createBucketEvalRpcClient({ child: mock.child, generateId: sequentialId() });
  mock.state.emit('{"type":"ready","loadedRows":0}\n');
  const promise = client.close();
  expect(mock.state.writes[0]).toBe('{"type":"exit"}\n');
  expect(mock.state.ended).toBe(true);
  mock.state.emit('{"type":"closed"}\n');
  await expect(promise).resolves.toBeUndefined();
});

test("createBucketEvalRpcClient handles partial lines split across data events", async () => {
  const mock = buildMockChild();
  const client = createBucketEvalRpcClient({ child: mock.child, generateId: sequentialId() });
  mock.state.emit('{"type":"rea');
  mock.state.emit('dy","loadedRows":5}\n');
  await expect(client.ready).resolves.toStrictEqual({ type: "ready", loadedRows: 5 });
});

test("createBucketEvalRpcClient ignores responses for unknown ids", async () => {
  const mock = buildMockChild();
  const client = createBucketEvalRpcClient({ child: mock.child, generateId: sequentialId() });
  mock.state.emit('{"type":"ready","loadedRows":0}\n');
  mock.state.emit('{"type":"rows","id":"unknown","seq":0,"rows":[],"done":true}\n');
  mock.state.emit('{"type":"ok","id":"unknown","rowcount":1}\n');
  mock.state.emit('{"type":"error","id":"unknown","message":"x","details":"y"}\n');
  const closePromise = client.close();
  mock.state.emit('{"type":"closed"}\n');
  await expect(closePromise).resolves.toBeUndefined();
});

test("createBucketEvalRpcClient fails ready and pending when server emits error with null id", async () => {
  const mock = buildMockChild();
  const client = createBucketEvalRpcClient({ child: mock.child, generateId: sequentialId() });
  const pendingQuery = client.query("select 1");
  mock.state.emit('{"type":"error","id":null,"message":"boom","details":""}\n');
  await expect(client.ready).rejects.toThrowError("RPC server error: boom");
  await expect(pendingQuery).rejects.toThrowError("RPC server error: boom");
});

test("createBucketEvalRpcClient handles Uint8Array data via TextDecoder", async () => {
  const writes: string[] = [];
  const listeners: ((chunk: string | Uint8Array) => void)[] = [];
  const stdout: BucketRpcReadable = {
    on: (_event, listener) => {
      listeners.push(listener);
    },
    setEncoding: () => {},
  };
  const stdin: BucketRpcWritable = {
    write: (chunk) => {
      writes.push(chunk);
      return true;
    },
  };
  const client = createBucketEvalRpcClient({
    child: { stdin, stdout },
    generateId: sequentialId(),
  });
  const encoder = new TextEncoder();
  listeners[0]?.(encoder.encode('{"type":"ready","loadedRows":1}\n'));
  await expect(client.ready).resolves.toStrictEqual({ type: "ready", loadedRows: 1 });
});

test("createBucketEvalRpcClient parseLine throws on non-object JSON", () => {
  const mock = buildMockChild();
  createBucketEvalRpcClient({ child: mock.child, generateId: sequentialId() });
  expect(() => mock.state.emit('"oops"\n')).toThrowError("RPC response is not a JSON object");
});

test("createBucketEvalRpcClient skips stdout setEncoding when undefined", async () => {
  const writes: string[] = [];
  const listeners: ((chunk: string) => void)[] = [];
  const stdout: BucketRpcReadable = {
    on: (_event, listener) => {
      listeners.push(listener as (chunk: string) => void);
    },
  };
  const stdin: BucketRpcWritable = {
    write: (chunk) => {
      writes.push(chunk);
      return true;
    },
  };
  const client = createBucketEvalRpcClient({
    child: { stdin, stdout },
    generateId: sequentialId(),
  });
  listeners[0]?.('{"type":"ready","loadedRows":0}\n');
  await expect(client.ready).resolves.toStrictEqual({ type: "ready", loadedRows: 0 });
  const closing = client.close();
  expect(writes.at(-1)).toBe('{"type":"exit"}\n');
  listeners[0]?.('{"type":"closed"}\n');
  await expect(closing).resolves.toBeUndefined();
});

test("createBucketEvalRpcClient uses default generateId when none provided", async () => {
  const mock = buildMockChild();
  const client = createBucketEvalRpcClient({ child: mock.child });
  mock.state.emit('{"type":"ready","loadedRows":0}\n');
  const promise = client.query("select 1");
  const writtenLine = mock.state.writes[0] ?? "";
  expect(writtenLine.startsWith('{"type":"sql","id":"rpc-')).toBe(true);
  const parsedUnknown: unknown = JSON.parse(writtenLine.trim());
  const parsedRecord =
    parsedUnknown !== null && typeof parsedUnknown === "object"
      ? (parsedUnknown as { id?: string })
      : { id: "" };
  const id = parsedRecord.id ?? "";
  mock.state.emit(`{"type":"rows","id":${JSON.stringify(id)},"seq":0,"rows":[],"done":true}\n`);
  await expect(promise).resolves.toStrictEqual({ rows: [] });
});

test("createBucketEvalRpcClient query falls back when server emits ok rather than rows", async () => {
  const mock = buildMockChild();
  const client = createBucketEvalRpcClient({ child: mock.child, generateId: sequentialId() });
  mock.state.emit('{"type":"ready","loadedRows":0}\n');
  const promise = client.query("upsert");
  mock.state.emit('{"type":"ok","id":"id-1","rowcount":3}\n');
  await expect(promise).resolves.toStrictEqual({ rows: [] });
});

test("createBucketEvalRpcClient logs no encoding call when setEncoding spy is used", async () => {
  const mock = buildMockChild();
  const spy = vi.fn<(encoding: "utf8") => void>();
  const wrapped: BucketRpcReadable = {
    on: mock.child.stdout.on,
    setEncoding: (encoding) => {
      spy(encoding);
    },
  };
  const client = createBucketEvalRpcClient({
    child: { stdin: mock.child.stdin, stdout: wrapped },
    generateId: sequentialId(),
  });
  expect(spy).toHaveBeenCalledWith("utf8");
  mock.state.emit('{"type":"ready","loadedRows":0}\n');
  await expect(client.ready).resolves.toStrictEqual({ type: "ready", loadedRows: 0 });
});
