// Runs with bun via Vitest; real workerd SQLite and RPC, not the Node test shim.
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { build } from "esbuild";
import { Miniflare } from "miniflare";
import { afterAll, beforeAll, expect, test } from "vitest";
import type { CatalogLeaseToken } from "./catalog-lease";

interface LeaseRequest {
  kind?: "control";
  scope: string;
  operation: string;
  args: unknown[];
}
const runtime: { worker: Miniflare | null; directory: string | null; script: string } = {
  worker: null,
  directory: null,
  script: "",
};
const instance = (): Miniflare => {
  if (runtime.worker === null) throw new Error("Lease test worker has not started");
  return runtime.worker;
};
const createWorker = (): Miniflare => {
  if (runtime.directory === null) throw new Error("Missing persistent test directory");
  return new Miniflare({
    name: "catalog-lease-test",
    modules: true,
    script: runtime.script,
    compatibilityDate: "2026-06-01",
    compatibilityFlags: ["nodejs_compat"],
    durableObjects: {
      LEASES: { className: "CatalogLeaseCoordinator", useSQLite: true },
      CONTROL: { className: "IngestionControlDatabase", useSQLite: true },
    },
    durableObjectsPersist: runtime.directory,
  });
};
const call = async (input: LeaseRequest): Promise<unknown> => {
  const result = await instance().dispatchFetch("https://lease.test", {
    method: "POST",
    body: JSON.stringify(input),
  });
  if (!result.ok)
    throw new Error(`Lease fixture HTTP ${String(result.status)}: ${await result.text()}`);
  return await result.json();
};
const lease = (value: unknown): CatalogLeaseToken => {
  if (
    typeof value !== "object" ||
    value === null ||
    !("owner" in value) ||
    typeof value.owner !== "string" ||
    !("fence" in value) ||
    typeof value.fence !== "number" ||
    !("expiresAt" in value) ||
    typeof value.expiresAt !== "number"
  )
    throw new Error("Invalid native lease response");
  return { owner: value.owner, fence: value.fence, expiresAt: value.expiresAt };
};

beforeAll(async () => {
  const entry: string = new URL("./catalog-lease.ts", import.meta.url).pathname;
  const controlEntry: string = new URL("./control-database.ts", import.meta.url).pathname;
  const bundle = await build({
    stdin: {
      contents: `import { CatalogLeaseCoordinator } from ${JSON.stringify(entry)};
import { IngestionControlDatabase } from ${JSON.stringify(controlEntry)};
export { CatalogLeaseCoordinator, IngestionControlDatabase };
export default { async fetch(request, env) {
  try {
    const body = await request.json();
    if (body.kind === 'control') {
      const db = env.CONTROL.getByName(body.scope);
      if (!['bootstrap', 'execute', 'outbox'].includes(body.operation)) return new Response(null, {status: 400});
      const result = await db[body.operation](...body.args);
      return result instanceof Response ? result : Response.json(result);
    }
    const stub = env.LEASES.getByName(body.scope);
    if (!['acquire', 'renew', 'release', 'validate', 'status'].includes(body.operation)) return new Response(null, {status: 400});
    return Response.json(await stub[body.operation](...body.args));
  } catch(error) { return Response.json({error: String(error)}, {status: 409}); }
} };`,
      loader: "ts",
      resolveDir: process.cwd(),
    },
    bundle: true,
    format: "esm",
    platform: "neutral",
    write: false,
    external: ["cloudflare:workers", "node:crypto"],
    logLevel: "silent",
  });
  const output = bundle.outputFiles[0];
  if (output === undefined) throw new Error("Missing lease fixture bundle");
  runtime.script = output.text;
  runtime.directory = await mkdtemp(join(tmpdir(), "catalog-lease-workerd-"));
  runtime.worker = createWorker();
  await instance().ready;
}, 20000);

afterAll(async () => {
  await runtime.worker?.dispose();
  if (runtime.directory !== null) await rm(runtime.directory, { recursive: true, force: true });
});

test("native control transactions retain replay receipts and an atomic export outbox", async () => {
  const base = { kind: "control", scope: "control-test" } satisfies Pick<
    LeaseRequest,
    "kind" | "scope"
  >;
  const schema = ["CREATE TABLE counts (id TEXT PRIMARY KEY, value INTEGER NOT NULL)"];
  expect(await call({ ...base, operation: "bootstrap", args: [schema] })).toBe(true);
  expect(await call({ ...base, operation: "bootstrap", args: [schema] })).toBe(false);
  const noMatch = {
    requestId: "no-match",
    statements: [{ sql: "UPDATE counts SET value = 99 WHERE id = ?", params: ["k"] }],
  };
  const noMatchResult = await call({ ...base, operation: "execute", args: [noMatch] });
  expect(noMatchResult).toMatchObject([{ meta: { changes: 0 } }]);
  const seed = {
    requestId: "seed",
    statements: [{ sql: "INSERT INTO counts VALUES (?, ?)", params: ["k", 0] }],
  };
  await call({ ...base, operation: "execute", args: [seed] });
  expect(await call({ ...base, operation: "execute", args: [noMatch] })).toStrictEqual(
    noMatchResult,
  );
  const increment = {
    requestId: "increment",
    statements: [
      { sql: "UPDATE counts SET value = value + 1 WHERE id = ?", params: ["k"] },
      { sql: "SELECT value FROM counts", params: [] },
    ],
  };
  const first = await call({ ...base, operation: "execute", args: [increment] });
  expect(first).toMatchObject([
    { meta: { changes: 1 } },
    { results: [{ value: 1 }], meta: { changes: 0 } },
  ]);
  expect(await call({ ...base, operation: "execute", args: [increment] })).toStrictEqual(first);
  await expect(
    call({
      ...base,
      operation: "execute",
      args: [{ ...increment, statements: [{ sql: "DELETE FROM counts", params: [] }] }],
    }),
  ).rejects.toThrow("reused");
  const rollback = {
    requestId: "rollback",
    statements: [
      { sql: "INSERT INTO counts VALUES (?, ?)", params: ["other", 2] },
      { sql: "INSERT INTO counts VALUES (?, ?)", params: ["k", 2] },
    ],
  };
  await expect(call({ ...base, operation: "execute", args: [rollback] })).rejects.toThrow("UNIQUE");
  expect(
    await call({
      ...base,
      operation: "execute",
      args: [
        {
          requestId: "read",
          statements: [{ sql: "SELECT * FROM counts ORDER BY id", params: [] }],
        },
      ],
    }),
  ).toMatchObject([{ results: [{ id: "k", value: 1 }] }]);
  expect(await call({ ...base, operation: "outbox", args: [0, 100] })).toMatchObject([
    { sequence: 1, request_id: "seed" },
    { sequence: 2, request_id: "increment" },
  ]);
});

test("real RPC races grant exactly one owner and isolate other Catalog tables", async () => {
  const results = await Promise.all(
    Array.from({ length: 8 }, (_, index) =>
      call({ scope: "race", operation: "acquire", args: [`owner-${index}`, 300000] }),
    ),
  );
  expect(results.filter((value) => value !== null)).toHaveLength(1);
  const token = lease(results.find((value) => value !== null));
  expect(await call({ scope: "race", operation: "validate", args: [token] })).toBe(true);
  expect(
    lease(await call({ scope: "other-table", operation: "acquire", args: ["other", 300000] }))
      .fence,
  ).toBe(1);
  expect(
    await call({
      scope: "race",
      operation: "release",
      args: [{ ...token, fence: token.fence + 1 }],
    }),
  ).toBe(false);
  expect(await call({ scope: "race", operation: "release", args: [token] })).toBe(true);
  const next = lease(
    await call({ scope: "race", operation: "acquire", args: [token.owner, 300000] }),
  );
  expect(next.fence).toBe(2);
  expect(await call({ scope: "race", operation: "renew", args: [token, 300000] })).toBeNull();
});

test("SQLite persists owner and fencing counter across a workerd restart", async () => {
  const original = lease(
    await call({ scope: "restart", operation: "acquire", args: ["owner", 300000] }),
  );
  await instance().dispose();
  runtime.worker = createWorker();
  await instance().ready;
  expect(await call({ scope: "restart", operation: "status", args: [] })).toStrictEqual(original);
  expect(
    await call({ scope: "restart", operation: "acquire", args: ["competitor", 300000] }),
  ).toBeNull();
  expect(await call({ scope: "restart", operation: "release", args: [original] })).toBe(true);
  expect(
    lease(await call({ scope: "restart", operation: "acquire", args: ["competitor", 300000] }))
      .fence,
  ).toBe(2);
}, 20000);
