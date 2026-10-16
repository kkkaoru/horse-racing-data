// Runs with bun via Vitest; native cross-worker RPC with offline SQL transport only.
// Use the installed workerd date ceiling; production compatibility remains unchanged.
import { build } from "esbuild";
import { createFetchMock, Miniflare } from "miniflare";
import { expect, it } from "vitest";
it("default binding RPC reads revision while public HTTP rejects without SQL", async () => {
  const upstream = createFetchMock();
  upstream.disableNetConnect();
  const calls: unknown[] = [];
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: "/api/v1/accounts/account/r2-sql/query/catalog", method: "POST" })
    .reply((options) => {
      calls.push(options.body);
      return { statusCode: 200, data: JSON.stringify({ success: true, result: { rows: [] } }) };
    })
    .persist();
  const bundle = await build({
    entryPoints: [new URL("./vector-gateway.ts", import.meta.url).pathname],
    bundle: true,
    format: "esm",
    platform: "neutral",
    conditions: ["workerd", "worker", "browser"],
    mainFields: ["browser", "module", "main"],
    write: false,
    external: ["cloudflare:workers", "node:*"],
    logLevel: "silent",
  });
  const output = bundle.outputFiles[0];
  if (output === undefined) throw new Error("Missing gateway bundle");
  const runtime = new Miniflare({
    workers: [
      {
        name: "viewer",
        modules: true,
        compatibilityDate: "2026-06-18",
        serviceBindings: { R2_CATALOG: "catalog" },
        script: `export default { async fetch(request, env) {
        const input = await request.json();
        try { return Response.json({ revision: await env.R2_CATALOG.getGateMappingRevision(input) }); }
        catch { return new Response("Invalid revision", {status: 503}); }
      } };`,
      },
      {
        name: "catalog",
        modules: true,
        compatibilityDate: "2026-06-18",
        compatibilityFlags: ["nodejs_compat"],
        script: output.text,
        fetchMock: upstream,
        bindings: {
          R2_SQL_ACCOUNT_ID: "account",
          R2_SQL_BUCKET_NAME: "catalog",
          R2_SQL_NAMESPACE: "pc_keiba",
          R2_SQL_TOKEN: "offline-only",
        },
      },
    ],
  });
  try {
    const catalog = await runtime.getWorker("catalog");
    const publicResponse = await catalog.fetch(
      "https://public.example/v1/gate-mapping-revision?year=2026&month=10&day=04&keibajoCode=A8&raceNumber=05",
    );
    expect(publicResponse.status).toBe(404);
    expect(calls).toStrictEqual([]);
    const spoof = await catalog.fetch(
      "https://pc-keiba-r2-catalog.internal/v1/gate-mapping-revision",
      { headers: { "x-internal": "true" } },
    );
    expect(spoof.status).toBe(404);
    expect(calls).toStrictEqual([]);
    const viewer = await runtime.getWorker("viewer");
    const response = await viewer.fetch("https://viewer.test/", {
      method: "POST",
      body: JSON.stringify({ date: "20261004", keibajoCode: "A8" }),
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toStrictEqual({
      revision: "gate-v1-e47c0fa0e22cdf31706db0c9568edaa6155ca86914e545d1950e88f8d2aa5229",
    });
    expect(calls).toHaveLength(6);
    if (!(calls[0] instanceof ReadableStream)) throw new Error("Expected streamed SQL request");
    const sql: string = await new Response(calls[0]).text();
    expect(sql).toMatch(/gate_number IS NULL/u);
    expect(sql).not.toMatch(/JOIN|GROUP BY/u);
    const invalid = await viewer.fetch("https://viewer.test/", {
      method: "POST",
      body: JSON.stringify({ date: "20260230", keibajoCode: "A8" }),
    });
    expect(invalid.status).toBe(503);
    expect(calls).toHaveLength(6);
    const domestic = await viewer.fetch("https://viewer.test/", {
      method: "POST",
      body: JSON.stringify({ date: "20261004", keibajoCode: "77" }),
    });
    expect(await domestic.json()).toStrictEqual({});
    expect(calls).toHaveLength(6);
  } finally {
    await runtime.dispose();
    await upstream.close();
  }
}, 30000);
