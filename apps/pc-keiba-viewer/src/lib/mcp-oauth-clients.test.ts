// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import {
  clientStorageKey,
  loadRegisteredClient,
  parseClientIdMetadataDocument,
  parseRegisteredClient,
  registerPublicClient,
  resolveClient,
} from "./mcp-oauth-clients";
import { createMemoryOauthStore } from "./mcp-oauth-store";

it("registers a public client and rejects bad redirect URIs", async () => {
  const store = createMemoryOauthStore();
  const bad = await registerPublicClient(store, { redirect_uris: ["http://evil.example.test/cb"] });
  expect(bad).toBe("invalid_redirect_uri");
  const empty = await registerPublicClient(store, {});
  expect(empty).toBe("invalid_redirect_uri");
  const ok = await registerPublicClient(store, {
    client_name: "Agent",
    redirect_uris: ["http://127.0.0.1:9/cb"],
  });
  expect(typeof ok === "object").toBe(true);
});

it("rejects a non-object DCR body and non-string redirect URIs", async () => {
  const store = createMemoryOauthStore();
  expect(await registerPublicClient(store, null)).toBe("invalid_client_metadata");
  expect(await registerPublicClient(store, { redirect_uris: [1] })).toBe("invalid_redirect_uri");
});

it("parses stored client JSON and rejects malformed records", async () => {
  const store = createMemoryOauthStore();
  expect(parseRegisteredClient("{")).toBe(null);
  expect(parseRegisteredClient("[]")).toBe(null);
  expect(parseRegisteredClient("{}")).toBe(null);
  expect(
    parseRegisteredClient(
      JSON.stringify({
        client_id: "c",
        client_name: "n",
        grant_types: ["authorization_code"],
        redirect_uris: ["http://127.0.0.1/cb"],
        token_endpoint_auth_method: "none",
      }),
    )?.client_id,
  ).toBe("c");
  expect(await loadRegisteredClient(store, "missing")).toBe(null);
});

it("resolves a stored client and a CIMD https client_id", async () => {
  const store = createMemoryOauthStore();
  const created = await registerPublicClient(store, {
    client_name: "Stored",
    redirect_uris: ["http://localhost:1/cb"],
  });
  if (typeof created === "string") {
    throw new Error("expected client");
  }
  const stored = await resolveClient(store, created.client_id, fetch);
  expect(stored?.client_name).toBe("Stored");
  const cimd = await resolveClient(
    store,
    "https://agents.example.test/client.json",
    async () =>
      new Response(
        JSON.stringify({
          client_id: "https://agents.example.test/client.json",
          client_name: "CIMD",
          grant_types: ["authorization_code"],
          redirect_uris: ["http://127.0.0.1/cb"],
          token_endpoint_auth_method: "none",
        }),
        { status: 200 },
      ),
  );
  expect(cimd?.client_name).toBe("CIMD");
});

it("resolves a CIMD document that omits grant_types and client_name", async () => {
  const store = createMemoryOauthStore();
  const cimd = await resolveClient(
    store,
    "https://agents.example.test/agent.json",
    async () =>
      new Response(
        JSON.stringify({
          client_id: "https://agents.example.test/agent.json",
          redirect_uris: ["http://localhost/cb"],
        }),
        { status: 200 },
      ),
  );
  expect(cimd?.client_name).toBe("MCP client");
  expect(cimd?.token_endpoint_auth_method).toBe("none");
});

it("rejects CIMD fetch failures, mismatched client_id, and disallowed redirects", async () => {
  const store = createMemoryOauthStore();
  expect(await resolveClient(store, "not-https", fetch)).toBe(null);
  const notOk = await resolveClient(
    store,
    "https://agents.example.test/missing.json",
    async () => new Response("nope", { status: 404 }),
  );
  expect(notOk).toBe(null);
  const mismatch = await resolveClient(
    store,
    "https://agents.example.test/client.json",
    async () =>
      new Response(JSON.stringify({ client_id: "other", redirect_uris: ["http://127.0.0.1/cb"] }), {
        status: 200,
      }),
  );
  expect(mismatch).toBe(null);
  const badRedirect = await resolveClient(
    store,
    "https://agents.example.test/client.json",
    async () =>
      new Response(
        JSON.stringify({
          client_id: "https://agents.example.test/client.json",
          redirect_uris: ["http://evil.example.test/cb"],
        }),
        { status: 200 },
      ),
  );
  expect(badRedirect).toBe(null);
  const invalidJson = await resolveClient(
    store,
    "https://agents.example.test/client.json",
    async () => new Response("not-json", { status: 200 }),
  );
  expect(invalidJson).toBe(null);
  expect(parseClientIdMetadataDocument(null, "https://agents.example.test/client.json")).toBe(null);
  expect(
    parseClientIdMetadataDocument(
      { client_id: "https://agents.example.test/client.json", redirect_uris: [] },
      "https://agents.example.test/client.json",
    ),
  ).toBe(null);
  expect(
    parseClientIdMetadataDocument(
      { client_id: "https://agents.example.test/client.json", redirect_uris: [1] },
      "https://agents.example.test/client.json",
    ),
  ).toBe(null);
  expect(
    parseClientIdMetadataDocument(
      {
        client_id: "https://agents.example.test/client.json",
        grant_types: [1],
        redirect_uris: ["http://127.0.0.1/cb"],
      },
      "https://agents.example.test/client.json",
    ),
  ).toBe(null);
  expect(
    parseClientIdMetadataDocument(
      {
        client_id: "https://agents.example.test/client.json",
        grant_types: [],
        redirect_uris: ["http://127.0.0.1/cb"],
      },
      "https://agents.example.test/client.json",
    ),
  ).toBe(null);
});

it("rejects non-object registration metadata", async () => {
  const store = createMemoryOauthStore();
  expect(await registerPublicClient(store, null)).toBe("invalid_client_metadata");
  expect(await registerPublicClient(store, "client")).toBe("invalid_client_metadata");
});

it("rejects non-string redirect URIs", async () => {
  const store = createMemoryOauthStore();
  expect(await registerPublicClient(store, { redirect_uris: ["http://127.0.0.1/cb", 1] })).toBe(
    "invalid_redirect_uri",
  );
});

it("defaults the client name when it is omitted", async () => {
  const store = createMemoryOauthStore();
  const created = await registerPublicClient(store, {
    redirect_uris: ["https://agents.example.test/callback"],
  });
  if (typeof created === "string") {
    throw new Error("expected client");
  }
  expect(created.client_name).toBe("MCP client");
  expect(created.token_endpoint_auth_method).toBe("none");
  expect(created.grant_types).toStrictEqual(["authorization_code", "refresh_token"]);
});

it("returns null for a missing stored client", async () => {
  const store = createMemoryOauthStore();
  expect(await loadRegisteredClient(store, "missing")).toBe(null);
});

it("returns null for corrupt stored client JSON", async () => {
  const store = createMemoryOauthStore();
  await store.put(clientStorageKey("broken"), "not-json", 60);
  expect(await loadRegisteredClient(store, "broken")).toBe(null);
});

it("parses a valid registered client and rejects malformed documents", () => {
  expect(
    parseRegisteredClient(
      JSON.stringify({
        client_id: "client-1",
        client_name: "Agent",
        grant_types: ["authorization_code"],
        redirect_uris: ["http://127.0.0.1/cb"],
        token_endpoint_auth_method: "none",
      }),
    ),
  ).toStrictEqual({
    client_id: "client-1",
    client_name: "Agent",
    grant_types: ["authorization_code"],
    redirect_uris: ["http://127.0.0.1/cb"],
    token_endpoint_auth_method: "none",
  });
  expect(parseRegisteredClient("not-json")).toBe(null);
  expect(parseRegisteredClient("[]")).toBe(null);
  expect(
    parseRegisteredClient(
      JSON.stringify({
        client_id: "",
        client_name: "Agent",
        grant_types: ["authorization_code"],
        redirect_uris: ["http://127.0.0.1/cb"],
        token_endpoint_auth_method: "none",
      }),
    ),
  ).toBe(null);
  expect(
    parseRegisteredClient(
      JSON.stringify({
        client_id: 1,
        client_name: "Agent",
        grant_types: ["authorization_code"],
        redirect_uris: ["http://127.0.0.1/cb"],
        token_endpoint_auth_method: "none",
      }),
    ),
  ).toBe(null);
  expect(
    parseRegisteredClient(
      JSON.stringify({
        client_id: "client-1",
        client_name: 1,
        grant_types: ["authorization_code"],
        redirect_uris: ["http://127.0.0.1/cb"],
        token_endpoint_auth_method: "none",
      }),
    ),
  ).toBe(null);
  expect(
    parseRegisteredClient(
      JSON.stringify({
        client_id: "client-1",
        client_name: "Agent",
        grant_types: ["authorization_code"],
        redirect_uris: ["http://127.0.0.1/cb"],
        token_endpoint_auth_method: 1,
      }),
    ),
  ).toBe(null);
  expect(
    parseRegisteredClient(
      JSON.stringify({
        client_id: "client-1",
        client_name: "Agent",
        grant_types: ["authorization_code"],
        redirect_uris: "http://127.0.0.1/cb",
        token_endpoint_auth_method: "none",
      }),
    ),
  ).toBe(null);
  expect(
    parseRegisteredClient(
      JSON.stringify({
        client_id: "client-1",
        client_name: "Agent",
        grant_types: ["authorization_code"],
        redirect_uris: ["http://127.0.0.1/cb", 1],
        token_endpoint_auth_method: "none",
      }),
    ),
  ).toBe(null);
  expect(
    parseRegisteredClient(
      JSON.stringify({
        client_id: "client-1",
        client_name: "Agent",
        grant_types: "authorization_code",
        redirect_uris: ["http://127.0.0.1/cb"],
        token_endpoint_auth_method: "none",
      }),
    ),
  ).toBe(null);
  expect(
    parseRegisteredClient(
      JSON.stringify({
        client_id: "client-1",
        client_name: "Agent",
        grant_types: ["authorization_code", 1],
        redirect_uris: ["http://127.0.0.1/cb"],
        token_endpoint_auth_method: "none",
      }),
    ),
  ).toBe(null);
});

it("does not fetch CIMD for non-https or root-path client ids", async () => {
  const store = createMemoryOauthStore();
  expect(await resolveClient(store, "not-a-url", fetch)).toBe(null);
  expect(await resolveClient(store, "http://agents.example.test/client.json", fetch)).toBe(null);
  expect(await resolveClient(store, "https://agents.example.test", fetch)).toBe(null);
  expect(await resolveClient(store, "https://agents.example.test/", fetch)).toBe(null);
  expect(await resolveClient(store, "https://", fetch)).toBe(null);
});

it("returns null when CIMD fetch is not ok", async () => {
  const store = createMemoryOauthStore();
  const resolved = await resolveClient(
    store,
    "https://agents.example.test/missing.json",
    async () => new Response("missing", { status: 404 }),
  );
  expect(resolved).toBe(null);
});

it("returns null when CIMD JSON is not a client document", async () => {
  const store = createMemoryOauthStore();
  const resolved = await resolveClient(
    store,
    "https://agents.example.test/client.json",
    async () => new Response("[]", { status: 200 }),
  );
  expect(resolved).toBe(null);
});

it("returns null when CIMD client_id does not match the URL", async () => {
  const store = createMemoryOauthStore();
  const resolved = await resolveClient(
    store,
    "https://agents.example.test/client.json",
    async () =>
      new Response(
        JSON.stringify({
          client_id: "https://other.example.test/client.json",
          client_name: "CIMD",
          grant_types: ["authorization_code"],
          redirect_uris: ["http://127.0.0.1/cb"],
          token_endpoint_auth_method: "none",
        }),
        { status: 200 },
      ),
  );
  expect(resolved).toBe(null);
});

it("returns null when CIMD redirect URIs are not allowed", async () => {
  const store = createMemoryOauthStore();
  const resolved = await resolveClient(
    store,
    "https://agents.example.test/client.json",
    async () =>
      new Response(
        JSON.stringify({
          client_id: "https://agents.example.test/client.json",
          client_name: "CIMD",
          grant_types: ["authorization_code"],
          redirect_uris: ["http://evil.example.test/cb"],
          token_endpoint_auth_method: "none",
        }),
        { status: 200 },
      ),
  );
  expect(resolved).toBe(null);
});

it("caches a successful CIMD client in the store", async () => {
  const store = createMemoryOauthStore();
  const first = await resolveClient(
    store,
    "https://agents.example.test/client.json",
    async () =>
      new Response(
        JSON.stringify({
          client_id: "https://agents.example.test/client.json",
          client_name: "Cached",
          grant_types: ["authorization_code"],
          redirect_uris: ["http://localhost/cb"],
          token_endpoint_auth_method: "none",
        }),
        { status: 200 },
      ),
  );
  expect(first?.client_name).toBe("Cached");
  const second = await resolveClient(
    store,
    "https://agents.example.test/client.json",
    async () => new Response("missing", { status: 500 }),
  );
  expect(second?.client_name).toBe("Cached");
});
