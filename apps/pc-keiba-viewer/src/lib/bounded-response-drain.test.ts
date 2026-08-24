// Run with bun (vitest).
// @vitest-environment node
import { expect, it } from "vitest";

import { drainResponseBody } from "./bounded-response-drain";

it("drainResponseBody consumes a response stream without buffering text", async () => {
  const response = new Response("ok");
  await expect(drainResponseBody(response)).resolves.toBe(response);
  expect(response.bodyUsed).toBe(true);
});

it("drainResponseBody accepts an empty response", async () => {
  const response = new Response(null, { status: 204 });
  await expect(drainResponseBody(response)).resolves.toBe(response);
  expect(response.bodyUsed).toBe(false);
});

it("drainResponseBody rejects a response over the bounded byte limit", async () => {
  const response = new Response(new Uint8Array(1024 * 1024 + 1));
  await expect(drainResponseBody(response)).rejects.toThrowError(
    "internal response exceeded byte limit",
  );
  expect(response.bodyUsed).toBe(true);
});
