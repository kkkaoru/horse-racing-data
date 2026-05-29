// Run with: bun run --filter sync-realtime-data-features test
import { expect, it } from "vitest";

import { jsonResponse } from "./http";

it("serialises payload as JSON with content-type header", async () => {
  const response = jsonResponse({ ok: true });
  expect(response.status).toBe(200);
  expect(response.headers.get("content-type")).toBe("application/json; charset=utf-8");
  await expect(response.text()).resolves.toBe('{"ok":true}');
});

it("respects status override and merges extra headers", () => {
  const response = jsonResponse({ error: "x" }, { status: 404, headers: { "x-custom": "v" } });
  expect(response.status).toBe(404);
  expect(response.headers.get("x-custom")).toBe("v");
});
