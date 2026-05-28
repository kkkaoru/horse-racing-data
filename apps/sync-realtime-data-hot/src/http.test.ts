// Run with bun.
import { expect, it } from "vitest";

import { jsonResponse, mergeJsonHeaders } from "./http";

it("mergeJsonHeaders returns a default JSON content-type when init undefined", () => {
  const headers = mergeJsonHeaders();
  expect(headers.get("content-type")).toBe("application/json; charset=utf-8");
});

it("mergeJsonHeaders returns a default JSON content-type when init.headers absent", () => {
  const headers = mergeJsonHeaders({ status: 200 });
  expect(headers.get("content-type")).toBe("application/json; charset=utf-8");
});

it("mergeJsonHeaders merges caller-provided headers", () => {
  const headers = mergeJsonHeaders({ headers: { "x-test": "1" } });
  expect(headers.get("x-test")).toBe("1");
  expect(headers.get("content-type")).toBe("application/json; charset=utf-8");
});

it("jsonResponse defaults to status 200", async () => {
  const response = jsonResponse({ ok: true });
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ ok: true });
});

it("jsonResponse honors init status", () => {
  expect(jsonResponse({ error: "x" }, { status: 400 }).status).toBe(400);
});
