// run with: bun run test
import { expect, it } from "vitest";
import { mergeJsonHeaders } from "./http";

it("returns json content-type when no init headers provided", () => {
  const headers = mergeJsonHeaders();
  expect(headers.get("content-type")).toBe("application/json; charset=utf-8");
});

it("returns json content-type when init has undefined headers", () => {
  const headers = mergeJsonHeaders({ status: 200 });
  expect(headers.get("content-type")).toBe("application/json; charset=utf-8");
});

it("merges additional headers from a Headers instance", () => {
  const init = { headers: new Headers({ "x-trace": "abc" }) };
  const headers = mergeJsonHeaders(init);
  expect(headers.get("content-type")).toBe("application/json; charset=utf-8");
  expect(headers.get("x-trace")).toBe("abc");
});

it("merges headers from a plain record", () => {
  const headers = mergeJsonHeaders({ headers: { "x-source": "test" } });
  expect(headers.get("x-source")).toBe("test");
  expect(headers.get("content-type")).toBe("application/json; charset=utf-8");
});

it("allows the caller to override the content-type", () => {
  const headers = mergeJsonHeaders({ headers: { "content-type": "text/plain" } });
  expect(headers.get("content-type")).toBe("text/plain");
});
