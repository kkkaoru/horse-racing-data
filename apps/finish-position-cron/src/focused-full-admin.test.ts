// Run with bun. Tests for the focused-full operator routes.

import { expect, test, vi } from "vitest";
import { parseClearWorkKeys, routeFocusedFullAdmin } from "./focused-full-admin";
import type { Env } from "./types";

interface CapturedRequest {
  body: string;
  method: string;
  url: string;
}

const makeEnv = (
  captured: CapturedRequest[],
): Pick<Env, "PREDICT_RUN_COORDINATOR" | "TRIGGER_TOKEN"> => {
  const fetchMock = vi.fn(async (request: Request) => {
    captured.push({ body: await request.text(), method: request.method, url: request.url });
    return Response.json({ forwarded: true });
  });
  return {
    PREDICT_RUN_COORDINATOR: {
      get: vi.fn(() => ({ fetch: fetchMock })),
      idFromName: vi.fn(() => "coordinator-id"),
    } as unknown as Env["PREDICT_RUN_COORDINATOR"],
    TRIGGER_TOKEN: "secret",
  };
};

test("parseClearWorkKeys accepts focused-full work keys", () => {
  expect(
    parseClearWorkKeys({
      workKeys: ["focused-full:20261017:jra:04:01", "focused-full:20261017:ban-ei:83:12"],
    }),
  ).toStrictEqual(["focused-full:20261017:jra:04:01", "focused-full:20261017:ban-ei:83:12"]);
});

test("parseClearWorkKeys rejects a missing body", () => {
  expect(parseClearWorkKeys(null)).toBeNull();
});

test("parseClearWorkKeys rejects a body without workKeys", () => {
  expect(parseClearWorkKeys({ keys: [] })).toBeNull();
});

test("parseClearWorkKeys rejects an empty list", () => {
  expect(parseClearWorkKeys({ workKeys: [] })).toBeNull();
});

test("parseClearWorkKeys rejects a non-array value", () => {
  expect(parseClearWorkKeys({ workKeys: "focused-full:20261017:jra:04:01" })).toBeNull();
});

test("parseClearWorkKeys rejects more than 100 keys", () => {
  expect(
    parseClearWorkKeys({
      workKeys: Array.from({ length: 101 }, () => "focused-full:20261017:jra:04:01"),
    }),
  ).toBeNull();
});

test("parseClearWorkKeys rejects a key outside the focused-full namespace", () => {
  expect(parseClearWorkKeys({ workKeys: ["day-base:20261017:jra"] })).toBeNull();
});

test("routeFocusedFullAdmin ignores unrelated paths", () => {
  expect(
    routeFocusedFullAdmin(new Request("https://x/api/other"), "/api/other", makeEnv([])),
  ).toBeNull();
});

test("routeFocusedFullAdmin ignores a wrong method on a known path", () => {
  expect(
    routeFocusedFullAdmin(
      new Request("https://x/api/admin/focused-full-diagnostics", { method: "POST" }),
      "/api/admin/focused-full-diagnostics",
      makeEnv([]),
    ),
  ).toBeNull();
});

test("diagnostics rejects a missing token", async () => {
  const response = await routeFocusedFullAdmin(
    new Request("https://x/api/admin/focused-full-diagnostics"),
    "/api/admin/focused-full-diagnostics",
    makeEnv([]),
  );
  expect(response?.status).toBe(401);
});

test("diagnostics forwards to the coordinator", async () => {
  const captured: CapturedRequest[] = [];
  const response = await routeFocusedFullAdmin(
    new Request("https://x/api/admin/focused-full-diagnostics", {
      headers: { authorization: "Bearer secret" },
    }),
    "/api/admin/focused-full-diagnostics",
    makeEnv(captured),
  );
  expect(await response?.json()).toStrictEqual({ forwarded: true });
  expect(captured).toStrictEqual([
    { body: "", method: "GET", url: "http://do/__admin/focused-full-diagnostics" },
  ]);
});

test("clear commits rejects a missing token", async () => {
  const response = await routeFocusedFullAdmin(
    new Request("https://x/api/admin/clear-focused-full-commits", { method: "POST" }),
    "/api/admin/clear-focused-full-commits",
    makeEnv([]),
  );
  expect(response?.status).toBe(401);
});

test("clear commits rejects an unparseable body", async () => {
  const response = await routeFocusedFullAdmin(
    new Request("https://x/api/admin/clear-focused-full-commits", {
      body: "not-json",
      headers: { authorization: "Bearer secret" },
      method: "POST",
    }),
    "/api/admin/clear-focused-full-commits",
    makeEnv([]),
  );
  expect(response?.status).toBe(400);
});

test("clear commits forwards validated work keys", async () => {
  const captured: CapturedRequest[] = [];
  const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const response = await routeFocusedFullAdmin(
    new Request("https://x/api/admin/clear-focused-full-commits", {
      body: JSON.stringify({ workKeys: ["focused-full:20261017:jra:04:01"] }),
      headers: { authorization: "Bearer secret" },
      method: "POST",
    }),
    "/api/admin/clear-focused-full-commits",
    makeEnv(captured),
  );
  expect(await response?.json()).toStrictEqual({ forwarded: true });
  expect(captured).toStrictEqual([
    {
      body: '{"workKeys":["focused-full:20261017:jra:04:01"]}',
      method: "POST",
      url: "http://do/__admin/clear-focused-full-commits",
    },
  ]);
  warn.mockRestore();
});
