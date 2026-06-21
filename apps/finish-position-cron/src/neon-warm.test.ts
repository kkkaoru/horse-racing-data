// Run with bun. Tests for the Neon pre-wake warm-up helper.

import { afterEach, beforeEach, expect, test, vi } from "vitest";

const { queryMock, neonMock } = vi.hoisted(() => {
  const query = vi.fn(async () => [{ "?column?": 1 }]);
  return { neonMock: vi.fn(() => ({ query })), queryMock: query };
});

vi.mock("@neondatabase/serverless", () => ({
  neon: neonMock,
}));

import { warmNeon } from "./neon-warm";

beforeEach(() => {
  neonMock.mockClear();
  queryMock.mockClear();
});

afterEach(() => {
  vi.restoreAllMocks();
});

test("warmNeon calls neon() with the provided URL and issues SELECT 1", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

  await warmNeon("postgres://example");

  expect(neonMock).toHaveBeenCalledTimes(1);
  expect(neonMock).toHaveBeenCalledWith("postgres://example");
  expect(queryMock).toHaveBeenCalledTimes(1);
  expect(queryMock).toHaveBeenCalledWith("SELECT 1");
  expect(consoleSpy).toHaveBeenCalledWith("neon warm ok");
});

test("warmNeon does not throw when sql.query() throws", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  queryMock.mockRejectedValueOnce(new Error("connection refused"));

  await expect(warmNeon("postgres://example")).resolves.toBeUndefined();
  expect(warnSpy).toHaveBeenCalledTimes(1);
});

test("warmNeon logs a warning message (not the URL) on error", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  queryMock.mockRejectedValueOnce(new Error("timeout"));

  await warmNeon("postgres://secret-url");

  const [label, detail] = warnSpy.mock.calls[0] as [string, string];
  expect(label).toBe("neon warm failed");
  expect(detail).toBe("Error: timeout");
});
