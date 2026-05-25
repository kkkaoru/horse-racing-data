// Run with bun: `bun run test src/scripts/backfill-daily-race-entries-trend-columns.test.ts`
import { afterEach, expect, test, vi } from "vitest";

import {
  buildJobBody,
  enumerateDates,
  findFlag,
  parseOptions,
  postJob,
  requireScope,
  requireYyyymmdd,
  runBackfill,
  runWithLimit,
} from "./backfill-daily-race-entries-trend-columns";

afterEach(() => {
  vi.restoreAllMocks();
});

test("findFlag returns the value following a flag", () => {
  expect(findFlag(["--from", "20260501", "--to", "20260502"], "from")).toBe("20260501");
});

test("findFlag returns null when the flag is absent", () => {
  expect(findFlag(["--scope", "all"], "from")).toBeNull();
});

test("findFlag returns null when the flag is the last argv element", () => {
  expect(findFlag(["--from"], "from")).toBeNull();
});

test("requireYyyymmdd accepts an 8-digit date string", () => {
  expect(requireYyyymmdd("20260525", "--from")).toBe("20260525");
});

test("requireYyyymmdd rejects null with a labelled message", () => {
  expect(() => requireYyyymmdd(null, "--from")).toThrow("--from must match YYYYMMDD: ");
});

test("requireYyyymmdd rejects malformed dates", () => {
  expect(() => requireYyyymmdd("2026-05-25", "--to")).toThrow(
    "--to must match YYYYMMDD: 2026-05-25",
  );
});

test("requireScope defaults to all when null", () => {
  expect(requireScope(null)).toBe("all");
});

test("requireScope accepts jra, nar, ban-ei", () => {
  expect(requireScope("jra")).toBe("jra");
  expect(requireScope("nar")).toBe("nar");
  expect(requireScope("ban-ei")).toBe("ban-ei");
});

test("requireScope rejects unknown values", () => {
  expect(() => requireScope("global")).toThrow("unknown scope: global");
});

test("parseOptions reads from argv when origin and token flags are present", () => {
  expect(
    parseOptions({
      argv: [
        "--from",
        "20260501",
        "--to",
        "20260502",
        "--scope",
        "nar",
        "--origin",
        "https://x.example",
        "--token",
        "secret",
      ],
      env: {},
    }),
  ).toStrictEqual({
    fromDate: "20260501",
    origin: "https://x.example",
    scope: "nar",
    toDate: "20260502",
    token: "secret",
  });
});

test("parseOptions falls back to env for origin and token", () => {
  expect(
    parseOptions({
      argv: ["--from", "20260501", "--to", "20260501"],
      env: { REALTIME_ADMIN_TOKEN: "envtoken", SYNC_REALTIME_ORIGIN: "https://y.example" },
    }),
  ).toStrictEqual({
    fromDate: "20260501",
    origin: "https://y.example",
    scope: "all",
    toDate: "20260501",
    token: "envtoken",
  });
});

test("parseOptions requires origin via flag or env", () => {
  expect(() =>
    parseOptions({
      argv: ["--from", "20260501", "--to", "20260501", "--token", "secret"],
      env: {},
    }),
  ).toThrow("--origin or SYNC_REALTIME_ORIGIN must be set");
});

test("parseOptions requires token via flag or env", () => {
  expect(() =>
    parseOptions({
      argv: ["--from", "20260501", "--to", "20260501", "--origin", "https://x"],
      env: {},
    }),
  ).toThrow("--token or REALTIME_ADMIN_TOKEN must be set");
});

test("enumerateDates returns a single date when from equals to", () => {
  expect(enumerateDates("20260525", "20260525")).toStrictEqual(["20260525"]);
});

test("enumerateDates spans a calendar month boundary correctly", () => {
  expect(enumerateDates("20260430", "20260502")).toStrictEqual([
    "20260430",
    "20260501",
    "20260502",
  ]);
});

test("enumerateDates rejects reversed ranges", () => {
  expect(() => enumerateDates("20260502", "20260501")).toThrow(
    "--to 20260501 is before --from 20260502",
  );
});

test("enumerateDates rejects ranges over 366 days", () => {
  expect(() => enumerateDates("20240101", "20260102")).toThrow("date range too large");
});

test("buildJobBody constructs a build-daily-features job", () => {
  expect(buildJobBody("20260525", "jra")).toStrictEqual({
    date: "20260525",
    sourceScope: "jra",
    type: "build-daily-features",
  });
});

test("postJob posts JSON and bearer auth to /api/jobs", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response("{}", { status: 200 }));
  await postJob({
    body: buildJobBody("20260525", "all"),
    options: {
      fromDate: "20260525",
      origin: "https://example.test",
      scope: "all",
      toDate: "20260525",
      token: "abc",
    },
  });
  expect(fetchSpy).toHaveBeenCalledOnce();
  const call = fetchSpy.mock.calls[0];
  expect(call?.[0]).toBe("https://example.test/api/jobs");
  expect(call?.[1]?.method).toBe("POST");
  expect(call?.[1]?.headers).toStrictEqual({
    authorization: "Bearer abc",
    "content-type": "application/json",
  });
  expect(call?.[1]?.body).toBe(
    '{"date":"20260525","sourceScope":"all","type":"build-daily-features"}',
  );
});

test("postJob throws when the response is not ok", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("nope", { status: 500 }));
  await expect(
    postJob({
      body: buildJobBody("20260525", "all"),
      options: {
        fromDate: "20260525",
        origin: "https://example.test",
        scope: "all",
        toDate: "20260525",
        token: "abc",
      },
    }),
  ).rejects.toThrow("POST /api/jobs 20260525 failed: HTTP 500 nope");
});

test("runWithLimit invokes the handler exactly once per item", async () => {
  const seen: string[] = [];
  await runWithLimit({
    handler: async (item: string): Promise<void> => {
      seen.push(item);
    },
    items: ["a", "b", "c", "d"],
    limit: 2,
  });
  expect(seen.sort()).toStrictEqual(["a", "b", "c", "d"]);
});

test("runWithLimit completes when items is empty", async () => {
  const handler = vi.fn(async (): Promise<void> => undefined);
  await runWithLimit({ handler, items: [], limit: 4 });
  expect(handler).not.toHaveBeenCalled();
});

test("runBackfill posts one job per date and returns the count", async () => {
  vi.spyOn(console, "log").mockImplementation(() => undefined);
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response("{}", { status: 200 }));
  const count = await runBackfill({
    fromDate: "20260501",
    origin: "https://example.test",
    scope: "all",
    toDate: "20260503",
    token: "abc",
  });
  expect(count).toBe(3);
  expect(fetchSpy).toHaveBeenCalledTimes(3);
});
