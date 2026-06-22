// Run with: bun run --filter finish-position-cron test
import { expect, test, vi } from "vitest";
import {
  buildRaceKey,
  encodeRaceKey,
  fetchOddsForRace,
  fetchWeightForRace,
  sourceForCategory,
} from "./rescore-realtime";

const jsonResponse = (body: unknown): Response =>
  new Response(JSON.stringify(body), { status: 200 });

test("buildRaceKey splits runYmd into YYYY and MMDD segments", () => {
  expect(
    buildRaceKey({ keibajoCode: "05", raceBango: "11", runYmd: "20260614", source: "jra" }),
  ).toBe("jra:2026:0614:05:11");
});

test("buildRaceKey builds a nar-source key for a NAR race", () => {
  expect(
    buildRaceKey({ keibajoCode: "44", raceBango: "01", runYmd: "20260610", source: "nar" }),
  ).toBe("nar:2026:0610:44:01");
});

test("sourceForCategory maps jra to jra", () => {
  expect(sourceForCategory("jra")).toBe("jra");
});

test("sourceForCategory maps nar to nar", () => {
  expect(sourceForCategory("nar")).toBe("nar");
});

test("sourceForCategory maps ban-ei to nar", () => {
  expect(sourceForCategory("ban-ei")).toBe("nar");
});

test("encodeRaceKey percent-encodes the colons", () => {
  expect(encodeRaceKey("jra:2026:0614:05:11")).toBe("jra%3A2026%3A0614%3A05%3A11");
});

test("fetchOddsForRace parses latest.tansho into an umaban-keyed Map", async () => {
  const fetchImpl = vi.fn(async () =>
    jsonResponse({
      latest: {
        tansho: [
          { combination: "1", odds: 3.5, rank: 2 },
          { combination: "2", odds: 2.1, rank: 1 },
        ],
      },
    }),
  );
  const result = await fetchOddsForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.get(1)).toStrictEqual({ tanshoNinkijun: 2, tanshoOdds: 3.5 });
  expect(result.get(2)).toStrictEqual({ tanshoNinkijun: 1, tanshoOdds: 2.1 });
});

test("fetchOddsForRace requests the hot worker with the UA + Accept headers", async () => {
  const fetchImpl = vi.fn(async () => jsonResponse({ latest: { tansho: [] } }));
  await fetchOddsForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  const call = fetchImpl.mock.calls[0] as unknown as [string, { headers: Record<string, string> }];
  expect(call[0]).toBe(
    "https://sync-realtime-data-hot.kkk4oru.com/api/odds/jra%3A2026%3A0614%3A05%3A11",
  );
  expect(call[1].headers["User-Agent"]).toBe("horse-racing-data-predict/1.0");
  expect(call[1].headers.Accept).toBe("application/json");
});

test("fetchOddsForRace returns an empty Map when latest is absent", async () => {
  const fetchImpl = vi.fn(async () => jsonResponse({ other: true }));
  const result = await fetchOddsForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.size).toBe(0);
});

test("fetchOddsForRace returns an empty Map when the response is a bare JSON number", async () => {
  const fetchImpl = vi.fn(async () => jsonResponse(42));
  const result = await fetchOddsForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.size).toBe(0);
});

test("fetchOddsForRace returns an empty Map when latest.tansho is missing", async () => {
  const fetchImpl = vi.fn(async () => jsonResponse({ latest: { other: 1 } }));
  const result = await fetchOddsForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.size).toBe(0);
});

test("fetchOddsForRace skips entries with missing fields", async () => {
  const fetchImpl = vi.fn(async () =>
    jsonResponse({
      latest: {
        tansho: [
          { combination: "1", odds: null, rank: 1 },
          { combination: null, odds: 2, rank: 2 },
          { combination: "3", odds: 2, rank: null },
          { combination: "4", odds: 5.5, rank: 3 },
        ],
      },
    }),
  );
  const result = await fetchOddsForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.size).toBe(1);
  expect(result.get(4)).toStrictEqual({ tanshoNinkijun: 3, tanshoOdds: 5.5 });
});

test("fetchOddsForRace parses a string odds value and skips a non-numeric string odds", async () => {
  const fetchImpl = vi.fn(async () =>
    jsonResponse({
      latest: {
        tansho: [
          { combination: "1", odds: "4.2", rank: 1 },
          { combination: "2", odds: "abc", rank: 2 },
        ],
      },
    }),
  );
  const result = await fetchOddsForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.size).toBe(1);
  expect(result.get(1)).toStrictEqual({ tanshoNinkijun: 1, tanshoOdds: 4.2 });
});

test("fetchOddsForRace skips an entry whose odds is a non-finite number", async () => {
  const fetchImpl = vi.fn(async () =>
    jsonResponse({ latest: { tansho: [{ combination: "1", odds: "Infinity", rank: 1 }] } }),
  );
  const result = await fetchOddsForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.size).toBe(0);
});

test("fetchOddsForRace skips an entry whose odds is a non-string non-number", async () => {
  const fetchImpl = vi.fn(async () =>
    jsonResponse({ latest: { tansho: [{ combination: "1", odds: true, rank: 1 }] } }),
  );
  const result = await fetchOddsForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.size).toBe(0);
});

test("fetchOddsForRace skips an entry whose umaban is a non-string non-number", async () => {
  const fetchImpl = vi.fn(async () =>
    jsonResponse({ latest: { tansho: [{ combination: true, odds: 3, rank: 1 }] } }),
  );
  const result = await fetchOddsForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.size).toBe(0);
});

test("fetchOddsForRace skips an entry whose combination string does not parse", async () => {
  const fetchImpl = vi.fn(async () =>
    jsonResponse({ latest: { tansho: [{ combination: "n/a", odds: 3, rank: 1 }] } }),
  );
  const result = await fetchOddsForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.size).toBe(0);
});

test("fetchOddsForRace skips entries with odds at or below zero", async () => {
  const fetchImpl = vi.fn(async () =>
    jsonResponse({ latest: { tansho: [{ combination: "1", odds: 0, rank: 1 }] } }),
  );
  const result = await fetchOddsForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.size).toBe(0);
});

test("fetchOddsForRace ignores non-object tansho entries", async () => {
  const fetchImpl = vi.fn(async () =>
    jsonResponse({ latest: { tansho: ["bad", { combination: "2", odds: 4, rank: 1 }] } }),
  );
  const result = await fetchOddsForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.size).toBe(1);
  expect(result.get(2)).toStrictEqual({ tanshoNinkijun: 1, tanshoOdds: 4 });
});

test("fetchOddsForRace returns an empty Map after exhausting retries on a thrown error", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const fetchImpl = vi.fn(async () => {
    throw new Error("network down");
  });
  const result = await fetchOddsForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.size).toBe(0);
  expect(fetchImpl).toHaveBeenCalledTimes(3);
  warnSpy.mockRestore();
});

test("fetchOddsForRace retries once then succeeds on the second attempt", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const fetchImpl = vi
    .fn()
    .mockRejectedValueOnce(new Error("transient"))
    .mockResolvedValueOnce(
      jsonResponse({ latest: { tansho: [{ combination: "1", odds: 3, rank: 1 }] } }),
    );
  const result = await fetchOddsForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.get(1)).toStrictEqual({ tanshoNinkijun: 1, tanshoOdds: 3 });
  expect(fetchImpl).toHaveBeenCalledTimes(2);
  warnSpy.mockRestore();
});

test("fetchOddsForRace treats a non-ok HTTP status as a retryable error", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const fetchImpl = vi.fn(async () => new Response("forbidden", { status: 403 }));
  const result = await fetchOddsForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.size).toBe(0);
  expect(fetchImpl).toHaveBeenCalledTimes(3);
  warnSpy.mockRestore();
});

test("fetchWeightForRace parses horses into an umaban-keyed bataiju Map", async () => {
  const fetchImpl = vi.fn(async () =>
    jsonResponse({
      horses: [
        { horseNumber: "1", weight: 484 },
        { horseNumber: "2", weight: 470 },
      ],
    }),
  );
  const result = await fetchWeightForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.get(1)).toBe(484);
  expect(result.get(2)).toBe(470);
});

test("fetchWeightForRace requests the weight worker URL", async () => {
  const fetchImpl = vi.fn(async () => jsonResponse({ horses: [] }));
  await fetchWeightForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  const call = fetchImpl.mock.calls[0] as unknown as [string];
  expect(call[0]).toBe(
    "https://sync-realtime-data.kkk4oru.com/api/horse-weight/jra%3A2026%3A0614%3A05%3A11",
  );
});

test("fetchWeightForRace parses a string horseNumber and weight", async () => {
  const fetchImpl = vi.fn(async () =>
    jsonResponse({ horses: [{ horseNumber: "3", weight: "458" }] }),
  );
  const result = await fetchWeightForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.get(3)).toBe(458);
});

test("fetchWeightForRace skips a horse with a non-parseable string weight", async () => {
  const fetchImpl = vi.fn(async () =>
    jsonResponse({ horses: [{ horseNumber: "3", weight: "n/a" }] }),
  );
  const result = await fetchWeightForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.size).toBe(0);
});

test("fetchWeightForRace skips horses with non-positive weight", async () => {
  const fetchImpl = vi.fn(async () =>
    jsonResponse({
      horses: [
        { horseNumber: "1", weight: 0 },
        { horseNumber: "2", weight: 466 },
      ],
    }),
  );
  const result = await fetchWeightForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.size).toBe(1);
  expect(result.get(2)).toBe(466);
});

test("fetchWeightForRace skips horses with missing fields and non-object entries", async () => {
  const fetchImpl = vi.fn(async () =>
    jsonResponse({
      horses: ["bad", { horseNumber: null, weight: 460 }, { horseNumber: "5", weight: null }],
    }),
  );
  const result = await fetchWeightForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.size).toBe(0);
});

test("fetchWeightForRace returns an empty Map when the response is a bare JSON string", async () => {
  const fetchImpl = vi.fn(async () => jsonResponse("nope"));
  const result = await fetchWeightForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.size).toBe(0);
});

test("fetchWeightForRace returns an empty Map when horses is missing", async () => {
  const fetchImpl = vi.fn(async () => jsonResponse({ other: 1 }));
  const result = await fetchWeightForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.size).toBe(0);
});

test("fetchWeightForRace returns an empty Map after a thrown error", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const fetchImpl = vi.fn(async () => {
    throw new Error("timeout");
  });
  const result = await fetchWeightForRace({
    fetchImpl: fetchImpl as unknown as typeof fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260614",
    source: "jra",
  });
  expect(result.size).toBe(0);
  warnSpy.mockRestore();
});
