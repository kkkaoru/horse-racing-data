// Run with bun (vitest).
import { expect, it, vi } from "vitest";

import {
  buildHeatmapWarmInstances,
  isHeatmapWarmWorkflowParams,
  runHeatmapWarmWorkflow,
  startHeatmapWarmWorkflows,
  warmHeatmapRace,
  type HeatmapWarmInstance,
  type HeatmapWarmRaceListItem,
  type HeatmapWarmStepConfig,
  type HeatmapWarmStep,
} from "./win-rate-heatmap-warm-workflow";

const heatmapResponse = (status: number, cacheHeader: string | null): Response =>
  new Response("{}", {
    headers: cacheHeader === null ? {} : { "X-Win-Rate-Heatmap-Cache": cacheHeader },
    status,
  });

const passThroughStep: HeatmapWarmStep = {
  do: (_name, _config, callback) => callback(),
};

it("accepts valid workflow params", () => {
  expect(
    isHeatmapWarmWorkflowParams({
      day: "23",
      keibajoCode: "30",
      month: "09",
      raceNumbers: ["01", "12"],
      source: "nar",
      year: "2026",
    }),
  ).toBe(true);
});

it("accepts jra workflow params", () => {
  expect(
    isHeatmapWarmWorkflowParams({
      day: "23",
      keibajoCode: "05",
      month: "09",
      raceNumbers: [],
      source: "jra",
      year: "2026",
    }),
  ).toBe(true);
});

it("rejects non-object workflow params", () => {
  expect(isHeatmapWarmWorkflowParams(null)).toBe(false);
  expect(isHeatmapWarmWorkflowParams([])).toBe(false);
  expect(isHeatmapWarmWorkflowParams("x")).toBe(false);
});

it("rejects workflow params with a bad year", () => {
  expect(
    isHeatmapWarmWorkflowParams({
      day: "23",
      keibajoCode: "30",
      month: "09",
      raceNumbers: ["01"],
      source: "nar",
      year: 2026,
    }),
  ).toBe(false);
  expect(
    isHeatmapWarmWorkflowParams({
      day: "23",
      keibajoCode: "30",
      month: "09",
      raceNumbers: ["01"],
      source: "nar",
      year: "26",
    }),
  ).toBe(false);
});

it("rejects workflow params with a bad month or day", () => {
  expect(
    isHeatmapWarmWorkflowParams({
      day: "23",
      keibajoCode: "30",
      month: "9",
      raceNumbers: ["01"],
      source: "nar",
      year: "2026",
    }),
  ).toBe(false);
  expect(
    isHeatmapWarmWorkflowParams({
      day: 23,
      keibajoCode: "30",
      month: "09",
      raceNumbers: ["01"],
      source: "nar",
      year: "2026",
    }),
  ).toBe(false);
});

it("rejects workflow params with a bad keibajo code", () => {
  expect(
    isHeatmapWarmWorkflowParams({
      day: "23",
      keibajoCode: 30,
      month: "09",
      raceNumbers: ["01"],
      source: "nar",
      year: "2026",
    }),
  ).toBe(false);
  expect(
    isHeatmapWarmWorkflowParams({
      day: "23",
      keibajoCode: "3",
      month: "09",
      raceNumbers: ["01"],
      source: "nar",
      year: "2026",
    }),
  ).toBe(false);
});

it("rejects workflow params with a bad source", () => {
  expect(
    isHeatmapWarmWorkflowParams({
      day: "23",
      keibajoCode: "30",
      month: "09",
      raceNumbers: ["01"],
      source: "ban-ei",
      year: "2026",
    }),
  ).toBe(false);
});

it("rejects workflow params with bad race numbers", () => {
  expect(
    isHeatmapWarmWorkflowParams({
      day: "23",
      keibajoCode: "30",
      month: "09",
      raceNumbers: "01",
      source: "nar",
      year: "2026",
    }),
  ).toBe(false);
  expect(
    isHeatmapWarmWorkflowParams({
      day: "23",
      keibajoCode: "30",
      month: "09",
      raceNumbers: ["1"],
      source: "nar",
      year: "2026",
    }),
  ).toBe(false);
});

it("skips the warm when the heatmap cache already hits", async () => {
  const fetchSelf = vi.fn<(request: Request) => Promise<Response>>(async () =>
    heatmapResponse(200, "HIT"),
  );
  const status = await warmHeatmapRace(
    fetchSelf,
    {
      day: "23",
      keibajoCode: "30",
      month: "09",
      raceNumbers: ["01"],
      source: "nar",
      year: "2026",
    },
    "01",
  );
  expect(status).toBe("hit");
  expect(fetchSelf).toHaveBeenCalledTimes(1);
  const request = fetchSelf.mock.calls[0]?.[0];
  expect(request?.url).toBe(
    "https://pc-keiba-viewer.local/api/races/2026/09/23/30/01/sections/win-rate-heatmap",
  );
  expect(request?.headers.get("X-PC-Keiba-Cache-Warm")).toBe("workflow");
});

it("does not read heatmap bodies larger than the bounded drain limit", async () => {
  const largeBody = new Response("x".repeat(2 * 1024 * 1024), {
    headers: { "X-Win-Rate-Heatmap-Cache": "HIT" },
  });
  const status = await warmHeatmapRace(
    async () => largeBody,
    {
      day: "23",
      keibajoCode: "30",
      month: "09",
      raceNumbers: ["01"],
      source: "nar",
      year: "2026",
    },
    "01",
  );
  expect(status).toBe("hit");
});

it("accepts responses without a body", async () => {
  const status = await warmHeatmapRace(
    async () => new Response(null, { headers: { "X-Win-Rate-Heatmap-Cache": "HIT" } }),
    {
      day: "23",
      keibajoCode: "30",
      month: "09",
      raceNumbers: ["01"],
      source: "nar",
      year: "2026",
    },
    "01",
  );
  expect(status).toBe("hit");
});

it("warms and stores the heatmap on a cache miss", async () => {
  const fetchSelf = vi
    .fn<(request: Request) => Promise<Response>>()
    .mockResolvedValueOnce(heatmapResponse(503, "MISS"))
    .mockResolvedValueOnce(heatmapResponse(200, "MISS-STORED"));
  const status = await warmHeatmapRace(
    fetchSelf,
    {
      day: "23",
      keibajoCode: "30",
      month: "09",
      raceNumbers: ["01"],
      source: "nar",
      year: "2026",
    },
    "01",
  );
  expect(status).toBe("stored");
  expect(fetchSelf.mock.calls[1]?.[0].url).toBe(
    "https://pc-keiba-viewer.local/api/races/2026/09/23/30/01/sections/win-rate-heatmap?__cacheWarm=1",
  );
});

it("treats a 200 read without a HIT header as a miss", async () => {
  const fetchSelf = vi
    .fn<(request: Request) => Promise<Response>>()
    .mockResolvedValueOnce(heatmapResponse(200, null))
    .mockResolvedValueOnce(heatmapResponse(200, "HIT"));
  const status = await warmHeatmapRace(
    fetchSelf,
    {
      day: "23",
      keibajoCode: "30",
      month: "09",
      raceNumbers: ["01"],
      source: "nar",
      year: "2026",
    },
    "01",
  );
  expect(status).toBe("stored");
  expect(fetchSelf).toHaveBeenCalledTimes(2);
});

it("throws when the warm response did not store the cache", async () => {
  const fetchSelf = vi
    .fn<(request: Request) => Promise<Response>>()
    .mockResolvedValueOnce(heatmapResponse(503, "MISS"))
    .mockResolvedValueOnce(heatmapResponse(503, null));
  await expect(
    warmHeatmapRace(
      fetchSelf,
      {
        day: "23",
        keibajoCode: "30",
        month: "09",
        raceNumbers: ["01"],
        source: "nar",
        year: "2026",
      },
      "01",
    ),
  ).rejects.toThrow(
    "heatmap warm not stored: 503 missing /api/races/2026/09/23/30/01/sections/win-rate-heatmap",
  );
});

it("throws when a 200 warm response has an unexpected cache header", async () => {
  const fetchSelf = vi
    .fn<(request: Request) => Promise<Response>>()
    .mockResolvedValueOnce(heatmapResponse(503, "MISS"))
    .mockResolvedValueOnce(heatmapResponse(200, "MISS"));
  await expect(
    warmHeatmapRace(
      fetchSelf,
      {
        day: "23",
        keibajoCode: "30",
        month: "09",
        raceNumbers: ["01"],
        source: "nar",
        year: "2026",
      },
      "01",
    ),
  ).rejects.toThrow(
    "heatmap warm not stored: 200 MISS /api/races/2026/09/23/30/01/sections/win-rate-heatmap",
  );
});

it("runs one durable step per race in order", async () => {
  const names: string[] = [];
  const configs: HeatmapWarmStepConfig[] = [];
  const step: HeatmapWarmStep = {
    do: (name, config, callback) => {
      names.push(name);
      configs.push(config);
      return callback();
    },
  };
  const fetchSelf = vi.fn<(request: Request) => Promise<Response>>(async () =>
    heatmapResponse(200, "HIT"),
  );
  const results = await runHeatmapWarmWorkflow({
    fetchSelf,
    params: {
      day: "23",
      keibajoCode: "30",
      month: "09",
      raceNumbers: ["01", "02"],
      source: "nar",
      year: "2026",
    },
    step,
  });
  expect(results).toStrictEqual([
    { raceNumber: "01", status: "hit" },
    { raceNumber: "02", status: "hit" },
  ]);
  expect(names).toStrictEqual(["warm-30-01", "warm-30-02"]);
  expect(configs[0]).toStrictEqual({
    retries: { backoff: "exponential", delay: "30 seconds", limit: 4 },
    timeout: "5 minutes",
  });
});

it("continues with the next race when a step exhausts its retries", async () => {
  const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const step: HeatmapWarmStep = {
    do: (name, _config, callback) =>
      name === "warm-30-01" ? Promise.reject(new Error("step failed")) : callback(),
  };
  const results = await runHeatmapWarmWorkflow({
    fetchSelf: async () => heatmapResponse(200, "HIT"),
    params: {
      day: "23",
      keibajoCode: "30",
      month: "09",
      raceNumbers: ["01", "02"],
      source: "nar",
      year: "2026",
    },
    step,
  });
  expect(results).toStrictEqual([
    { raceNumber: "01", status: "failed" },
    { raceNumber: "02", status: "hit" },
  ]);
  expect(consoleError).toHaveBeenCalledWith(
    "[pc-keiba-viewer] heatmap warm workflow step failed",
    "step failed",
  );
  consoleError.mockRestore();
});

it("logs non-Error step failures as strings", async () => {
  const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const results = await runHeatmapWarmWorkflow({
    fetchSelf: async () => heatmapResponse(200, "HIT"),
    params: {
      day: "23",
      keibajoCode: "30",
      month: "09",
      raceNumbers: ["01"],
      source: "nar",
      year: "2026",
    },
    step: {
      do: () => Promise.reject(new Error("boom").message),
    },
  });
  expect(results).toStrictEqual([{ raceNumber: "01", status: "failed" }]);
  expect(consoleError).toHaveBeenCalledWith(
    "[pc-keiba-viewer] heatmap warm workflow step failed",
    "boom",
  );
  consoleError.mockRestore();
});

it("rejects invalid workflow payloads", async () => {
  await expect(
    runHeatmapWarmWorkflow({
      fetchSelf: async () => heatmapResponse(200, "HIT"),
      params: { year: "2026" },
      step: passThroughStep,
    }),
  ).rejects.toThrow("invalid heatmap warm workflow params");
});

it("builds one instance per venue with sorted unique race numbers", () => {
  const instances = buildHeatmapWarmInstances({
    date: { day: "23", month: "09", year: "2026" },
    nowMs: 1_790_117_126_705,
    races: [
      { keibajoCode: "30", raceBango: "02", source: "nar" },
      { keibajoCode: "42", raceBango: "01", source: "nar" },
      { keibajoCode: "30", raceBango: "01", source: "nar" },
      { keibajoCode: "30", raceBango: "02", source: "nar" },
    ],
  });
  expect(instances).toStrictEqual([
    {
      id: "heatmap-20260923-30-16mqj",
      params: {
        day: "23",
        keibajoCode: "30",
        month: "09",
        raceNumbers: ["01", "02"],
        source: "nar",
        year: "2026",
      },
    },
    {
      id: "heatmap-20260923-42-16mqj",
      params: {
        day: "23",
        keibajoCode: "42",
        month: "09",
        raceNumbers: ["01"],
        source: "nar",
        year: "2026",
      },
    },
  ]);
});

it("keeps the same instance id inside one 15 minute slot", () => {
  const first = buildHeatmapWarmInstances({
    date: { day: "23", month: "09", year: "2026" },
    nowMs: 1_790_117_100_000,
    races: [{ keibajoCode: "30", raceBango: "01", source: "nar" }],
  });
  const second = buildHeatmapWarmInstances({
    date: { day: "23", month: "09", year: "2026" },
    nowMs: 1_790_117_700_000,
    races: [{ keibajoCode: "30", raceBango: "01", source: "nar" }],
  });
  const next = buildHeatmapWarmInstances({
    date: { day: "23", month: "09", year: "2026" },
    nowMs: 1_790_118_000_000,
    races: [{ keibajoCode: "30", raceBango: "01", source: "nar" }],
  });
  expect(first[0]?.id).toBe("heatmap-20260923-30-16mqj");
  expect(second[0]?.id).toBe("heatmap-20260923-30-16mqj");
  expect(next[0]?.id).toBe("heatmap-20260923-30-16mqk");
});

it("creates workflow instances in batches of at most 100", async () => {
  const createBatch = vi.fn<(batch: HeatmapWarmInstance[]) => Promise<unknown[]>>(async () => []);
  const venueRaces: HeatmapWarmRaceListItem[] = Array.from({ length: 101 }, (_value, index) => ({
    keibajoCode: `${String.fromCharCode(65 + Math.floor(index / 10))}${index % 10}`,
    raceBango: "01",
    source: "nar",
  }));
  const result = await startHeatmapWarmWorkflows({
    date: { day: "23", month: "09", year: "2026" },
    nowMs: 0,
    races: venueRaces,
    workflow: { createBatch },
  });
  expect(result.raceCount).toBe(101);
  expect(result.instanceIds.length).toBe(101);
  expect(createBatch).toHaveBeenCalledTimes(2);
  expect(createBatch.mock.calls[0]?.[0].length).toBe(100);
  expect(createBatch.mock.calls[1]?.[0].length).toBe(1);
});

it("does not call createBatch when there are no races", async () => {
  const createBatch = vi.fn<(batch: HeatmapWarmInstance[]) => Promise<unknown[]>>(async () => []);
  const result = await startHeatmapWarmWorkflows({
    date: { day: "23", month: "09", year: "2026" },
    nowMs: 0,
    races: [],
    workflow: { createBatch },
  });
  expect(result).toStrictEqual({ instanceIds: [], raceCount: 0 });
  expect(createBatch).not.toHaveBeenCalled();
});
