// run with: bun run test
import { afterEach, expect, it, vi } from "vitest";
import type { RunningStyleDateProgressRow } from "../src/running-style-date-progress";
import type { Env } from "../src/types";
import {
  formatRunningStyleDateProgressLine,
  parseRunningStyleDateCliArgs,
  printIncompleteRows,
  processIncompleteRaces,
  run,
  toPredictionJob,
} from "./run-running-style-date";

vi.mock("../src/running-style-queue", () => ({
  handleRunningStylePredictionJob: vi.fn(async () => ({
    cacheWritten: true,
    featuresR2Key: "tmp/key",
    writtenCount: 4,
  })),
}));

vi.mock("wrangler", () => ({
  getPlatformProxy: vi.fn(async () => ({
    cf: {},
    ctx: { passThroughOnException: () => {}, waitUntil: () => {} },
    dispose: vi.fn(async () => undefined),
    env: {} as Env,
  })),
}));

vi.mock("../src/running-style-race-list", () => ({
  listRunningStyleRacesByDate: vi.fn(async () => ({ races: [], source: "d1" })),
}));

vi.mock("../src/running-style-cron", () => ({
  planRunningStylePredictionsForDate: vi.fn(async () => ({
    completed: 0,
    enqueued: 0,
    missingFeatures: 0,
    scanned: 0,
  })),
  refreshViewerRunningStyleCachesForDate: vi.fn(async () => ({
    refreshed: 0,
    scanned: 0,
    skipped: 0,
  })),
}));

vi.mock("../src/running-style-date-progress", async () => {
  const actual =
    await vi.importActual<typeof import("../src/running-style-date-progress")>(
      "../src/running-style-date-progress",
    );
  return {
    ...actual,
    collectRunningStyleDateProgress: vi.fn(async () => []),
  };
});

vi.mock("../src/running-style-model-register", async () => {
  const actual =
    await vi.importActual<typeof import("../src/running-style-model-register")>(
      "../src/running-style-model-register",
    );
  return {
    ...actual,
    ensureRunningStyleModels: vi.fn(async () => ({ registered: [], synced: [] })),
    listRequiredRunningStyleModelSources: vi.fn(() => []),
  };
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

it("parseRunningStyleDateCliArgs accepts --register-model flags", () => {
  const args = parseRunningStyleDateCliArgs(
    [
      "--date",
      "20260524",
      "--register-model",
      "jra:tmp/model.flatbin",
      "--register-model",
      "nar:tmp/nar.flatbin",
    ],
    new Date(),
  );
  expect(args.registerModels.length).toBe(2);
  expect(args.registerModels[0]!.source).toBe("jra");
  expect(args.registerModels[1]!.source).toBe("nar");
});

it("parseRunningStyleDateCliArgs handles --remote-models flag", () => {
  const args = parseRunningStyleDateCliArgs(
    [
      "--date",
      "20260524",
      "--register-model",
      "jra:tmp/model.flatbin",
      "--remote-models",
    ],
    new Date(),
  );
  expect(args.remoteModels).toBe(true);
  expect(args.registerModels[0]!.remote).toBe(true);
});

it("parseRunningStyleDateCliArgs handles --no-ensure-models and --no-sync-models flags", () => {
  const args = parseRunningStyleDateCliArgs(
    ["--date", "20260524", "--no-ensure-models", "--no-sync-models"],
    new Date(),
  );
  expect(args.ensureModels).toBe(false);
  expect(args.syncModels).toBe(false);
});

it("parseRunningStyleDateCliArgs handles --schedule-only flag", () => {
  const args = parseRunningStyleDateCliArgs(
    ["--date", "20260524", "--schedule-only"],
    new Date(),
  );
  expect(args.scheduleOnly).toBe(true);
});

it("parseRunningStyleDateCliArgs reads --year as integer", () => {
  const args = parseRunningStyleDateCliArgs(
    ["--date", "05-24", "--year", "2026"],
    new Date(),
  );
  expect(args.dateYmd).toBe("20260524");
});

it("parseRunningStyleDateCliArgs throws on non-numeric --year", () => {
  expect(() =>
    parseRunningStyleDateCliArgs(["--date", "05-24", "--year", "abc"], new Date()),
  ).toThrow("--year must be a number");
});

it("parseRunningStyleDateCliArgs reads timing flags --poll-ms / --delay-ms / --max-rounds", () => {
  const args = parseRunningStyleDateCliArgs(
    [
      "--date",
      "20260524",
      "--poll-ms",
      "3000",
      "--delay-ms",
      "500",
      "--max-rounds",
      "60",
    ],
    new Date(),
  );
  expect(args.pollMs).toBe(3000);
  expect(args.delayMs).toBe(500);
  expect(args.maxRounds).toBe(60);
});

it("parseRunningStyleDateCliArgs throws on negative --poll-ms", () => {
  expect(() =>
    parseRunningStyleDateCliArgs(
      ["--date", "20260524", "--poll-ms", "-1"],
      new Date(),
    ),
  ).toThrow("--poll-ms must be a non-negative number");
});

it("parseRunningStyleDateCliArgs throws on negative --delay-ms", () => {
  expect(() =>
    parseRunningStyleDateCliArgs(
      ["--date", "20260524", "--delay-ms", "-5"],
      new Date(),
    ),
  ).toThrow("--delay-ms must be a non-negative number");
});

it("parseRunningStyleDateCliArgs throws on non-positive --max-rounds", () => {
  expect(() =>
    parseRunningStyleDateCliArgs(
      ["--date", "20260524", "--max-rounds", "0"],
      new Date(),
    ),
  ).toThrow("--max-rounds must be a positive number");
});

it("parseRunningStyleDateCliArgs throws on unknown argument", () => {
  expect(() =>
    parseRunningStyleDateCliArgs(["--date", "20260524", "--other"], new Date()),
  ).toThrow("Unknown argument: --other");
});

it("toPredictionJob splits raceKey into kaisaiNen/kaisaiTsukihi/keibajo/raceBango", () => {
  const job = toPredictionJob(
    {
      cacheReady: false,
      d1Count: 0,
      displayReady: false,
      expectedHorses: 12,
      featuresReady: true,
      inferenceStatus: "pending",
      parquetReady: false,
      raceKey: "jra:20260512:08:01",
      source: "jra",
    },
    "2026-05-12T11:00:00+09:00",
  );
  expect(job).toStrictEqual({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
    keibajoCode: "08",
    predictedAt: "2026-05-12T11:00:00+09:00",
    raceBango: "01",
    raceKey: "jra:20260512:08:01",
    source: "jra",
    type: "generate-running-style-predictions",
  });
});

it("toPredictionJob falls back to empty strings when raceKey has no date segment", () => {
  const job = toPredictionJob(
    {
      cacheReady: false,
      d1Count: 0,
      displayReady: false,
      expectedHorses: 0,
      featuresReady: false,
      inferenceStatus: "pending",
      parquetReady: false,
      raceKey: "malformed",
      source: "jra",
    },
    "2026-05-12T11:00:00+09:00",
  );
  expect(job.kaisaiNen).toBe("");
  expect(job.kaisaiTsukihi).toBe("");
});

it("printIncompleteRows returns early when no incomplete rows", () => {
  const spy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  printIncompleteRows([
    {
      cacheReady: true,
      d1Count: 12,
      displayReady: true,
      expectedHorses: 12,
      featuresReady: true,
      inferenceStatus: "completed",
      parquetReady: true,
      raceKey: "jra:20260512:08:01",
      source: "jra",
    },
  ]);
  expect(spy).not.toHaveBeenCalled();
});

it("printIncompleteRows logs rows with parquetReady/cacheReady true variants", () => {
  const spy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const rows: RunningStyleDateProgressRow[] = [
    {
      cacheReady: true,
      d1Count: 0,
      displayReady: false,
      expectedHorses: 10,
      featuresReady: false,
      inferenceStatus: "pending",
      parquetReady: true,
      raceKey: "jra:20260512:08:01",
      source: "jra",
    },
  ];
  printIncompleteRows(rows);
  expect(spy).toHaveBeenCalledTimes(1);
  expect(spy.mock.calls[0]?.[0]).toContain("parquet=ok");
  expect(spy.mock.calls[0]?.[0]).toContain("cache=ok");
});

it("printIncompleteRows logs first 20 incomplete rows then summarizes the rest", () => {
  const spy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const rows: RunningStyleDateProgressRow[] = Array.from({ length: 25 }, (_, index) => ({
    cacheReady: false,
    d1Count: 0,
    displayReady: false,
    expectedHorses: 10,
    featuresReady: false,
    inferenceStatus: "pending",
    parquetReady: false,
    raceKey: `jra:20260512:08:${String(index + 1).padStart(2, "0")}`,
    source: "jra",
  }));
  printIncompleteRows(rows);
  expect(spy).toHaveBeenCalledTimes(21);
  expect(spy.mock.calls[20]?.[0]).toBe("  ... 5 more incomplete races");
});

it("processIncompleteRaces runs handleRunningStylePredictionJob for each featuresReady row", async () => {
  const { handleRunningStylePredictionJob } = await import("../src/running-style-queue");
  vi.spyOn(console, "log").mockImplementation(() => undefined);
  await processIncompleteRaces(
    {} as Env,
    [
      {
        cacheReady: false,
        d1Count: 0,
        displayReady: false,
        expectedHorses: 10,
        featuresReady: true,
        inferenceStatus: "pending",
        parquetReady: false,
        raceKey: "jra:20260512:08:01",
        source: "jra",
      },
      {
        cacheReady: false,
        d1Count: 0,
        displayReady: false,
        expectedHorses: 10,
        featuresReady: false,
        inferenceStatus: "pending",
        parquetReady: false,
        raceKey: "jra:20260512:08:02",
        source: "jra",
      },
    ],
    0,
  );
  expect(handleRunningStylePredictionJob).toHaveBeenCalledTimes(1);
});

it("processIncompleteRaces sleeps between rounds when delayMs > 0 and multiple targets", async () => {
  const { handleRunningStylePredictionJob } = await import("../src/running-style-queue");
  vi.mocked(handleRunningStylePredictionJob).mockResolvedValue({
    cacheWritten: false,
    featuresR2Key: null,
    writtenCount: 0,
  } as never);
  vi.spyOn(console, "log").mockImplementation(() => undefined);
  await processIncompleteRaces(
    {} as Env,
    [
      {
        cacheReady: false,
        d1Count: 0,
        displayReady: false,
        expectedHorses: 10,
        featuresReady: true,
        inferenceStatus: "pending",
        parquetReady: false,
        raceKey: "jra:20260512:08:01",
        source: "jra",
      },
      {
        cacheReady: false,
        d1Count: 0,
        displayReady: false,
        expectedHorses: 10,
        featuresReady: true,
        inferenceStatus: "pending",
        parquetReady: false,
        raceKey: "jra:20260512:08:02",
        source: "jra",
      },
    ],
    1,
  );
  expect(handleRunningStylePredictionJob).toHaveBeenCalled();
});

it("processIncompleteRaces logs failure when the predictor throws", async () => {
  const { handleRunningStylePredictionJob } = await import("../src/running-style-queue");
  vi.mocked(handleRunningStylePredictionJob).mockRejectedValueOnce(new Error("boom"));
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  vi.spyOn(console, "log").mockImplementation(() => undefined);
  await processIncompleteRaces(
    {} as Env,
    [
      {
        cacheReady: false,
        d1Count: 0,
        displayReady: false,
        expectedHorses: 10,
        featuresReady: true,
        inferenceStatus: "pending",
        parquetReady: false,
        raceKey: "jra:20260512:08:01",
        source: "jra",
      },
    ],
    0,
  );
  expect(errorSpy).toHaveBeenCalledTimes(1);
});

it("formatRunningStyleDateProgressLine renders a single-line summary string", () => {
  const line = formatRunningStyleDateProgressLine(
    {
      cacheReady: 5,
      d1Ready: 5,
      displayReady: 5,
      expectedHorses: 80,
      featureReady: 5,
      incomplete: 0,
      parquetReady: 5,
      scanned: 5,
    },
    "20260524",
    3,
  );
  expect(line).toBe(
    "[running-style:date] date=20260524 round=3 races=5 features=5 d1=5 parquet=5 cache=5 display=5 incomplete=0",
  );
});

it("run throws when no races are found for the target date", async () => {
  vi.stubGlobal("process", {
    ...process,
    argv: ["bun", "scripts/run.ts", "--date", "20260524"],
  });
  vi.spyOn(console, "log").mockImplementation(() => undefined);
  await expect(run()).rejects.toThrow(/No races found/);
});

it("run completes the polling loop and refreshes cache when displayReady === scanned", async () => {
  const { listRunningStyleRacesByDate } = await import("../src/running-style-race-list");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValueOnce({
    races: [
      {
        keibajoCode: "08",
        raceBango: "01",
        raceKey: "jra:20260524:08:01",
        source: "jra",
      },
    ],
    source: "d1",
  } as never);
  vi.stubGlobal("process", {
    ...process,
    argv: [
      "bun",
      "scripts/run.ts",
      "--date",
      "20260524",
      "--no-ensure-models",
      "--max-rounds",
      "1",
      "--poll-ms",
      "0",
      "--delay-ms",
      "0",
    ],
  });
  vi.spyOn(console, "log").mockImplementation(() => undefined);
  await run();
});

it("run throws when finalSummary.displayReady is less than scanned", async () => {
  const { listRunningStyleRacesByDate } = await import("../src/running-style-race-list");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValueOnce({
    races: [
      {
        keibajoCode: "08",
        raceBango: "01",
        raceKey: "jra:20260524:08:01",
        source: "jra",
      },
    ],
    source: "d1",
  } as never);
  const { collectRunningStyleDateProgress } = await import("../src/running-style-date-progress");
  vi.mocked(collectRunningStyleDateProgress).mockResolvedValue([
    {
      cacheReady: false,
      d1Count: 0,
      displayReady: false,
      expectedHorses: 12,
      featuresReady: false,
      inferenceStatus: "pending",
      parquetReady: false,
      raceKey: "jra:20260524:08:01",
      source: "jra",
    },
  ]);
  vi.stubGlobal("process", {
    ...process,
    argv: [
      "bun",
      "scripts/run.ts",
      "--date",
      "20260524",
      "--no-ensure-models",
      "--max-rounds",
      "1",
      "--poll-ms",
      "0",
      "--delay-ms",
      "0",
    ],
  });
  vi.spyOn(console, "log").mockImplementation(() => undefined);
  await expect(run()).rejects.toThrow(/races are not cached for viewer display/);
});

it("run returns early in schedule-only mode after planning", async () => {
  const { listRunningStyleRacesByDate } = await import("../src/running-style-race-list");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValueOnce({
    races: [
      {
        keibajoCode: "08",
        raceBango: "01",
        raceKey: "jra:20260524:08:01",
        source: "jra",
      },
    ],
    source: "d1",
  } as never);
  vi.stubGlobal("process", {
    ...process,
    argv: [
      "bun",
      "scripts/run.ts",
      "--date",
      "20260524",
      "--schedule-only",
      "--no-ensure-models",
    ],
  });
  vi.spyOn(console, "log").mockImplementation(() => undefined);
  await run();
});
