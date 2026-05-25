// run with: bun run test
import { expect, it } from "vitest";
import {
  formatRunningStyleDateProgressLine,
  parseRunningStyleDateCliArgs,
} from "./run-running-style-date";

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
