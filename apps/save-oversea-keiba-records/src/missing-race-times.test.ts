// This file runs with Bun and Vitest. The database pool and the file write are mocked.
import { beforeEach, expect, it, vi } from "vitest";
import { runMissingRaceTimes } from "./missing-race-times";

const ports = vi.hoisted(() => ({
  createTargetPool: vi.fn(),
  query: vi.fn(),
  end: vi.fn(),
  writeFile: vi.fn(),
}));
vi.mock("node:fs/promises", () => ({ writeFile: ports.writeFile }));
vi.mock("./storage/history-connection", () => ({ createTargetPool: ports.createTargetPool }));

const ENV = {
  POSTGRES_PORT: "15432",
  POSTGRES_USER: "u",
  POSTGRES_PASSWORD: "p",
  POSTGRES_DB: "d",
};

beforeEach(() => {
  vi.resetAllMocks();
  ports.createTargetPool.mockReturnValue({ pool: { query: ports.query, end: ports.end } });
  ports.end.mockResolvedValue(undefined);
  ports.writeFile.mockResolvedValue(undefined);
});

it("writes the meeting runners' history races without a time from local PostgreSQL", async () => {
  ports.query.mockResolvedValue({
    rows: [
      { source_horse_id: "h1", source_race_id: "r1" },
      { source_horse_id: "h1", source_race_id: "r2" },
      { source_horse_id: "h2", source_race_id: "r3" },
    ],
  });
  const report = await runMissingRaceTimes({
    argv: ["missing-race-times", "2026", "1103", "B6", "/private/missing.json"],
    env: ENV,
  });
  expect(report).toStrictEqual({ status: "complete", operation: "listed", horses: 2, missing: 3 });
  expect(ports.createTargetPool.mock.calls[0]?.[1]).toBe("local");
  expect(ports.query.mock.calls[0]?.[1]).toStrictEqual(["2026", "1103", "B6"]);
  expect(ports.writeFile.mock.calls[0]?.[0]).toBe("/private/missing.json");
  expect(ports.writeFile.mock.calls[0]?.[1]).toBe(
    '[{"source_horse_id":"h1","source_race_id":"r1"},{"source_horse_id":"h1","source_race_id":"r2"},{"source_horse_id":"h2","source_race_id":"r3"}]',
  );
  expect(ports.end).toHaveBeenCalledTimes(1);
});

it("rejects bad arguments before connecting", async () => {
  await expect(
    runMissingRaceTimes({
      argv: ["missing-race-times", "2026", "1103", "b6", "/o.json"],
      env: ENV,
    }),
  ).rejects.toThrow("Use missing-race-times YYYY MMDD VENUE OUTPUT_JSON.");
  expect(ports.createTargetPool).not.toHaveBeenCalled();
});
