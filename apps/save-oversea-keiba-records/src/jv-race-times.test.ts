// This file runs with Bun and Vitest. The database pool and the race time writer are mocked.
import { beforeEach, expect, it, vi } from "vitest";
import { jvClosingThree, jvRaceTimeRow, jvRaceTimeSeconds, runJvRaceTimes } from "./jv-race-times";

const ports = vi.hoisted(() => ({
  createTargetPool: vi.fn(),
  query: vi.fn(),
  end: vi.fn(),
  applyRaceTimeRows: vi.fn(),
}));
vi.mock("./storage/history-connection", () => ({ createTargetPool: ports.createTargetPool }));
vi.mock("./race-time-store", () => ({ applyRaceTimeRows: ports.applyRaceTimeRows }));

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
  ports.applyRaceTimeRows.mockResolvedValue({ fills: [{}], conflicts: [], unmatched: 0 });
});

it("jvRaceTimeSeconds reads M SS T and treats zero as unavailable", () => {
  expect(jvRaceTimeSeconds("1475")).toBe(107.5);
  expect(jvRaceTimeSeconds("0000")).toBeNull();
  expect(jvRaceTimeSeconds("")).toBeNull();
});

it("jvClosingThree reads SS T and treats zero as unavailable", () => {
  expect(jvClosingThree("345")).toBe("34.5");
  expect(jvClosingThree("000")).toBeNull();
  expect(jvClosingThree(" ")).toBeNull();
});

it("jvRaceTimeRow keeps official values as measured and skips empty rows", () => {
  expect(
    jvRaceTimeRow({ source_horse_id: "h1", source_race_id: "r1", soha_time: "1475", kohan_3f: "" }),
  ).toStrictEqual({
    sourceHorseId: "h1",
    sourceRaceId: "r1",
    raceTimeText: "1:47.5",
    raceTimeSeconds: "107.50",
    lastThreeFurlongsSeconds: null,
    raceTimeSource: "jra-van",
    raceTimeKind: "measured",
  });
  expect(
    jvRaceTimeRow({
      source_horse_id: "h1",
      source_race_id: "r1",
      soha_time: "0000",
      kohan_3f: "345",
    }),
  ).toStrictEqual({
    sourceHorseId: "h1",
    sourceRaceId: "r1",
    raceTimeText: null,
    raceTimeSeconds: null,
    lastThreeFurlongsSeconds: "34.5",
    raceTimeSource: "jra-van",
    raceTimeKind: "measured",
  });
  expect(
    jvRaceTimeRow({
      source_horse_id: "h1",
      source_race_id: "r1",
      soha_time: "0000",
      kohan_3f: "000",
    }),
  ).toBeNull();
});

it("runJvRaceTimes reads local official rows and applies them to the target", async () => {
  ports.query.mockResolvedValue({
    rows: [
      { source_horse_id: "h1", source_race_id: "r1", soha_time: "1475", kohan_3f: "345" },
      { source_horse_id: "h2", source_race_id: "r2", soha_time: null, kohan_3f: null },
    ],
  });
  const report = await runJvRaceTimes({
    argv: ["jv-race-times-apply", "production", "--confirm-write"],
    env: ENV,
  });
  expect(report).toStrictEqual({
    status: "complete",
    operation: "applied",
    target: "production",
    officialRows: 1,
    filled: 1,
    conflicts: [],
  });
  expect(ports.createTargetPool.mock.calls[0]?.[1]).toBe("local");
  expect(ports.applyRaceTimeRows.mock.calls[0]?.[0].target).toBe("production");
  expect(ports.end).toHaveBeenCalledTimes(1);
});

it("runJvRaceTimes plans without writing and rejects bad arguments", async () => {
  ports.query.mockResolvedValue({ rows: [] });
  const report = await runJvRaceTimes({ argv: ["jv-race-times-plan", "local"], env: ENV });
  expect(report.operation).toBe("planned");
  expect(ports.applyRaceTimeRows.mock.calls[0]?.[0].apply).toBe(false);
  await expect(
    runJvRaceTimes({ argv: ["jv-race-times-apply", "local"], env: ENV }),
  ).rejects.toThrow("Use jv-race-times-plan");
});
