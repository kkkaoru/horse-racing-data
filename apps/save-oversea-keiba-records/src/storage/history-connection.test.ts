// This file runs with Bun. No native database connection is made.
import { beforeEach, expect, it, vi } from "vitest";
import { Pool } from "pg";
import { createHistoryConnection } from "./history-connection";
import type { SecondaryPersonResult } from "../sources/secondary-result-parser";
const pg = vi.hoisted(() => ({ query: vi.fn(), connect: vi.fn(), release: vi.fn(), end: vi.fn() }));
vi.mock("pg", () => ({
  Pool: vi.fn(function () {
    return { query: pg.query, connect: pg.connect, end: pg.end };
  }),
}));
const row: SecondaryPersonResult = {
  personKind: "owner",
  sourcePersonId: "owner1",
  sourceRaceId: "race1",
  raceDate: "2026-09-01",
  venue: "Venue",
  raceNumber: "1",
  raceName: "Race",
  sourceRaceUrl: "https://example.test/event/race1",
  sourceHorseId: "horse1",
  horseName: "Horse",
  finishPosition: 2,
  finishPositionText: "2",
  surface: "Dirt",
  distanceMetres: 1800,
  going: "Good",
};

beforeEach(() => {
  vi.resetAllMocks();
  pg.query.mockResolvedValue({ rowCount: 0, rows: [] });
  pg.connect.mockResolvedValue({ query: pg.query, release: pg.release });
  pg.end.mockResolvedValue(undefined);
});
it("constructs a bounded explicit local pool without opening a socket", async () => {
  const connection = createHistoryConnection(
    { OVERSEA_HISTORY_LOCAL_DATABASE_URL: "postgres://operator:secret@localhost:15432/racing" },
    "local",
  );
  expect(Pool).toHaveBeenCalledWith({
    connectionString: "postgres://operator:secret@localhost:15432/racing",
    max: 1,
    connectionTimeoutMillis: 10000,
    statement_timeout: 60000,
  });
  expect(connection.targetFingerprint).toMatch(/^[a-f0-9]{64}$/u);
  expect(pg.connect).not.toHaveBeenCalled();
  await connection.close();
  expect(pg.end).toHaveBeenCalledTimes(1);
});
it("accepts an explicitly TLS-enabled production connection", () => {
  createHistoryConnection(
    {
      OVERSEA_HISTORY_PRODUCTION_DATABASE_URL:
        "postgresql://operator:secret@db.example.test/racing?sslmode=verify-full",
    },
    "production",
  );
  expect(Pool).toHaveBeenCalledTimes(1);
});
it.each([
  "",
  "http://operator@db.example.test/racing",
  "postgres://db.example.test/racing",
  "postgres://operator@db.example.test/",
  "postgres://operator@db.example.test/racing#fragment",
])("rejects absent or invalid connection configuration %s", (url) => {
  expect(() =>
    createHistoryConnection({ OVERSEA_HISTORY_LOCAL_DATABASE_URL: url }, "local"),
  ).toThrow();
  expect(Pool).not.toHaveBeenCalled();
});
it("rejects a missing configured environment variable", () => {
  expect(() => createHistoryConnection({}, "local")).toThrow(
    "An explicit private history database URL is required.",
  );
});
it.each([
  "postgres://operator@db.example.test/racing",
  "postgres://operator@db.example.test/racing?sslmode=disable",
])("rejects production without required TLS %s", (url) => {
  expect(() =>
    createHistoryConnection({ OVERSEA_HISTORY_PRODUCTION_DATABASE_URL: url }, "production"),
  ).toThrow("Production history database requires an explicit TLS mode.");
});
it("preserves JSON candidate objects in the native query adapter", async () => {
  pg.query.mockResolvedValue({ rowCount: 1, rows: [{ row }] });
  const connection = createHistoryConnection(
    { OVERSEA_HISTORY_LOCAL_DATABASE_URL: "postgres://operator@localhost/racing" },
    "local",
  );
  expect(await connection.database.prepare({ horses: [], people: [row] })).toHaveLength(1);
  expect(pg.query).toHaveBeenCalledWith(
    expect.stringMatching(/^select row_to_json/u),
    expect.any(Array),
  );
});
it("passes transaction commands and releases the native client", async () => {
  const connection = createHistoryConnection(
    { OVERSEA_HISTORY_LOCAL_DATABASE_URL: "postgres://operator@localhost/racing" },
    "local",
  );
  expect(await connection.database.apply({ horses: [], people: [] })).toStrictEqual({
    submittedRows: 0,
    insertedRows: 0,
    verifiedRows: 0,
  });
  expect(pg.query).toHaveBeenCalledWith("BEGIN", undefined);
  expect(pg.query).toHaveBeenLastCalledWith("COMMIT", undefined);
  expect(pg.release).toHaveBeenCalledTimes(1);
});
