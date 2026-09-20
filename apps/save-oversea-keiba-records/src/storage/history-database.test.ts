// This file runs with Bun. Database I/O is entirely mocked.
import { beforeEach, expect, it, vi } from "vitest";
import { createHistoryDatabase } from "./history-database";
import type { PostgresPool, PostgresPoolClient } from "./pg-client";
import type { SecondaryPersonResult } from "../sources/secondary-result-parser";

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
const query = vi.fn<PostgresPoolClient["query"]>();
const release = vi.fn();
const client: PostgresPoolClient = { query, release };
const connect = vi.fn<PostgresPool["connect"]>();
const poolQuery = vi.fn<PostgresPool["query"]>();
const pool: PostgresPool = { connect, query: poolQuery, end: vi.fn() };

beforeEach(() => {
  vi.resetAllMocks();
  connect.mockResolvedValue(client);
  query.mockResolvedValue({ rowCount: 0, rows: [] });
});

it("prepares only exact missing candidates without opening a transaction", async () => {
  poolQuery.mockResolvedValue({ rowCount: 1, rows: [{ row }] });
  expect(await createHistoryDatabase(pool).prepare({ horses: [], people: [row] })).toHaveLength(1);
  expect(connect).not.toHaveBeenCalled();
  expect(poolQuery).toHaveBeenCalledWith(
    expect.stringMatching(/^select row_to_json/u),
    expect.any(Array),
  );
});

it.each([null, {}, { ...row, sourcePersonId: "other" }])(
  "rejects invalid or out-of-scope database candidates %j",
  async (candidate) => {
    poolQuery.mockResolvedValue({ rowCount: 1, rows: [{ row: candidate }] });
    await expect(
      createHistoryDatabase(pool).prepare({ horses: [], people: [row] }),
    ).rejects.toThrow("Database returned a history candidate outside the validated input.");
  },
);

it("rejects duplicate candidate results", async () => {
  poolQuery.mockResolvedValue({ rowCount: 2, rows: [{ row }, { row }] });
  await expect(createHistoryDatabase(pool).prepare({ horses: [], people: [row] })).rejects.toThrow(
    "Database returned duplicate history candidates.",
  );
});

it("inserts missing rows and verifies both the delta and the complete input before commit", async () => {
  query
    .mockResolvedValueOnce({ rowCount: 0, rows: [] })
    .mockResolvedValueOnce({ rowCount: 1, rows: [{ row }] })
    .mockResolvedValueOnce({ rowCount: 1, rows: [] })
    .mockResolvedValueOnce({ rowCount: 1, rows: [{ mismatches: 0 }] })
    .mockResolvedValueOnce({ rowCount: 1, rows: [{ mismatches: 0 }] });
  expect(await createHistoryDatabase(pool).apply({ horses: [], people: [row] })).toStrictEqual({
    submittedRows: 1,
    insertedRows: 1,
    verifiedRows: 1,
  });
  expect(query).toHaveBeenLastCalledWith("COMMIT");
  expect(release).toHaveBeenCalledTimes(1);
});

it("reverifies already stored rows without submitting another insert", async () => {
  query
    .mockResolvedValueOnce({ rowCount: 0, rows: [] })
    .mockResolvedValueOnce({ rowCount: 0, rows: [] })
    .mockResolvedValueOnce({ rowCount: 1, rows: [{ mismatches: 0 }] });
  expect(await createHistoryDatabase(pool).apply({ horses: [], people: [row] })).toStrictEqual({
    submittedRows: 0,
    insertedRows: 0,
    verifiedRows: 1,
  });
  expect(query).toHaveBeenCalledTimes(4);
  expect(query).toHaveBeenLastCalledWith("COMMIT");
});

it("validates input before connecting", async () => {
  await expect(
    createHistoryDatabase(pool).apply({ horses: [], people: [{ ...row, finishPositionText: "" }] }),
  ).rejects.toThrow("History provenance or required published text is missing.");
  expect(connect).not.toHaveBeenCalled();
});

it("rolls back if already stored input fails the final all-column readback", async () => {
  query
    .mockResolvedValueOnce({ rowCount: 0, rows: [] })
    .mockResolvedValueOnce({ rowCount: 0, rows: [] })
    .mockResolvedValueOnce({ rowCount: 1, rows: [{ mismatches: 1 }] });
  await expect(createHistoryDatabase(pool).apply({ horses: [], people: [row] })).rejects.toThrow(
    "Full history readback differs; transaction must roll back.",
  );
  expect(query).toHaveBeenLastCalledWith("ROLLBACK");
  expect(release).toHaveBeenCalledTimes(1);
});

it.each([null, -1, 0.5])("rolls back an invalid inserted-row count %j", async (rowCount) => {
  query
    .mockResolvedValueOnce({ rowCount: 0, rows: [] })
    .mockResolvedValueOnce({ rowCount: 1, rows: [{ row }] })
    .mockResolvedValueOnce({ rowCount, rows: [] });
  await expect(createHistoryDatabase(pool).apply({ horses: [], people: [row] })).rejects.toThrow(
    "History insert returned an invalid affected-row count.",
  );
  expect(query).toHaveBeenLastCalledWith("ROLLBACK");
});

it.each([undefined, "0", -1, 0.5])("rejects malformed mismatch count %j", async (mismatches) => {
  query
    .mockResolvedValueOnce({ rowCount: 0, rows: [] })
    .mockResolvedValueOnce({ rowCount: 0, rows: [] })
    .mockResolvedValueOnce({ rowCount: 1, rows: [{ mismatches }] });
  await expect(createHistoryDatabase(pool).apply({ horses: [], people: [row] })).rejects.toThrow(
    "History readback returned an invalid mismatch count.",
  );
  expect(query).toHaveBeenLastCalledWith("ROLLBACK");
});

it("rejects absent mismatch result rows", async () => {
  await expect(createHistoryDatabase(pool).apply({ horses: [], people: [row] })).rejects.toThrow(
    "History readback returned an invalid mismatch count.",
  );
  expect(query).toHaveBeenLastCalledWith("ROLLBACK");
});

it("releases a failed BEGIN without attempting rollback", async () => {
  query.mockRejectedValueOnce(new Error("Begin failed"));
  await expect(createHistoryDatabase(pool).apply({ horses: [], people: [row] })).rejects.toThrow(
    "Begin failed",
  );
  expect(query).toHaveBeenCalledTimes(1);
  expect(release).toHaveBeenCalledTimes(1);
});

it("preserves transaction and rollback failures", async () => {
  query
    .mockResolvedValueOnce({ rowCount: 0, rows: [] })
    .mockRejectedValueOnce(new Error("Read failed"))
    .mockRejectedValueOnce(new Error("Rollback failed"));
  await expect(createHistoryDatabase(pool).apply({ horses: [], people: [row] })).rejects.toThrow(
    "History transaction and rollback failed.",
  );
  expect(release).toHaveBeenCalledTimes(1);
});

it("does not claim success if commit fails", async () => {
  query
    .mockResolvedValueOnce({ rowCount: 0, rows: [] })
    .mockRejectedValueOnce(new Error("Commit failed"));
  await expect(createHistoryDatabase(pool).apply({ horses: [], people: [] })).rejects.toThrow(
    "Commit failed",
  );
  expect(query).toHaveBeenLastCalledWith("ROLLBACK");
  expect(release).toHaveBeenCalledTimes(1);
});
