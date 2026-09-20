// This file runs with Bun.
import { expect, it, vi } from "vitest";
import {
  buildHistoryStatements,
  buildHistoryVerificationStatements,
  buildMissingHistoryStatements,
  publishHistory,
  type HistoryPublicationPorts,
  type HistoryTransaction,
} from "./history-publication";
import type {
  SecondaryHorseResult,
  SecondaryPersonResult,
} from "../sources/secondary-result-parser";

const horse: SecondaryHorseResult = {
  sourceHorseId: "horse1",
  sourceRaceId: "race1",
  raceDate: "2026-09-01",
  venue: "Example",
  raceDaySequence: 1,
  raceName: "Published race",
  sourceRaceUrl: "https://example.test/race/race1",
  finishPosition: 2,
  finishPositionText: "2",
  jockeyName: "Published jockey",
  sourceJockeyId: "jockey1",
  surface: "Dirt",
  distanceMetres: 1800,
  going: "Good",
};
const person: SecondaryPersonResult = {
  personKind: "trainer",
  sourcePersonId: "trainer1",
  sourceRaceId: "race1",
  raceDate: "2026-09-01",
  venue: "Example",
  raceNumber: "1",
  raceName: "Published race",
  sourceRaceUrl: "https://example.test/race/race1",
  sourceHorseId: "horse1",
  horseName: "Published horse",
  finishPosition: 2,
  finishPositionText: "2",
  surface: "Dirt",
  distanceMetres: 1800,
  going: "Good",
};

it("selects only rows without an exact all-column match for incremental publication", () => {
  const statements = buildMissingHistoryStatements({
    horses: [horse],
    people: [person, { ...person, sourceHorseId: null }],
  });
  expect(statements).toHaveLength(3);
  expect(statements[0]?.text).toMatch(/^select row_to_json\(r\) as row/u);
  expect(statements[0]?.text).toMatch(/where not exists/u);
  expect(statements[0]?.text).toMatch(
    /h\.source_horse_id is not distinct from r\."sourceHorseId"/u,
  );
  expect(statements[1]?.text).toMatch(/h\.horse_name is not distinct from r\."horseName"/u);
  expect(statements[0]?.values).toHaveLength(1);
  expect(statements[0]?.text).not.toMatch(/Published race/u);
});

it("has no incremental query for an empty batch", () => {
  expect(buildMissingHistoryStatements({ horses: [], people: [] })).toStrictEqual([]);
});

it("validates incremental candidates before constructing SQL", () => {
  expect(() =>
    buildMissingHistoryStatements({
      horses: [{ ...horse, sourceHorseId: "../invalid" }],
      people: [],
    }),
  ).toThrow("Horse history has invalid required fields.");
});

it("writes all batches and verifies their full contents inside one transaction", async () => {
  const transaction: HistoryTransaction = {
    write: vi.fn().mockResolvedValue(undefined),
    countMismatches: vi.fn().mockResolvedValue(0),
  };
  const ports: HistoryPublicationPorts = {
    withTransaction: vi.fn(async (run) => {
      await run(transaction);
    }),
  };
  await publishHistory({ horses: [horse], people: [person] }, ports);
  expect(ports.withTransaction).toHaveBeenCalledTimes(1);
  expect(transaction.write).toHaveBeenCalledTimes(2);
  expect(transaction.countMismatches).toHaveBeenCalledTimes(2);
});

it("rejects mismatched stored data so the transaction adapter rolls back", async () => {
  const transaction: HistoryTransaction = {
    write: vi.fn().mockResolvedValue(undefined),
    countMismatches: vi.fn().mockResolvedValue(1),
  };
  const ports: HistoryPublicationPorts = {
    withTransaction: vi.fn(async (run) => {
      await run(transaction);
    }),
  };
  await expect(publishHistory({ horses: [horse], people: [] }, ports)).rejects.toThrow(
    "History content readback differs; transaction must roll back.",
  );
});

it("does not turn database write failures into successful verification", async () => {
  const transaction: HistoryTransaction = {
    write: vi.fn().mockRejectedValue(new Error("Insert failed")),
    countMismatches: vi.fn().mockResolvedValue(0),
  };
  const ports: HistoryPublicationPorts = {
    withTransaction: vi.fn(async (run) => {
      await run(transaction);
    }),
  };
  await expect(publishHistory({ horses: [horse], people: [] }, ports)).rejects.toThrow(
    "Insert failed",
  );
  expect(transaction.countMismatches).not.toHaveBeenCalled();
});

it("does not open a transaction for empty or invalid input", async () => {
  const ports: HistoryPublicationPorts = { withTransaction: vi.fn().mockResolvedValue(undefined) };
  await publishHistory({ horses: [], people: [] }, ports);
  await expect(
    publishHistory({ horses: [{ ...horse, raceDate: "bad" }], people: [] }, ports),
  ).rejects.toThrow("History contains an invalid source identity or calendar date.");
  expect(ports.withTransaction).not.toHaveBeenCalled();
});

it("does not build statements for empty input", () => {
  expect(buildHistoryStatements({ horses: [], people: [] })).toStrictEqual([]);
});

it("uses parameterized, idempotent inserts without touching JV data", () => {
  const statements = buildHistoryStatements({ horses: [horse], people: [person] });
  expect(statements).toHaveLength(2);
  expect(statements[0]?.text).toMatch(/^insert into oversea_horse_race_history/);
  expect(statements[1]?.text).toMatch(/^insert into oversea_person_race_history/);
  expect(statements[0]?.text).toMatch(/jsonb_to_recordset\(\$1::jsonb\)/);
  expect(statements[1]?.text).toMatch(
    /on conflict \(source, person_kind, source_person_id, source_race_id, source_horse_id\) where source_horse_id is not null do nothing\nreturning history_id$/,
  );
  expect(statements[0]?.text).toMatch(
    /on conflict \(source, source_horse_id, source_race_id\) where source_race_id is not null do nothing/,
  );
  expect(statements[0]?.text).not.toMatch(/Published|jvd_|delete|update/i);
  expect(statements[1]?.text).not.toMatch(/Published|jvd_|delete|update/i);
  expect(JSON.parse(statements[0]?.values[0] ?? "null")).toStrictEqual([
    {
      sourceHorseId: "horse1",
      sourceRaceId: "race1",
      raceDate: "2026-09-01",
      venue: "Example",
      raceDaySequence: 1,
      raceName: "Published race",
      sourceRaceUrl: "https://example.test/race/race1",
      finishPosition: 2,
      finishPositionText: "2",
      jockeyName: "Published jockey",
      sourceJockeyId: "jockey1",
      surface: "Dirt",
      distanceMetres: 1800,
      going: "Good",
    },
  ]);
});

it("preserves explicit unknown values and name-only runner identities", () => {
  const statements = buildHistoryStatements({
    horses: [
      { ...horse, finishPosition: null, finishPositionText: "Withdrawn", sourceJockeyId: null },
    ],
    people: [
      {
        ...person,
        sourceHorseId: null,
        venue: null,
        surface: null,
        distanceMetres: null,
        going: null,
        finishPosition: null,
        finishPositionText: "Withdrawn",
      },
    ],
  });
  expect(JSON.parse(statements[1]?.values[0] ?? "null")).toStrictEqual([
    {
      personKind: "trainer",
      sourcePersonId: "trainer1",
      sourceRaceId: "race1",
      raceDate: "2026-09-01",
      venue: null,
      raceNumber: "1",
      raceName: "Published race",
      sourceRaceUrl: "https://example.test/race/race1",
      sourceHorseId: null,
      horseName: "Published horse",
      finishPosition: null,
      finishPositionText: "Withdrawn",
      surface: null,
      distanceMetres: null,
      going: null,
    },
  ]);
});

it("never interpolates source text into SQL", () => {
  const statements = buildHistoryStatements({
    horses: [],
    people: [{ ...person, horseName: "Horse'); DROP TABLE jvd_se; --" }],
  });
  expect(statements).toHaveLength(1);
  expect(statements[0]?.text).not.toMatch(/DROP TABLE/);
  expect(statements[0]?.values[0]).toMatch(/DROP TABLE/);
});

it.each([
  { sourceRaceId: "" },
  { raceDate: "2026/09/01" },
  { raceDate: "2026-02-30" },
  { raceDate: "2026-99-99" },
  { sourceRaceUrl: "http://example.test/race" },
  { sourceRaceUrl: "https://user@example.test/race" },
  { sourceRaceUrl: "https://:secret@example.test/race" },
  { raceName: " " },
  { finishPositionText: " " },
  { finishPosition: 0 },
  { finishPosition: 1.5 },
  { distanceMetres: 0 },
  { distanceMetres: Number.NaN },
])("rejects invalid common fields %j before SQL is built", (patch) => {
  expect(() => buildHistoryStatements({ horses: [{ ...horse, ...patch }], people: [] })).toThrow();
});

it.each([
  { sourceHorseId: "" },
  { raceDaySequence: 0 },
  { venue: " " },
  { jockeyName: " " },
  { surface: " " },
  { sourceJockeyId: " " },
])("rejects invalid required horse fields %j", (patch) => {
  expect(() => buildHistoryStatements({ horses: [{ ...horse, ...patch }], people: [] })).toThrow(
    "Horse history has invalid required fields.",
  );
});

it.each([
  { sourcePersonId: "" },
  { sourceHorseId: " " },
  { sourceHorseId: null, horseName: null },
  { sourceHorseId: null, horseName: " " },
])("rejects unidentifiable person rows %j", (patch) => {
  expect(() => buildHistoryStatements({ horses: [], people: [{ ...person, ...patch }] })).toThrow(
    "Person history lacks a valid person or identifiable runner.",
  );
});

it("supports owner rows with real source horse identities but no published horse name", () => {
  expect(
    buildHistoryStatements({
      horses: [],
      people: [{ ...person, personKind: "owner", horseName: null }],
    }),
  ).toHaveLength(1);
});

it("splits person identity strategies and never suppresses unrelated primary-key collisions", () => {
  const statements = buildHistoryStatements({
    horses: [],
    people: [person, { ...person, sourceHorseId: null }],
  });
  expect(statements).toHaveLength(2);
  expect(statements[0]?.text).toMatch(
    /source_horse_id\) where source_horse_id is not null do nothing/,
  );
  expect(statements[1]?.text).toMatch(
    /horse_name\) where source_horse_id is null and horse_name is not null do nothing/,
  );
  expect(statements[0]?.text).not.toMatch(/on conflict do nothing/);
  expect(statements[1]?.text).not.toMatch(/on conflict do nothing/);
});

it("builds a horse-only batch", () => {
  expect(buildHistoryStatements({ horses: [horse], people: [] })).toHaveLength(1);
});

it("verifies every published column including nullable fields, not merely row counts", () => {
  const statements = buildHistoryVerificationStatements({ horses: [horse], people: [person] });
  expect(statements).toHaveLength(2);
  expect(statements[0]?.text).toMatch(/^select count\(\*\)::integer as mismatches/);
  expect(statements[0]?.text).toMatch(/from oversea_horse_race_history h/);
  expect(statements[1]?.text).toMatch(/from oversea_person_race_history h/);
  expect(statements[1]?.text).toMatch(/h\.source_horse_id is not distinct from r\."sourceHorseId"/);
  expect(statements[1]?.text).toMatch(
    /h\.finish_position is not distinct from r\."finishPosition"/,
  );
  expect(statements[1]?.text).not.toMatch(/insert|update|delete/i);
  expect(statements[1]?.values[0]).toMatch(/trainer1/);
});

it("rejects runtime person kinds outside the supported union", () => {
  const row: SecondaryPersonResult = { ...person };
  Reflect.set(row, "personKind", "unsupported");
  expect(() => buildHistoryStatements({ horses: [], people: [row] })).toThrow(
    "Person history lacks a valid person or identifiable runner.",
  );
});

it("does not accept a runtime null horse distance as a complete horse result", () => {
  const row: SecondaryHorseResult = { ...horse };
  Reflect.set(row, "distanceMetres", null);
  expect(() => buildHistoryStatements({ horses: [row], people: [] })).toThrow(
    "Horse history has invalid required fields.",
  );
});
