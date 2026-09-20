// This file runs with Bun and Vitest.
import { expect, test, vi } from "vitest";
import layouts from "../../daily-keiba-sync/src/generated/record-layouts.json";
import {
  buildProductionRequest,
  submitProductionRequest,
  type ProductionRequestInput,
} from "./production-request";

const RACE: Readonly<Record<string, string | null>> = {
  ...Object.fromEntries(layouts.tables.jvd_ra.columns.map((column) => [column.name, ""])),
  kaisai_nen: "2026",
  kaisai_tsukihi: "0919",
  keibajo_code: "A4",
  race_bango: "05",
  shusso_tosu: "01",
};
const RUNNER: Readonly<Record<string, string | null>> = {
  ...Object.fromEntries(layouts.tables.jvd_se.columns.map((column) => [column.name, ""])),
  kaisai_nen: "2026",
  kaisai_tsukihi: "0919",
  keibajo_code: "A4",
  race_bango: "05",
  umaban: "07",
  ketto_toroku_bango: "2021105727",
};
const INPUT: ProductionRequestInput = {
  runId: "12345678-abcd-4321-9876-123456789abc",
  createdAt: "2026-09-18T18:00:00.000Z",
  race: RACE,
  runners: [RUNNER],
};

test("builds scoped manual staging and jobs without advancing the acquisition cursor", () => {
  const plan = buildProductionRequest(INPUT);
  expect(plan.runDate).toBe("20260919");
  expect(plan.stages.map((stage) => stage.tableName)).toStrictEqual(["jvd_ra", "jvd_se"]);
  expect(plan.stages[0]?.key).toBe(
    "source-staging/v1/jv/20260919/12345678-abcd-4321-9876-123456789abc/tables/jvd_ra.json",
  );
  expect(plan.stages[0]?.sha256).toMatch(/^[a-f0-9]{64}$/);
  expect(plan.jobs).toStrictEqual([
    {
      type: "catalog-table",
      provider: "jv",
      runDate: "20260919",
      runId: "12345678-abcd-4321-9876-123456789abc",
      tableName: "jvd_ra",
      tableStagingKey:
        "source-staging/v1/jv/20260919/12345678-abcd-4321-9876-123456789abc/tables/jvd_ra.json",
    },
    {
      type: "catalog-table",
      provider: "jv",
      runDate: "20260919",
      runId: "12345678-abcd-4321-9876-123456789abc",
      tableName: "jvd_se",
      tableStagingKey:
        "source-staging/v1/jv/20260919/12345678-abcd-4321-9876-123456789abc/tables/jvd_se.json",
    },
  ]);
  expect(plan.statements).toHaveLength(5);
  expect(plan.statements[0]?.sql).toMatch(/advance_cursor.*'manual',1,0,'catalog_pending'/);
  expect(plan.statements[0]?.params).toStrictEqual([
    "12345678-abcd-4321-9876-123456789abc",
    "jv:20260919:manual:12345678-abcd-4321-9876-123456789abc",
    "20260919",
    2,
    "2026-09-18T18:00:00.000Z",
    "2026-09-18T18:00:00.000Z",
  ]);
  expect(plan.statements[2]?.params).toStrictEqual([
    "12345678-abcd-4321-9876-123456789abc",
    "jvd_ra",
    "2026",
  ]);
});

test("rejects a malformed date", () => {
  expect(() => buildProductionRequest({ ...INPUT, race: { ...RACE, kaisai_nen: "bad" } })).toThrow(
    "Invalid overseas race date",
  );
});
test("rejects a nonexistent day", () => {
  expect(() =>
    buildProductionRequest({ ...INPUT, race: { ...RACE, kaisai_tsukihi: "0230" } }),
  ).toThrow("Invalid overseas race date");
});
test("rejects an invalid month", () => {
  expect(() =>
    buildProductionRequest({ ...INPUT, race: { ...RACE, kaisai_tsukihi: "1301" } }),
  ).toThrow("Invalid overseas race date");
});
test("rejects an invalid run identity", () => {
  expect(() => buildProductionRequest({ ...INPUT, runId: "../other-run" })).toThrow(
    "Invalid production request identity",
  );
});
test("rejects an invalid receipt time", () => {
  expect(() => buildProductionRequest({ ...INPUT, createdAt: "bad" })).toThrow(
    "Invalid production request identity",
  );
});
test("refuses domestic venues", () => {
  expect(() => buildProductionRequest({ ...INPUT, race: { ...RACE, keibajo_code: "05" } })).toThrow(
    "Production request requires an overseas venue",
  );
});
test("refuses absent venues", () => {
  expect(() => buildProductionRequest({ ...INPUT, race: { ...RACE, keibajo_code: null } })).toThrow(
    "Production request requires an overseas venue",
  );
});
test("refuses missing race numbers", () => {
  expect(() => buildProductionRequest({ ...INPUT, race: { ...RACE, race_bango: null } })).toThrow(
    "Invalid overseas race number",
  );
});
test("refuses zero race numbers", () => {
  expect(() => buildProductionRequest({ ...INPUT, race: { ...RACE, race_bango: "00" } })).toThrow(
    "Invalid overseas race number",
  );
});
test("refuses an empty field", () => {
  expect(() => buildProductionRequest({ ...INPUT, runners: [] })).toThrow(
    "Active runner count does not match the race",
  );
});
test("refuses an unbounded field", () => {
  expect(() =>
    buildProductionRequest({ ...INPUT, runners: Array.from({ length: 41 }, () => RUNNER) }),
  ).toThrow("Active runner count does not match the race");
});
test("refuses a mismatched field count", () => {
  expect(() => buildProductionRequest({ ...INPUT, race: { ...RACE, shusso_tosu: "02" } })).toThrow(
    "Active runner count does not match the race",
  );
});
test("refuses rows from another race", () => {
  expect(() =>
    buildProductionRequest({ ...INPUT, runners: [{ ...RUNNER, race_bango: "06" }] }),
  ).toThrow("Production request contains a different race");
});
test("refuses missing runner numbers", () => {
  expect(() =>
    buildProductionRequest({ ...INPUT, runners: [{ ...RUNNER, umaban: null }] }),
  ).toThrow("Invalid or duplicate runner number");
});
test("refuses invalid runner numbers", () => {
  expect(() =>
    buildProductionRequest({ ...INPUT, runners: [{ ...RUNNER, umaban: "00" }] }),
  ).toThrow("Invalid or duplicate runner number");
});
test("refuses duplicate runners", () => {
  expect(() =>
    buildProductionRequest({
      ...INPUT,
      race: { ...RACE, shusso_tosu: "02" },
      runners: [RUNNER, RUNNER],
    }),
  ).toThrow("Invalid or duplicate runner number");
});
test("rejects missing layout columns", () => {
  const { record_id, ...incomplete } = RACE;
  expect(record_id).toBe("");
  expect(() => buildProductionRequest({ ...INPUT, race: incomplete })).toThrow(
    "Source columns do not match the production layout",
  );
});
test("rejects an unknown column replacing a layout column", () => {
  const { record_id, ...incomplete } = RACE;
  expect(record_id).toBe("");
  expect(() =>
    buildProductionRequest({ ...INPUT, race: { ...incomplete, unexpected: "" } }),
  ).toThrow("Source columns do not match the production layout");
});
test("verifies both objects before registration and enqueues only after registration", async () => {
  const events: string[] = [];
  await submitProductionRequest(INPUT, {
    uploadAndVerify: async (stage) => {
      events.push(stage.tableName);
    },
    register: async () => {
      events.push("register");
    },
    enqueue: async () => {
      events.push("enqueue");
    },
  });
  expect(events).toStrictEqual(["jvd_ra", "jvd_se", "register", "enqueue"]);
});
test("does not register or enqueue after failed storage verification", async () => {
  const register = vi.fn();
  const enqueue = vi.fn();
  await expect(
    submitProductionRequest(INPUT, {
      uploadAndVerify: async () => {
        throw new Error("hash mismatch");
      },
      register,
      enqueue,
    }),
  ).rejects.toThrow("hash mismatch");
  expect(register).not.toHaveBeenCalled();
  expect(enqueue).not.toHaveBeenCalled();
});
test("does not enqueue after failed registration", async () => {
  const enqueue = vi.fn();
  await expect(
    submitProductionRequest(INPUT, {
      uploadAndVerify: async () => {},
      register: async () => {
        throw new Error("registration failed");
      },
      enqueue,
    }),
  ).rejects.toThrow("registration failed");
  expect(enqueue).not.toHaveBeenCalled();
});
test("does not hide enqueue errors", async () => {
  await expect(
    submitProductionRequest(INPUT, {
      uploadAndVerify: async () => {},
      register: async () => {},
      enqueue: async () => {
        throw new Error("enqueue failed");
      },
    }),
  ).rejects.toThrow("enqueue failed");
});
