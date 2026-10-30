// This file runs with Bun and Vitest. Database pools and the filesystem are mocked.
import { beforeEach, expect, it, vi } from "vitest";
import { planSourceIdMirror, runSourceIdMirror } from "./source-id-mirror";

const ports = vi.hoisted(() => ({
  createTargetPool: vi.fn(),
  mkdir: vi.fn(),
  writeFile: vi.fn(),
  localQuery: vi.fn(),
  remoteQuery: vi.fn(),
  clientQuery: vi.fn(),
  release: vi.fn(),
  end: vi.fn(),
}));
vi.mock("./storage/history-connection", () => ({ createTargetPool: ports.createTargetPool }));
vi.mock("node:fs/promises", () => ({ mkdir: ports.mkdir, writeFile: ports.writeFile }));

const ENV = {
  POSTGRES_PORT: "15432",
  POSTGRES_USER: "user",
  POSTGRES_PASSWORD: "secret",
  POSTGRES_DB: "db",
  OVERSEA_HISTORY_PRODUCTION_DATABASE_URL: "postgresql://user@example.test/db?sslmode=require",
};
const LOCAL_ROW = {
  race_source: "jra",
  kaisai_nen: "2026",
  kaisai_tsukihi: "1101",
  keibajo_code: "A4",
  race_bango: "06",
  umaban: "01",
  source: "netkeiba",
  source_horse_id: "000a000001",
  source_jockey_id: "a0001",
  source_trainer_id: "a0002",
  source_owner_id: null,
  source_url: null,
  gate_number: "1",
};

beforeEach(() => {
  vi.resetAllMocks();
  ports.createTargetPool.mockImplementation((_env: unknown, target: string) => ({
    targetFingerprint: target,
    pool: {
      query: target === "local" ? ports.localQuery : ports.remoteQuery,
      connect: async () => ({ query: ports.clientQuery, release: ports.release }),
      end: ports.end,
    },
  }));
  ports.end.mockResolvedValue(undefined);
  ports.mkdir.mockResolvedValue(undefined);
  ports.writeFile.mockResolvedValue(undefined);
});

it("plans inserts, null fills, conflicts and unchanged rows without touching real values", () => {
  expect(
    planSourceIdMirror(
      [
        LOCAL_ROW,
        { ...LOCAL_ROW, umaban: "02", source_owner_id: "a0003" },
        { ...LOCAL_ROW, umaban: "03" },
        { ...LOCAL_ROW, umaban: "04" },
      ],
      [
        { ...LOCAL_ROW, umaban: "02" },
        { ...LOCAL_ROW, umaban: "03", source_jockey_id: "a9999" },
        { ...LOCAL_ROW, umaban: "04", source_url: "https://example.test/" },
      ],
    ),
  ).toStrictEqual({
    inserts: [LOCAL_ROW],
    fills: [
      {
        row: { ...LOCAL_ROW, umaban: "02", source_owner_id: "a0003" },
        columns: ["source_owner_id"],
      },
    ],
    conflicts: ["jra/2026/1101/A4/06/03/netkeiba:source_jockey_id"],
    unchanged: 2,
  });
});

it("reports a read-only plan against production", async () => {
  ports.localQuery.mockResolvedValue({ rowCount: 1, rows: [{ ...LOCAL_ROW, gate_number: 1 }] });
  ports.remoteQuery.mockResolvedValue({ rowCount: 0, rows: [] });
  await expect(
    runSourceIdMirror({ argv: ["source-ids-plan", "2026", "1101", "A4"], env: ENV }),
  ).resolves.toStrictEqual({
    status: "complete",
    operation: "planned",
    localRows: 1,
    inserted: 1,
    filled: 0,
    unchanged: 0,
    conflicts: [],
  });
  expect(ports.clientQuery).not.toHaveBeenCalled();
  expect(ports.end).toHaveBeenCalledTimes(2);
});

it("inserts and fills inside one verified transaction and writes a receipt", async () => {
  const owned = { ...LOCAL_ROW, umaban: "02", source_owner_id: "a0003" };
  ports.localQuery.mockResolvedValue({ rowCount: 2, rows: [LOCAL_ROW, owned] });
  ports.clientQuery.mockImplementation(async (text: string) => {
    const selects = ports.clientQuery.mock.calls.filter(([sql]) =>
      String(sql).startsWith("SELECT"),
    );
    if (!text.startsWith("SELECT")) return { rowCount: 0, rows: [] };
    return selects.length === 1
      ? { rowCount: 1, rows: [{ ...owned, source_owner_id: null }] }
      : { rowCount: 2, rows: [LOCAL_ROW, owned] };
  });
  await expect(
    runSourceIdMirror({
      argv: [
        "source-ids-production",
        "2026",
        "1101",
        "A4",
        "/private/receipt",
        "--confirm-production",
      ],
      env: ENV,
    }),
  ).resolves.toStrictEqual({
    status: "complete",
    operation: "applied",
    localRows: 2,
    inserted: 1,
    filled: 1,
    unchanged: 0,
    conflicts: [],
  });
  expect(ports.clientQuery.mock.calls.map(([sql]) => String(sql).split(" ")[0])).toStrictEqual([
    "BEGIN",
    "LOCK",
    "SELECT",
    "INSERT",
    "UPDATE",
    "SELECT",
    "COMMIT",
  ]);
  expect(ports.clientQuery.mock.calls[3]?.[1]).toStrictEqual([
    "jra",
    "2026",
    "1101",
    "A4",
    "06",
    "01",
    "netkeiba",
    "000a000001",
    "a0001",
    "a0002",
    "",
    "",
    "1",
  ]);
  expect(ports.writeFile).toHaveBeenCalledTimes(2);
  expect(ports.release).toHaveBeenCalledTimes(1);
});

it("rolls back without writing when production holds a conflicting id", async () => {
  ports.localQuery.mockResolvedValue({ rowCount: 1, rows: [LOCAL_ROW] });
  ports.clientQuery.mockImplementation(async (text: string) =>
    text.startsWith("SELECT")
      ? { rowCount: 1, rows: [{ ...LOCAL_ROW, source_horse_id: "000a999999" }] }
      : { rowCount: 0, rows: [] },
  );
  await expect(
    runSourceIdMirror({
      argv: [
        "source-ids-production",
        "2026",
        "1101",
        "A4",
        "/private/receipt",
        "--confirm-production",
      ],
      env: ENV,
    }),
  ).rejects.toThrow("Production has conflicting source ids; nothing written.");
  expect(ports.clientQuery.mock.calls.map(([sql]) => String(sql).split(" ")[0])).toStrictEqual([
    "BEGIN",
    "LOCK",
    "SELECT",
    "ROLLBACK",
  ]);
  expect(ports.writeFile).toHaveBeenCalledTimes(1);
});

it("rolls back when the in-transaction readback differs", async () => {
  ports.localQuery.mockResolvedValue({ rowCount: 1, rows: [LOCAL_ROW] });
  ports.clientQuery.mockResolvedValue({ rowCount: 0, rows: [] });
  await expect(
    runSourceIdMirror({
      argv: [
        "source-ids-production",
        "2026",
        "1101",
        "A4",
        "/private/receipt",
        "--confirm-production",
      ],
      env: ENV,
    }),
  ).rejects.toThrow("Production readback differs from local source ids.");
  expect(ports.clientQuery.mock.calls.at(-1)?.[0]).toBe("ROLLBACK");
});

it("refuses a meeting without local source ids", async () => {
  ports.localQuery.mockResolvedValue({ rowCount: 0, rows: [] });
  await expect(
    runSourceIdMirror({ argv: ["source-ids-plan", "2026", "1101", "A4"], env: ENV }),
  ).rejects.toThrow("No local source ids exist for this meeting.");
});

it("rejects malformed arguments and missing confirmation", async () => {
  await expect(
    runSourceIdMirror({ argv: ["source-ids-plan", "2026", "11", "A4"], env: ENV }),
  ).rejects.toThrow(
    "Use source-ids-plan YEAR MMDD VENUE or source-ids-production YEAR MMDD VENUE NEW_RECEIPT_DIRECTORY --confirm-production.",
  );
  await expect(
    runSourceIdMirror({
      argv: ["source-ids-production", "2026", "1101", "A4", "/private/receipt"],
      env: ENV,
    }),
  ).rejects.toThrow(
    "Use source-ids-plan YEAR MMDD VENUE or source-ids-production YEAR MMDD VENUE NEW_RECEIPT_DIRECTORY --confirm-production.",
  );
});
