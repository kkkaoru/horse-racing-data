// This file runs with Bun and Vitest. Database pools and the filesystem are mocked.
import { beforeEach, expect, it, vi } from "vitest";
import {
  buildEntityPlan,
  eventEntities,
  parseEntityLines,
  runEventEntities,
} from "./event-entities";

const ports = vi.hoisted(() => ({
  createTargetPool: vi.fn(),
  query: vi.fn(),
  end: vi.fn(),
  readFile: vi.fn(),
  writeFile: vi.fn(),
  open: vi.fn(),
  handleWrite: vi.fn(),
  handleClose: vi.fn(),
}));
vi.mock("./storage/history-connection", () => ({ createTargetPool: ports.createTargetPool }));
vi.mock("node:fs/promises", () => ({
  readFile: ports.readFile,
  writeFile: ports.writeFile,
  open: ports.open,
}));

const ENV = {
  POSTGRES_PORT: "15432",
  POSTGRES_USER: "user",
  POSTGRES_PASSWORD: "secret",
  POSTGRES_DB: "db",
  OVERSEA_HISTORY_PLAN_TEMPLATES_PATH: "/private/templates.json",
};
const TEMPLATES = JSON.stringify({
  horse: {
    urlTemplate: "https://example.test/horse/{ID}/",
    plan: { encoding: "utf-8", profile: { a: 1 } },
  },
  jockey: { urlTemplate: "https://example.test/jockey/{ID}/", plan: { encoding: "utf-8" } },
  trainer: { urlTemplate: "https://example.test/trainer/{ID}/", plan: { encoding: "utf-8" } },
  owner: { urlTemplate: "https://example.test/owner/{ID}/", plan: { encoding: "utf-8" } },
});

beforeEach(() => {
  vi.resetAllMocks();
  ports.createTargetPool.mockReturnValue({
    targetFingerprint: "local",
    pool: { query: ports.query, end: ports.end },
  });
  ports.end.mockResolvedValue(undefined);
  ports.writeFile.mockResolvedValue(undefined);
  ports.handleWrite.mockResolvedValue(undefined);
  ports.handleClose.mockResolvedValue(undefined);
  ports.open.mockResolvedValue({ writeFile: ports.handleWrite, close: ports.handleClose });
});

it("lists horses first, then people, once each, skipping ids the partial source left empty", () => {
  expect(
    eventEntities([
      {
        race: "20261101A406",
        source_horse_id: "000a2",
        source_jockey_id: "j1",
        source_trainer_id: "t1",
        source_owner_id: null,
      },
      {
        race: "20251101A406",
        source_horse_id: "000a1",
        source_jockey_id: "j1",
        source_trainer_id: " ",
        source_owner_id: "o9",
      },
    ]),
  ).toStrictEqual([
    { kind: "horse", sourceId: "000a1" },
    { kind: "horse", sourceId: "000a2" },
    { kind: "jockey", sourceId: "j1" },
    { kind: "trainer", sourceId: "t1" },
    { kind: "owner", sourceId: "o9" },
  ]);
});

it("parses entity lines and rejects unknown kinds", () => {
  expect(parseEntityLines("horse 000a1\n\njockey j1\n")).toStrictEqual([
    { kind: "horse", sourceId: "000a1" },
    { kind: "jockey", sourceId: "j1" },
  ]);
  expect(() => parseEntityLines("breeder b1\n")).toThrow("Invalid entity line: breeder b1");
});

it("builds a plan from the private template with the entity id in its URL", () => {
  expect(
    buildEntityPlan(
      { urlTemplate: "https://example.test/horse/{ID}/", plan: { encoding: "utf-8" } },
      { kind: "horse", sourceId: "000a1" },
    ),
  ).toStrictEqual({
    encoding: "utf-8",
    kind: "horse",
    sourceId: "000a1",
    initialUrl: "https://example.test/horse/000a1/",
  });
});

it("writes the event entity list for the meeting and its past editions", async () => {
  ports.query.mockResolvedValue({
    rowCount: 2,
    rows: [
      {
        race: "20261101A406",
        source_horse_id: "000a2",
        source_jockey_id: "j1",
        source_trainer_id: null,
        source_owner_id: null,
      },
      {
        race: "20251101A411",
        source_horse_id: "000a1",
        source_jockey_id: null,
        source_trainer_id: "t1",
        source_owner_id: null,
      },
    ],
  });
  await expect(
    runEventEntities({
      argv: ["event-entities", "2026", "1101", "A4", "/private/q.txt"],
      env: ENV,
    }),
  ).resolves.toStrictEqual({
    status: "complete",
    operation: "entities",
    races: 2,
    entities: 4,
    written: 4,
  });
  expect(ports.query.mock.calls[0]?.[1]).toStrictEqual(["2026", "1101", "A4"]);
  expect(ports.writeFile).toHaveBeenCalledWith(
    "/private/q.txt",
    "horse 000a1\nhorse 000a2\njockey j1\ntrainer t1\n",
    { mode: 0o600 },
  );
  expect(ports.end).toHaveBeenCalledTimes(1);
});

it("fails closed when the event has no source-id rows and still closes the pool", async () => {
  ports.query.mockResolvedValue({ rowCount: 0, rows: [] });
  await expect(
    runEventEntities({
      argv: ["event-entities", "2026", "1101", "A4", "/private/q.txt"],
      env: ENV,
    }),
  ).rejects.toThrow("No source-id rows exist for this event.");
  expect(ports.writeFile).not.toHaveBeenCalled();
  expect(ports.end).toHaveBeenCalledTimes(1);
});

it("rejects malformed meeting keys and unknown actions", async () => {
  await expect(
    runEventEntities({
      argv: ["event-entities", "2026", "1101", "a4", "/private/q.txt"],
      env: ENV,
    }),
  ).rejects.toThrow(
    "Use event-entities YEAR MMDD VENUE OUTPUT_FILE or event-plans ENTITIES_FILE PLANS_DIRECTORY.",
  );
  await expect(runEventEntities({ argv: ["event-other"], env: ENV })).rejects.toThrow(
    "Use event-entities YEAR MMDD VENUE OUTPUT_FILE or event-plans ENTITIES_FILE PLANS_DIRECTORY.",
  );
});

it("writes only missing plans and keeps existing ones", async () => {
  ports.readFile.mockImplementation(async (path: string) =>
    path === "/private/templates.json" ? TEMPLATES : "horse 000a1\nowner o9\n",
  );
  ports.open
    .mockResolvedValueOnce({ writeFile: ports.handleWrite, close: ports.handleClose })
    .mockRejectedValueOnce(Object.assign(new Error("exists"), { code: "EEXIST" }));
  await expect(
    runEventEntities({ argv: ["event-plans", "/private/q.txt", "/private/plans"], env: ENV }),
  ).resolves.toStrictEqual({
    status: "complete",
    operation: "plans",
    races: 0,
    entities: 2,
    written: 1,
  });
  expect(ports.open.mock.calls.map((call) => call[0])).toStrictEqual([
    "/private/plans/horse-000a1.json",
    "/private/plans/owner-o9.json",
  ]);
  expect(JSON.parse(String(ports.handleWrite.mock.calls[0]?.[0]))).toStrictEqual({
    encoding: "utf-8",
    profile: { a: 1 },
    kind: "horse",
    sourceId: "000a1",
    initialUrl: "https://example.test/horse/000a1/",
  });
  expect(ports.handleClose).toHaveBeenCalledTimes(1);
});

it("propagates plan write errors other than an existing file", async () => {
  ports.readFile.mockImplementation(async (path: string) =>
    path === "/private/templates.json" ? TEMPLATES : "horse 000a1\n",
  );
  ports.open.mockRejectedValue(new Error("Disk full"));
  await expect(
    runEventEntities({ argv: ["event-plans", "/private/q.txt", "/private/plans"], env: ENV }),
  ).rejects.toThrow("Disk full");
});

it("requires the private template path and complete per-kind templates", async () => {
  await expect(
    runEventEntities({
      argv: ["event-plans", "/private/q.txt", "/private/plans"],
      env: { ...ENV, OVERSEA_HISTORY_PLAN_TEMPLATES_PATH: "" },
    }),
  ).rejects.toThrow("OVERSEA_HISTORY_PLAN_TEMPLATES_PATH is required.");
  ports.readFile.mockResolvedValue(JSON.stringify({ horse: { urlTemplate: "no-id", plan: {} } }));
  await expect(
    runEventEntities({ argv: ["event-plans", "/private/q.txt", "/private/plans"], env: ENV }),
  ).rejects.toThrow("Plan template for horse needs urlTemplate with {ID} and a plan object.");
  ports.readFile.mockResolvedValue("[]");
  await expect(
    runEventEntities({ argv: ["event-plans", "/private/q.txt", "/private/plans"], env: ENV }),
  ).rejects.toThrow("Plan templates must be an object.");
  await expect(
    runEventEntities({ argv: ["event-plans", "/private/q.txt"], env: ENV }),
  ).rejects.toThrow(
    "Use event-entities YEAR MMDD VENUE OUTPUT_FILE or event-plans ENTITIES_FILE PLANS_DIRECTORY.",
  );
});
