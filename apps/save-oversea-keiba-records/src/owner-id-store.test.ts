// This file runs with Bun and Vitest. The filesystem, profile and database pools are synthetic mocks.
import { beforeEach, expect, it, vi } from "vitest";
import { archivedOwnerIds, ownerIdFromPage, runOwnerIdStore } from "./owner-id-store";

const ports = vi.hoisted(() => ({
  readFile: vi.fn(),
  readdir: vi.fn(),
  createTargetPool: vi.fn(),
  query: vi.fn(),
  end: vi.fn(),
  connect: vi.fn(),
  clientQuery: vi.fn(),
  release: vi.fn(),
}));
vi.mock("node:fs/promises", () => ({ readFile: ports.readFile, readdir: ports.readdir }));
vi.mock("./storage/history-connection", () => ({ createTargetPool: ports.createTargetPool }));

const PATTERN: RegExp = /owner:([0-9a-z?]+)/u;
const ENV = {
  POSTGRES_PORT: "15432",
  POSTGRES_USER: "u",
  POSTGRES_PASSWORD: "p",
  POSTGRES_DB: "d",
  OVERSEA_OWNER_ID_PROFILE_PATH: "/private/owner.json",
};
const PAGES: Readonly<Record<string, string>> = {
  "/h/horse-h1/pages/a.raw": "owner:o1",
  "/h/horse-h2-v2/pages/a.raw": "no owner",
  "/h/horse-h2-v2/pages/b.raw": "owner:o2",
  "/h/horse-h3/pages/a.raw": "owner:bad?",
};

beforeEach(() => {
  vi.resetAllMocks();
  ports.readdir.mockImplementation(async (path: string) =>
    path === "/h"
      ? ["horse-h1", "horse-h2-v2", "horse-h3", "jockey-j1"]
      : ["b.raw", "a.raw", "a.raw.meta.json"].filter((name) =>
          Object.hasOwn(PAGES, `${path}/${name}`),
        ),
  );
  ports.readFile.mockImplementation(async (path: string) =>
    path === "/private/owner.json"
      ? JSON.stringify({ ownerIdPattern: "owner:([0-9a-z?]+)" })
      : PAGES[path],
  );
  ports.createTargetPool.mockReturnValue({
    pool: { query: ports.query, end: ports.end, connect: ports.connect },
  });
  ports.connect.mockResolvedValue({ query: ports.clientQuery, release: ports.release });
  ports.end.mockResolvedValue(undefined);
  ports.query.mockResolvedValue({
    rows: [{ source_horse_id: "h1" }, { source_horse_id: "h2" }, { source_horse_id: "h9" }],
  });
  ports.clientQuery.mockResolvedValue({ rows: [{ source_horse_id: "h9" }] });
});

it("ownerIdFromPage keeps only a plain source id", () => {
  expect(ownerIdFromPage("owner:o1", PATTERN)).toBe("o1");
  expect(ownerIdFromPage("owner:bad?", PATTERN)).toBeNull();
  expect(ownerIdFromPage("none", PATTERN)).toBeNull();
});

it("archivedOwnerIds reads the first owner link of each archived horse", async () => {
  expect(await archivedOwnerIds("/h", PATTERN)).toStrictEqual(
    new Map([
      ["h1", "o1"],
      ["h2", "o2"],
    ]),
  );
});

it("runOwnerIdStore plans fills for the meeting's runners without an owner", async () => {
  expect(
    await runOwnerIdStore({
      argv: ["owner-ids-plan", "/h", "2026", "1103", "B6", "local"],
      env: ENV,
    }),
  ).toStrictEqual({
    status: "complete",
    operation: "planned",
    target: "local",
    archivedOwners: 2,
    runnersWithoutOwner: 3,
    filled: 2,
  });
  expect(ports.query.mock.calls[0]?.[1]).toStrictEqual(["2026", "1103", "B6"]);
  expect(ports.connect).not.toHaveBeenCalled();
  expect(ports.end).toHaveBeenCalledTimes(1);
});

it("runOwnerIdStore fills only empty owners in one verified transaction", async () => {
  const report = await runOwnerIdStore({
    argv: ["owner-ids-apply", "/h", "2026", "1103", "B6", "production", "--confirm-write"],
    env: ENV,
  });
  expect(report.filled).toBe(2);
  expect(ports.createTargetPool.mock.calls[0]?.[1]).toBe("production");
  expect(ports.clientQuery.mock.calls.map((call) => call[1] ?? call[0])).toStrictEqual([
    "BEGIN",
    ["2026", "1103", "B6", "h1", "o1"],
    ["2026", "1103", "B6", "h2", "o2"],
    ["2026", "1103", "B6"],
    "COMMIT",
  ]);
  expect(ports.release).toHaveBeenCalledTimes(1);
});

it("runOwnerIdStore rolls back when the readback still misses a filled owner", async () => {
  ports.clientQuery.mockImplementation(async (sql: string) =>
    sql.startsWith("SELECT") ? { rows: [{ source_horse_id: "h1" }] } : { rows: [] },
  );
  await expect(
    runOwnerIdStore({
      argv: ["owner-ids-apply", "/h", "2026", "1103", "B6", "local", "--confirm-write"],
      env: ENV,
    }),
  ).rejects.toThrow("Owner id readback is missing filled values.");
  expect(ports.clientQuery.mock.calls.at(-1)?.[0]).toBe("ROLLBACK");
  expect(ports.release).toHaveBeenCalledTimes(1);
  expect(ports.end).toHaveBeenCalledTimes(1);
});

it("runOwnerIdStore rejects bad arguments and incomplete profiles", async () => {
  await expect(
    runOwnerIdStore({
      argv: ["owner-ids-apply", "/h", "2026", "1103", "B6", "local"],
      env: ENV,
    }),
  ).rejects.toThrow("Use owner-ids-plan");
  await expect(
    runOwnerIdStore({
      argv: ["owner-ids-plan", "/h", "2026", "1103", "B6", "local"],
      env: { ...ENV, OVERSEA_OWNER_ID_PROFILE_PATH: "" },
    }),
  ).rejects.toThrow("OVERSEA_OWNER_ID_PROFILE_PATH is required.");
  ports.readFile.mockResolvedValue("{}");
  await expect(
    runOwnerIdStore({
      argv: ["owner-ids-plan", "/h", "2026", "1103", "B6", "local"],
      env: ENV,
    }),
  ).rejects.toThrow("The owner id profile needs an ownerIdPattern.");
});
