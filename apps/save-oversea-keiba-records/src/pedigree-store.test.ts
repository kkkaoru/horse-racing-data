// This file runs with Bun and Vitest. Database pools and the filesystem are mocked.
import { beforeEach, expect, it, vi } from "vitest";
import { planPedigreeStore, runPedigreeStore } from "./pedigree-store";

const ports = vi.hoisted(() => ({
  createTargetPool: vi.fn(),
  poolQuery: vi.fn(),
  clientQuery: vi.fn(),
  release: vi.fn(),
  end: vi.fn(),
  readFile: vi.fn(),
  readdir: vi.fn(),
}));
vi.mock("./storage/history-connection", () => ({ createTargetPool: ports.createTargetPool }));
vi.mock("node:fs/promises", () => ({ readFile: ports.readFile, readdir: ports.readdir }));

const ENV = {
  POSTGRES_PORT: "15432",
  POSTGRES_USER: "user",
  POSTGRES_PASSWORD: "secret",
  POSTGRES_DB: "db",
  OVERSEA_PEDIGREE_MARKUP_PROFILE_PATH: "/private/pedigree.json",
};
const PROFILE = JSON.stringify({
  tableMarker: 'class="ped"',
  horsePathPrefix: "/horse/",
  sourceUrlTemplate: "https://source.test/horse/ped/{HORSE_ID}/",
});
const PAGE = `<html><table class="ped">
<tr><td><a href="/horse/s1/">Sire</a></td><td><a href="/horse/ss1/">Sire Sire</a></td></tr>
<tr><td><a href="/horse/sd1/">Sire Dam</a></td></tr>
<tr><td><a href="/horse/d1/">Dam</a></td><td><a href="/horse/ds1/">Dam Sire</a></td></tr>
<tr><td><a href="/horse/dd1/">Dam Dam</a></td></tr>
</table></html>`;
const ROW = {
  source: "netkeiba",
  source_horse_id: "h1",
  sire_source_id: "s1",
  sire_name: "Sire",
  sire_sire_source_id: "ss1",
  sire_sire_name: "Sire Sire",
  dam_source_id: "d1",
  dam_name: "Dam",
  dam_sire_source_id: "ds1",
  dam_sire_name: "Dam Sire",
  source_url: "https://source.test/horse/ped/h1/",
};

beforeEach(() => {
  vi.resetAllMocks();
  ports.createTargetPool.mockImplementation((_env: unknown, target: string) => ({
    targetFingerprint: target,
    pool: {
      query: ports.poolQuery,
      connect: async () => ({ query: ports.clientQuery, release: ports.release }),
      end: ports.end,
    },
  }));
  ports.end.mockResolvedValue(undefined);
  ports.readdir.mockImplementation(async (path: string) =>
    path === "/private/history"
      ? ["horse-h1", "horse-h1-v2", "horse-h2", "jockey-j1"]
      : ["a.raw", "a.raw.meta.json"],
  );
  ports.readFile.mockImplementation(async (path: string) => {
    if (path === "/private/pedigree.json") return PROFILE;
    return path.includes("horse-h2") ? "<html>no pedigree table</html>" : PAGE;
  });
});

it("plans inserts, unchanged rows and conflicts without touching stored values", () => {
  expect(
    planPedigreeStore(
      [ROW, { ...ROW, source_horse_id: "h2" }, { ...ROW, source_horse_id: "h3" }],
      [
        { ...ROW, source_horse_id: "h2" },
        { ...ROW, source_horse_id: "h3", dam_name: "Other" },
      ],
    ),
  ).toStrictEqual({
    inserts: [ROW],
    conflicts: ["h3:dam_name"],
    unchanged: 2,
  });
});

it("plans from archived horse pages, skipping horses whose page has no pedigree table", async () => {
  ports.poolQuery.mockResolvedValue({ rowCount: 0, rows: [] });
  await expect(
    runPedigreeStore({ argv: ["pedigree-plan", "/private/history", "local"], env: ENV }),
  ).resolves.toStrictEqual({
    status: "complete",
    operation: "planned",
    target: "local",
    archivedHorses: 2,
    parsed: 1,
    withoutPedigree: 1,
    inserted: 1,
    unchanged: 0,
    conflicts: [],
  });
  expect(ports.poolQuery.mock.calls[0]?.[1]).toStrictEqual(["netkeiba", ["h1"]]);
  expect(ports.end).toHaveBeenCalledTimes(1);
});

it("inserts missing rows in one locked transaction and keeps differing stored rows", async () => {
  ports.readdir.mockImplementation(async (path: string) =>
    path === "/private/history" ? ["horse-h1", "horse-h9"] : ["a.raw"],
  );
  const stored = {
    ...ROW,
    source_horse_id: "h9",
    sire_name: "Stored",
    source_url: "https://source.test/horse/ped/h9/",
  };
  ports.clientQuery.mockImplementation(async (text: string) =>
    text.startsWith("SELECT")
      ? {
          rowCount: 0,
          rows:
            ports.clientQuery.mock.calls.filter(([sql]) => String(sql).startsWith("INSERT"))
              .length > 0
              ? [ROW, stored]
              : [stored],
        }
      : { rowCount: 1, rows: [] },
  );
  await expect(
    runPedigreeStore({
      argv: ["pedigree-apply", "/private/history", "production", "--confirm-write"],
      env: ENV,
    }),
  ).resolves.toStrictEqual({
    status: "complete",
    operation: "applied",
    target: "production",
    archivedHorses: 2,
    parsed: 2,
    withoutPedigree: 0,
    inserted: 1,
    unchanged: 1,
    conflicts: ["h9:sire_name"],
  });
  const statements: string[] = ports.clientQuery.mock.calls.map(
    ([sql]) => String(sql).split(" ")[0] ?? "",
  );
  expect(statements).toStrictEqual(["BEGIN", "LOCK", "SELECT", "INSERT", "SELECT", "COMMIT"]);
  expect(ports.release).toHaveBeenCalledTimes(1);
});

it("rolls back when the readback misses an inserted row", async () => {
  ports.clientQuery.mockImplementation(async (text: string) =>
    text.startsWith("SELECT") ? { rowCount: 0, rows: [] } : { rowCount: 1, rows: [] },
  );
  await expect(
    runPedigreeStore({
      argv: ["pedigree-apply", "/private/history", "local", "--confirm-write"],
      env: ENV,
    }),
  ).rejects.toThrow("Pedigree readback is missing inserted rows.");
  expect(ports.clientQuery.mock.calls.at(-1)?.[0]).toBe("ROLLBACK");
  expect(ports.release).toHaveBeenCalledTimes(1);
});

it("does not open a transaction when no archived page has a pedigree", async () => {
  ports.readFile.mockImplementation(async (path: string) =>
    path === "/private/pedigree.json" ? PROFILE : "<html></html>",
  );
  await expect(
    runPedigreeStore({
      argv: ["pedigree-apply", "/private/history", "local", "--confirm-write"],
      env: ENV,
    }),
  ).resolves.toStrictEqual({
    status: "complete",
    operation: "applied",
    target: "local",
    archivedHorses: 2,
    parsed: 0,
    withoutPedigree: 2,
    inserted: 0,
    unchanged: 0,
    conflicts: [],
  });
  expect(ports.clientQuery).not.toHaveBeenCalled();
  expect(ports.poolQuery).not.toHaveBeenCalled();
});

it("requires a valid action, target, confirmation and private profile", async () => {
  await expect(
    runPedigreeStore({ argv: ["pedigree-apply", "/private/history", "local"], env: ENV }),
  ).rejects.toThrow(
    "Use pedigree-plan HISTORY_ROOT TARGET or pedigree-apply HISTORY_ROOT TARGET --confirm-write; TARGET is local or production.",
  );
  await expect(
    runPedigreeStore({ argv: ["pedigree-plan", "/private/history", "staging"], env: ENV }),
  ).rejects.toThrow(
    "Use pedigree-plan HISTORY_ROOT TARGET or pedigree-apply HISTORY_ROOT TARGET --confirm-write; TARGET is local or production.",
  );
  await expect(
    runPedigreeStore({
      argv: ["pedigree-plan", "/private/history", "local"],
      env: { ...ENV, OVERSEA_PEDIGREE_MARKUP_PROFILE_PATH: "" },
    }),
  ).rejects.toThrow("OVERSEA_PEDIGREE_MARKUP_PROFILE_PATH is required.");
  ports.readFile.mockResolvedValue(JSON.stringify({ tableMarker: "x" }));
  await expect(
    runPedigreeStore({ argv: ["pedigree-plan", "/private/history", "local"], env: ENV }),
  ).rejects.toThrow("The pedigree markup profile is incomplete.");
});
