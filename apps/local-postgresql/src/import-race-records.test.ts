// Run with bun via vitest; no file or database I/O.
import { afterEach, expect, it, vi } from "vitest";
import { layoutByTable } from "../../daily-keiba-sync/src/layouts";
import {
  buildRaceImportSql,
  parseRaceImportOptions,
  planRaceImport,
  runRaceImport,
} from "./import-race-records";

interface MockClient {
  query: (sql: string, values?: unknown[]) => Promise<unknown>;
  release: () => void;
}
const mocks = vi.hoisted(() => ({
  readFile: vi.fn<() => Promise<string>>(),
  query: vi.fn<MockClient["query"]>(),
  release: vi.fn<() => void>(),
  end: vi.fn<() => Promise<void>>(),
  connect: vi.fn<() => Promise<MockClient>>(),
  pool: vi.fn<(options: unknown) => void>(),
}));
vi.mock("node:fs/promises", () => ({ readFile: mocks.readFile }));
vi.mock("pg", () => ({
  Pool: class {
    constructor(options: unknown) {
      mocks.pool(options);
    }
    connect = mocks.connect;
    end = mocks.end;
  },
}));

const range = { fromDate: "20261003", toDate: "20261004" };
const options = {
  ...range,
  input: "race.txt",
  apply: false,
  databaseUrl: "postgresql://localhost/racing",
};
const record = (table: string, overrides: Record<string, string> = {}): string => {
  const fields: Record<string, string> = {
    record_id: table === "jvd_ra" ? "RA" : "SE",
    data_kubun: "2",
    data_sakusei_nengappi: "20261003",
    kaisai_nen: "2026",
    kaisai_tsukihi: "1004",
    keibajo_code: "08",
    race_bango: "01",
    umaban: "01",
    ketto_toroku_bango: "2024100001",
    ...overrides,
  };
  return layoutByTable(table)
    .columns.map((column) => (fields[column.name] ?? "").padEnd(column.width, " "))
    .join("");
};

afterEach(() => vi.resetAllMocks());

it("parses dates, defaults to dry-run and only permits local PostgreSQL", () => {
  expect(
    parseRaceImportOptions([
      "--input",
      "race.txt",
      "--from-date",
      "2026-10-03",
      "--to-date",
      "2026-10-04",
    ]),
  ).toStrictEqual({
    input: "race.txt",
    fromDate: "20261003",
    toDate: "20261004",
    apply: false,
    databaseUrl: "postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing",
  });
  expect(
    parseRaceImportOptions(
      ["--input", "race.txt", "--from-date", "2026-10-04", "--to-date", "2026-10-04", "--apply"],
      "postgres://localhost/racing",
    ).apply,
  ).toBe(true);
});

it.each([
  [[], "--input is required"],
  [["--input", "race.txt"], "Dates must use YYYY-MM-DD"],
  [["--input", "race.txt", "--from-date", "20261004"], "Dates must use YYYY-MM-DD"],
  [["--input", "race.txt", "--from-date", "2026-02-30"], "Invalid calendar date"],
  [["--input", "race.txt", "--from-date", "2026-13-01"], "Invalid calendar date"],
  [
    ["--input", "race.txt", "--from-date", "2026-10-04", "--to-date", "2026-10-03"],
    "--from-date must not exceed --to-date",
  ],
])("rejects invalid arguments %j", (args, message) => {
  expect(() => parseRaceImportOptions(args)).toThrow(message);
});

it.each(["postgresql://example.com/racing", "https://localhost/racing"])(
  "rejects non-local or non-PG connections %s",
  (url) => {
    expect(() =>
      parseRaceImportOptions(
        ["--input", "race.txt", "--from-date", "2026-10-04", "--to-date", "2026-10-04"],
        url,
      ),
    ).toThrow("Only a local PostgreSQL database is allowed");
  },
);

it("parses both record types, deduplicates newer revisions, and counts ignored records", () => {
  const text = [
    record("jvd_ra"),
    record("jvd_ra", { data_kubun: "7" }),
    record("jvd_ra"),
    record("jvd_se"),
    record("jvd_ra", { kaisai_tsukihi: "1002" }),
    record("jvd_ra", { kaisai_tsukihi: "1005" }),
    "JGignored",
    "JGignored",
  ].join("\r\n");
  const plan = planRaceImport(text, range);
  expect(plan.tables.get("jvd_ra")?.rows.size).toBe(1);
  expect([...plan.tables.get("jvd_ra")!.rows.values()][0]?.data_kubun).toBe("7");
  expect(plan.tables.get("jvd_se")?.rows.size).toBe(1);
  expect(plan.skippedTypes).toStrictEqual({ JG: 2 });
  expect(plan.outsideRange).toBe(2);
});

it("rejects empty ranges, malformed records and unsupported known tables", () => {
  expect(() => planRaceImport("", range)).toThrow("No RA/SE records");
  expect(() => planRaceImport("RAshort", range)).toThrow("Invalid jvd_ra record length");
  expect(() => planRaceImport("UMunsupported", range)).toThrow("No RA/SE records");
});

it("builds a parameterized upsert guarded against downgrading results", () => {
  const sql = buildRaceImportSql({
    tableName: 'test"table',
    recordType: "RA",
    recordBytes: 0,
    columns: [
      { name: "id", width: 1 },
      { name: "value", width: 1 },
    ],
    primaryKey: ["id"],
  });
  expect(sql).toBe(
    'insert into "test""table" ("id","value") values ($1,$2) on conflict ("id") do update set "value"=excluded."value" where (excluded.data_sakusei_nengappi,excluded.data_kubun) >= ("test""table".data_sakusei_nengappi,"test""table".data_kubun)',
  );
});

it("dry-run parses before connecting and reports exact candidate counts", async () => {
  mocks.readFile.mockResolvedValue(record("jvd_ra"));
  expect(await runRaceImport(options)).toStrictEqual({
    applied: false,
    records: { jvd_ra: 1 },
    skippedTypes: {},
    outsideRange: 0,
  });
  expect(mocks.pool).not.toHaveBeenCalled();
});

it("commits RA and SE atomically and cleans provisional runners", async () => {
  mocks.readFile.mockResolvedValue(
    [record("jvd_ra"), record("jvd_se"), record("jvd_se", { umaban: "00" })].join("\n"),
  );
  mocks.connect.mockResolvedValue({ query: mocks.query, release: mocks.release });
  expect(await runRaceImport({ ...options, apply: true })).toStrictEqual({
    applied: true,
    records: { jvd_ra: 1, jvd_se: 1 },
    skippedTypes: {},
    outsideRange: 0,
  });
  expect(mocks.query).toHaveBeenNthCalledWith(1, "begin");
  expect(mocks.query).toHaveBeenCalledWith(
    "delete from jvd_se where kaisai_nen=$1 and kaisai_tsukihi=$2 and keibajo_code=$3 and race_bango=$4 and ketto_toroku_bango=$5 and umaban='00'",
    ["2026", "1004", "08", "01", "2024100001"],
  );
  expect(mocks.query).toHaveBeenLastCalledWith("commit");
  expect(mocks.release).toHaveBeenCalledOnce();
  expect(mocks.end).toHaveBeenCalledOnce();
});

it("rolls back and releases on query failure", async () => {
  mocks.readFile.mockResolvedValue(record("jvd_ra"));
  mocks.connect.mockResolvedValue({ query: mocks.query, release: mocks.release });
  mocks.query.mockRejectedValueOnce(new Error("database failed"));
  await expect(runRaceImport({ ...options, apply: true })).rejects.toThrow("database failed");
  expect(mocks.query).toHaveBeenLastCalledWith("rollback");
  expect(mocks.release).toHaveBeenCalledOnce();
  expect(mocks.end).toHaveBeenCalledOnce();
});

it("closes the pool when connection fails", async () => {
  mocks.readFile.mockResolvedValue(record("jvd_ra"));
  mocks.connect.mockRejectedValue(new Error("connection failed"));
  await expect(runRaceImport({ ...options, apply: true })).rejects.toThrow("connection failed");
  expect(mocks.release).not.toHaveBeenCalled();
  expect(mocks.end).toHaveBeenCalledOnce();
});
