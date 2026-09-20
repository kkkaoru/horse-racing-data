// Runs with bun via Vitest.
import { beforeEach, describe, expect, test, vi } from "vitest";
import { layoutByTable } from "./layouts";
import { syncNeonTable } from "./neon-sync";
import type { RecordRow, TableStage } from "./types";

const mocks = vi.hoisted(() => ({ query: vi.fn() }));

vi.mock("@neondatabase/serverless", () => ({
  neon: () => ({ query: mocks.query }),
}));

const completeRow = (tableName: string): RecordRow => {
  const row: Record<string, string> = {};
  for (const column of layoutByTable(tableName).columns) row[column.name] = "";
  row.record_id = tableName.slice(4).toUpperCase();
  return row;
};

const stage = (
  tableName = "nvd_ra",
  records: readonly RecordRow[] = [completeRow(tableName)],
): TableStage => ({
  formatVersion: 1,
  provider: tableName.startsWith("jvd") ? "jv" : "nv",
  records,
  runId: "run-1",
  tableName,
});

beforeEach(() => {
  mocks.query.mockReset();
  mocks.query.mockResolvedValue([]);
});

describe("Neon differential sync", () => {
  test("backs up course identity and deletion markers without a fabricated race year", async () => {
    await expect(
      syncNeonTable(
        stage("jvd_cs", [
          {
            record_id: "CS",
            data_kubun: "0",
            data_sakusei_nengappi: "20260918",
            keibajo_code: "05",
            kyori: "1600",
            track_code: "11",
            course_kaishu_nengappi: "20240106",
            course_setsumei: "コース説明",
          },
        ]),
        { NEON_DATABASE_URL: "postgresql://example" },
      ),
    ).resolves.toBe(1);
    const sql: string = String(mocks.query.mock.calls[1]?.[0]);
    expect(sql).toMatch(/^insert into "jvd_cs"/);
    expect(sql).toMatch(
      /on conflict \("keibajo_code", "kyori", "track_code", "course_kaishu_nengappi"\)/,
    );
    expect(sql).toMatch(/"data_kubun" = excluded\."data_kubun"/);
    expect(sql).toMatch(/is distinct from row\(/);
    expect(sql).not.toMatch(/kaisai_nen|delete from/i);
    expect(mocks.query.mock.calls[1]?.[1]).toStrictEqual([
      "CS",
      "0",
      "20260918",
      "05",
      "1600",
      "11",
      "20240106",
      "コース説明",
    ]);
  });

  test("warms compute before a primary-key upsert", async () => {
    await expect(
      syncNeonTable(stage(), { NEON_DATABASE_URL: "postgresql://example" }),
    ).resolves.toBe(1);

    expect(mocks.query).toHaveBeenCalledTimes(2);
    expect(mocks.query.mock.calls[0]).toEqual(["select 1"]);
    expect(mocks.query.mock.calls[1]?.[0]).toContain('insert into "nvd_ra"');
    expect(mocks.query.mock.calls[1]?.[0]).toContain('on conflict ("kaisai_nen"');
    expect(mocks.query.mock.calls[1]?.[1]).toHaveLength(62);
  });

  test("deletes a stale provisional JRA runner in the same upsert statement", async () => {
    const row = completeRow("jvd_se");
    Object.assign(row, {
      kaisai_nen: "2026",
      kaisai_tsukihi: "0912",
      keibajo_code: "06",
      race_bango: "01",
      umaban: "03",
      ketto_toroku_bango: "2023100001",
    });

    await expect(
      syncNeonTable(stage("jvd_se", [row]), { NEON_DATABASE_URL: "postgresql://example" }),
    ).resolves.toBe(1);

    const sql = String(mocks.query.mock.calls[1]?.[0]);
    expect(sql).toContain("deleted_provisional as");
    expect(sql).toContain('delete from "jvd_se" existing using incoming');
    expect(sql).toContain("existing.\"umaban\" = '00'");
    expect(mocks.query.mock.calls[1]?.[1]).toHaveLength(70);
  });

  test.each(["nvd_ra", "jvd_se"])(
    "updates %s backup rows only when non-key values differ, including nulls",
    async (tableName: string) => {
      await syncNeonTable(stage(tableName), { NEON_DATABASE_URL: "postgresql://example" });

      const sql: string = String(mocks.query.mock.calls[1]?.[0]);
      expect(sql).toMatch(/where row\(/);
      expect(sql).toMatch(/is distinct from row\(excluded\."record_id"/);
      expect(sql).toMatch(/do update set "record_id" = excluded\."record_id"/);
      expect(sql).not.toMatch(/set "kaisai_nen"/);
      expect(sql).not.toMatch(/where row\([^)]*\."kaisai_nen"/);
    },
  );

  test("retries a cold Neon compute before writing", async () => {
    mocks.query.mockRejectedValueOnce(new Error("cold"));
    mocks.query.mockRejectedValueOnce(new Error("cold"));
    mocks.query.mockResolvedValue([]);

    await expect(
      syncNeonTable(stage(), { NEON_DATABASE_URL: "postgresql://example" }),
    ).resolves.toBe(1);
    expect(mocks.query).toHaveBeenCalledTimes(4);
  });

  test("fails closed when Neon never becomes ready", async () => {
    mocks.query.mockRejectedValue(new Error("offline"));
    await expect(
      syncNeonTable(stage(), { NEON_DATABASE_URL: "postgresql://example" }),
    ).rejects.toMatchObject({ safeStage: "neon-connect" });
    expect(mocks.query).toHaveBeenCalledTimes(3);
  });

  test("splits a wide-table upsert under the PostgreSQL parameter limit", async () => {
    const row = completeRow("nvd_wf");
    const records = Array.from({ length: 225 }, () => row);
    await expect(
      syncNeonTable(stage("nvd_wf", records), { NEON_DATABASE_URL: "postgresql://example" }),
    ).resolves.toBe(225);
    expect(mocks.query).toHaveBeenCalledTimes(8);
  });

  test.each([
    ["42703", "neon-schema"],
    ["22001", "neon-data"],
    ["23505", "neon-integrity"],
    ["XX000", "neon-upsert"],
    [undefined, "neon-upsert"],
  ])("classifies a PostgreSQL upsert failure safely", async (code, safeStage) => {
    const error = Object.assign(new Error("private query detail"), { code });
    mocks.query.mockResolvedValueOnce([]).mockRejectedValueOnce(error);
    await expect(
      syncNeonTable(stage(), { NEON_DATABASE_URL: "postgresql://example" }),
    ).rejects.toMatchObject({ safeStage });
  });

  test("rejects empty stages and missing columns", async () => {
    await expect(
      syncNeonTable(stage("nvd_ra", []), { NEON_DATABASE_URL: "postgresql://example" }),
    ).rejects.toThrow("no records");
    await expect(
      syncNeonTable(stage("nvd_ra", [{}]), { NEON_DATABASE_URL: "postgresql://example" }),
    ).rejects.toThrow("missing a column");
  });
});
