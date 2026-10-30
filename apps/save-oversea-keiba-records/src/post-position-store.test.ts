// This file runs with Bun and Vitest. Database pools, the filesystem and fetch are mocked.
import iconv from "iconv-lite";
import { beforeEach, expect, it, vi } from "vitest";
import { planPostPositions, runPostPositions, type StoredRunner } from "./post-position-store";

const ports = vi.hoisted(() => ({
  createTargetPool: vi.fn(),
  query: vi.fn(),
  release: vi.fn(),
  end: vi.fn(),
  readFile: vi.fn(),
  readdir: vi.fn(),
  open: vi.fn(),
  handleWrite: vi.fn(),
  handleClose: vi.fn(),
}));
vi.mock("./storage/history-connection", () => ({ createTargetPool: ports.createTargetPool }));
vi.mock("node:fs/promises", () => ({
  readFile: ports.readFile,
  readdir: ports.readdir,
  open: ports.open,
}));

const ENV = {
  POSTGRES_PORT: "15432",
  POSTGRES_USER: "user",
  POSTGRES_PASSWORD: "secret",
  POSTGRES_DB: "db",
};
const PAGE: string = `<BIG>ブリーダーズカップ・クラシック
<FONT COLOR=RED>(GI)</FONT></BIG>
2025/11/1　米・デルマー9R
<TABLE>
<TR align=center bgcolor=silver><td>着順<td>馬番<td>枠順<td>馬名<td>性齢<td>斤量</TR>
<tr align=center bgcolor=lightgreen><td>1着<td>5番<td>5番<td>A<td>牡4<td>57</tr>
<tr align=center><td>2着<td>7番<td>6番<td>B<td>牝4<td>55.5</tr>
</TABLE>`;
const RACE = {
  date: "20251101",
  title: "ブリーダーズカップクラシック",
  runners: [
    { finish: 1, umaban: 5, gate: 5, sexCode: "1", age: 4, weightKg: 57 },
    { finish: 2, umaban: 7, gate: 6, sexCode: "2", age: 4, weightKg: 55.5 },
    { finish: 3, umaban: null, gate: 2, sexCode: "1", age: 3, weightKg: 56 },
    { finish: null, umaban: 9, gate: 8, sexCode: "1", age: 5, weightKg: 57 },
    { finish: 4, umaban: 4, gate: 4, sexCode: "1", age: 4, weightKg: 57 },
    { finish: 5, umaban: null, gate: 1, sexCode: null, age: null, weightKg: null },
  ],
};
const body = (html: string): Uint8Array<ArrayBuffer> =>
  Uint8Array.from(iconv.encode(html, "cp932"));
const stored = (umaban: string, finish: string, extra: Partial<StoredRunner>): StoredRunner => ({
  race_bango: "09",
  umaban,
  kakutei_chakujun: finish,
  ijo_kubun_code: "0",
  has_source_row: true,
  gate_number: null,
  seibetsu_code: "1",
  barei: "04",
  futan_juryo: "570",
  ...extra,
});
const row = (umaban: string, finish: string, gate: string | null): Record<string, unknown> => ({
  kaisai_nen: "2025",
  kaisai_tsukihi: "1101",
  race_bango: "09",
  umaban,
  kakutei_chakujun: finish,
  seibetsu_code: umaban === "07" ? "2" : "1",
  barei: umaban === "07" ? "00" : "04",
  futan_juryo: umaban === "07" ? "555" : "570",
  has_source_row: true,
  gate_number: gate,
});

beforeEach(() => {
  vi.resetAllMocks();
  ports.createTargetPool.mockReturnValue({
    targetFingerprint: "local",
    pool: {
      connect: async () => ({ query: ports.query, release: ports.release }),
      end: ports.end,
    },
  });
  ports.end.mockResolvedValue(undefined);
  ports.readdir.mockResolvedValue(["bcclassic_2025.html", "bcclassic.html", "notes.txt"]);
  ports.readFile.mockResolvedValue(iconv.encode(PAGE, "cp932"));
  ports.handleWrite.mockResolvedValue(undefined);
  ports.handleClose.mockResolvedValue(undefined);
  ports.open.mockResolvedValue({ writeFile: ports.handleWrite, close: ports.handleClose });
});

it("matches by finish, checks horse numbers, and only fills empty values", () => {
  expect(
    planPostPositions(RACE, [
      stored("05", "01", {}),
      stored("07", "02", { gate_number: "6", seibetsu_code: "0", barei: "00", futan_juryo: "" }),
      stored("03", "03", { has_source_row: false }),
      stored("09", "00", { gate_number: "7", futan_juryo: "560" }),
      stored("08", "04", {}),
    ]),
  ).toStrictEqual({
    updates: [{ raceBango: "09", umaban: "05", gate: 5 }],
    profileFills: [
      { raceBango: "09", umaban: "07", column: "seibetsu_code", value: "2" },
      { raceBango: "09", umaban: "07", column: "barei", value: "04" },
      { raceBango: "09", umaban: "07", column: "futan_juryo", value: "555" },
    ],
    profileConflicts: [
      "20251101:ブリーダーズカップクラシック:3:2:barei",
      "20251101:ブリーダーズカップクラシック:3:2:futan_juryo",
      "20251101:ブリーダーズカップクラシック:-:8:barei",
      "20251101:ブリーダーズカップクラシック:-:8:futan_juryo",
    ],
    unchanged: ["20251101:ブリーダーズカップクラシック:2:6"],
    withoutSourceRow: ["20251101:ブリーダーズカップクラシック:3:2"],
    notStored: ["20251101:ブリーダーズカップクラシック:5:1"],
    mismatches: ["20251101:ブリーダーズカップクラシック:4:4"],
    conflicts: ["20251101:ブリーダーズカップクラシック:-:8"],
  });
});

it("plans from archived pages, reporting pages it cannot parse or match", async () => {
  ports.readdir.mockResolvedValue(["bcclassic_2025.html", "bcmile_2025.html", "bcturf_2025.html"]);
  ports.readFile.mockImplementation(async (path: string) =>
    iconv.encode(path.includes("bcturf") ? "<html></html>" : PAGE, "cp932"),
  );
  ports.query
    .mockResolvedValueOnce({ rowCount: 2, rows: [row("05", "01", null), row("07", "02", "6")] })
    .mockResolvedValueOnce({ rowCount: 0, rows: [] });
  await expect(
    runPostPositions({ argv: ["post-positions-plan", "/private/pages", "A4", "local"], env: ENV }),
  ).resolves.toStrictEqual({
    status: "complete",
    operation: "planned",
    pages: 3,
    races: 1,
    unparsedPages: ["bcturf_2025.html"],
    unmatchedPages: ["bcmile_2025.html"],
    updated: 1,
    profileFilled: 1,
    profileConflicts: [],
    unchanged: 1,
    withoutSourceRow: 0,
    notStored: 0,
    mismatches: [],
    conflicts: [],
  });
  expect(ports.query.mock.calls[0]?.[1]).toStrictEqual([
    "A4",
    ["20251101", "20251102"],
    "ブリーダーズカップクラシック",
  ]);
  expect(ports.release).toHaveBeenCalledTimes(1);
  expect(ports.end).toHaveBeenCalledTimes(1);
});

it("applies gates and profile fills in one transaction", async () => {
  ports.query.mockImplementation(async (text: string) =>
    text.startsWith("SELECT")
      ? { rowCount: 2, rows: [row("05", "01", null), row("07", "02", "6")] }
      : { rowCount: 1, rows: [] },
  );
  await expect(
    runPostPositions({
      argv: ["post-positions-apply", "/private/pages", "A4", "local", "--confirm-write"],
      env: ENV,
    }),
  ).resolves.toMatchObject({ operation: "applied", updated: 1, profileFilled: 1 });
  expect(ports.query.mock.calls.map(([sql]) => String(sql).split(/\s/u)[0])).toStrictEqual([
    "SELECT",
    "BEGIN",
    "UPDATE",
    "UPDATE",
    "COMMIT",
  ]);
  expect(ports.query.mock.calls[2]?.[1]).toStrictEqual(["5", "2025", "1101", "A4", "09", "05"]);
  expect(ports.query.mock.calls[3]?.[1]).toStrictEqual(["04", "2025", "1101", "A4", "09", "07"]);
});

it("rolls back when an update does not hit exactly one row", async () => {
  ports.query.mockImplementation(async (text: string) =>
    text.startsWith("SELECT")
      ? { rowCount: 1, rows: [row("05", "01", null)] }
      : { rowCount: text.startsWith("UPDATE") ? 0 : 1, rows: [] },
  );
  await expect(
    runPostPositions({
      argv: ["post-positions-apply", "/private/pages", "A4", "local", "--confirm-write"],
      env: ENV,
    }),
  ).rejects.toThrow("A post-position update did not match exactly one row.");
  expect(ports.query.mock.calls.at(-1)?.[0]).toBe("ROLLBACK");
  expect(ports.release).toHaveBeenCalledTimes(1);
});

it("does not open a transaction when nothing needs filling", async () => {
  ports.query.mockResolvedValue({ rowCount: 0, rows: [] });
  await expect(
    runPostPositions({
      argv: ["post-positions-apply", "/private/pages", "A4", "local", "--confirm-write"],
      env: ENV,
    }),
  ).resolves.toMatchObject({ operation: "applied", races: 0, updated: 0 });
  expect(ports.query).toHaveBeenCalledTimes(1);
});

it("fetches index and missing year pages serially, keeping archived ones", async () => {
  const sleep = vi.fn(async () => undefined);
  const fetchImpl = vi.fn(async (url: string) =>
    url.endsWith("bcclassic.html")
      ? new Response(
          body(
            '<A HREF="bcclassic_2025.html"><A HREF="bcclassic_2024.html"><A HREF="bcclassic_1990.html">',
          ),
        )
      : new Response(body(PAGE)),
  );
  ports.readdir.mockResolvedValue(["bcclassic_2024.html"]);
  ports.open.mockRejectedValueOnce(Object.assign(new Error("exists"), { code: "EEXIST" }));
  await expect(
    runPostPositions({
      argv: ["post-positions-fetch", "/private/pages", "1996", "bcclassic"],
      env: ENV,
      fetchImpl,
      sleep,
    }),
  ).resolves.toMatchObject({ operation: "fetched", pages: 0, races: 2 });
  expect(fetchImpl.mock.calls.map(([url]) => url)).toStrictEqual([
    "https://ahonoora.com/bcclassic.html",
    "https://ahonoora.com/bcclassic_2025.html",
  ]);
  expect(sleep).toHaveBeenCalledWith(2000);
  expect(ports.open.mock.calls[0]?.[0]).toBe("/private/pages/bcclassic_2025.html");
});

it("writes a newly fetched page and propagates fetch and write failures", async () => {
  const fetchImpl = vi.fn(async (url: string) =>
    url.endsWith("bcmile.html")
      ? new Response(body('<A HREF="bcmile_2025.html">'))
      : new Response(body(PAGE)),
  );
  ports.readdir.mockResolvedValue([]);
  await expect(
    runPostPositions({
      argv: ["post-positions-fetch", "/private/pages", "1996", "bcmile"],
      env: ENV,
      fetchImpl,
      sleep: async () => undefined,
    }),
  ).resolves.toMatchObject({ pages: 1, races: 1 });
  expect(ports.handleClose).toHaveBeenCalledTimes(1);
  ports.open.mockRejectedValueOnce(new Error("Disk full"));
  await expect(
    runPostPositions({
      argv: ["post-positions-fetch", "/private/pages", "1996", "bcmile"],
      env: ENV,
      fetchImpl,
      sleep: async () => undefined,
    }),
  ).rejects.toThrow("Disk full");
  await expect(
    runPostPositions({
      argv: ["post-positions-fetch", "/private/pages", "1996", "bcmile"],
      env: ENV,
      fetchImpl: async () => new Response("gone", { status: 404 }),
      sleep: async () => undefined,
    }),
  ).rejects.toThrow("Ahonoora page bcmile.html returned 404.");
});

it("requires a valid action, venue, target and confirmation", async () => {
  const usage =
    "Use post-positions-fetch DIRECTORY MIN_YEAR SERIES... or post-positions-plan|post-positions-apply DIRECTORY VENUE local [--confirm-write].";
  await expect(
    runPostPositions({ argv: ["post-positions-fetch", "/private/pages", "96"], env: ENV }),
  ).rejects.toThrow(usage);
  await expect(
    runPostPositions({ argv: ["post-positions-plan", "/private/pages", "05", "local"], env: ENV }),
  ).rejects.toThrow(usage);
  await expect(
    runPostPositions({
      argv: ["post-positions-plan", "/private/pages", "A4", "production"],
      env: ENV,
    }),
  ).rejects.toThrow(usage);
  await expect(
    runPostPositions({ argv: ["post-positions-apply", "/private/pages", "A4", "local"], env: ENV }),
  ).rejects.toThrow(usage);
});

it("never matches an unplaced page runner to a stored scratched row", () => {
  expect(
    planPostPositions(
      {
        date: "20251101",
        title: "ブリーダーズカップクラシック",
        runners: [{ finish: null, umaban: 6, gate: 6, sexCode: null, age: null, weightKg: null }],
      },
      [stored("06", "00", { ijo_kubun_code: "1" })],
    ),
  ).toStrictEqual({
    updates: [],
    profileFills: [],
    profileConflicts: [],
    unchanged: [],
    withoutSourceRow: [],
    notStored: ["20251101:ブリーダーズカップクラシック:-:6"],
    mismatches: [],
    conflicts: [],
  });
});
