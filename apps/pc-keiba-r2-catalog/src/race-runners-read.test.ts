// Runs with bun through Vitest; no provider I/O.
import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import {
  buildRaceRunnersIdentitySql,
  buildRaceRunnersReadSql,
  readRaceRunners,
  type RaceRunnersReadInput,
} from "./race-runners-read";

const JRA: RaceRunnersReadInput = {
  namespace: "pc_keiba",
  source: "jra",
  date: "20260920",
  keibajoCode: "06",
  raceBango: "01",
};
const NAR: RaceRunnersReadInput = { ...JRA, source: "nar", keibajoCode: "54", raceBango: "08" };

// The R2 catalog mirror is generated from the local PostgreSQL mirror, so the
// checked-in reference holds the offline source of truth for the columns R2 SQL
// can project. Keeping the projection inside that list is what the bloodline
// regression broke: `se.sire_name` is absent from the JV/NV `se` snapshot, R2
// SQL answered 40004 (No field named se.sire_name), the read 503'd, and the
// viewer replaced the whole race page with an error boundary.
const MIRROR_REFERENCE: string = readFileSync(
  new URL("../../../apps/local-postgresql/docs/pc-keiba-postgresql-reference.md", import.meta.url),
  "utf8",
);
const mirrorColumns = (table: string): Set<string> => {
  const section: string = MIRROR_REFERENCE.split(`### \`${table}\``)[1]!.split("\n### ")[0]!;
  return new Set([...section.matchAll(/^\| `([a-z0-9_]+)`/gmu)].map((match) => match[1]!));
};

const runnerRow = (overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
  wakuban: "1",
  umaban: "01",
  ketto_toroku_bango: "2021106753",
  bamei: "ヴァルタリ　　　　　　　　　　　　　",
  moshoku_code: "1",
  seibetsu_code: "1",
  barei: "4",
  futan_juryo: "550",
  kishumei_ryakusho: "浜尚美　",
  chokyoshimei_ryakusho: "田中　",
  banushimei: "　",
  bataiju: "480",
  zogen_fugo: "+",
  zogen_sa: "002",
  kakutei_chakujun: "01",
  tansho_odds: "0123",
  tansho_ninkijun: "02",
  soha_time: "1234",
  time_sa: "0005",
  corner_1: "03",
  corner_2: "02",
  corner_3: "01",
  corner_4: "01",
  kohan_3f: "345",
  blinker_shiyo_kubun: "0",
  sire_name: "Nicobar                             ",
  sire_sire_name: "　　　　　　　　　　　　　　　　　　",
  dam_sire_name: "Kaldounevees                        ",
  ...overrides,
});
const identityRow = (overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
  umaban: "05",
  source: "netkeiba",
  source_horse_id: "2021100675",
  source_url: "https://example.test/horse/2021100675",
  horse_name_full: " Horse Name ",
  jockey_name_full: "　",
  trainer_name_full: null,
  owner_name_full: "Owner",
  ...overrides,
});

it("builds a JRA runner read with the provisional filter and bloodline join", () => {
  const sql: string = buildRaceRunnersReadSql(JRA);
  expect(sql).toMatch("FROM pc_keiba.jvd_se se");
  expect(sql).toMatch(
    "LEFT JOIN pc_keiba.jvd_um um ON um.ketto_toroku_bango = se.ketto_toroku_bango",
  );
  expect(sql).toMatch("um.ketto_joho_01b AS sire_name");
  expect(sql).toMatch("AND coalesce(btrim(se.ijo_kubun_code), '0') NOT IN ('1', '2')");
  expect(sql).toMatch(
    "AND try_cast(nullif(btrim(coalesce(se.umaban, '')), '') AS INT) BETWEEN 1 AND 18",
  );
  expect(sql).toMatch(
    "ORDER BY try_cast(nullif(se.umaban, '') AS INT) ASC, se.ketto_toroku_bango ASC",
  );
  expect(sql).toMatch("LIMIT 19");
  expect(sql).not.toMatch("regexp_replace");
  expect(sql).not.toMatch("oversea_runner_identity");
});

it("projects bloodline names from the master join only", () => {
  // JV/NV `se` snapshots have no bloodline columns, so selecting `se.sire_name`
  // makes R2 SQL reject the whole statement (error 40004) and the read 503s.
  for (const input of [JRA, NAR]) {
    const sql: string = buildRaceRunnersReadSql(input);
    expect(sql).not.toMatch(/se\.sire_name/u);
    expect(sql).not.toMatch(/se\.sire_sire_name/u);
    expect(sql).not.toMatch(/se\.dam_sire_name/u);
  }
  const projection: string = buildRaceRunnersReadSql(JRA).split("\nFROM ")[0]!;
  // Duplicate mirror rows must collapse in the engine before the row bound.
  expect(projection).toMatch("SELECT DISTINCT ");
  const select: string = projection.replace(/^SELECT DISTINCT /u, "");
  expect(select.split(", ")).toHaveLength(28);
});

it("projects only columns that exist in the mirror tables", () => {
  for (const input of [JRA, NAR]) {
    const sql: string = buildRaceRunnersReadSql(input);
    const aliases = new Map(
      [...sql.matchAll(/(?:FROM|JOIN)\s+\S+\.(\w+)\s+(\w+)/gu)].map((match) => [
        match[2]!,
        match[1]!,
      ]),
    );
    expect(aliases.size, `${input.source} runner SQL table aliases`).toBe(
      input.source === "jra" ? 2 : 1,
    );
    for (const [, alias, column] of sql.matchAll(/\b(\w+)\.(\w+)/gu)) {
      const table: string | undefined = aliases.get(alias!);
      if (table === undefined) continue;
      expect(
        mirrorColumns(table).has(column!),
        `${table}.${column} is not a column of the mirror table`,
      ).toBe(true);
    }
  }
});

it("builds a NAR runner read without the provisional filter or bloodline join", () => {
  const sql: string = buildRaceRunnersReadSql(NAR);
  expect(sql).toMatch("FROM pc_keiba.nvd_se se");
  expect(sql).toMatch("NULL AS sire_name, NULL AS sire_sire_name, NULL AS dam_sire_name");
  expect(sql).not.toMatch("jvd_um");
  expect(sql).not.toMatch("ijo_kubun_code");
  expect(sql).toMatch("AND se.keibajo_code = '54'");
});

it.each([
  { ...JRA, namespace: "pc-keiba" },
  { ...JRA, namespace: "1bad" },
  { ...JRA, date: "20260229" },
  { ...JRA, date: "2026092" },
  { ...JRA, keibajoCode: "6" },
  { ...JRA, keibajoCode: "a1" },
  { ...JRA, raceBango: "1" },
  { ...JRA, raceBango: "aa" },
  { ...JRA, source: "ban-ei" as "jra" },
])("rejects invalid runner input %j", (input) => {
  expect(() => buildRaceRunnersReadSql(input)).toThrow("Invalid race runners input");
});

it("builds the overseas identity read for JRA only", () => {
  const sql: string = buildRaceRunnersIdentitySql(JRA);
  expect(sql).toMatch("FROM pc_keiba.oversea_runner_identity");
  expect(sql).toMatch("WHERE race_source = 'jra'");
  expect(sql).toMatch("AND keibajo_code = '06'");
  expect(sql).toMatch("LIMIT 19");
  expect(() => buildRaceRunnersIdentitySql(NAR)).toThrow(
    "Overseas identities exist for JRA races only",
  );
});

const expectedRunner = (): Record<string, string | null> => ({
  wakuban: "1",
  umaban: "01",
  kettoTorokuBango: "2021106753",
  bamei: "ヴァルタリ　　　　　　　　　　　　　",
  moshokuCode: "1",
  seibetsuCode: "1",
  barei: "4",
  futanJuryo: "550",
  kishumeiRyakusho: "浜尚美　",
  chokyoshimeiRyakusho: "田中　",
  banushimei: "　",
  bataiju: "480",
  zogenFugo: "+",
  zogenSa: "002",
  kakuteiChakujun: "01",
  tanshoOdds: "0123",
  tanshoNinkijun: "02",
  sohaTime: "1234",
  timeSa: "0005",
  corner1: "03",
  corner2: "02",
  corner3: "01",
  corner4: "01",
  kohan3f: "345",
  blinkerShiyoKubun: "0",
  sireName: "Nicobar                             ",
  sireSireName: "　　　　　　　　　　　　　　　　　　",
  damSireName: "Kaldounevees                        ",
});

it("returns runners with the raw mirror values and merged identities", async () => {
  const queries: string[] = [];
  const result = await readRaceRunners({
    input: JRA,
    query: async (sql) => {
      queries.push(sql);
      return sql.includes("oversea_runner_identity") ? [identityRow()] : [runnerRow()];
    },
  });
  expect(queries).toHaveLength(2);
  expect(result.runners[0]).toStrictEqual(expectedRunner());
  expect(result.runners[0]?.bamei).toBe("ヴァルタリ　　　　　　　　　　　　　");
  expect(result.runners[0]?.sireName).toBe("Nicobar                             ");
  expect(result.identities).toStrictEqual([
    {
      umaban: "05",
      identitySource: "netkeiba",
      sourceHorseId: "2021100675",
      sourceUrl: "https://example.test/horse/2021100675",
      horseNameFull: "Horse Name",
      // PostgreSQL `btrim` strips ASCII spaces only, so the ideographic space survives.
      jockeyNameFull: "　",
      trainerNameFull: null,
      ownerNameFull: "Owner",
    },
  ]);
});

it("returns an empty runner list without identities for NAR", async () => {
  const queries: string[] = [];
  const result = await readRaceRunners({
    input: NAR,
    query: async (sql) => {
      queries.push(sql);
      return [];
    },
  });
  expect(queries).toHaveLength(1);
  expect(result).toStrictEqual({ runners: [], identities: [] });
});

it.each([
  [{ ...runnerRow(), umaban: "19" }, "Invalid race runner umaban"],
  [{ ...runnerRow(), umaban: "00" }, "Invalid race runner umaban"],
  [{ ...runnerRow(), ketto_toroku_bango: "" }, "Invalid race runner horse identity"],
  [{ ...runnerRow(), wakuban: 1 }, "Missing or invalid race runner field"],
  [{ ...runnerRow(), extra: "1" }, "Missing or invalid race runner field"],
  [{ ...runnerRow(), sire_name: null, dam_sire_name: 1 }, "Missing or invalid race runner field"],
])("rejects an invalid runner row %j", async (row, message) => {
  await expect(readRaceRunners({ input: JRA, query: async () => [row] })).rejects.toThrow(message);
});

it("rejects duplicate, unordered and oversized runner lists", async () => {
  await expect(
    readRaceRunners({
      input: JRA,
      // Same umaban and horse but a different value: not a byte-identical
      // duplicate, so the identity conflict must still fail.
      query: async () => [runnerRow(), runnerRow({ tansho_odds: "0456" })],
    }),
  ).rejects.toThrow("Duplicate race runner identity");
  await expect(
    readRaceRunners({
      input: JRA,
      query: async () => [
        runnerRow({ umaban: "02", ketto_toroku_bango: "2021106754" }),
        runnerRow(),
      ],
    }),
  ).rejects.toThrow("Unordered race runners");
  await expect(
    readRaceRunners({
      input: JRA,
      query: async () =>
        Array.from({ length: 19 }, (_unused, index) =>
          runnerRow({
            umaban: String(index + 1).padStart(2, "0"),
            ketto_toroku_bango: `20211067${String(index).padStart(2, "0")}`,
          }),
        ),
    }),
  ).rejects.toThrow("Too many race runners");
});

it("collapses byte-identical duplicate rows before the identity guards", async () => {
  // 2026-09-22 54/03 held four identical copies of every NAR runner row; they
  // tripped the duplicate-identity guard and the race page failed.
  const result = await readRaceRunners({
    input: JRA,
    query: async (sql) =>
      sql.includes("oversea_runner_identity")
        ? [identityRow(), identityRow()]
        : [runnerRow(), runnerRow(), runnerRow()],
  });
  expect(result.runners).toHaveLength(1);
  expect(result.identities).toHaveLength(1);
});

it("rejects NAR rows that carry bloodline names and malformed identity rows", async () => {
  await expect(
    readRaceRunners({ input: NAR, query: async () => [runnerRow({ sire_name: "Nicobar" })] }),
  ).rejects.toThrow("NAR runners must not carry bloodline names");
  await expect(
    readRaceRunners({
      input: JRA,
      query: async (sql) =>
        sql.includes("oversea_runner_identity")
          ? [{ ...identityRow(), umaban: "1" }]
          : [runnerRow()],
    }),
  ).rejects.toThrow("Invalid overseas identity umaban");
  await expect(
    readRaceRunners({
      input: JRA,
      query: async (sql) =>
        sql.includes("oversea_runner_identity")
          ? [identityRow(), { ...identityRow(), source_horse_id: "2021100676" }]
          : [runnerRow()],
    }),
  ).rejects.toThrow("Duplicate overseas identity");
  await expect(
    readRaceRunners({
      input: JRA,
      query: async (sql) =>
        sql.includes("oversea_runner_identity") ? [{ ...identityRow(), extra: 1 }] : [runnerRow()],
    }),
  ).rejects.toThrow("Missing or invalid overseas identity field");
});
