// This file runs with Bun.

import { expect, test, vi } from "vitest";

import {
  buildHorseLookupQuery,
  buildJockeyLookupQuery,
  buildOwnerLookupQuery,
  buildTrainerLookupQuery,
  createMasterLookupPort,
  normalizeEuropeanOwnerName,
  normalizeOwnerName,
  pickOwnerCode,
  pickOwnerRecord,
  shapeEntityRecords,
  shapeExistingCodes,
  shapeOwnerMatches,
  shapeTrainerRecords,
  type MasterLookupPort,
  type MasterLookupQueryRunner,
  type MasterLookupResult,
  type MasterLookupRow,
  type MasterLookupStatement,
} from "./master-lookup";

const EMPTY_RESULT: MasterLookupResult = { rows: [] };
const HORSE_LOOKUP_SQL: string = `SELECT
  trim(ketto_toroku_bango) AS code,
  trim(bamei) AS canonical_name
FROM jvd_um
WHERE ketto_toroku_bango = ANY($1::text[])`;
const JOCKEY_LOOKUP_SQL: string = `SELECT
  trim(kishu_code) AS code,
  trim(kishumei_ryakusho) AS canonical_name
FROM jvd_ks
WHERE kishu_code = ANY($1::text[])`;
const TRAINER_LOOKUP_SQL: string = `SELECT
  trim(chokyoshi_code) AS code,
  trim(chokyoshimei_ryakusho) AS canonical_name,
  trim(tozai_shozoku_code) AS tozai_shozoku_code
FROM jvd_ch
WHERE chokyoshi_code = ANY($1::text[])`;
const OWNER_ROWS: readonly MasterLookupRow[] = [
  {
    banushi_code: "166803",
    banushimei: "アガ・カーン・スタッズ",
    banushimei_hojinkaku: "アガ・カーン・スタッズ",
    banushimei_eur: "Aga Khan Studs",
  },
  {
    banushi_code: "147803",
    banushimei: "レゾリュート・ブラッドストック＆Ｐ．ウルマン男爵",
    banushimei_hojinkaku: "レゾリュート・ブラッドストック＆Ｐ．ウルマン男爵",
    banushimei_eur: "Resolute Bloodstock&Baron Philip Von Ullmann",
  },
  {
    banushi_code: "415800",
    banushimei: "社台レースホース",
    banushimei_hojinkaku: "株式会社　社台レースホース",
    banushimei_eur: "Shadai Race Horse Co. Ltd.",
  },
  {
    banushi_code: "758005",
    banushimei: "吉田　照哉",
    banushimei_hojinkaku: "吉田　照哉",
    banushimei_eur: "Teruya Yoshida",
  },
];

test("code query builders select the canonical SE name columns", () => {
  expect(buildHorseLookupQuery(["FR123", "GB456"])).toStrictEqual({
    text: HORSE_LOOKUP_SQL,
    values: [["FR123", "GB456"]],
  });
  expect(buildJockeyLookupQuery(["01234"])).toStrictEqual({
    text: JOCKEY_LOOKUP_SQL,
    values: [["01234"]],
  });
  expect(buildTrainerLookupQuery(["05701", "01038"])).toStrictEqual({
    text: TRAINER_LOOKUP_SQL,
    values: [["05701", "01038"]],
  });
});

test("owner query builder normalizes Japanese names and safe European roots", () => {
  expect(
    buildOwnerLookupQuery(["AGA KHAN STUDS SCEA", "RESOLUTE BLOODSTOCK,ET AL.", "吉田　照哉", "X"]),
  ).toStrictEqual({
    text: "SELECT\n  trim(banushi_code) AS banushi_code,\n  trim(banushimei) AS banushimei,\n  trim(banushimei_hojinkaku) AS banushimei_hojinkaku,\n  trim(banushimei_eur) AS banushimei_eur\nFROM jvd_bn\nWHERE\n  regexp_replace(replace(trim(banushimei), '　', ''), '[[:space:]]', '', 'g') = ANY($1::text[])\n  OR regexp_replace(replace(trim(banushimei_hojinkaku), '　', ''), '[[:space:]]', '', 'g') = ANY($1::text[])\n  OR EXISTS (\n    SELECT 1\n    FROM unnest($2::text[]) AS requested(root)\n    WHERE upper(regexp_replace(trim(banushimei_eur), '[^A-Za-z0-9]', '', 'g')) LIKE requested.root || '%'\n  )",
    values: [
      ["AGAKHANSTUDSSCEA", "RESOLUTEBLOODSTOCK,ETAL.", "吉田照哉", "X"],
      ["AGAKHANSTUDS", "RESOLUTEBLOODSTOCK"],
    ],
  });
});

test("owner normalization handles width, spaces, case, punctuation, and known suffixes", () => {
  expect(normalizeOwnerName("吉田　 照哉")).toBe("吉田照哉");
  expect(normalizeEuropeanOwnerName("ＡＧＡ KHAN STUDS SCEA")).toBe("AGAKHANSTUDS");
  expect(normalizeEuropeanOwnerName("Resolute Bloodstock, et al.")).toBe("RESOLUTEBLOODSTOCK");
  expect(normalizeEuropeanOwnerName("Shadai Race Horse Co. Ltd.")).toBe("SHADAIRACEHORSECOLTD");
});

test("entity result shaping preserves existence and normalizes canonical names", () => {
  expect(
    Array.from(
      shapeEntityRecords([
        { code: "A001", canonical_name: " Master A " },
        { code: "B002", canonical_name: "" },
        { code: "C003" },
        {},
      ]).entries(),
    ),
  ).toStrictEqual([
    ["A001", "Master A"],
    ["B002", null],
    ["C003", null],
  ]);
  expect(
    Array.from(shapeExistingCodes([{ code: "A001" }, {}, { code: "A001" }, { code: "B002" }])),
  ).toStrictEqual(["A001", "B002"]);
});

test("trainer result shaping includes canonical short name and affiliation", () => {
  expect(
    Array.from(
      shapeTrainerRecords([
        { code: "05701", canonical_name: "グラファ", tozai_shozoku_code: "4" },
        { code: "01038", canonical_name: "", tozai_shozoku_code: " 1 " },
        { code: "05518" },
        {},
      ]).entries(),
    ),
  ).toStrictEqual([
    ["05701", { exists: true, canonicalName: "グラファ", tozaiShozokuCode: "4" }],
    ["01038", { exists: true, canonicalName: null, tozaiShozokuCode: "1" }],
    ["05518", { exists: true, canonicalName: null, tozaiShozokuCode: null }],
  ]);
});

test("owner shaping returns canonical banushimei for all verified card-name forms", () => {
  expect(pickOwnerRecord("AGA KHAN STUDS SCEA", OWNER_ROWS)).toStrictEqual({
    code: "166803",
    canonicalName: "アガ・カーン・スタッズ",
  });
  expect(pickOwnerRecord("RESOLUTE BLOODSTOCK,ET AL.", OWNER_ROWS)).toStrictEqual({
    code: "147803",
    canonicalName: "レゾリュート・ブラッドストック＆Ｐ．ウルマン男爵",
  });
  expect(pickOwnerCode("社台レースホース", OWNER_ROWS)).toBe("415800");
  expect(pickOwnerCode("吉田 照哉", OWNER_ROWS)).toBe("758005");
});

test("owner shaping returns absence for no match, ambiguity, short roots, and missing codes", () => {
  expect(pickOwnerRecord("Unknown Owner", OWNER_ROWS)).toStrictEqual({
    code: null,
    canonicalName: null,
  });
  expect(
    pickOwnerRecord("AGA KHAN STUDS SCEA", [
      OWNER_ROWS[0] ?? {},
      { banushi_code: "999999", banushimei_eur: "Aga Khan Studs Holdings" },
    ]),
  ).toStrictEqual({ code: null, canonicalName: null });
  expect(pickOwnerCode("Tiny", [{ banushi_code: "000001", banushimei_eur: "Tiny Owner" }])).toBe(
    null,
  );
  expect(pickOwnerCode("Named Owner", [{ banushimei: "Named Owner" }])).toBe(null);
});

test("owner shaping keeps a resolved code when canonical banushimei is blank", () => {
  expect(
    pickOwnerRecord("Blank Master", [
      {
        banushi_code: "123456",
        banushimei: "",
        banushimei_hojinkaku: "",
        banushimei_eur: "Blank Master Holdings",
      },
    ]),
  ).toStrictEqual({ code: "123456", canonicalName: null });
  expect(
    Array.from(shapeOwnerMatches(["社台レースホース", "Unknown Owner"], OWNER_ROWS).entries()),
  ).toStrictEqual([
    ["社台レースホース", { code: "415800", canonicalName: "社台レースホース" }],
    ["Unknown Owner", { code: null, canonicalName: null }],
  ]);
});

test("individual lookup methods query once and reuse positive and negative cache entries", async () => {
  const runnerMock: ReturnType<typeof vi.fn<MasterLookupQueryRunner>> = vi
    .fn<MasterLookupQueryRunner>()
    .mockResolvedValueOnce({ rows: [{ code: "H001", canonical_name: "Horse Master" }] })
    .mockResolvedValueOnce(EMPTY_RESULT)
    .mockResolvedValueOnce({ rows: [{ code: "J001", canonical_name: "Jockey" }] })
    .mockResolvedValueOnce({
      rows: [{ code: "T001", canonical_name: "Trainer", tozai_shozoku_code: "4" }],
    })
    .mockResolvedValueOnce({ rows: OWNER_ROWS });
  const port: MasterLookupPort = createMasterLookupPort(runnerMock);

  expect(await port.findHorse("H001")).toStrictEqual({
    exists: true,
    canonicalName: "Horse Master",
  });
  expect(await port.findHorse("H001")).toStrictEqual({
    exists: true,
    canonicalName: "Horse Master",
  });
  expect(await port.findHorse("H404")).toStrictEqual({ exists: false, canonicalName: null });
  expect(await port.findHorse("H404")).toStrictEqual({ exists: false, canonicalName: null });
  expect(await port.findJockey("J001")).toStrictEqual({ exists: true, canonicalName: "Jockey" });
  expect(await port.findTrainer("T001")).toStrictEqual({
    exists: true,
    canonicalName: "Trainer",
    tozaiShozokuCode: "4",
  });
  expect(await port.findOwnerByName("吉田 照哉")).toStrictEqual({
    code: "758005",
    canonicalName: "吉田　照哉",
  });
  expect(await port.findOwnerByName("吉田 照哉")).toStrictEqual({
    code: "758005",
    canonicalName: "吉田　照哉",
  });
  expect(runnerMock).toHaveBeenCalledTimes(5);
});

test("trainer and owner lookups cache absent and blank-name records", async () => {
  const runnerMock: ReturnType<typeof vi.fn<MasterLookupQueryRunner>> = vi
    .fn<MasterLookupQueryRunner>()
    .mockResolvedValueOnce(EMPTY_RESULT)
    .mockResolvedValueOnce({
      rows: [{ code: "05519", canonical_name: "", tozai_shozoku_code: "" }],
    })
    .mockResolvedValueOnce(EMPTY_RESULT);
  const port: MasterLookupPort = createMasterLookupPort(runnerMock);

  expect(await port.findTrainer("T404")).toStrictEqual({
    exists: false,
    canonicalName: null,
    tozaiShozokuCode: null,
  });
  expect(await port.findTrainer("T404")).toStrictEqual({
    exists: false,
    canonicalName: null,
    tozaiShozokuCode: null,
  });
  expect(await port.findTrainer("05519")).toStrictEqual({
    exists: true,
    canonicalName: null,
    tozaiShozokuCode: null,
  });
  expect(await port.findOwnerByName("Unknown Owner")).toStrictEqual({
    code: null,
    canonicalName: null,
  });
  expect(await port.findOwnerByName("Unknown Owner")).toStrictEqual({
    code: null,
    canonicalName: null,
  });
  expect(runnerMock).toHaveBeenCalledTimes(3);
});

test("prefetch loads all entity categories in four parallel batch queries", async () => {
  const statements: MasterLookupStatement[] = [];
  const runner: MasterLookupQueryRunner = async (
    statement: MasterLookupStatement,
  ): Promise<MasterLookupResult> => {
    statements.push(statement);
    if (statement.text === HORSE_LOOKUP_SQL) {
      return { rows: [{ code: "H001", canonical_name: "Horse Master" }] };
    }
    if (statement.text === JOCKEY_LOOKUP_SQL) {
      return { rows: [{ code: "J001", canonical_name: "Jockey" }] };
    }
    if (statement.text === TRAINER_LOOKUP_SQL) {
      return {
        rows: [{ code: "T001", canonical_name: "Trainer", tozai_shozoku_code: "2" }],
      };
    }
    return { rows: OWNER_ROWS };
  };
  const port: MasterLookupPort = createMasterLookupPort(runner);

  await port.prefetch({
    horseRegistrationNumbers: ["H001", "H404"],
    jockeyCodes: ["J001", "J404"],
    trainerCodes: ["T001", "T404"],
    ownerNames: ["AGA KHAN STUDS SCEA", "Unknown Owner"],
  });

  expect(statements).toHaveLength(4);
  expect(await port.findHorse("H001")).toStrictEqual({
    exists: true,
    canonicalName: "Horse Master",
  });
  expect(await port.findHorse("H404")).toStrictEqual({ exists: false, canonicalName: null });
  expect(await port.findJockey("J001")).toStrictEqual({ exists: true, canonicalName: "Jockey" });
  expect(await port.findJockey("J404")).toStrictEqual({ exists: false, canonicalName: null });
  expect(await port.findTrainer("T001")).toStrictEqual({
    exists: true,
    canonicalName: "Trainer",
    tozaiShozokuCode: "2",
  });
  expect(await port.findTrainer("T404")).toStrictEqual({
    exists: false,
    canonicalName: null,
    tozaiShozokuCode: null,
  });
  expect(await port.findOwnerByName("AGA KHAN STUDS SCEA")).toStrictEqual({
    code: "166803",
    canonicalName: "アガ・カーン・スタッズ",
  });
  expect(await port.findOwnerByName("Unknown Owner")).toStrictEqual({
    code: null,
    canonicalName: null,
  });
  expect(statements).toHaveLength(4);
});

test("empty prefetch avoids all database queries", async () => {
  const runnerMock: ReturnType<typeof vi.fn<MasterLookupQueryRunner>> = vi
    .fn<MasterLookupQueryRunner>()
    .mockResolvedValue(EMPTY_RESULT);
  const port: MasterLookupPort = createMasterLookupPort(runnerMock);

  await port.prefetch({
    horseRegistrationNumbers: [],
    jockeyCodes: [],
    trainerCodes: [],
    ownerNames: [],
  });

  expect(runnerMock).toHaveBeenCalledTimes(0);
});
