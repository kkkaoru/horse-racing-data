// This test runs with Bun and Vitest.
import { expect, test } from "vitest";
import type { MasterBackfillCandidate } from "./master-backfill";
import { buildMasterRow, buildMasterRows } from "./master-row-builder";

const HORSE_CANDIDATE: MasterBackfillCandidate = {
  kind: "horse",
  code: "2021190001",
  displayName: "テストホース",
  horseName: "テストホース",
  sex: "せん",
  coatColour: "鹿",
  sire: "Test Sire",
  dam: "Test Dam",
  damsire: "Test Damsire",
  jockeyAbbrev: "M.ジョッキー",
  trainerAbbrev: "F.トレーナー",
  owner: "TEST OWNER",
  trainerCode: "05701",
  raceDate: "20260725",
};

const JOCKEY_CANDIDATE: MasterBackfillCandidate = {
  ...HORSE_CANDIDATE,
  kind: "jockey",
  code: "05504",
  displayName: "M.ジョッキー",
};

const TRAINER_CANDIDATE: MasterBackfillCandidate = {
  ...HORSE_CANDIDATE,
  kind: "trainer",
  code: "05701",
  displayName: "F.トレーナー",
};

test("horse master row has sire/dam/damsire in *b slots and zeros in *a", () => {
  const built = buildMasterRow(HORSE_CANDIDATE);

  expect(built.table).toBe("jvd_um");
  expect(built.primaryKeyColumn).toBe("ketto_toroku_bango");
  expect(built.primaryKeyValue).toBe("2021190001");
  expect(built.row.record_id).toBe("UM");
  expect(built.row.massho_kubun).toBe("1");
  expect(built.row.tozai_shozoku_code).toBe("4");
  expect(built.row.seibetsu_code).toBe("3");
  expect(built.row.moshoku_code).toBe("03");
  expect(built.row.chokyoshi_code).toBe("05701");
  expect(built.row.ketto_joho_01a).toBe("0000000000");
  expect(built.row.ketto_joho_02a).toBe("0000000000");
  expect(built.row.ketto_joho_05a).toBe("0000000000");
  expect(built.row.ketto_joho_01b?.trim()).toBe("Test Sire");
  expect(built.row.ketto_joho_02b?.trim()).toBe("Test Dam");
  expect(built.row.ketto_joho_05b?.trim()).toBe("Test Damsire");
  expect(built.row.ketto_joho_03a).toBe("0000000000");
  expect(built.row.sogo).toBe("0".repeat(18));
  expect(built.row.kyakushitsu_keiko).toBe("0".repeat(12));
  expect(built.row.toroku_race_su).toBe("000");
  expect(Object.keys(built.row)).toHaveLength(89);
});

test("horse master falls back trainer code to placeholder when invalid", () => {
  const built = buildMasterRow({
    ...HORSE_CANDIDATE,
    trainerCode: "00a01",
  });
  expect(built.row.chokyoshi_code).toBe("00000");
});

test("jockey master row uses KS record and overseas affiliation", () => {
  const built = buildMasterRow(JOCKEY_CANDIDATE);
  expect(built.table).toBe("jvd_ks");
  expect(built.primaryKeyColumn).toBe("kishu_code");
  expect(built.row.record_id).toBe("KS");
  expect(built.row.kishu_code).toBe("05504");
  expect(built.row.massho_kubun).toBe("1");
  expect(built.row.tozai_shozoku_code).toBe("4");
  expect(built.row.seiseki_joho_1).toBe("0".repeat(1052));
  expect((built.row.kishumei_ryakusho ?? "").trim().length).toBeGreaterThan(0);
});

test("trainer master row uses CH record and overseas affiliation", () => {
  const built = buildMasterRow(TRAINER_CANDIDATE);
  expect(built.table).toBe("jvd_ch");
  expect(built.primaryKeyColumn).toBe("chokyoshi_code");
  expect(built.row.record_id).toBe("CH");
  expect(built.row.chokyoshi_code).toBe("05701");
  expect(built.row.massho_kubun).toBe("1");
  expect(built.row.tozai_shozoku_code).toBe("4");
  expect(built.row.seiseki_joho_1).toBe("0".repeat(1052));
});

test("buildMasterRows maps every candidate", () => {
  const rows = buildMasterRows([HORSE_CANDIDATE, JOCKEY_CANDIDATE, TRAINER_CANDIDATE]);
  expect(rows.map((r) => r.table)).toStrictEqual(["jvd_um", "jvd_ks", "jvd_ch"]);
});
