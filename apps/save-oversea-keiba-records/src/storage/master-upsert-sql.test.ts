// This test runs with Bun and Vitest.
import { expect, test } from "vitest";
import type { MasterBackfillCandidate } from "../domain/master-backfill";
import { buildMasterRow } from "../domain/master-row-builder";
import { buildMasterInsertDoNothing, buildMasterInsertStatements } from "./master-upsert-sql";

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

test("buildMasterInsertDoNothing emits INSERT ON CONFLICT DO NOTHING for jvd_um PK", () => {
  const built = buildMasterRow(HORSE_CANDIDATE);
  const statement = buildMasterInsertDoNothing(built);

  expect(statement.text.startsWith("INSERT INTO jvd_um (")).toBe(true);
  expect(statement.text).toContain("ON CONFLICT (ketto_toroku_bango) DO NOTHING");
  expect(statement.text).not.toContain("DO UPDATE");
  expect(statement.values).toHaveLength(Object.keys(built.row).length);
  expect(statement.values[3]).toBe("2021190001");
});

test("buildMasterInsertDoNothing targets jvd_ks / jvd_ch PKs", () => {
  const jockey = buildMasterInsertDoNothing(
    buildMasterRow({
      ...HORSE_CANDIDATE,
      kind: "jockey",
      code: "05504",
      displayName: "M.ジョッキー",
    }),
  );
  const trainer = buildMasterInsertDoNothing(
    buildMasterRow({
      ...HORSE_CANDIDATE,
      kind: "trainer",
      code: "05701",
      displayName: "F.トレーナー",
    }),
  );

  expect(jockey.text).toContain("INSERT INTO jvd_ks");
  expect(jockey.text).toContain("ON CONFLICT (kishu_code) DO NOTHING");
  expect(trainer.text).toContain("INSERT INTO jvd_ch");
  expect(trainer.text).toContain("ON CONFLICT (chokyoshi_code) DO NOTHING");
});

test("buildMasterInsertStatements maps each built row", () => {
  const statements = buildMasterInsertStatements([
    buildMasterRow(HORSE_CANDIDATE),
    buildMasterRow({
      ...HORSE_CANDIDATE,
      kind: "jockey",
      code: "05504",
      displayName: "M.ジョッキー",
    }),
  ]);
  expect(statements).toHaveLength(2);
  expect(statements[0]?.text).toContain("jvd_um");
  expect(statements[1]?.text).toContain("jvd_ks");
});

test("buildMasterInsertDoNothing throws when a column value is missing", () => {
  expect(() =>
    buildMasterInsertDoNothing({
      table: "jvd_um",
      primaryKeyColumn: "ketto_toroku_bango",
      primaryKeyValue: "2021190001",
      row: {
        ketto_toroku_bango: "2021190001",
        missing_is_undefined: undefined as unknown as string,
      },
    }),
  ).toThrowError("Missing required master INSERT column: missing_is_undefined");
});
