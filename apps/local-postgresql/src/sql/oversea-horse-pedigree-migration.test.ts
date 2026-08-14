import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { expect, it } from "vitest";

const migration = readFileSync(
  resolve(process.cwd(), "sql/20260815040000_create_oversea_horse_pedigree.sql"),
  "utf8",
);

it("creates a source-native overseas pedigree table without JV foreign keys", () => {
  expect(migration).toMatch(/create table if not exists oversea_horse_pedigree/u);
  expect(migration).toMatch(/primary key \(source, source_horse_id\)/u);
  expect(migration).toMatch(/sire_source_id text/u);
  expect(migration).toMatch(/sire_sire_source_id text/u);
  expect(migration).toMatch(/dam_sire_source_id text/u);
  expect(migration).toMatch(/oversea_horse_pedigree_ancestor_names_idx/u);
  expect(migration).not.toMatch(/references\s+jvd_/u);
});

it("rejects empty ancestor names and non-HTTP provenance URLs", () => {
  expect(migration).toMatch(/oversea_horse_pedigree_sire_nonempty/u);
  expect(migration).toMatch(/oversea_horse_pedigree_sire_sire_nonempty/u);
  expect(migration).toMatch(/oversea_horse_pedigree_dam_nonempty/u);
  expect(migration).toMatch(/oversea_horse_pedigree_dam_sire_nonempty/u);
  expect(migration).toMatch(/source_url ~ '\^https\?:\/\/'/u);
});
