import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { expect, it } from "vitest";

const migration = readFileSync(
  resolve(process.cwd(), "sql/20260815020000_create_oversea_runner_source_id.sql"),
  "utf8",
);

it("keys each overseas runner mapping by race entry and source", () => {
  expect(migration).toMatch(
    /primary key \(\s*race_source,\s*kaisai_nen,\s*kaisai_tsukihi,\s*keibajo_code,\s*race_bango,\s*umaban,\s*source\s*\)/u,
  );
  expect(migration).toMatch(/logically reference the matching identity row/u);
  expect(migration).not.toMatch(/^\s*foreign key/gimu);
});

it("stores source-native horse and people identities without a source allowlist", () => {
  expect(migration).toMatch(/source text not null check \(btrim\(source\) <> ''\)/u);
  expect(migration).toMatch(/source_horse_id text not null/u);
  expect(migration).toMatch(/source_jockey_id text/u);
  expect(migration).toMatch(/source_trainer_id text/u);
  expect(migration).toMatch(/source_owner_id text/u);
  expect(migration).not.toMatch(/source in \('jra-van', 'netkeiba'\)/u);
});
