import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { expect, it } from "vitest";

const horses = readFileSync(
  resolve(process.cwd(), "sql/20260815030100_seed_jacques_le_marois_netkeiba_horse_history.sql"),
  "utf8",
);
const people = readFileSync(
  resolve(process.cwd(), "sql/20260815030200_seed_jacques_le_marois_netkeiba_person_history.sql"),
  "utf8",
);

it("seeds every cached netkeiba horse result with source provenance", () => {
  expect(horses.match(/^  \('netkeiba'/gmu)).toHaveLength(104);
  expect(horses).toMatch(
    /on conflict \(source, source_horse_id, source_race_id\) where source_race_id is not null/u,
  );
});

it("seeds linked and name-only person results through their partial unique indexes", () => {
  expect(people.match(/^  \('netkeiba'/gmu)).toHaveLength(454);
  expect(people.match(/^  \('netkeiba', 'jockey'/gmu)).toHaveLength(200);
  expect(people.match(/^  \('netkeiba', 'trainer'/gmu)).toHaveLength(176);
  expect(people.match(/^  \('netkeiba', 'owner'/gmu)).toHaveLength(78);
  expect(people).toMatch(/where source_horse_id is not null/u);
  expect(people).toMatch(/where source_horse_id is null/u);
  expect(people.match(/, null\),?$/gmu)).toHaveLength(4);
  expect(people).not.toMatch(/未取得/u);
});
