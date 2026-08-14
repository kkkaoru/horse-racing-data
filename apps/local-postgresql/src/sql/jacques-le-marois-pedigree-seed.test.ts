import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { expect, it } from "vitest";

const seed = readFileSync(
  resolve(process.cwd(), "sql/20260815040100_seed_jacques_le_marois_netkeiba_pedigree.sql"),
  "utf8",
);

it("seeds one netkeiba pedigree for every Jacques le Marois runner", () => {
  const sourceHorseIds = Array.from(
    seed.matchAll(/\('netkeiba', '([^']+)'/gu),
    (match) => match[1],
  );
  expect(sourceHorseIds).toStrictEqual([
    "000a02d639",
    "000a029c22",
    "2021105724",
    "2021105744",
    "000a02d00c",
    "000a027210",
    "000a02d629",
    "000a02ca97",
    "000a02d63e",
    "000a02ca51",
  ]);
});

it("keeps ancestor IDs, names, and source URLs in idempotent upserts", () => {
  expect(seed).toMatch(/'Night of Thunder', '000a0115e2', 'Dubawi'/u);
  expect(seed).toMatch(/'キズナ', '2002100816', 'ディープインパクト'/u);
  expect(seed).toMatch(/'Starspangledbanner', '000a010d4c', 'Choisir'/u);
  expect(seed).toMatch(/on conflict \(source, source_horse_id\)/u);
  expect(seed).toMatch(/dam_sire_name = excluded\.dam_sire_name/u);
  expect(seed).toMatch(/updated_at = now\(\)/u);
});
