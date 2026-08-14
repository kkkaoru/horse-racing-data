import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { expect, it } from "vitest";

const migration = readFileSync(
  resolve(process.cwd(), "sql/20260815050000_create_oversea_person_win_rate_stats.sql"),
  "utf8",
);

it("creates an additive keyed overseas person win-rate snapshot", () => {
  expect(migration).toMatch(/create table if not exists oversea_person_win_rate_stats/u);
  expect(migration).toMatch(/primary key \([\s\S]*category,[\s\S]*name/u);
  expect(migration).toMatch(/calculated_at timestamptz not null/u);
  expect(migration).toMatch(/calculated_through date not null/u);
  expect(migration).toMatch(/minimum_starts smallint not null default 20/u);
  expect(migration).toMatch(/scope = 'all_venues_all_conditions'/u);
  expect(migration).not.toMatch(/references\s+/u);
  expect(migration).not.toMatch(/\b(?:delete|drop|truncate)\b/iu);
});
