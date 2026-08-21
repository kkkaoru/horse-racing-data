import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { expect, it } from "vitest";

const sql = readFileSync(
  resolve(process.cwd(), "sql/20260822000000_create_netkeiba_training_workouts.sql"),
  "utf8",
);

it("creates additive JVD-compatible netkeiba workout storage idempotently", () => {
  expect(sql).toContain("create table if not exists netkeiba_training_workouts");
  expect(sql).toContain("record_id varchar(2) not null default 'NK'");
  expect(sql).toContain("data_sakusei_nengappi varchar(8) not null");
  expect(sql).toContain("tracen_kubun varchar(1)");
  expect(sql).toContain("chokyo_nengappi varchar(8) not null");
  expect(sql).toContain("time_gokei_10f varchar(4)");
  expect(sql).toContain("lap_time_1f varchar(3)");
  expect(sql).toContain("create index if not exists");
  expect(sql).not.toMatch(/\b(?:delete|drop|truncate|update)\b/iu);
});

it("keys workouts by full race identity, horse, and a sha256 workout key", () => {
  expect(sql).toContain("source_race_id varchar(12) not null");
  expect(sql).toContain("umaban varchar(2) not null");
  expect(sql).toContain("ketto_toroku_bango varchar(10) not null");
  expect(sql).toContain("workout_key varchar(64) not null");
  expect(sql).toMatch(
    /primary key\s*\(\s*kaisai_nen,\s*kaisai_tsukihi,\s*keibajo_code,\s*race_bango,\s*ketto_toroku_bango,\s*workout_key\s*\)/isu,
  );
});

it("preserves evaluation provenance and lifecycle timestamps", () => {
  expect(sql).toContain("evaluation_grade text");
  expect(sql).toContain("evaluation_text text");
  expect(sql).toContain("comment_text text");
  expect(sql).toContain("source_url text");
  expect(sql).toContain("fetched_at timestamptz not null");
  expect(sql).toContain("created_at timestamptz not null default now()");
  expect(sql).toContain("updated_at timestamptz not null default now()");
});
