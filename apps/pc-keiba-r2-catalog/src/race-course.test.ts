// Run with bun via the package test scripts.
import { expect, it } from "vitest";
import { buildRaceCourseQuery, normaliseRaceCourse } from "./race-course";
import type { R2SqlCatalogConfig } from "./types";

const config: R2SqlCatalogConfig = {
  R2_SQL_ACCOUNT_ID: "account",
  R2_SQL_BUCKET_NAME: "bucket",
  R2_SQL_NAMESPACE: "pc_keiba",
  R2_SQL_TOKEN: "test-token",
};
const url = (): URL =>
  new URL("https://catalog.test/v1/race-course?keibajoCode=05&kyori=1600&trackCode=11");

it("selects the latest non-deleted course with legacy null markers preserved", () => {
  expect(buildRaceCourseQuery(config, url())).toBe(`SELECT course_kaishu_nengappi, course_setsumei
FROM pc_keiba.jvd_cs
WHERE keibajo_code = '05' AND kyori = '1600' AND track_code = '11'
  AND (data_kubun IS NULL OR data_kubun <> '0')
ORDER BY course_kaishu_nengappi DESC
LIMIT 1`);
});

it.each(["keibajoCode", "kyori", "trackCode"])("rejects missing or injected %s", (name) => {
  const query = url();
  query.searchParams.delete(name);
  expect(() => buildRaceCourseQuery(config, query)).toThrow("must contain");
  query.searchParams.set(name, "' OR 1=1 --");
  expect(() => buildRaceCourseQuery(config, query)).toThrow("must contain");
});

it("accepts foreign venue codes without SQL interpolation risk", () => {
  const query = url();
  query.searchParams.set("keibajoCode", "A1");
  expect(buildRaceCourseQuery(config, query)).toMatch(/keibajo_code = 'A1'/u);
});

it("rejects an unsafe namespace", () => {
  expect(() =>
    buildRaceCourseQuery({ ...config, R2_SQL_NAMESPACE: "pc; DROP TABLE jvd_cs" }, url()),
  ).toThrow("must be an unquoted SQL identifier");
});

it("preserves text without trimming and handles absent courses", () => {
  expect(normaliseRaceCourse([])).toBeNull();
  expect(
    normaliseRaceCourse([
      { course_kaishu_nengappi: "20240106", course_setsumei: " コース説明\n " },
    ]),
  ).toStrictEqual({ courseKaishuNengappi: "20240106", courseSetsumei: " コース説明\n " });
  expect(
    normaliseRaceCourse([{ course_kaishu_nengappi: "20240106", course_setsumei: null }]),
  ).toStrictEqual({ courseKaishuNengappi: "20240106", courseSetsumei: null });
});

it.each(
  [
    [{}],
    [{ course_kaishu_nengappi: 20240106, course_setsumei: null }],
    [{ course_kaishu_nengappi: "20240106" }],
    [{ course_kaishu_nengappi: "20240106", course_setsumei: 123 }],
    [
      { course_kaishu_nengappi: "20240106", course_setsumei: null },
      { course_kaishu_nengappi: "20200101", course_setsumei: null },
    ],
  ].map((rows) => ({ rows })),
)("fails closed on malformed or multiple rows: %j", ({ rows }) => {
  expect(() => normaliseRaceCourse(rows)).toThrow("Malformed R2 course result");
});
