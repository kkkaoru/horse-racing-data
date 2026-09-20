// Runs with bun via the package Vitest scripts.
import { getTableColumns } from "drizzle-orm";
import { expect, test, vi } from "vitest";

vi.mock("server-only", () => ({}));

import { jvdCs } from "./schema";

test("maps the existing nullable course deletion marker without adding a database default", () => {
  const columns = getTableColumns(jvdCs);
  expect(columns.dataKubun.name).toBe("data_kubun");
  expect(columns.dataKubun.notNull).toBe(false);
  expect(columns.dataKubun.hasDefault).toBe(false);
  expect(columns.courseKaishuNengappi.name).toBe("course_kaishu_nengappi");
});
