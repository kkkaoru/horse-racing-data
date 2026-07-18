import { expect, it, vi } from "vitest";

import { parseRunningStyleSmokeArgs, runRunningStyleSmoke } from "./running-style-smoke";
import type { Fetcher } from "./types";

it("parses only fixed running-style race arguments", () => {
  expect(parseRunningStyleSmokeArgs(["20260715", "ban-ei", "83", "9"])).toStrictEqual({
    date: "20260715",
    keibajoCode: "83",
    raceBango: "09",
    source: "ban-ei",
  });
  expect(() => parseRunningStyleSmokeArgs([])).toThrow("usage: smoke:running-style");
  expect(() => parseRunningStyleSmokeArgs(["20260715", "all", "05", "1"])).toThrow(
    "source must be jra, nar, or ban-ei",
  );
  expect(() => parseRunningStyleSmokeArgs(["20260715", "jra", "005", "1"])).toThrow(
    "keibajoCode must contain one or two digits",
  );
  expect(() => parseRunningStyleSmokeArgs(["20260715", "jra", "05"])).toThrow(
    "raceBango must contain one or two digits",
  );
});

it("runs EXPLAIN before the exact fixed query", async () => {
  const fetchMock = vi.fn<Fetcher>(async (_input, init) => {
    const body = String(init?.body);
    const explain = body.includes("EXPLAIN FORMAT JSON");
    expect(body).toMatch("FROM pc_keiba.jvd_se");
    expect(body).toMatch("limit 18");
    return Response.json({
      result: { rows: explain ? [{ plan: "ok" }] : [{ source: "jra" }, { source: "jra" }] },
      success: true,
    });
  });
  await expect(
    runRunningStyleSmoke(
      {
        R2_SQL_ACCOUNT_ID: "account",
        R2_SQL_BUCKET_NAME: "bucket",
        R2_SQL_NAMESPACE: "pc_keiba",
        R2_SQL_TOKEN: "token",
      },
      { date: "20260715", keibajoCode: "05", raceBango: "01", source: "jra" },
      fetchMock,
      true,
    ),
  ).resolves.toStrictEqual({ explainRows: 1, queryRows: 2 });
  expect(fetchMock).toHaveBeenCalledTimes(2);
});
