import { expect, it, vi } from "vitest";

import { parseR2SqlSmokeArgs, runR2SqlSmoke } from "./r2-sql-smoke";
import type { Fetcher } from "./types";

it("parses smoke filters with a safe JRA default", () => {
  expect(parseR2SqlSmokeArgs(["20260715"])).toStrictEqual({
    date: "20260715",
    keibajoCode: undefined,
    raceBango: undefined,
    source: "jra",
  });
  expect(parseR2SqlSmokeArgs(["20260715", "ban-ei", "83", "9"])).toStrictEqual({
    date: "20260715",
    keibajoCode: "83",
    raceBango: "09",
    source: "ban-ei",
  });
});

it("rejects missing and unsafe smoke arguments", () => {
  expect(() => parseR2SqlSmokeArgs([])).toThrow(
    "usage: smoke:r2-sql YYYYMMDD [source] [keibajoCode] [raceBango]",
  );
  expect(() => parseR2SqlSmokeArgs(["20260715", "banei"])).toThrow(
    "source must be jra, nar, ban-ei, or all",
  );
  expect(() => parseR2SqlSmokeArgs(["20260715", "jra", "1;"])).toThrow(
    "keibajoCode must contain one or two digits",
  );
  expect(() => parseR2SqlSmokeArgs(["20260715", "jra", "05", "123"])).toThrow(
    "raceBango must contain one or two digits",
  );
});

it("executes the production race-key and feature SQL builders", async () => {
  const callState = { count: 0 };
  const fetchMock = vi.fn<Fetcher>(async (_input, init) => {
    callState.count += 1;
    const body = String(init?.body);
    if (callState.count === 1) {
      expect(body).toMatch("FROM pc_keiba.jvd_ra");
      expect(body).not.toMatch("UNION ALL");
      return Response.json({ result: { rows: [{ source: "jra" }] }, success: true });
    }
    if (callState.count === 2) {
      expect(body).toMatch("FROM pc_keiba.nvd_ra");
      expect(body).not.toMatch("UNION ALL");
      return Response.json({ result: { rows: [{ source: "nar" }] }, success: true });
    }
    expect(body).toMatch("FROM pc_keiba.jvd_se");
    expect(body).toMatch("INNER JOIN jra_ra ra");
    return Response.json({
      result: { rows: [{ source: "jra" }, { source: "jra" }] },
      success: true,
    });
  });
  await expect(
    runR2SqlSmoke(
      {
        R2_SQL_ACCOUNT_ID: "account",
        R2_SQL_BUCKET_NAME: "bucket",
        R2_SQL_NAMESPACE: "pc_keiba",
        R2_SQL_TOKEN: "token",
      },
      { date: "20260715", source: "jra" },
      fetchMock,
    ),
  ).resolves.toStrictEqual({ featureRows: 2, raceKeyRows: 2 });
  expect(fetchMock).toHaveBeenCalledTimes(3);
});
