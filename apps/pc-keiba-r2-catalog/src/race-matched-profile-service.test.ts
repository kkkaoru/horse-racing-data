// Runs with bun through Vitest; upstream I/O is mocked.
import { beforeEach, expect, it, vi } from "vitest";
import { R2SqlQueryError } from "./r2-sql";
import {
  classifyMatchedProfileFailure,
  handleRaceMatchedProfileRead,
} from "./race-matched-profile-service";
import type { R2SqlCatalogConfig } from "./types";
const mocks = vi.hoisted(() => ({
  query: vi.fn<typeof import("./r2-sql").executeR2Sql>(),
  alertSend: vi.fn<(message: unknown) => Promise<void>>(),
}));
vi.mock("./r2-sql", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./r2-sql")>()),
  executeR2Sql: mocks.query,
}));
const env: R2SqlCatalogConfig = {
  R2_SQL_ACCOUNT_ID: "account",
  R2_SQL_BUCKET_NAME: "catalog",
  R2_SQL_NAMESPACE: "pc_keiba",
  R2_SQL_TOKEN: "test-provider-token",
  INGESTION_ALERTS: { send: mocks.alertSend },
};
const FLAGS: string = [
  "includeVenue=1",
  "includeDistance=1",
  "includeAge=1",
  "includeClass=1",
  "includeConditionKey=1",
  "includeTrackCode=1",
  "includeGrade=1",
  "includeRaceTitle=1",
  "includeMonthWindow=0",
  "includeRunnerCount=0",
].join("&");
const validUrl: string = `https://catalog.internal/v1/race-matched-profile?source=jra&date=20260920&keibajoCode=06&raceBango=01&kyori=1600&kyosoShubetsuCode=11&kyosoJokenCode=999&kyosoJokenMeisho=%E3%82%AA%E3%83%BC%E3%83%97%E3%83%B3&trackCode=11&gradeCode=A&kyosomeiHondai=&years=10&limit=5000&${FLAGS}`;
const profileRow: Record<string, unknown> = {
  target_race_time: 834.5,
  target_last3f: 375,
  target_body_weight: 475.2,
  target_carried_weight: 560,
  target_margin: 23,
};
beforeEach(() => {
  mocks.query.mockReset().mockResolvedValue([profileRow]);
  mocks.alertSend.mockReset().mockResolvedValue(undefined);
});

it.each(["POST", "PUT", "PATCH", "DELETE", "HEAD"])("rejects %s before I/O", async (method) => {
  const response: Response = await handleRaceMatchedProfileRead(
    new Request(validUrl, { method }),
    env,
  );
  expect(response.status).toBe(405);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(mocks.query).not.toHaveBeenCalled();
});
it.each([
  "",
  "?source=jra",
  validUrl.replace("includeVenue=1", "includeVenue=yes"),
  validUrl.replace("&years=10", ""),
  validUrl.replace("&limit=5000", "&limit=0"),
  validUrl.replace("&years=10", "&years=0"),
  validUrl.replace("source=jra", "source=all"),
  `${validUrl}&sql=SELECT%201`,
  `${validUrl}&runnerCount=16&runnerCount=17`,
  validUrl.replace("&date=20260920", "&date=20260229"),
])("rejects invalid query %s", async (query) => {
  const url: string =
    query === ""
      ? "https://catalog.internal/v1/race-matched-profile"
      : query.startsWith("?")
        ? `https://catalog.internal/v1/race-matched-profile${query}`
        : query;
  const response: Response = await handleRaceMatchedProfileRead(new Request(url), env);
  expect(response.status).toBe(400);
  expect(await response.json()).toStrictEqual({ error: "Invalid matched profile request" });
  expect(mocks.query).not.toHaveBeenCalled();
});
it("returns the target profile with a bounded matched-race query", async () => {
  const response: Response = await handleRaceMatchedProfileRead(new Request(validUrl), env);
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({
    profile: {
      targetRaceTime: 834.5,
      targetLast3f: 375,
      targetBodyWeight: 475.2,
      targetCarriedWeight: 560,
      targetMargin: 23,
    },
  });
  const sql: string = mocks.query.mock.calls[0]?.[1] ?? "";
  expect(sql).toMatch("FROM pc_keiba.jvd_ra ra");
  expect(sql).toMatch("concat(ra.kaisai_nen, ra.kaisai_tsukihi) >= '20160920'");
  expect(sql).toMatch("se.kakutei_chakujun IN ('01', '02', '03')");
  expect(mocks.alertSend).not.toHaveBeenCalled();
});
it("sanitizes provider failures and alerts", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  mocks.query.mockRejectedValue(new Error("private provider detail"));
  const response: Response = await handleRaceMatchedProfileRead(new Request(validUrl), env);
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({ error: "Catalog matched profile unavailable" });
  expect(log).toHaveBeenCalledWith(
    '{"event":"race_matched_profile_read_failed","reason":"unknown"}',
  );
  expect(JSON.stringify(log.mock.calls)).not.toContain("private provider detail");
  expect(mocks.alertSend).toHaveBeenCalledWith(
    expect.objectContaining({
      checkName: "catalog-read-failure",
      fields: [{ name: "event", value: "race_matched_profile_read_failed" }],
    }),
  );
  log.mockRestore();
});

it("classifies R2 SQL matched profile failures by status and code", () => {
  expect(
    classifyMatchedProfileFailure(
      new R2SqlQueryError("R2 SQL HTTP 400: 40004 Expected: ), found: when", 40004, 400),
    ),
  ).toBe("r2_sql:400:40004:R2 SQL HTTP 400: 40004 Expected: ), found: when");
  expect(classifyMatchedProfileFailure(new R2SqlQueryError("private", 40018, 400))).toBe(
    "r2_sql:400:40018",
  );
  expect(classifyMatchedProfileFailure(new R2SqlQueryError("private", undefined))).toBe(
    "r2_sql:-:-",
  );
});

it("classifies aborted, invalid, oversized and unknown matched profile failures", () => {
  expect(
    classifyMatchedProfileFailure(new DOMException("The operation timed out", "TimeoutError")),
  ).toBe("abort:TimeoutError");
  expect(classifyMatchedProfileFailure(new Error("Invalid target profile result"))).toBe(
    "invalid_result",
  );
  expect(
    classifyMatchedProfileFailure(new Error("Audit provider response exceeds byte limit")),
  ).toBe("byte_limit");
  expect(classifyMatchedProfileFailure("raw")).toBe("unknown");
});
