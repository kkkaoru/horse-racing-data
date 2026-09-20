// Runs with bun through Vitest; all provider I/O is mocked.
import { beforeEach, expect, it, vi } from "vitest";
import { mockDeep } from "vitest-mock-extended";
import { RaceDetailReadService } from "./race-detail-service";
import type { R2SqlCatalogConfig } from "./types";
const mocks = vi.hoisted(() => ({ query: vi.fn<typeof import("./r2-sql").executeR2Sql>() }));
vi.mock("./r2-sql", () => ({ executeR2Sql: mocks.query }));
const env: R2SqlCatalogConfig = {
  R2_SQL_ACCOUNT_ID: "account",
  R2_SQL_BUCKET_NAME: "catalog",
  R2_SQL_NAMESPACE: "pc_keiba",
  R2_SQL_TOKEN: "private-token",
};
beforeEach(() => {
  mocks.query.mockReset().mockResolvedValue([]);
});
it.each(["/", "/health", "/v1/internal/d1/audit", "/v1/internal/ingestion/status"])(
  "the binding cannot access other Catalog capabilities: %s",
  async (path) => {
    const service: RaceDetailReadService = new RaceDetailReadService(
      mockDeep<ExecutionContext>(),
      env,
    );
    const response: Response = await service.fetch(new Request(`https://catalog.internal${path}`));
    expect(response.status).toBe(404);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(mocks.query).not.toHaveBeenCalled();
  },
);
it("allows the dedicated read path without sharing an administrator credential", async () => {
  const service: RaceDetailReadService = new RaceDetailReadService(
    mockDeep<ExecutionContext>(),
    env,
  );
  const response: Response = await service.fetch(
    new Request(
      "https://catalog.internal/v1/race-detail?source=jra&date=20260816&keibajoCode=A8&raceBango=04",
    ),
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({ row: null });
  expect(mocks.query).toHaveBeenCalledTimes(1);
  expect(mocks.query.mock.calls[0]?.[1]).toMatch(/FROM pc_keiba\.jvd_ra/u);
  expect(mocks.query.mock.calls[0]?.[1]).toMatch(/AND keibajo_code = 'A8'/u);
});
it("serves the fixed calendar projection through the same read-only binding", async () => {
  const service: RaceDetailReadService = new RaceDetailReadService(
    mockDeep<ExecutionContext>(),
    env,
  );
  const response: Response = await service.fetch(
    new Request("https://catalog.internal/v1/race-calendar?year=2026"),
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({ days: [] });
  expect(mocks.query).toHaveBeenCalledTimes(1);
});
it("serves fixed year summaries through the existing read-only binding", async () => {
  mocks.query.mockResolvedValue([{ year: "2026", race_count: "500", day_count: "260" }]);
  const service: RaceDetailReadService = new RaceDetailReadService(
    mockDeep<ExecutionContext>(),
    env,
  );
  const response: Response = await service.fetch(
    new Request("https://catalog.internal/v1/race-years"),
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({
    years: [{ year: "2026", raceCount: 500, dayCount: 260 }],
  });
  expect(mocks.query).toHaveBeenCalledTimes(1);
});
it("serves empty day listings through the existing private binding", async () => {
  const service: RaceDetailReadService = new RaceDetailReadService(
    mockDeep<ExecutionContext>(),
    env,
  );
  const response: Response = await service.fetch(
    new Request("https://catalog.internal/v1/race-day-list?date=20240229"),
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({ races: [] });
  expect(mocks.query).toHaveBeenCalledTimes(1);
});
it("serves jockey-aware lists only through the named read binding", async () => {
  const service: RaceDetailReadService = new RaceDetailReadService(
    mockDeep<ExecutionContext>(),
    env,
  );
  const response: Response = await service.fetch(
    new Request("https://catalog.internal/v1/race-day-list-with-jockeys?date=20240229"),
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({ races: [] });
  expect(mocks.query).toHaveBeenCalledTimes(1);
  expect(mocks.query.mock.calls[0]?.[1]).toMatch(/^WITH races AS /u);
});
it("rejects jockey-list mutations from a binding holder", async () => {
  const service: RaceDetailReadService = new RaceDetailReadService(
    mockDeep<ExecutionContext>(),
    env,
  );
  const response: Response = await service.fetch(
    new Request("https://catalog.internal/v1/race-day-list-with-jockeys?date=20240229", {
      method: "POST",
    }),
  );
  expect(response.status).toBe(405);
  expect(mocks.query).not.toHaveBeenCalled();
});
it("rejects mutations even for a trusted binding holder", async () => {
  const service: RaceDetailReadService = new RaceDetailReadService(
    mockDeep<ExecutionContext>(),
    env,
  );
  const response: Response = await service.fetch(
    new Request("https://catalog.internal/v1/race-detail", { method: "POST" }),
  );
  expect(response.status).toBe(405);
  expect(mocks.query).not.toHaveBeenCalled();
});
it("rejects namespace or SQL injection through the service URL", async () => {
  const service: RaceDetailReadService = new RaceDetailReadService(
    mockDeep<ExecutionContext>(),
    env,
  );
  const response: Response = await service.fetch(
    new Request(
      "https://catalog.internal/v1/race-detail?source=jra&date=20260816&keibajoCode=A8&raceBango=04&namespace=other",
    ),
  );
  expect(response.status).toBe(400);
  expect(mocks.query).not.toHaveBeenCalled();
});
it("sanitizes provider failures without confusing failure with absence", async () => {
  const errorLog = vi.spyOn(console, "error").mockImplementation(() => undefined);
  mocks.query.mockRejectedValueOnce(new Error("sensitive provider detail"));
  const service: RaceDetailReadService = new RaceDetailReadService(
    mockDeep<ExecutionContext>(),
    env,
  );
  const response: Response = await service.fetch(
    new Request(
      "https://catalog.internal/v1/race-detail?source=nar&date=20260914&keibajoCode=83&raceBango=01",
    ),
  );
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({ error: "Catalog race detail unavailable" });
  expect(mocks.query).toHaveBeenCalledTimes(1);
  errorLog.mockRestore();
});
