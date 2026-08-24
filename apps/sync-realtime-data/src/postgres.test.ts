// run with: bun run test -- src/postgres.test.ts
import { afterEach, beforeEach, expect, it, vi } from "vitest";

interface MockClientOptions {
  connectionString: string;
}

const pgMock = vi.hoisted(() => ({
  connect: vi.fn(async () => undefined),
  end: vi.fn(async () => undefined),
  Client: vi.fn(function Client(_options: MockClientOptions) {
    return {
      connect: pgMock.connect,
      end: pgMock.end,
      query: pgMock.query,
    };
  }),
  query: vi.fn(),
}));

vi.mock("pg", () => ({ Client: pgMock.Client }));
vi.mock("pg-cloudflare", () => ({}));

beforeEach(async () => {
  const { clearDailyPgCache } = await import("./daily-pg-cache");
  clearDailyPgCache();
  pgMock.Client.mockClear();
  pgMock.connect.mockClear();
  pgMock.end.mockClear();
  pgMock.query.mockReset();
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("uses one request-scoped Hyperdrive client and closes it after a NAR query", async () => {
  pgMock.query.mockResolvedValueOnce({
    rows: [
      {
        hasso_jikoku: "1200",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0824",
        keibajo_code: "35",
        kyosomei_hondai: "test race",
        race_bango: "01",
      },
    ],
  });
  const { fetchNarRacesByDate } = await import("./postgres");

  const rows = await fetchNarRacesByDate(
    {
      DATABASE_TARGET: "cloudflare",
      HYPERDRIVE: { connectionString: "postgres://hyperdrive" },
    },
    "20260824",
  );

  expect(rows).toStrictEqual([
    {
      hasso_jikoku: "1200",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0824",
      keibajo_code: "35",
      kyosomei_hondai: "test race",
      race_bango: "01",
    },
  ]);
  expect(pgMock.Client).toHaveBeenCalledWith({
    connectionString: "postgres://hyperdrive",
  });
  expect(pgMock.connect).toHaveBeenCalledTimes(1);
  expect(pgMock.query).toHaveBeenCalledWith(expect.stringMatching(/from nvd_ra/u), [
    "2026",
    "0824",
  ]);
  expect(pgMock.end).toHaveBeenCalledTimes(1);
});

it("closes the scoped client and emits a stage when a JRA query fails", async () => {
  const queryError = new Error("remaining connection slots are reserved");
  pgMock.query.mockRejectedValueOnce(queryError);
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const { fetchJraRacesByDate } = await import("./postgres");

  await expect(
    fetchJraRacesByDate({ DATABASE_URL_NEON: "postgres://neon" }, "20260824"),
  ).rejects.toBe(queryError);

  expect(pgMock.Client).toHaveBeenCalledWith({
    connectionString: "postgres://neon",
  });
  expect(pgMock.connect).toHaveBeenCalledTimes(1);
  expect(pgMock.end).toHaveBeenCalledTimes(1);
  expect(errorSpy).toHaveBeenCalledWith(
    '{"error":"remaining connection slots are reserved","message":"Postgres daily race fetch failed","stage":"postgres.fetch-jra-races","targetDate":"20260824"}',
  );
});

it("closes the scoped client when Hyperdrive connect is rejected", async () => {
  const connectError = new Error("failed to acquire connection");
  pgMock.connect.mockRejectedValueOnce(connectError);
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  const { fetchNarRacesByDate } = await import("./postgres");

  await expect(
    fetchNarRacesByDate({ HYPERDRIVE: { connectionString: "postgres://hyperdrive" } }, "20260824"),
  ).rejects.toBe(connectError);

  expect(pgMock.query).not.toHaveBeenCalled();
  expect(pgMock.end).toHaveBeenCalledTimes(1);
});

it("returns a cached date without opening another client", async () => {
  pgMock.query.mockResolvedValueOnce({ rows: [] });
  const { fetchNarRacesByDate } = await import("./postgres");

  await fetchNarRacesByDate(
    { HYPERDRIVE: { connectionString: "postgres://hyperdrive" } },
    "20260824",
  );
  const rows = await fetchNarRacesByDate(
    { HYPERDRIVE: { connectionString: "postgres://hyperdrive" } },
    "20260824",
  );

  expect(rows).toStrictEqual([]);
  expect(pgMock.Client).toHaveBeenCalledTimes(1);
  expect(pgMock.connect).toHaveBeenCalledTimes(1);
  expect(pgMock.query).toHaveBeenCalledTimes(1);
  expect(pgMock.end).toHaveBeenCalledTimes(1);
});

it("rejects missing Postgres bindings without creating a client", async () => {
  const { fetchNarRacesByDate } = await import("./postgres");

  await expect(fetchNarRacesByDate({}, "20260824")).rejects.toThrow(
    "HYPERDRIVE or DATABASE_URL_NEON is required.",
  );

  expect(pgMock.Client).not.toHaveBeenCalled();
  expect(pgMock.connect).not.toHaveBeenCalled();
  expect(pgMock.end).not.toHaveBeenCalled();
});
