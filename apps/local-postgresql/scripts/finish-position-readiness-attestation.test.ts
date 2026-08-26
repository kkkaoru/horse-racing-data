import { expect, it, vi } from "vitest";
import {
  attestPreWeightPredictionReadiness,
  parseFinishPositionReadiness,
} from "./finish-position-readiness-attestation";

it("parses the minimal readiness contract", () => {
  expect(
    parseFinishPositionReadiness({
      races: [
        {
          keibajoCode: "43",
          raceBango: "09",
          raceKey: "nar:43:09",
          source: "nar",
          started: false,
          preWeight: { complete: true, kvComplete: true },
        },
      ],
      runYmd: "20260825",
    }),
  ).toStrictEqual({
    races: [
      {
        keibajoCode: "43",
        raceBango: "09",
        raceKey: "nar:43:09",
        source: "nar",
        started: false,
        preWeight: { complete: true, kvComplete: true },
      },
    ],
    runYmd: "20260825",
  });
});

it("rejects invalid readiness envelopes and races", () => {
  expect(() => parseFinishPositionReadiness(null)).toThrow("invalid response");
  expect(() => parseFinishPositionReadiness({ races: [], runYmd: 20260825 })).toThrow(
    "invalid response",
  );
  expect(() =>
    parseFinishPositionReadiness({ races: [{ raceKey: "nar:43:09" }], runYmd: "20260825" }),
  ).toThrow("invalid race");
});

it("polls until every upcoming race has complete pre-weight Neon and KV data", async () => {
  const fetcher = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(
      Response.json({
        races: [
          {
            keibajoCode: "43",
            raceBango: "09",
            raceKey: "nar:43:09",
            source: "nar",
            started: false,
            preWeight: { complete: true, kvComplete: false },
          },
          {
            keibajoCode: "43",
            raceBango: "08",
            raceKey: "nar:43:08",
            source: "nar",
            started: true,
            preWeight: { complete: false, kvComplete: false },
          },
        ],
        runYmd: "20260825",
      }),
    )
    .mockResolvedValueOnce(new Response(null, { status: 202 }))
    .mockResolvedValueOnce(
      Response.json({
        races: [
          {
            keibajoCode: "43",
            raceBango: "09",
            raceKey: "nar:43:09",
            source: "nar",
            started: false,
            preWeight: { complete: true, kvComplete: true },
          },
        ],
        runYmd: "20260825",
      }),
    );
  const retryDelay = vi.fn<(milliseconds: number) => Promise<void>>().mockResolvedValue(undefined);
  const log = vi.fn<(message: string) => void>();

  await attestPreWeightPredictionReadiness({
    baseUrl: "https://finish-position-cron.example",
    fetcher,
    log,
    nowMilliseconds: () => 1_000,
    pollIntervalMilliseconds: 10,
    pollTimeoutMilliseconds: 100,
    retryDelay,
    runYmd: "20260825",
    token: "secret-token",
  });

  expect(fetcher).toHaveBeenCalledTimes(3);
  expect(fetcher.mock.calls[0]).toStrictEqual([
    "https://finish-position-cron.example/api/internal/prediction-readiness?runYmd=20260825",
    { headers: { Authorization: "Bearer secret-token" } },
  ]);
  expect(retryDelay).toHaveBeenCalledWith(10);
  expect(fetcher.mock.calls[1]).toStrictEqual([
    new URL("https://finish-position-cron.example/api/admin/run-focused-full-race"),
    {
      body: JSON.stringify({
        category: "nar",
        force: true,
        keibajoCode: "43",
        raceBango: "09",
        runYmd: "20260825",
      }),
      headers: { Authorization: "Bearer secret-token", "Content-Type": "application/json" },
      method: "POST",
    },
  ]);
  expect(log.mock.calls).toStrictEqual([
    [
      "Finish-position readiness attempt 1: 0/1 upcoming races have complete pre-weight predictions and KV.",
    ],
    ["Finish-position readiness requested repair for nar:43:09."],
    [
      "Finish-position readiness attempt 2: 1/1 upcoming races have complete pre-weight predictions and KV.",
    ],
  ]);
});

it("succeeds when no races remain before post time", async () => {
  const retryDelay = vi.fn<(milliseconds: number) => Promise<void>>().mockResolvedValue(undefined);
  await attestPreWeightPredictionReadiness({
    baseUrl: "https://finish-position-cron.example",
    fetcher: vi.fn<typeof fetch>().mockResolvedValue(
      Response.json({
        races: [
          {
            keibajoCode: "43",
            raceBango: "08",
            raceKey: "nar:43:08",
            source: "nar",
            started: true,
            preWeight: { complete: false, kvComplete: false },
          },
        ],
        runYmd: "20260825",
      }),
    ),
    log: vi.fn(),
    nowMilliseconds: () => 1_000,
    pollIntervalMilliseconds: 10,
    pollTimeoutMilliseconds: 100,
    retryDelay,
    runYmd: "20260825",
    token: "secret-token",
  });
  expect(retryDelay).not.toHaveBeenCalled();
});

it("fails closed on date mismatch and authorization failure", async () => {
  await expect(
    attestPreWeightPredictionReadiness({
      baseUrl: "https://finish-position-cron.example",
      fetcher: vi
        .fn<typeof fetch>()
        .mockResolvedValue(Response.json({ races: [], runYmd: "20260826" })),
      log: vi.fn(),
      nowMilliseconds: () => 1_000,
      pollIntervalMilliseconds: 10,
      pollTimeoutMilliseconds: 100,
      retryDelay: vi.fn(),
      runYmd: "20260825",
      token: "secret-token",
    }),
  ).rejects.toThrow("date mismatch");
  await expect(
    attestPreWeightPredictionReadiness({
      baseUrl: "https://finish-position-cron.example",
      fetcher: vi.fn<typeof fetch>().mockResolvedValue(new Response("", { status: 401 })),
      log: vi.fn(),
      nowMilliseconds: () => 1_000,
      pollIntervalMilliseconds: 10,
      pollTimeoutMilliseconds: 100,
      retryDelay: vi.fn(),
      runYmd: "20260825",
      token: "secret-token",
    }),
  ).rejects.toThrow("authorization failed with HTTP 401");
});

it("retries transient readiness failures within the bounded timeout", async () => {
  const fetcher = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(new Response("bad gateway", { status: 502 }))
    .mockResolvedValueOnce(Response.json({ races: [], runYmd: "20260825" }));
  const retryDelay = vi.fn<(milliseconds: number) => Promise<void>>().mockResolvedValue(undefined);
  const log = vi.fn<(message: string) => void>();

  await attestPreWeightPredictionReadiness({
    baseUrl: "https://finish-position-cron.example",
    fetcher,
    log,
    nowMilliseconds: () => 1_000,
    pollIntervalMilliseconds: 10,
    pollTimeoutMilliseconds: 100,
    retryDelay,
    runYmd: "20260825",
    token: "secret-token",
  });

  expect(fetcher).toHaveBeenCalledTimes(2);
  expect(retryDelay).toHaveBeenCalledWith(10);
  expect(log).toHaveBeenCalledWith(
    "Finish-position readiness attempt 1 was unavailable; retrying.",
  );
});

it("bounds repeated readiness request failures", async () => {
  await expect(
    attestPreWeightPredictionReadiness({
      baseUrl: "https://finish-position-cron.example",
      fetcher: vi.fn<typeof fetch>().mockRejectedValue(new Error("network down")),
      log: vi.fn(),
      nowMilliseconds: vi.fn().mockReturnValueOnce(1_000).mockReturnValueOnce(1_100),
      pollIntervalMilliseconds: 10,
      pollTimeoutMilliseconds: 100,
      retryDelay: vi.fn(),
      runYmd: "20260825",
      token: "secret-token",
    }),
  ).rejects.toThrow("timed out after request failure: Error: network down");
});

it("times out with the exact incomplete upcoming race keys", async () => {
  await expect(
    attestPreWeightPredictionReadiness({
      baseUrl: "https://finish-position-cron.example",
      fetcher: vi.fn<typeof fetch>().mockResolvedValue(
        Response.json({
          races: [
            {
              keibajoCode: "43",
              raceBango: "09",
              raceKey: "nar:43:09",
              source: "nar",
              started: false,
              preWeight: { complete: false, kvComplete: false },
            },
          ],
          runYmd: "20260825",
        }),
      ),
      log: vi.fn(),
      nowMilliseconds: vi.fn().mockReturnValueOnce(1_000).mockReturnValueOnce(1_100),
      pollIntervalMilliseconds: 10,
      pollTimeoutMilliseconds: 100,
      retryDelay: vi.fn(),
      runYmd: "20260825",
      token: "secret-token",
    }),
  ).rejects.toThrow("timed out with incomplete upcoming races: nar:43:09");
});
