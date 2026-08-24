import { expect, it, vi } from "vitest";
import {
  addDaysToRealtimeDiscoveryDate,
  getJstDate,
  readEnvValue,
  resolveRealtimeAdminToken,
  resolveRealtimeDiscoveryDate,
  resolveRealtimeDiscoveryDates,
  triggerRealtimeDiscoveryAfterReplica,
} from "./realtime-discovery";

it("formats the target date in JST and accepts an explicit date", () => {
  expect(getJstDate(new Date("2026-08-21T15:30:00.000Z"))).toBe("20260822");
  expect(resolveRealtimeDiscoveryDate(undefined, new Date("2026-08-21T15:30:00.000Z"))).toBe(
    "20260822",
  );
  expect(resolveRealtimeDiscoveryDate("", new Date("2026-08-21T15:30:00.000Z"))).toBe("20260822");
  expect(resolveRealtimeDiscoveryDate("20260825", new Date("2026-08-21T15:30:00.000Z"))).toBe(
    "20260825",
  );
  expect(addDaysToRealtimeDiscoveryDate("20261231", 1)).toBe("20270101");
  expect(
    resolveRealtimeDiscoveryDates(undefined, new Date("2026-08-21T15:30:00.000Z")),
  ).toStrictEqual({ base: "20260822", next: "20260823" });
});

it("rejects an invalid configured date", () => {
  expect(() =>
    resolveRealtimeDiscoveryDate("2026-08-25", new Date("2026-08-21T15:30:00.000Z")),
  ).toThrow("SYNC_REALTIME_DATA_DATE must use YYYYMMDD format: 2026-08-25");
});

it("reads plain and quoted dotenv values", () => {
  expect(readEnvValue("A=1\nREALTIME_ADMIN_TOKEN=plain\n", "REALTIME_ADMIN_TOKEN")).toBe("plain");
  expect(readEnvValue('REALTIME_ADMIN_TOKEN="double quoted"\n', "REALTIME_ADMIN_TOKEN")).toBe(
    "double quoted",
  );
  expect(readEnvValue("REALTIME_ADMIN_TOKEN='single quoted'\n", "REALTIME_ADMIN_TOKEN")).toBe(
    "single quoted",
  );
  expect(readEnvValue("A=1\n", "REALTIME_ADMIN_TOKEN")).toBeUndefined();
});

it("prefers an explicit admin token and falls back to dev vars", () => {
  expect(resolveRealtimeAdminToken("explicit", "REALTIME_ADMIN_TOKEN=file-token\n")).toBe(
    "explicit",
  );
  expect(resolveRealtimeAdminToken(undefined, "REALTIME_ADMIN_TOKEN=file-token\n")).toBe(
    "file-token",
  );
  expect(resolveRealtimeAdminToken("", "REALTIME_ADMIN_TOKEN=file-token\n")).toBe("file-token");
});

it("fails closed when no admin token is configured", () => {
  expect(() => resolveRealtimeAdminToken(undefined, "A=1\n")).toThrow(
    "REALTIME_ADMIN_TOKEN is required; set it in the environment or apps/sync-realtime-data/.dev.vars",
  );
});

it("enqueues discovery, polls completion, then enqueues planning in strict date order", async () => {
  const fetcher = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(new Response('{"ok":true}', { status: 200 }))
    .mockResolvedValueOnce(
      new Response(
        '{"complete":false,"d1JraRaceCount":12,"date":"20260822","neonJraRaceCount":36}',
        { headers: { "retry-after": "2" }, status: 200 },
      ),
    )
    .mockResolvedValueOnce(
      new Response(
        '{"complete":true,"d1JraRaceCount":36,"date":"20260822","neonJraRaceCount":36}',
        { status: 200 },
      ),
    )
    .mockResolvedValueOnce(new Response('{"ok":true}', { status: 200 }))
    .mockResolvedValueOnce(new Response('{"ok":true}', { status: 200 }))
    .mockResolvedValueOnce(
      new Response('{"complete":true,"d1JraRaceCount":0,"date":"20260823","neonJraRaceCount":0}', {
        status: 200,
      }),
    )
    .mockResolvedValueOnce(new Response('{"ok":true}', { status: 200 }));
  const retryDelay = vi.fn<(milliseconds: number) => Promise<void>>().mockResolvedValue(undefined);
  const log = vi.fn<(message: string) => void>();

  await triggerRealtimeDiscoveryAfterReplica({
    baseUrl: "https://sync-realtime-data.example/",
    dates: { base: "20260822", next: "20260823" },
    fetcher,
    log,
    pollIntervalMilliseconds: 10_000,
    pollTimeoutMilliseconds: 60_000,
    retryDelay,
    token: "secret",
  });

  expect(fetcher.mock.calls.map((call) => [call[0], call[1]?.method, call[1]?.body])).toStrictEqual(
    [
      [
        "https://sync-realtime-data.example/api/jobs",
        "POST",
        '{"date":"20260822","type":"discover-urls"}',
      ],
      [
        "https://sync-realtime-data.example/api/internal/discovery-status?date=20260822",
        "GET",
        undefined,
      ],
      [
        "https://sync-realtime-data.example/api/internal/discovery-status?date=20260822",
        "GET",
        undefined,
      ],
      [
        "https://sync-realtime-data.example/api/jobs",
        "POST",
        '{"date":"20260822","type":"plan-premium-race-data-fetches"}',
      ],
      [
        "https://sync-realtime-data.example/api/jobs",
        "POST",
        '{"date":"20260823","type":"discover-urls"}',
      ],
      [
        "https://sync-realtime-data.example/api/internal/discovery-status?date=20260823",
        "GET",
        undefined,
      ],
      [
        "https://sync-realtime-data.example/api/jobs",
        "POST",
        '{"date":"20260823","type":"plan-premium-race-data-fetches"}',
      ],
    ],
  );
  expect(retryDelay.mock.calls).toStrictEqual([[2_000]]);
  expect(log.mock.calls).toStrictEqual([
    ["Discovery pending for 20260822: D1 JRA 12/36; polling again in 2000ms."],
    ["Discovery completed for 20260822: D1 JRA 36/36."],
    ["Discovery completed for 20260823: D1 JRA 0/0."],
  ]);
});

it("retries transient enqueue failures before polling", async () => {
  const fetcher = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(new Response("busy", { status: 503 }))
    .mockRejectedValueOnce(new Error("network down"))
    .mockResolvedValueOnce(new Response('{"ok":true}', { status: 200 }))
    .mockResolvedValueOnce(
      new Response('{"complete":true,"d1JraRaceCount":1,"date":"20260822","neonJraRaceCount":1}', {
        status: 200,
      }),
    )
    .mockResolvedValueOnce(new Response('{"ok":true}', { status: 200 }))
    .mockResolvedValueOnce(new Response('{"ok":true}', { status: 200 }))
    .mockResolvedValueOnce(
      new Response('{"complete":true,"d1JraRaceCount":0,"date":"20260823","neonJraRaceCount":0}', {
        status: 200,
      }),
    )
    .mockResolvedValueOnce(new Response('{"ok":true}', { status: 200 }));
  const retryDelay = vi.fn<(milliseconds: number) => Promise<void>>().mockResolvedValue(undefined);

  await triggerRealtimeDiscoveryAfterReplica({
    baseUrl: "https://sync-realtime-data.example",
    dates: { base: "20260822", next: "20260823" },
    fetcher,
    log: vi.fn(),
    pollIntervalMilliseconds: 10_000,
    pollTimeoutMilliseconds: 60_000,
    retryDelay,
    token: "secret",
  });
  expect(retryDelay.mock.calls).toStrictEqual([[250], [1_000]]);
});

it("logs sanitized discovery status failures with request context before retrying", async () => {
  const fetcher = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(new Response('{"ok":true}', { status: 200 }))
    .mockResolvedValueOnce(
      new Response(`{"code":"D1_ERROR","token":"top-secret","detail":"${"x".repeat(300)}"}`, {
        status: 500,
      }),
    )
    .mockResolvedValueOnce(
      new Response(
        '{"complete":true,"d1JraRaceCount":36,"date":"20260825","neonJraRaceCount":36}',
        { status: 200 },
      ),
    )
    .mockResolvedValueOnce(new Response('{"ok":true}', { status: 200 }))
    .mockResolvedValueOnce(new Response('{"ok":true}', { status: 200 }))
    .mockResolvedValueOnce(
      new Response('{"complete":true,"d1JraRaceCount":0,"date":"20260826","neonJraRaceCount":0}', {
        status: 200,
      }),
    )
    .mockResolvedValueOnce(new Response('{"ok":true}', { status: 200 }));
  const retryDelay = vi.fn<(milliseconds: number) => Promise<void>>().mockResolvedValue(undefined);
  const log = vi.fn<(message: string) => void>();

  await triggerRealtimeDiscoveryAfterReplica({
    baseUrl: "https://sync-realtime-data.example",
    dates: { base: "20260825", next: "20260826" },
    fetcher,
    log,
    pollIntervalMilliseconds: 10_000,
    pollTimeoutMilliseconds: 60_000,
    retryDelay,
    token: "secret",
  });

  expect(log.mock.calls[0]?.[0]).toMatch(
    /^Realtime request non-2xx url=https:\/\/sync-realtime-data\.example\/api\/internal\/discovery-status\?date=20260825 date=20260825 attempt=1\/3 status=500 body="\{\\"code\\":\\"D1_ERROR\\",\\"token\\":\\"\[REDACTED\]\\",\\"detail\\":\\"x+…"$/u,
  );
  expect(log.mock.calls[0]?.[0]).not.toMatch(/top-secret/u);
  expect(log.mock.calls).toHaveLength(4);
  expect(retryDelay.mock.calls).toStrictEqual([[250]]);
});

it("fails closed on authorization and exhausted network errors", async () => {
  await expect(
    triggerRealtimeDiscoveryAfterReplica({
      baseUrl: "https://sync-realtime-data.example",
      dates: { base: "20260822", next: "20260823" },
      fetcher: vi.fn<typeof fetch>().mockResolvedValue(
        new Response('{"error":"forbidden"}', {
          status: 403,
        }),
      ),
      log: vi.fn(),
      pollIntervalMilliseconds: 10_000,
      pollTimeoutMilliseconds: 60_000,
      retryDelay: vi.fn().mockResolvedValue(undefined),
      token: "wrong",
    }),
  ).rejects.toMatchObject({
    cause: new Error('HTTP 403: {"error":"forbidden"}'),
    message:
      'Realtime request enqueue discover-urls 20260822 failed with HTTP 403: {"error":"forbidden"}',
  });
  const fetcher = vi.fn<typeof fetch>().mockRejectedValue(new Error("offline"));
  await expect(
    triggerRealtimeDiscoveryAfterReplica({
      baseUrl: "https://sync-realtime-data.example",
      dates: { base: "20260822", next: "20260823" },
      fetcher,
      log: vi.fn(),
      pollIntervalMilliseconds: 10_000,
      pollTimeoutMilliseconds: 60_000,
      retryDelay: vi.fn().mockResolvedValue(undefined),
      token: "secret",
    }),
  ).rejects.toMatchObject({
    cause: new Error("offline"),
    message: "Realtime request enqueue discover-urls 20260822 failed after 3 attempts: offline",
  });
  expect(fetcher).toHaveBeenCalledTimes(3);
});

it("fails closed on invalid status schema and bounded polling timeout", async () => {
  const invalidStatusFetcher = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(new Response('{"ok":true}', { status: 200 }))
    .mockResolvedValueOnce(new Response('{"complete":true}', { status: 200 }));
  await expect(
    triggerRealtimeDiscoveryAfterReplica({
      baseUrl: "https://sync-realtime-data.example",
      dates: { base: "20260822", next: "20260823" },
      fetcher: invalidStatusFetcher,
      log: vi.fn(),
      pollIntervalMilliseconds: 10_000,
      pollTimeoutMilliseconds: 60_000,
      retryDelay: vi.fn().mockResolvedValue(undefined),
      token: "secret",
    }),
  ).rejects.toThrow("Invalid discovery status response for 20260822");

  const pending = '{"complete":false,"d1JraRaceCount":1,"date":"20260822","neonJraRaceCount":36}';
  const timeoutFetcher = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(new Response('{"ok":true}', { status: 200 }))
    .mockImplementation(
      async () => new Response(pending, { headers: { "retry-after": "invalid" }, status: 200 }),
    );
  const retryDelay = vi.fn<(milliseconds: number) => Promise<void>>().mockResolvedValue(undefined);
  await expect(
    triggerRealtimeDiscoveryAfterReplica({
      baseUrl: "https://sync-realtime-data.example",
      dates: { base: "20260822", next: "20260823" },
      fetcher: timeoutFetcher,
      log: vi.fn(),
      pollIntervalMilliseconds: 10_000,
      pollTimeoutMilliseconds: 20_000,
      retryDelay,
      token: "secret",
    }),
  ).rejects.toThrow("Timed out waiting for discovery 20260822: D1 JRA 1/36");
  expect(retryDelay.mock.calls).toStrictEqual([[10_000], [10_000]]);
});

it("validates dates, auth, and positive poll bounds before enqueue", async () => {
  const fetcher = vi.fn<typeof fetch>();
  await expect(
    triggerRealtimeDiscoveryAfterReplica({
      baseUrl: "https://sync-realtime-data.example",
      dates: { base: "bad", next: "20260823" },
      fetcher,
      log: vi.fn(),
      pollIntervalMilliseconds: 10_000,
      pollTimeoutMilliseconds: 60_000,
      retryDelay: vi.fn().mockResolvedValue(undefined),
      token: "secret",
    }),
  ).rejects.toThrow("SYNC_REALTIME_DATA_DATE must use YYYYMMDD format: bad");
  await expect(
    triggerRealtimeDiscoveryAfterReplica({
      baseUrl: "https://sync-realtime-data.example",
      dates: { base: "20260822", next: "20260823" },
      fetcher,
      log: vi.fn(),
      pollIntervalMilliseconds: 10_000,
      pollTimeoutMilliseconds: 60_000,
      retryDelay: vi.fn().mockResolvedValue(undefined),
      token: "",
    }),
  ).rejects.toThrow("REALTIME_ADMIN_TOKEN must not be empty");
  await expect(
    triggerRealtimeDiscoveryAfterReplica({
      baseUrl: "https://sync-realtime-data.example",
      dates: { base: "20260822", next: "20260823" },
      fetcher,
      log: vi.fn(),
      pollIntervalMilliseconds: 0,
      pollTimeoutMilliseconds: 60_000,
      retryDelay: vi.fn().mockResolvedValue(undefined),
      token: "secret",
    }),
  ).rejects.toThrow("Discovery poll interval and timeout must be positive");
  expect(fetcher).not.toHaveBeenCalled();
});
