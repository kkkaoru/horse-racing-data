// Run with bun. `bun run --filter pc-keiba-viewer test`
import { beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const mocks = vi.hoisted(() => ({
  getPaddockLiveUrlMock: vi.fn<(...args: never[]) => unknown>(),
  getPaddockStateMock: vi.fn<(...args: never[]) => unknown>(),
  isPaddockRealtimeAvailableMock: vi.fn<(...args: never[]) => unknown>(),
  updatePaddockStateMock: vi.fn<(...args: never[]) => unknown>(),
}));

vi.mock("../../../../../../../../../lib/paddock-server", async () => {
  const actual = await vi.importActual<
    typeof import("../../../../../../../../../lib/paddock-server")
  >("../../../../../../../../../lib/paddock-server");
  return {
    getPaddockLiveUrl: mocks.getPaddockLiveUrlMock,
    getPaddockState: mocks.getPaddockStateMock,
    isPaddockAction: actual.isPaddockAction,
    isPaddockRaceParams: actual.isPaddockRaceParams,
    isPaddockRealtimeAvailable: mocks.isPaddockRealtimeAvailableMock,
    updatePaddockState: mocks.updatePaddockStateMock,
  };
});

const {
  getPaddockLiveUrlMock,
  getPaddockStateMock,
  isPaddockRealtimeAvailableMock,
  updatePaddockStateMock,
} = mocks;

import { GET, OPTIONS, POST, parsePaddockActionBody } from "./route";

beforeEach(() => {
  getPaddockLiveUrlMock.mockReset();
  getPaddockStateMock.mockReset();
  isPaddockRealtimeAvailableMock.mockReset();
  updatePaddockStateMock.mockReset();
});

it("parsePaddockActionBody returns null when body is null", () => {
  expect(parsePaddockActionBody(null)).toBeNull();
});

it("parsePaddockActionBody returns null when body is not a paddock action shape", () => {
  expect(parsePaddockActionBody({ foo: "bar" })).toBeNull();
});

it("parsePaddockActionBody passes through valid score action without userId", () => {
  expect(
    parsePaddockActionBody({
      category: "paddock",
      delta: 1,
      horseName: "Alpha",
      horseNumber: "1",
    }),
  ).toStrictEqual({
    action: {
      category: "paddock",
      delta: 1,
      horseName: "Alpha",
      horseNumber: "1",
    },
  });
});

it("parsePaddockActionBody attaches valid userId to score action", () => {
  expect(
    parsePaddockActionBody({
      category: "preference",
      delta: -1,
      horseName: "Beta",
      horseNumber: "2",
      userId: "user-abc_123",
    }),
  ).toStrictEqual({
    action: {
      category: "preference",
      delta: -1,
      horseName: "Beta",
      horseNumber: "2",
      userId: "user-abc_123",
    },
  });
});

it("parsePaddockActionBody returns invalid_user_id error when userId contains forbidden characters", () => {
  expect(
    parsePaddockActionBody({
      category: "paddock",
      delta: 1,
      horseName: "Gamma",
      horseNumber: "3",
      userId: "abc<>def",
    }),
  ).toStrictEqual({ error: "invalid_user_id" });
});

it("parsePaddockActionBody returns invalid_user_id error when userId exceeds 128 chars", () => {
  expect(
    parsePaddockActionBody({
      category: "paddock",
      delta: 1,
      horseName: "Delta",
      horseNumber: "4",
      userId: "a".repeat(129),
    }),
  ).toStrictEqual({ error: "invalid_user_id" });
});

it("parsePaddockActionBody returns invalid_user_id error when userId is an empty string", () => {
  expect(
    parsePaddockActionBody({
      category: "paddock",
      delta: 1,
      horseName: "Epsilon",
      horseNumber: "5",
      userId: "",
    }),
  ).toStrictEqual({ error: "invalid_user_id" });
});

it("parsePaddockActionBody returns invalid_user_id error when userId is not a string", () => {
  expect(
    parsePaddockActionBody({
      category: "paddock",
      delta: 1,
      horseName: "Zeta",
      horseNumber: "6",
      userId: 12345,
    }),
  ).toStrictEqual({ error: "invalid_user_id" });
});

it("parsePaddockActionBody drops userId from official-rank action", () => {
  expect(
    parsePaddockActionBody({
      horseName: "Eta",
      horseNumber: "7",
      rank: 3,
      type: "official-rank",
      userId: "should-be-ignored",
    }),
  ).toStrictEqual({
    action: {
      horseName: "Eta",
      horseNumber: "7",
      rank: 3,
      type: "official-rank",
      userId: "should-be-ignored",
    },
  });
});

it("OPTIONS returns 204", async () => {
  const response = await OPTIONS(new Request("https://example.com/api/x"));
  expect(response.status).toBe(204);
});

it("GET returns 404 when race params are invalid", async () => {
  const response = await GET(new Request("https://example.com/api/x"), {
    params: Promise.resolve({
      day: "BAD",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      year: "2026",
    }),
  });
  expect(response.status).toBe(404);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({ error: "not_found" });
});

it("GET returns 200 with paddock state and headers when params are valid and live url exists", async () => {
  getPaddockLiveUrlMock.mockResolvedValue("https://relay.example.com/live");
  getPaddockStateMock.mockResolvedValue({
    history: [],
    horses: {},
    raceKey: "20260529:05:01",
    updatedAt: "2026-05-29T07:30:00.000Z",
  });
  isPaddockRealtimeAvailableMock.mockResolvedValue(true);
  const response = await GET(new Request("https://example.com/api/x"), {
    params: Promise.resolve({
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  expect(response.headers.get("X-Paddock-Live-Url")).toBe("https://relay.example.com/live");
  expect(response.headers.get("X-Paddock-Realtime")).toBe("1");
  const body: unknown = await response.json();
  expect(body).toStrictEqual({
    history: [],
    horses: {},
    raceKey: "20260529:05:01",
    updatedAt: "2026-05-29T07:30:00.000Z",
  });
});

it("GET returns 200 without live url header when getPaddockLiveUrl returns null", async () => {
  getPaddockLiveUrlMock.mockResolvedValue(null);
  getPaddockStateMock.mockResolvedValue({
    history: [],
    horses: {},
    raceKey: "20260529:05:01",
    updatedAt: "2026-05-29T07:30:00.000Z",
  });
  isPaddockRealtimeAvailableMock.mockResolvedValue(false);
  const response = await GET(new Request("https://example.com/api/x"), {
    params: Promise.resolve({
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  expect(response.headers.get("X-Paddock-Live-Url")).toBeNull();
  expect(response.headers.get("X-Paddock-Realtime")).toBe("0");
});

it("POST returns 404 when race params are invalid", async () => {
  const response = await POST(
    new Request("https://example.com/api/x", {
      body: JSON.stringify({
        category: "paddock",
        delta: 1,
        horseName: "Alpha",
        horseNumber: "1",
      }),
      method: "POST",
    }),
    {
      params: Promise.resolve({
        day: "BAD",
        keibajoCode: "05",
        month: "05",
        raceNumber: "01",
        year: "2026",
      }),
    },
  );
  expect(response.status).toBe(404);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({ error: "not_found" });
});

it("POST returns 400 with invalid_action when body is unparseable JSON", async () => {
  const response = await POST(
    new Request("https://example.com/api/x", {
      body: "not-json",
      method: "POST",
    }),
    {
      params: Promise.resolve({
        day: "29",
        keibajoCode: "05",
        month: "05",
        raceNumber: "01",
        year: "2026",
      }),
    },
  );
  expect(response.status).toBe(400);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({ error: "invalid_action" });
});

it("POST returns 400 with invalid_action when body shape is wrong", async () => {
  const response = await POST(
    new Request("https://example.com/api/x", {
      body: JSON.stringify({ foo: "bar" }),
      method: "POST",
    }),
    {
      params: Promise.resolve({
        day: "29",
        keibajoCode: "05",
        month: "05",
        raceNumber: "01",
        year: "2026",
      }),
    },
  );
  expect(response.status).toBe(400);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({ error: "invalid_action" });
});

it("post-score-action-with-user-id-saves-to-history", async () => {
  updatePaddockStateMock.mockResolvedValue({
    history: [
      {
        at: "2026-05-29T07:30:00.000Z",
        category: "paddock",
        delta: 1,
        horseName: "Alpha",
        horseNumber: "1",
        id: "2026-05-29T07:30:00.000Z:1:paddock:0",
        scores: {
          attention: 0,
          kaeshi: 0,
          officialRank: null,
          paddock: 1,
          preference: 0,
          total: 1,
        },
        type: "score",
        userId: "viewer-42",
      },
    ],
    horses: {},
    raceKey: "20260529:05:01",
    updatedAt: "2026-05-29T07:30:00.000Z",
  });
  const response = await POST(
    new Request("https://example.com/api/x", {
      body: JSON.stringify({
        category: "paddock",
        delta: 1,
        horseName: "Alpha",
        horseNumber: "1",
        userId: "viewer-42",
      }),
      method: "POST",
    }),
    {
      params: Promise.resolve({
        day: "29",
        keibajoCode: "05",
        month: "05",
        raceNumber: "01",
        year: "2026",
      }),
    },
  );
  expect(response.status).toBe(200);
  expect(updatePaddockStateMock).toHaveBeenCalledWith(
    {
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      year: "2026",
    },
    {
      category: "paddock",
      delta: 1,
      horseName: "Alpha",
      horseNumber: "1",
      userId: "viewer-42",
    },
  );
  const body: unknown = await response.json();
  expect(body).toStrictEqual({
    history: [
      {
        at: "2026-05-29T07:30:00.000Z",
        category: "paddock",
        delta: 1,
        horseName: "Alpha",
        horseNumber: "1",
        id: "2026-05-29T07:30:00.000Z:1:paddock:0",
        scores: {
          attention: 0,
          kaeshi: 0,
          officialRank: null,
          paddock: 1,
          preference: 0,
          total: 1,
        },
        type: "score",
        userId: "viewer-42",
      },
    ],
    horses: {},
    raceKey: "20260529:05:01",
    updatedAt: "2026-05-29T07:30:00.000Z",
  });
});

it("post-score-action-without-user-id-still-succeeds", async () => {
  updatePaddockStateMock.mockResolvedValue({
    history: [
      {
        at: "2026-05-29T07:30:00.000Z",
        category: "paddock",
        delta: 1,
        horseName: "Alpha",
        horseNumber: "1",
        id: "2026-05-29T07:30:00.000Z:1:paddock:0",
        scores: {
          attention: 0,
          kaeshi: 0,
          officialRank: null,
          paddock: 1,
          preference: 0,
          total: 1,
        },
        type: "score",
      },
    ],
    horses: {},
    raceKey: "20260529:05:01",
    updatedAt: "2026-05-29T07:30:00.000Z",
  });
  const response = await POST(
    new Request("https://example.com/api/x", {
      body: JSON.stringify({
        category: "paddock",
        delta: 1,
        horseName: "Alpha",
        horseNumber: "1",
      }),
      method: "POST",
    }),
    {
      params: Promise.resolve({
        day: "29",
        keibajoCode: "05",
        month: "05",
        raceNumber: "01",
        year: "2026",
      }),
    },
  );
  expect(response.status).toBe(200);
  expect(updatePaddockStateMock).toHaveBeenCalledWith(
    {
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      year: "2026",
    },
    {
      category: "paddock",
      delta: 1,
      horseName: "Alpha",
      horseNumber: "1",
    },
  );
});

it("post-official-rank-action-ignores-user-id", async () => {
  updatePaddockStateMock.mockResolvedValue({
    history: [
      {
        at: "2026-05-29T07:30:00.000Z",
        horseName: "Alpha",
        horseNumber: "1",
        id: "2026-05-29T07:30:00.000Z:1:official-rank:0",
        officialRank: 1,
        scores: {
          attention: 0,
          kaeshi: 0,
          officialRank: 1,
          paddock: 0,
          preference: 0,
          total: 0,
        },
        type: "official-rank",
      },
    ],
    horses: {},
    raceKey: "20260529:05:01",
    updatedAt: "2026-05-29T07:30:00.000Z",
  });
  const response = await POST(
    new Request("https://example.com/api/x", {
      body: JSON.stringify({
        horseName: "Alpha",
        horseNumber: "1",
        rank: 1,
        type: "official-rank",
        userId: "viewer-42",
      }),
      method: "POST",
    }),
    {
      params: Promise.resolve({
        day: "29",
        keibajoCode: "05",
        month: "05",
        raceNumber: "01",
        year: "2026",
      }),
    },
  );
  expect(response.status).toBe(200);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({
    history: [
      {
        at: "2026-05-29T07:30:00.000Z",
        horseName: "Alpha",
        horseNumber: "1",
        id: "2026-05-29T07:30:00.000Z:1:official-rank:0",
        officialRank: 1,
        scores: {
          attention: 0,
          kaeshi: 0,
          officialRank: 1,
          paddock: 0,
          preference: 0,
          total: 0,
        },
        type: "official-rank",
      },
    ],
    horses: {},
    raceKey: "20260529:05:01",
    updatedAt: "2026-05-29T07:30:00.000Z",
  });
});

it("validation-rejects-too-long-user-id-string", async () => {
  const response = await POST(
    new Request("https://example.com/api/x", {
      body: JSON.stringify({
        category: "paddock",
        delta: 1,
        horseName: "Alpha",
        horseNumber: "1",
        userId: "a".repeat(130),
      }),
      method: "POST",
    }),
    {
      params: Promise.resolve({
        day: "29",
        keibajoCode: "05",
        month: "05",
        raceNumber: "01",
        year: "2026",
      }),
    },
  );
  expect(response.status).toBe(400);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({ error: "invalid_user_id" });
  expect(updatePaddockStateMock).not.toHaveBeenCalled();
});

it("validation-rejects-invalid-chars-user-id", async () => {
  const response = await POST(
    new Request("https://example.com/api/x", {
      body: JSON.stringify({
        category: "paddock",
        delta: 1,
        horseName: "Alpha",
        horseNumber: "1",
        userId: "abc<>def",
      }),
      method: "POST",
    }),
    {
      params: Promise.resolve({
        day: "29",
        keibajoCode: "05",
        month: "05",
        raceNumber: "01",
        year: "2026",
      }),
    },
  );
  expect(response.status).toBe(400);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({ error: "invalid_user_id" });
  expect(updatePaddockStateMock).not.toHaveBeenCalled();
});
