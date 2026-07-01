// run with: bun run test
import { afterEach, expect, it, vi } from "vitest";
import {
  HORSE_WEIGHT_STORAGE_KEY,
  HorseWeightDO,
  proxyHorseWeightLatestFromStub,
  proxyHorseWeightStreamFromStub,
  writeHorseWeightSnapshotToStub,
  type HorseWeightSnapshot,
} from "./horse-weight-do";

const SNAPSHOT: HorseWeightSnapshot = {
  fetchedAt: "2026-05-30T14:14:00+09:00",
  horses: [
    {
      changeAmount: 10,
      changeSign: "+",
      horseName: "TestHorse",
      horseNumber: "1",
      weight: 538,
    },
  ],
};

const decodeStream = async (response: Response, maxChunks: number): Promise<string> => {
  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  const chunks: string[] = [];
  const collect = async (remaining: number): Promise<void> => {
    if (remaining <= 0) return;
    const { value, done } = await reader.read();
    if (done) return;
    chunks.push(decoder.decode(value));
    return collect(remaining - 1);
  };
  await collect(maxChunks);
  await reader.cancel();
  return chunks.join("");
};

afterEach(() => {
  vi.restoreAllMocks();
});

it("PUT /weights stores the snapshot and returns ok", async () => {
  const cache = HorseWeightDO.createForTest();
  const response = await cache.fetch(
    new Request("https://horse-weight-do/weights", {
      body: JSON.stringify(SNAPSHOT),
      method: "PUT",
    }),
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ ok: true });
});

it("PUT /weights rejects payloads missing required fields", async () => {
  const cache = HorseWeightDO.createForTest();
  const response = await cache.fetch(
    new Request("https://horse-weight-do/weights", {
      body: JSON.stringify({ horses: [] }),
      method: "PUT",
    }),
  );
  expect(response.status).toBe(400);
  expect(await response.json()).toStrictEqual({ error: "invalid body" });
});

it("PUT /weights rejects an empty horse list", async () => {
  const cache = HorseWeightDO.createForTest();
  const response = await cache.fetch(
    new Request("https://horse-weight-do/weights", {
      body: JSON.stringify({ fetchedAt: "2026-05-30T14:14:00+09:00", horses: [] }),
      method: "PUT",
    }),
  );
  expect(response.status).toBe(400);
  expect(await response.json()).toStrictEqual({ error: "invalid body" });
});

it("PUT /weights rejects malformed horse entries", async () => {
  const cache = HorseWeightDO.createForTest();
  const response = await cache.fetch(
    new Request("https://horse-weight-do/weights", {
      body: JSON.stringify({
        fetchedAt: "2026-05-30T14:14:00+09:00",
        horses: [
          {
            changeAmount: null,
            changeSign: null,
            horseName: null,
            horseNumber: 1,
            weight: 538,
          },
        ],
      }),
      method: "PUT",
    }),
  );
  expect(response.status).toBe(400);
  expect(await response.json()).toStrictEqual({ error: "invalid body" });
});

it("PUT /weights rejects non-object payloads", async () => {
  const cache = HorseWeightDO.createForTest();
  const response = await cache.fetch(
    new Request("https://horse-weight-do/weights", {
      body: JSON.stringify(null),
      method: "PUT",
    }),
  );
  expect(response.status).toBe(400);
});

it("GET /weights returns 204 when no snapshot has been stored", async () => {
  const cache = HorseWeightDO.createForTest();
  const response = await cache.fetch(new Request("https://horse-weight-do/weights"));
  expect(response.status).toBe(204);
});

it("GET /weights returns the stored snapshot after PUT", async () => {
  const cache = HorseWeightDO.createForTest();
  await cache.fetch(
    new Request("https://horse-weight-do/weights", {
      body: JSON.stringify(SNAPSHOT),
      method: "PUT",
    }),
  );
  const response = await cache.fetch(new Request("https://horse-weight-do/weights"));
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual(SNAPSHOT);
});

it("GET /stream sends the retry hint then broadcasts the next PUT", async () => {
  const cache = HorseWeightDO.createForTest();
  const stream = await cache.fetch(new Request("https://horse-weight-do/stream"));
  expect(stream.status).toBe(200);
  expect(stream.headers.get("Content-Type")).toBe("text/event-stream");
  const putPromise = cache.fetch(
    new Request("https://horse-weight-do/weights", {
      body: JSON.stringify(SNAPSHOT),
      method: "PUT",
    }),
  );
  await putPromise;
  const body = await decodeStream(stream, 2);
  expect(body).toBe(
    'retry: 5000\n\nevent: weights\ndata: {"fetchedAt":"2026-05-30T14:14:00+09:00","horses":[{"changeAmount":10,"changeSign":"+","horseName":"TestHorse","horseNumber":"1","weight":538}]}\n\n',
  );
});

it("GET /stream replays the current snapshot to a newly attached subscriber", async () => {
  const cache = HorseWeightDO.createForTest();
  await cache.fetch(
    new Request("https://horse-weight-do/weights", {
      body: JSON.stringify(SNAPSHOT),
      method: "PUT",
    }),
  );
  const stream = await cache.fetch(new Request("https://horse-weight-do/stream"));
  const body = await decodeStream(stream, 2);
  expect(body).toBe(
    'retry: 5000\n\nevent: weights\ndata: {"fetchedAt":"2026-05-30T14:14:00+09:00","horses":[{"changeAmount":10,"changeSign":"+","horseName":"TestHorse","horseNumber":"1","weight":538}]}\n\n',
  );
});

it("returns 404 for an unknown method", async () => {
  const cache = HorseWeightDO.createForTest();
  const response = await cache.fetch(
    new Request("https://horse-weight-do/weights", { method: "DELETE" }),
  );
  expect(response.status).toBe(404);
});

it("returns 404 for an unknown path", async () => {
  const cache = HorseWeightDO.createForTest();
  const response = await cache.fetch(new Request("https://horse-weight-do/unknown"));
  expect(response.status).toBe(404);
});

it("broadcast skips dead subscribers and continues delivering to live ones", async () => {
  const cache = HorseWeightDO.createForTest();
  const deadStream = await cache.fetch(new Request("https://horse-weight-do/stream"));
  await deadStream.body!.cancel();
  const liveStream = await cache.fetch(new Request("https://horse-weight-do/stream"));
  await cache.fetch(
    new Request("https://horse-weight-do/weights", {
      body: JSON.stringify(SNAPSHOT),
      method: "PUT",
    }),
  );
  const body = await decodeStream(liveStream, 2);
  expect(body).toBe(
    'retry: 5000\n\nevent: weights\ndata: {"fetchedAt":"2026-05-30T14:14:00+09:00","horses":[{"changeAmount":10,"changeSign":"+","horseName":"TestHorse","horseNumber":"1","weight":538}]}\n\n',
  );
});

it("writeHorseWeightSnapshotToStub issues a PUT against the stub", async () => {
  const stubFetch = vi.fn(
    async (_url: string, _init?: RequestInit): Promise<Response> =>
      new Response(null, { status: 200 }),
  );
  await writeHorseWeightSnapshotToStub({ snapshot: SNAPSHOT, stub: { fetch: stubFetch } });
  expect(stubFetch).toHaveBeenCalledTimes(1);
  expect(stubFetch.mock.calls[0]![0]).toBe("https://horse-weight-do/weights");
  expect(stubFetch.mock.calls[0]![1]!.method).toBe("PUT");
  expect(stubFetch.mock.calls[0]![1]!.body).toBe(JSON.stringify(SNAPSHOT));
});

it("proxyHorseWeightStreamFromStub issues a GET to /stream on the stub", async () => {
  const upstream = new Response("upstream-stream", { status: 200 });
  const stubFetch = vi.fn(async (_url: string, _init?: RequestInit): Promise<Response> => upstream);
  const response = await proxyHorseWeightStreamFromStub({ fetch: stubFetch });
  expect(stubFetch).toHaveBeenCalledTimes(1);
  expect(stubFetch.mock.calls[0]![0]).toBe("https://horse-weight-do/stream");
  expect(stubFetch.mock.calls[0]![1]!.method).toBe("GET");
  expect(response).toBe(upstream);
});

it("proxyHorseWeightLatestFromStub issues a GET to /weights on the stub", async () => {
  const upstream = new Response(null, { status: 204 });
  const stubFetch = vi.fn(async (_url: string, _init?: RequestInit): Promise<Response> => upstream);
  const response = await proxyHorseWeightLatestFromStub({ fetch: stubFetch });
  expect(stubFetch).toHaveBeenCalledTimes(1);
  expect(stubFetch.mock.calls[0]![0]).toBe("https://horse-weight-do/weights");
  expect(stubFetch.mock.calls[0]![1]!.method).toBe("GET");
  expect(response).toBe(upstream);
});

it("exposes snapshot as the storage key constant", () => {
  expect(HORSE_WEIGHT_STORAGE_KEY).toBe("snapshot");
});

it("hydrates snapshot from storage on construction", async () => {
  const storageGet = vi.fn(
    async (_key: string): Promise<HorseWeightSnapshot | undefined> => ({
      fetchedAt: "2026-05-30T14:14:00+09:00",
      horses: [
        {
          changeAmount: 10,
          changeSign: "+",
          horseName: "TestHorse",
          horseNumber: "1",
          weight: 538,
        },
      ],
    }),
  );
  const storagePut = vi.fn(async (_key: string, _value: HorseWeightSnapshot): Promise<void> => {});
  const blockConcurrencyWhile = vi.fn((callback: () => Promise<void>): Promise<void> => callback());
  const cache = await HorseWeightDO.createForTestWithStorage({
    state: {
      blockConcurrencyWhile,
      storage: { get: storageGet, put: storagePut },
    },
  });
  expect(blockConcurrencyWhile).toHaveBeenCalledTimes(1);
  expect(storageGet).toHaveBeenCalledTimes(1);
  expect(storageGet.mock.calls[0]![0]).toBe("snapshot");
  const response = await cache.fetch(new Request("https://horse-weight-do/weights"));
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({
    fetchedAt: "2026-05-30T14:14:00+09:00",
    horses: [
      {
        changeAmount: 10,
        changeSign: "+",
        horseName: "TestHorse",
        horseNumber: "1",
        weight: 538,
      },
    ],
  });
});

it("returns 204 when storage is empty on construction", async () => {
  const storageGet = vi.fn(
    async (_key: string): Promise<HorseWeightSnapshot | undefined> => undefined,
  );
  const storagePut = vi.fn(async (_key: string, _value: HorseWeightSnapshot): Promise<void> => {});
  const blockConcurrencyWhile = vi.fn((callback: () => Promise<void>): Promise<void> => callback());
  const cache = await HorseWeightDO.createForTestWithStorage({
    state: {
      blockConcurrencyWhile,
      storage: { get: storageGet, put: storagePut },
    },
  });
  expect(storageGet).toHaveBeenCalledTimes(1);
  const response = await cache.fetch(new Request("https://horse-weight-do/weights"));
  expect(response.status).toBe(204);
});

it("persists snapshot via storage.put on PUT", async () => {
  const storageGet = vi.fn(
    async (_key: string): Promise<HorseWeightSnapshot | undefined> => undefined,
  );
  const storagePut = vi.fn(async (_key: string, _value: HorseWeightSnapshot): Promise<void> => {});
  const blockConcurrencyWhile = vi.fn((callback: () => Promise<void>): Promise<void> => callback());
  const cache = await HorseWeightDO.createForTestWithStorage({
    state: {
      blockConcurrencyWhile,
      storage: { get: storageGet, put: storagePut },
    },
  });
  const response = await cache.fetch(
    new Request("https://horse-weight-do/weights", {
      body: JSON.stringify(SNAPSHOT),
      method: "PUT",
    }),
  );
  expect(response.status).toBe(200);
  expect(storagePut).toHaveBeenCalledTimes(1);
  expect(storagePut.mock.calls[0]![0]).toBe("snapshot");
  expect(storagePut.mock.calls[0]![1]).toStrictEqual({
    fetchedAt: "2026-05-30T14:14:00+09:00",
    horses: [
      {
        changeAmount: 10,
        changeSign: "+",
        horseName: "TestHorse",
        horseNumber: "1",
        weight: 538,
      },
    ],
  });
});

it("PUT with invalid body does not write to storage", async () => {
  const storageGet = vi.fn(
    async (_key: string): Promise<HorseWeightSnapshot | undefined> => undefined,
  );
  const storagePut = vi.fn(async (_key: string, _value: HorseWeightSnapshot): Promise<void> => {});
  const blockConcurrencyWhile = vi.fn((callback: () => Promise<void>): Promise<void> => callback());
  const cache = await HorseWeightDO.createForTestWithStorage({
    state: {
      blockConcurrencyWhile,
      storage: { get: storageGet, put: storagePut },
    },
  });
  const response = await cache.fetch(
    new Request("https://horse-weight-do/weights", {
      body: JSON.stringify({ horses: [] }),
      method: "PUT",
    }),
  );
  expect(response.status).toBe(400);
  expect(storagePut).not.toHaveBeenCalled();
});
