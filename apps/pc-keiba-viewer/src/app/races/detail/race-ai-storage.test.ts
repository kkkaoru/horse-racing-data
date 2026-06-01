import "fake-indexeddb/auto";
import { beforeEach, describe, expect, it } from "vitest";

import {
  deleteCachedModelParts,
  deleteRaceAiLog,
  getRaceAiLog,
  listAllCachedModelPartInfos,
  listCachedModelPartInfos,
  loadCachedModelPart,
  modelPartCacheKey,
  saveCachedModelPart,
  saveRaceAiLog,
} from "./race-ai-storage";

const resetIndexedDb = async (): Promise<void> => {
  await new Promise<void>((resolve, reject) => {
    const request = indexedDB.deleteDatabase("pc-keiba-race-ai");
    request.addEventListener("success", () => {
      resolve();
    });
    request.addEventListener("error", () => {
      reject(request.error);
    });
    request.addEventListener("blocked", () => {
      reject(new Error("indexedDB deleteDatabase blocked"));
    });
  });
};

describe("race ai storage", () => {
  beforeEach(async () => {
    await resetIndexedDb();
  });

  it("deletes only the log for the requested race", async () => {
    await saveRaceAiLog({
      messages: [
        {
          content: "race one",
          createdAt: "2026-05-18T00:00:00.000Z",
          id: "message-1",
          role: "user",
        },
      ],
      raceKey: "nar:2026:0518:35:01",
      thoughtLogs: [
        {
          content: "thought one",
          createdAt: "2026-05-18T00:00:00.000Z",
          dataFingerprint: "fingerprint-1",
          id: "thought-1",
          modelVersion: "v20260518",
          trigger: "chat",
        },
      ],
      updatedAt: "2026-05-18T00:00:00.000Z",
    });
    await saveRaceAiLog({
      messages: [
        {
          content: "race two",
          createdAt: "2026-05-18T00:01:00.000Z",
          id: "message-2",
          role: "assistant",
        },
      ],
      raceKey: "nar:2026:0518:35:02",
      thoughtLogs: [],
      updatedAt: "2026-05-18T00:01:00.000Z",
    });

    await deleteRaceAiLog("nar:2026:0518:35:01");

    expect(await getRaceAiLog("nar:2026:0518:35:01")).toMatchObject({
      messages: [],
      raceKey: "nar:2026:0518:35:01",
      thoughtLogs: [],
    });
    expect(await getRaceAiLog("nar:2026:0518:35:02")).toMatchObject({
      messages: [
        {
          content: "race two",
          id: "message-2",
          role: "assistant",
        },
      ],
      raceKey: "nar:2026:0518:35:02",
      thoughtLogs: [],
    });
  });

  it("stores, lists, loads, and deletes model download parts", async () => {
    const key = modelPartCacheKey({
      end: 3,
      modelCacheKey: "model-a",
      partIndex: 0,
      start: 0,
    });
    await saveCachedModelPart({
      buffer: new Uint8Array([1, 2, 3, 4]),
      end: 3,
      key,
      modelCacheKey: "model-a",
      modelVersion: "v-test",
      partIndex: 0,
      sourceUrl: "/api/models/test",
      start: 0,
      totalBytes: 8,
    });
    await saveCachedModelPart({
      buffer: new Uint8Array([5, 6, 7, 8]),
      end: 7,
      key: modelPartCacheKey({
        end: 7,
        modelCacheKey: "model-b",
        partIndex: 1,
        start: 4,
      }),
      modelCacheKey: "model-b",
      modelVersion: "v-test",
      partIndex: 1,
      sourceUrl: "/api/models/test",
      start: 4,
      totalBytes: 8,
    });

    expect(await listCachedModelPartInfos("model-a")).toMatchObject([
      {
        end: 3,
        key,
        modelCacheKey: "model-a",
        partIndex: 0,
        size: 4,
        start: 0,
        totalBytes: 8,
      },
    ]);
    expect(await listAllCachedModelPartInfos()).toHaveLength(2);
    expect(
      Array.from(new Uint8Array((await loadCachedModelPart(key)) ?? new ArrayBuffer(0))),
    ).toEqual([1, 2, 3, 4]);

    await deleteCachedModelParts("model-a");

    expect(await listCachedModelPartInfos("model-a")).toEqual([]);
    expect(await listCachedModelPartInfos("model-b")).toHaveLength(1);
  });
});
