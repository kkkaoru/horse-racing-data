import "fake-indexeddb/auto";
import { beforeEach, describe, expect, it } from "vitest";

import { deleteRaceAiLog, getRaceAiLog, saveRaceAiLog } from "./race-ai-storage";

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
      raceKey: "nar:20260518:35:01",
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
      raceKey: "nar:20260518:35:02",
      thoughtLogs: [],
      updatedAt: "2026-05-18T00:01:00.000Z",
    });

    await deleteRaceAiLog("nar:20260518:35:01");

    expect(await getRaceAiLog("nar:20260518:35:01")).toMatchObject({
      messages: [],
      raceKey: "nar:20260518:35:01",
      thoughtLogs: [],
    });
    expect(await getRaceAiLog("nar:20260518:35:02")).toMatchObject({
      messages: [
        {
          content: "race two",
          id: "message-2",
          role: "assistant",
        },
      ],
      raceKey: "nar:20260518:35:02",
      thoughtLogs: [],
    });
  });
});
