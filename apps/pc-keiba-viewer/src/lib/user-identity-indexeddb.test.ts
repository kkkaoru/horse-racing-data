// Run with: bunx vitest run src/lib/user-identity-indexeddb.test.ts

import "fake-indexeddb/auto";
import { beforeEach, expect, test, vi } from "vitest";

import { getFavorites, saveFavorites } from "./favorites-indexeddb";
import { getOrCreateUserId, getUserId, setUserId } from "./user-identity-indexeddb";

const DB_NAME = "pc-keiba-viewer";

const resetIndexedDb = (): Promise<void> =>
  new Promise<void>((resolve, reject) => {
    const request = indexedDB.deleteDatabase(DB_NAME);
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

beforeEach(async () => {
  await resetIndexedDb();
  vi.restoreAllMocks();
});

test("get-returns-null-when-missing", async () => {
  expect(await getUserId()).toBe(null);
});

test("get-or-create-generates-when-missing", async () => {
  const randomSpy = vi.spyOn(crypto, "randomUUID");
  randomSpy.mockReturnValue("11111111-1111-4111-8111-111111111111");

  const created = await getOrCreateUserId();
  expect(created).toBe("11111111-1111-4111-8111-111111111111");

  const fetched = await getUserId();
  expect(fetched).toBe("11111111-1111-4111-8111-111111111111");
});

test("get-or-create-returns-existing-when-found", async () => {
  await setUserId("preset-user-id");

  const randomSpy = vi.spyOn(crypto, "randomUUID");
  randomSpy.mockReturnValue("22222222-2222-4222-8222-222222222222");

  const result = await getOrCreateUserId();
  expect(result).toBe("preset-user-id");
  expect(randomSpy).not.toHaveBeenCalled();
});

test("set-overwrites-existing", async () => {
  await setUserId("first-id");
  await setUserId("second-id");
  expect(await getUserId()).toBe("second-id");
});

const readRawRow = async (): Promise<unknown> => {
  const dbRequest = indexedDB.open(DB_NAME);
  const db = await new Promise<IDBDatabase>((resolve, reject) => {
    dbRequest.addEventListener("success", () => resolve(dbRequest.result));
    dbRequest.addEventListener("error", () => reject(dbRequest.error));
  });
  const row = await new Promise<unknown>((resolve, reject) => {
    const tx = db.transaction("userIdentity", "readonly");
    const req = tx.objectStore("userIdentity").get("singleton");
    req.addEventListener("success", () => resolve(req.result));
    req.addEventListener("error", () => reject(req.error));
  });
  db.close();
  return row;
};

const readCreatedAt = (row: unknown): string =>
  typeof row === "object" && row !== null && "createdAt" in row && typeof row.createdAt === "string"
    ? row.createdAt
    : "";

const readUserIdField = (row: unknown): string =>
  typeof row === "object" && row !== null && "userId" in row && typeof row.userId === "string"
    ? row.userId
    : "";

test("set-preserves-createdAt-across-overwrites", async () => {
  await setUserId("original");
  const beforeRow = await readRawRow();
  const beforeCreatedAt = readCreatedAt(beforeRow);

  await setUserId("updated");

  const afterRow = await readRawRow();
  expect(readUserIdField(afterRow)).toBe("updated");
  expect(readCreatedAt(afterRow) === beforeCreatedAt).toBe(true);
});

test("v2-migration-keeps-favorites-store-working", async () => {
  await saveFavorites([{ id: "h1", kind: "horse", label: "alpha" }]);
  await setUserId("user-after-favorites");
  expect(await getUserId()).toBe("user-after-favorites");
  expect(await getFavorites()).toStrictEqual([{ id: "h1", kind: "horse", label: "alpha" }]);
});

test("v2-migration-from-v1-creates-userIdentity-store", async () => {
  await new Promise<void>((resolve, reject) => {
    const v1Request = indexedDB.open(DB_NAME, 1);
    v1Request.addEventListener("upgradeneeded", () => {
      v1Request.result.createObjectStore("favorites", { keyPath: "key" });
    });
    v1Request.addEventListener("success", () => {
      v1Request.result.close();
      resolve();
    });
    v1Request.addEventListener("error", () => reject(v1Request.error));
  });
  await setUserId("post-migration-id");
  expect(await getUserId()).toBe("post-migration-id");
});

test("v2-migration-creates-favorites-store-when-fresh", async () => {
  await setUserId("fresh-install");
  await saveFavorites([{ id: "h-fresh", kind: "horse", label: "fresh" }]);
  expect(await getFavorites()).toStrictEqual([{ id: "h-fresh", kind: "horse", label: "fresh" }]);
});

test("returns-fallback-when-window-undefined", async () => {
  const target = globalThis as Record<string, unknown>;
  const originalWindow = target.window;
  target.window = undefined;
  try {
    expect(await getUserId()).toBe(null);
    expect(await getOrCreateUserId()).toBe("");
    await setUserId("ssr-ignored");
  } finally {
    target.window = originalWindow;
  }
});
