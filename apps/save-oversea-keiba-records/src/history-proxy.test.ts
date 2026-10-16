// This file runs with Bun. All filesystem and network operations are mocked.
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { fetchProxyHistoryPage, historyProxyUrl, isHistoryProxySource } from "./history-proxy";
const fs = vi.hoisted(() => ({ access: vi.fn(), mkdir: vi.fn(), writeFile: vi.fn() }));
vi.mock("node:fs/promises", () => fs);
const network = vi.fn<typeof fetch>();
const input = {
  url: "https://db.netkeiba.com/history?page=2",
  encoding: "euc-jp",
  rawPath: "/private/pages/page.raw",
};
beforeEach(() => {
  vi.resetAllMocks();
  fs.access.mockRejectedValue({ code: "ENOENT" });
  fs.mkdir.mockResolvedValue(undefined);
  fs.writeFile.mockResolvedValue(undefined);
  vi.stubGlobal("fetch", network);
});
afterEach(() => {
  vi.unstubAllGlobals();
});
it("encodes the entire source URL under the fixed proxy origin", () => {
  expect(historyProxyUrl("https://db.netkeiba.com/history?page=2&word=%A4%A2")).toBe(
    "https://reverse-proxy.api.kkk4oru.com/playwright?url=https%3A%2F%2Fdb.netkeiba.com%2Fhistory%3Fpage%3D2%26word%3D%25A4%25A2",
  );
});
it.each([
  "https://netkeiba.com/",
  "https://db.netkeiba.com./",
  "https://race.netkeiba.com/",
  "http://db.netkeiba.com/",
])("recognizes source hosts without permitting direct fallback: %s", (source) => {
  expect(isHistoryProxySource(source)).toBe(true);
});
it.each(["https://notnetkeiba.com/", "https://netkeiba.com.example.test/"])(
  "does not misclassify unrelated hosts: %s",
  (source) => {
    expect(isHistoryProxySource(source)).toBe(false);
  },
);
it.each([
  "http://db.netkeiba.com/",
  "https://db.netkeiba.com:444/",
  "https://name@db.netkeiba.com/",
  "https://:password@db.netkeiba.com/",
  "https://db.netkeiba.com/#fragment",
  "https://example.test/",
])("rejects unsafe proxy input without a request: %s", (source) => {
  expect(() => historyProxyUrl(source)).toThrow("public HTTPS source");
  expect(network).not.toHaveBeenCalled();
});
it("saves exact UTF-8 bytes and provenance despite a legacy source encoding", async () => {
  network.mockResolvedValue(
    new Response("日本語", {
      headers: { "content-type": "text/html; charset=utf-8", "x-cache": "HIT" },
    }),
  );
  expect(await fetchProxyHistoryPage(input)).toBe("日本語");
  expect(network).toHaveBeenCalledTimes(1);
  expect(network).toHaveBeenCalledWith(
    "https://reverse-proxy.api.kkk4oru.com/playwright?url=https%3A%2F%2Fdb.netkeiba.com%2Fhistory%3Fpage%3D2",
    { redirect: "manual", signal: expect.any(AbortSignal) },
  );
  expect(fs.writeFile).toHaveBeenNthCalledWith(
    1,
    "/private/pages/page.raw.pending.json",
    expect.stringMatching(/"transportUrl":"https:\/\/reverse-proxy/u),
    { flag: "wx", mode: 0o600 },
  );
  expect(fs.writeFile).toHaveBeenNthCalledWith(
    2,
    "/private/pages/page.raw",
    new TextEncoder().encode("日本語"),
    { flag: "wx", mode: 0o600 },
  );
  expect(fs.writeFile).toHaveBeenNthCalledWith(
    3,
    "/private/pages/page.raw.meta.json",
    expect.stringMatching(/"encoding":"utf-8","bytes":9,"sha256":"[a-f0-9]{64}","cache":"HIT"/u),
    { flag: "wx", mode: 0o600 },
  );
});
it.each([400, 429, 302])(
  "preserves HTTP %s evidence without retries or redirects",
  async (status) => {
    network.mockResolvedValue(
      new Response("failure", { status, headers: { "retry-after": "60" } }),
    );
    await expect(fetchProxyHistoryPage(input)).rejects.toThrow("inspect preserved raw evidence");
    expect(network).toHaveBeenCalledTimes(1);
    expect(fs.writeFile).toHaveBeenCalledTimes(3);
  },
);
it.each(["text/plain", "text/html; charset=euc-jp"])(
  "rejects an untrusted response content type: %s",
  async (contentType) => {
    network.mockResolvedValue(new Response("body", { headers: { "content-type": contentType } }));
    await expect(fetchProxyHistoryPage(input)).rejects.toThrow("successful UTF-8 HTML");
  },
);
it("retains invalid UTF-8 before rejecting decoding", async () => {
  network.mockResolvedValue(
    new Response(new Uint8Array([255]), {
      headers: { "content-type": "text/html; charset=utf-8" },
    }),
  );
  await expect(fetchProxyHistoryPage(input)).rejects.toThrow();
  expect(fs.writeFile).toHaveBeenCalledTimes(3);
});
it("rejects a missing content type", async () => {
  network.mockResolvedValue(new Response(new Uint8Array([65])));
  await expect(fetchProxyHistoryPage(input)).rejects.toThrow("successful UTF-8 HTML");
});
it("does not fetch when raw evidence already exists", async () => {
  fs.access.mockResolvedValueOnce(undefined);
  await expect(fetchProxyHistoryPage(input)).rejects.toThrow("never overwritten or refetched");
  expect(network).not.toHaveBeenCalled();
});
it("does not fetch when only metadata already exists", async () => {
  fs.access.mockRejectedValueOnce({ code: "ENOENT" }).mockResolvedValueOnce(undefined);
  await expect(fetchProxyHistoryPage(input)).rejects.toThrow("never overwritten or refetched");
  expect(network).not.toHaveBeenCalled();
});
it.each([{ code: "EACCES" }, new Error("filesystem failure")])(
  "does not hide filesystem errors",
  async (error) => {
    fs.access.mockRejectedValue(error);
    await expect(fetchProxyHistoryPage(input)).rejects.toBeDefined();
    expect(network).not.toHaveBeenCalled();
  },
);
it("blocks an existing in-flight or failed reservation before network access", async () => {
  fs.writeFile.mockRejectedValue({ code: "EEXIST" });
  await expect(fetchProxyHistoryPage(input)).rejects.toStrictEqual({ code: "EEXIST" });
  expect(network).not.toHaveBeenCalled();
});
it("retains the reservation after a network timeout without retrying", async () => {
  network.mockRejectedValue(new Error("timeout"));
  await expect(fetchProxyHistoryPage(input)).rejects.toThrow("timeout");
  expect(network).toHaveBeenCalledTimes(1);
  expect(fs.writeFile).toHaveBeenCalledTimes(1);
});
