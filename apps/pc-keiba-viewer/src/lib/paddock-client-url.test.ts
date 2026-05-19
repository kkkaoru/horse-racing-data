import { afterEach, describe, expect, it, vi } from "vitest";

import { getPaddockLiveUrl, getPaddockRequestUrl, isLocalPaddockHost } from "./paddock-client-url";

const localhost = {
  host: "localhost",
  hostname: "localhost",
  protocol: "https:",
};

const production = {
  host: "pc-keiba-viewer.kkk4oru.com",
  hostname: "pc-keiba-viewer.kkk4oru.com",
  protocol: "https:",
};

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("paddock client urls", () => {
  it("detects local paddock hosts", () => {
    expect(isLocalPaddockHost("localhost")).toBe(true);
    expect(isLocalPaddockHost("127.0.0.1")).toBe(true);
    expect(isLocalPaddockHost("0.0.0.0")).toBe(true);
    expect(isLocalPaddockHost("pc-keiba-viewer.kkk4oru.com")).toBe(false);
  });

  it("uses the remote paddock API from localhost by default", () => {
    expect(getPaddockRequestUrl("/api/races/2026/05/19/44/12/paddock", localhost)).toBe(
      "https://pc-keiba-viewer.kkk4oru.com/api/races/2026/05/19/44/12/paddock",
    );
  });

  it("keeps relative paddock URLs on non-local hosts", () => {
    expect(getPaddockRequestUrl("/api/races/2026/05/19/44/12/paddock", production)).toBe(
      "/api/races/2026/05/19/44/12/paddock",
    );
  });

  it("allows remote paddock API sync to be disabled explicitly", () => {
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PADDOCK_REMOTE_BINDINGS", "0");

    expect(getPaddockRequestUrl("/api/races/2026/05/19/44/12/paddock", localhost)).toBe(
      "/api/races/2026/05/19/44/12/paddock",
    );
  });

  it("builds websocket URLs from the selected request origin", () => {
    expect(getPaddockLiveUrl("/api/races/2026/05/19/44/12/paddock/live", localhost)).toBe(
      "wss://pc-keiba-viewer.kkk4oru.com/api/races/2026/05/19/44/12/paddock/live",
    );
    expect(getPaddockLiveUrl("/api/races/2026/05/19/44/12/paddock/live", production)).toBe(
      "wss://pc-keiba-viewer.kkk4oru.com/api/races/2026/05/19/44/12/paddock/live",
    );
  });

  it("uses a configured remote origin without duplicating slashes", () => {
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PADDOCK_REMOTE_ORIGIN", "https://example.test/");

    expect(getPaddockRequestUrl("/api/races/2026/05/19/44/12/paddock", localhost)).toBe(
      "https://example.test/api/races/2026/05/19/44/12/paddock",
    );
  });
});
