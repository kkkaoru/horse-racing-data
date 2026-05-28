import { afterEach, describe, expect, it, vi } from "vitest";

import {
  getPaddockLiveUrl,
  getPaddockRequestUrl,
  getRaceTrendLiveUrl,
  isLocalPaddockHost,
} from "./paddock-client-url";

const localhost = {
  host: "localhost",
  hostname: "localhost",
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

  it("keeps relative paddock URLs on localhost", () => {
    expect(getPaddockRequestUrl("/api/races/2026/05/19/44/12/paddock", localhost)).toBe(
      "/api/races/2026/05/19/44/12/paddock",
    );
  });

  it("uses the production live relay in dev when enabled", () => {
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PRODUCTION_API_PROXY", "1");
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PRODUCTION_LIVE_RELAY_PORT", "3010");

    expect(getPaddockLiveUrl("/api/races/2026/05/19/44/12/paddock/live", localhost)).toBe(
      "ws://127.0.0.1:3010/api/races/2026/05/19/44/12/paddock/live",
    );
    expect(getRaceTrendLiveUrl("/api/races/2026/05/19/44/12/trends/live?source=jra")).toBe(
      "ws://127.0.0.1:3010/api/races/2026/05/19/44/12/trends/live?source=jra",
    );
  });

  it("builds websocket URLs from the current host when relay is disabled", () => {
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PRODUCTION_API_PROXY", "0");

    expect(getPaddockLiveUrl("/api/races/2026/05/19/44/12/paddock/live", localhost)).toBe(
      "wss://localhost/api/races/2026/05/19/44/12/paddock/live",
    );
    expect(
      getPaddockLiveUrl("api/races/live", {
        host: "localhost:3000",
        hostname: "localhost",
        protocol: "http:",
      }),
    ).toBe("ws://localhost:3000/api/races/live");
    expect(getPaddockLiveUrl("/api/live", null)).toBe("/api/live");
  });

  it("builds race trend websocket URLs from the current browser location", () => {
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PRODUCTION_API_PROXY", "0");
    vi.stubGlobal("window", {
      location: new URL("http://localhost:3000/races"),
    });

    expect(getRaceTrendLiveUrl("/api/races/2026/05/19/44/12/trends/live?source=jra")).toBe(
      "ws://localhost:3000/api/races/2026/05/19/44/12/trends/live?source=jra",
    );
    expect(getRaceTrendLiveUrl("api/trends/live")).toBe("ws://localhost:3000/api/trends/live");
  });

  it("returns the path when race trend live url runs without a browser", () => {
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PRODUCTION_API_PROXY", "0");
    vi.stubGlobal("window", undefined);
    expect(getRaceTrendLiveUrl("/api/trends/live")).toBe("/api/trends/live");
  });

  it("upgrades https-served race trend URLs to wss", () => {
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PRODUCTION_API_PROXY", "0");
    vi.stubGlobal("window", {
      location: new URL("https://pc-keiba-viewer.kkk4oru.com/races"),
    });
    expect(getRaceTrendLiveUrl("/api/trends/live")).toBe(
      "wss://pc-keiba-viewer.kkk4oru.com/api/trends/live",
    );
  });

  it("defaults paddock live url location to the current window when omitted", () => {
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PRODUCTION_API_PROXY", "0");
    vi.stubGlobal("window", {
      location: {
        host: "example.com",
        hostname: "example.com",
        protocol: "https:",
      },
    });
    expect(getPaddockLiveUrl("/api/live")).toBe("wss://example.com/api/live");
  });

  it("defaults paddock request url location to null when window is undefined", () => {
    vi.stubGlobal("window", undefined);
    expect(getPaddockRequestUrl("/api/relative")).toBe("/api/relative");
  });
});
