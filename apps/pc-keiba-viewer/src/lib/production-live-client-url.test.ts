import { afterEach, describe, expect, it, vi } from "vitest";

import { getProductionLiveWebSocketUrl } from "./production-live-client-url";

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("production live client url", () => {
  it("returns null when the production relay is disabled", () => {
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PRODUCTION_API_PROXY", "0");
    expect(getProductionLiveWebSocketUrl("/api/live")).toBeNull();
  });

  it("uses configured relay origin and normalizes paths", () => {
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PRODUCTION_LIVE_RELAY_ORIGIN", "ws://relay.example.com/");
    expect(getProductionLiveWebSocketUrl("api/live")).toBe("ws://relay.example.com/api/live");
    expect(getProductionLiveWebSocketUrl("/api/live")).toBe("ws://relay.example.com/api/live");
  });

  it("falls back to the default relay port when no port env is configured", () => {
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PRODUCTION_API_PROXY", "1");
    delete process.env.NEXT_PUBLIC_PC_KEIBA_PRODUCTION_LIVE_RELAY_ORIGIN;
    delete process.env.NEXT_PUBLIC_PC_KEIBA_PRODUCTION_LIVE_RELAY_PORT;
    expect(getProductionLiveWebSocketUrl("/api/live")).toBe("ws://127.0.0.1:3010/api/live");
  });
});
