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
});
