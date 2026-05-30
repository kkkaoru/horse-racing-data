// Run with bun via the pc-keiba-viewer vitest config.
import { afterEach, describe, expect, it, vi } from "vitest";

import { getProductionLiveWebSocketUrl } from "./production-live-client-url";

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("production live client url", () => {
  it("returns null when the production relay is disabled", () => {
    vi.stubEnv("NODE_ENV", "development");
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PRODUCTION_API_PROXY", "0");
    expect(getProductionLiveWebSocketUrl("/api/live")).toBeNull();
  });

  it("uses configured relay origin and normalizes paths in development", () => {
    vi.stubEnv("NODE_ENV", "development");
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PRODUCTION_LIVE_RELAY_ORIGIN", "ws://relay.example.com/");
    expect(getProductionLiveWebSocketUrl("api/live")).toBe("ws://relay.example.com/api/live");
    expect(getProductionLiveWebSocketUrl("/api/live")).toBe("ws://relay.example.com/api/live");
  });

  it("uses configured relay origin and normalizes paths in production", () => {
    vi.stubEnv("NODE_ENV", "production");
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PRODUCTION_LIVE_RELAY_ORIGIN", "wss://relay.example.com/");
    expect(getProductionLiveWebSocketUrl("api/live")).toBe("wss://relay.example.com/api/live");
    expect(getProductionLiveWebSocketUrl("/api/live")).toBe("wss://relay.example.com/api/live");
  });

  it("falls back to the default relay port in development when no port env is configured", () => {
    vi.stubEnv("NODE_ENV", "development");
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PRODUCTION_API_PROXY", "1");
    delete process.env.NEXT_PUBLIC_PC_KEIBA_PRODUCTION_LIVE_RELAY_ORIGIN;
    delete process.env.NEXT_PUBLIC_PC_KEIBA_PRODUCTION_LIVE_RELAY_PORT;
    expect(getProductionLiveWebSocketUrl("/api/live")).toBe("ws://127.0.0.1:3010/api/live");
  });

  it("uses the configured relay port in development when set", () => {
    vi.stubEnv("NODE_ENV", "development");
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PRODUCTION_API_PROXY", "1");
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PRODUCTION_LIVE_RELAY_PORT", "4242");
    delete process.env.NEXT_PUBLIC_PC_KEIBA_PRODUCTION_LIVE_RELAY_ORIGIN;
    expect(getProductionLiveWebSocketUrl("/api/live")).toBe("ws://127.0.0.1:4242/api/live");
  });

  it("returns null in production when no relay origin env is configured", () => {
    vi.stubEnv("NODE_ENV", "production");
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PRODUCTION_API_PROXY", "1");
    delete process.env.NEXT_PUBLIC_PC_KEIBA_PRODUCTION_LIVE_RELAY_ORIGIN;
    delete process.env.NEXT_PUBLIC_PC_KEIBA_PRODUCTION_LIVE_RELAY_PORT;
    expect(getProductionLiveWebSocketUrl("/api/live")).toBeNull();
  });

  it("returns null in production even when only the port env is set", () => {
    vi.stubEnv("NODE_ENV", "production");
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PRODUCTION_API_PROXY", "1");
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PRODUCTION_LIVE_RELAY_PORT", "9999");
    delete process.env.NEXT_PUBLIC_PC_KEIBA_PRODUCTION_LIVE_RELAY_ORIGIN;
    expect(getProductionLiveWebSocketUrl("/api/live")).toBeNull();
  });

  it("returns null in production when the proxy is explicitly disabled", () => {
    vi.stubEnv("NODE_ENV", "production");
    vi.stubEnv("NEXT_PUBLIC_PC_KEIBA_PRODUCTION_API_PROXY", "0");
    expect(getProductionLiveWebSocketUrl("/api/live")).toBeNull();
  });
});
