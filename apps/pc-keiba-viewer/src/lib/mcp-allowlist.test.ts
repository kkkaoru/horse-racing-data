// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import {
  isMcpAllowedApiPath,
  resolveMcpApiPath,
  resolveMcpPaddockWritePath,
} from "./mcp-allowlist";

it("allows the spec, top-races, and favorites APIs", () => {
  expect(isMcpAllowedApiPath("/api/spec")).toBe(true);
  expect(isMcpAllowedApiPath("/api/top-races")).toBe(true);
  expect(isMcpAllowedApiPath("/api/mypage/favorites")).toBe(true);
  expect(isMcpAllowedApiPath("/api/mypage/favorites/search")).toBe(true);
});

it("allows race section and supporting race APIs", () => {
  expect(isMcpAllowedApiPath("/api/races/2026/08/20/45/12/sections/win-rate-heatmap")).toBe(true);
  expect(isMcpAllowedApiPath("/api/races/2026/08/27/50/05/sections/finish-prediction")).toBe(true);
  expect(isMcpAllowedApiPath("/api/races/2026/08/20/45/12/realtime")).toBe(true);
  expect(isMcpAllowedApiPath("/api/races/2026/08/27/50/05/entity-recent-results")).toBe(true);
  expect(isMcpAllowedApiPath("/api/races/2026/08/20/45/12/paddock")).toBe(true);
  expect(isMcpAllowedApiPath("/api/horses/1234567890/running-styles")).toBe(true);
});

it("rejects cache-warm, internal, and live mutation paths", () => {
  expect(isMcpAllowedApiPath("/api/internal/race-cache-bust")).toBe(false);
  expect(isMcpAllowedApiPath("/api/cache-warm/race-trends")).toBe(false);
  expect(isMcpAllowedApiPath("/api/races/2026/08/20/45/12/trends/live")).toBe(false);
  expect(isMcpAllowedApiPath("/api/races/2026/08/20/45/12/paddock/live")).toBe(false);
  expect(isMcpAllowedApiPath("/api/races/2026/08/20/45/12/paddock/discord")).toBe(false);
});

it("resolveMcpApiPath rejects relative paths and disallowed APIs", () => {
  expect(resolveMcpApiPath("api/spec")).toBe(null);
  expect(resolveMcpApiPath("/api/internal/x")).toBe(null);
});

it("resolveMcpApiPath keeps allowlisted paths and query strings", () => {
  expect(resolveMcpApiPath("/api/spec")).toBe("/api/spec");
  expect(resolveMcpApiPath("/api/top-races?debug=1")).toBe("/api/top-races?debug=1");
  expect(resolveMcpApiPath("/api/races/2026/08/20/45/12/paddock")).toBe(
    "/api/races/2026/08/20/45/12/paddock",
  );
});

it("resolveMcpPaddockWritePath allows only the paddock state POST path", () => {
  expect(resolveMcpPaddockWritePath("/api/races/2026/08/20/45/12/paddock")).toBe(
    "/api/races/2026/08/20/45/12/paddock",
  );
  expect(resolveMcpPaddockWritePath("/api/races/2026/08/20/45/12/paddock?x=1")).toBe(
    "/api/races/2026/08/20/45/12/paddock?x=1",
  );
  expect(resolveMcpPaddockWritePath("api/races/2026/08/20/45/12/paddock")).toBe(null);
  expect(resolveMcpPaddockWritePath("/api/races/2026/08/20/45/12/paddock/discord")).toBe(null);
  expect(resolveMcpPaddockWritePath("/api/races/2026/08/20/45/12/realtime")).toBe(null);
});
