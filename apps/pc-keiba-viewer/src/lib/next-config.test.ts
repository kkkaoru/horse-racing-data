// Run with bun via the pc-keiba-viewer vitest config.
import { pathToFileURL } from "node:url";

import { expect, it } from "vitest";

import {
  getCloudflareDevConfigPath,
  getCloudflareDevContextOptions,
  parseAllowedDevOrigins,
  resolveAllowedDevOrigins,
  shouldEnableCloudflareRemoteBindings,
} from "./next-config";

const NEXT_CONFIG_URL = pathToFileURL(`${process.cwd()}/next.config.ts`).href;

it("parseAllowedDevOrigins trims comma-separated origins and drops blanks", () => {
  const origins = parseAllowedDevOrigins(" localhost:3000,127.0.0.1:3000, ,192.168.1.219 ");
  expect(origins).toStrictEqual(["localhost:3000", "127.0.0.1:3000", "192.168.1.219"]);
});

it("parseAllowedDevOrigins returns an empty array when the env is missing", () => {
  const origins = parseAllowedDevOrigins(undefined);
  expect(origins).toStrictEqual([]);
});

it("resolveAllowedDevOrigins always includes https 443 hosts used by local next", () => {
  expect(resolveAllowedDevOrigins(undefined)).toStrictEqual([
    "localhost",
    "127.0.0.1",
    "192.168.1.219",
    "localhost:3000",
    "127.0.0.1:3000",
    "localhost:443",
    "127.0.0.1:443",
    "192.168.1.219:443",
  ]);
});

it("resolveAllowedDevOrigins appends extra env origins without duplicating defaults", () => {
  expect(resolveAllowedDevOrigins("localhost,example.test")).toStrictEqual([
    "localhost",
    "127.0.0.1",
    "192.168.1.219",
    "localhost:3000",
    "127.0.0.1:3000",
    "localhost:443",
    "127.0.0.1:443",
    "192.168.1.219:443",
    "example.test",
  ]);
});

it("shouldEnableCloudflareRemoteBindings only enables remote bindings with explicit opt-in", () => {
  expect(shouldEnableCloudflareRemoteBindings("1")).toBe(true);
  expect(shouldEnableCloudflareRemoteBindings("true")).toBe(false);
  expect(shouldEnableCloudflareRemoteBindings(undefined)).toBe(false);
});

it("getCloudflareDevConfigPath resolves wrangler.dev.jsonc beside next.config.ts", () => {
  const configPath = getCloudflareDevConfigPath(NEXT_CONFIG_URL);
  expect(configPath.endsWith("/apps/pc-keiba-viewer/wrangler.dev.jsonc")).toBe(true);
});

it("getCloudflareDevContextOptions disables remote bindings by default", () => {
  const options = getCloudflareDevContextOptions(NEXT_CONFIG_URL, undefined);
  expect(options.remoteBindings).toBe(false);
  expect(options.configPath?.endsWith("/apps/pc-keiba-viewer/wrangler.dev.jsonc")).toBe(true);
});

it("getCloudflareDevContextOptions enables remote bindings when explicitly requested", () => {
  const options = getCloudflareDevContextOptions(NEXT_CONFIG_URL, "1");
  expect(options.remoteBindings).toBe(true);
  expect(options.configPath?.endsWith("/apps/pc-keiba-viewer/wrangler.dev.jsonc")).toBe(true);
});
