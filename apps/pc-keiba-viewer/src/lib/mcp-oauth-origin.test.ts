// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import {
  isCanonicalMcpResource,
  mcpResourceUrl,
  normalizeMcpResource,
  originFromForwardedHeaders,
  originFromRequestUrl,
} from "./mcp-oauth-origin";

it("derives origin and MCP resource URLs", () => {
  expect(originFromRequestUrl("https://viewer.example.test/oauth/authorize?x=1")).toBe(
    "https://viewer.example.test",
  );
  expect(mcpResourceUrl("https://viewer.example.test")).toBe("https://viewer.example.test/mcp");
  expect(
    isCanonicalMcpResource("https://viewer.example.test", "https://viewer.example.test/mcp"),
  ).toBe(true);
  expect(isCanonicalMcpResource("https://viewer.example.test", "https://viewer.example.test")).toBe(
    true,
  );
  expect(
    isCanonicalMcpResource("https://viewer.example.test", "https://other.example.test/mcp"),
  ).toBe(false);
  expect(normalizeMcpResource("https://viewer.example.test", "https://viewer.example.test")).toBe(
    "https://viewer.example.test/mcp",
  );
  expect(
    normalizeMcpResource("https://viewer.example.test", "https://other.example.test/mcp"),
  ).toBe(null);
});

it("reads origin from forwarded headers and rejects a missing host", () => {
  expect(
    originFromForwardedHeaders(
      new Headers({
        "x-forwarded-host": " viewer.example.test ",
        "x-forwarded-proto": "https",
      }),
    ),
  ).toBe("https://viewer.example.test");
  expect(originFromForwardedHeaders(new Headers({ host: "viewer.example.test" }))).toBe(
    "https://viewer.example.test",
  );
  expect(
    originFromForwardedHeaders(
      new Headers({ host: "viewer.example.test", "x-forwarded-proto": "http" }),
    ),
  ).toBe("http://viewer.example.test");
  expect(originFromForwardedHeaders(new Headers())).toBe(null);
  expect(originFromForwardedHeaders(new Headers({ host: "   " }))).toBe(null);
});
