// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import {
  displayedMcpUrl,
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

it("displays an absolute MCP URL from the request origin or the browser origin", () => {
  expect(displayedMcpUrl("https://viewer.example.test/mcp", "https://other.example.test")).toBe(
    "https://viewer.example.test/mcp",
  );
  expect(displayedMcpUrl("/mcp", "https://viewer.example.test")).toBe(
    "https://viewer.example.test/mcp",
  );
  expect(displayedMcpUrl("/mcp", "https://viewer.example.test/")).toBe(
    "https://viewer.example.test/mcp",
  );
  expect(displayedMcpUrl("http://127.0.0.1:3000/mcp", null)).toBe("http://127.0.0.1:3000/mcp");
  expect(displayedMcpUrl("/mcp", null)).toBe("/mcp");
  expect(displayedMcpUrl("/mcp", "   ")).toBe("/mcp");
});
