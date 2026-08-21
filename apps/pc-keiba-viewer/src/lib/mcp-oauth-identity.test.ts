// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import { readAccessUserSubject } from "./mcp-oauth-identity";

it("reads the Access email header", () => {
  const request = new Request("https://viewer.example.test/oauth/authorize", {
    headers: { "Cf-Access-Authenticated-User-Email": "user@example.test" },
  });
  expect(readAccessUserSubject(request)).toBe("user@example.test");
});

it("falls back to the Access JWT email claim", () => {
  const payload = btoa(JSON.stringify({ email: "jwt@example.test" }))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replaceAll("=", "");
  const request = new Request("https://viewer.example.test/oauth/authorize", {
    headers: { "Cf-Access-Jwt-Assertion": `header.${payload}.sig` },
  });
  expect(readAccessUserSubject(request)).toBe("jwt@example.test");
});

it("falls back to the Access JWT sub claim", () => {
  const payload = btoa(JSON.stringify({ sub: "access-sub-1" }))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replaceAll("=", "");
  const request = new Request("https://viewer.example.test/oauth/authorize", {
    headers: { "Cf-Access-Jwt-Assertion": `header.${payload}.sig` },
  });
  expect(readAccessUserSubject(request)).toBe("access-sub-1");
});

it("returns null when Access identity is missing", () => {
  expect(readAccessUserSubject(new Request("https://viewer.example.test/oauth/authorize"))).toBe(
    null,
  );
});

it("trims the Access email header and ignores a blank value", () => {
  expect(
    readAccessUserSubject(
      new Request("https://viewer.example.test/oauth/authorize", {
        headers: { "Cf-Access-Authenticated-User-Email": "  user@example.test  " },
      }),
    ),
  ).toBe("user@example.test");
  expect(
    readAccessUserSubject(
      new Request("https://viewer.example.test/oauth/authorize", {
        headers: { "Cf-Access-Authenticated-User-Email": "   " },
      }),
    ),
  ).toBe(null);
});

it("returns null for malformed Access JWT assertions", () => {
  expect(
    readAccessUserSubject(
      new Request("https://viewer.example.test/oauth/authorize", {
        headers: { "Cf-Access-Jwt-Assertion": "only-one-part" },
      }),
    ),
  ).toBe(null);
  expect(
    readAccessUserSubject(
      new Request("https://viewer.example.test/oauth/authorize", {
        headers: { "Cf-Access-Jwt-Assertion": "header.!!!.sig" },
      }),
    ),
  ).toBe(null);
  const notJson = btoa("not-json").replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
  expect(
    readAccessUserSubject(
      new Request("https://viewer.example.test/oauth/authorize", {
        headers: { "Cf-Access-Jwt-Assertion": `header.${notJson}.sig` },
      }),
    ),
  ).toBe(null);
  const arrayPayload = btoa("[]").replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
  expect(
    readAccessUserSubject(
      new Request("https://viewer.example.test/oauth/authorize", {
        headers: { "Cf-Access-Jwt-Assertion": `header.${arrayPayload}.sig` },
      }),
    ),
  ).toBe(null);
});

it("returns null when JWT email and sub claims are empty", () => {
  const payload = btoa(JSON.stringify({ email: "", sub: "" }))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replaceAll("=", "");
  expect(
    readAccessUserSubject(
      new Request("https://viewer.example.test/oauth/authorize", {
        headers: { "Cf-Access-Jwt-Assertion": `header.${payload}.sig` },
      }),
    ),
  ).toBe(null);
});

it("uses sub when email is not a non-empty string", () => {
  const payload = btoa(JSON.stringify({ email: 1, sub: "access-sub-2" }))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replaceAll("=", "");
  expect(
    readAccessUserSubject(
      new Request("https://viewer.example.test/oauth/authorize", {
        headers: { "Cf-Access-Jwt-Assertion": `header.${payload}.sig` },
      }),
    ),
  ).toBe("access-sub-2");
});
