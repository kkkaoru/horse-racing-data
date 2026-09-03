// Run with bun.
import { describe, expect, it } from "vitest";
import { isAuthorized, unauthorizedResponse } from "./auth";

describe("Worker API authentication", () => {
  it.each([
    [undefined, "secret", false],
    ["Basic abc", "secret", false],
    ["Bearer secret", "", false],
    ["Bearer wrong", "secret", false],
    ["Bearer secret", "secret", true],
  ])("validates only the configured bearer token", async (authorization, secret, expected) => {
    const headers: HeadersInit =
      authorization === undefined ? {} : { Authorization: authorization };
    await expect(
      isAuthorized(new Request("https://example.test", { headers }), secret),
    ).resolves.toBe(expected);
  });

  it("returns a bearer challenge", async () => {
    const response: Response = unauthorizedResponse();
    expect(response.status).toBe(401);
    expect(response.headers.get("WWW-Authenticate")).toBe("Bearer");
    await expect(response.json()).resolves.toEqual({ error: "Unauthorized" });
  });
});
