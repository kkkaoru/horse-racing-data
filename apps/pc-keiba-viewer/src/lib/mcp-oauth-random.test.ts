// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import { randomToken } from "./mcp-oauth-random";

it("returns a unique unpadded base64url token", () => {
  const first = randomToken();
  const second = randomToken();
  expect(first.length > 0).toBe(true);
  expect(first.indexOf("+")).toBe(-1);
  expect(first.indexOf("/")).toBe(-1);
  expect(first.indexOf("=")).toBe(-1);
  expect(first === second).toBe(false);
});
