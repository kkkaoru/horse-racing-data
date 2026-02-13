// Run with: bun
// Tests for health.ts route

import { it, expect } from "vitest";
import { healthRoute } from "./health.ts";

it("should return 200 with ok status", async () => {
  const request = new Request("http://localhost/health");
  const response = await healthRoute.request(request);

  expect(response.status).toStrictEqual(200);

  const body = await response.json();
  expect(body).toStrictEqual({ status: "ok" });
});

it("should return JSON content type", async () => {
  const request = new Request("http://localhost/health");
  const response = await healthRoute.request(request);
  const contentType = response.headers.get("content-type");

  expect(contentType).toStrictEqual("application/json");
});
