// Run with: bun run --filter sync-realtime-data-features test
import { expect, it, vi } from "vitest";

import { fetchCatalogRows, isRecord } from "./catalog-client";

it("fetchCatalogRows sends a GET request and returns rows", async () => {
  const requests: Request[] = [];
  const fetch = vi.fn(async (request: Request): Promise<Response> => {
    requests.push(request);
    return Response.json([{ raceKey: "jra:2026:0529:05:01" }]);
  });
  const rows = await fetchCatalogRows(
    { fetch },
    new URL("https://pc-keiba-r2-catalog/v1/race-keys?date=20260529"),
  );
  expect(requests[0]?.method).toBe("GET");
  expect(requests[0]?.url).toBe("https://pc-keiba-r2-catalog/v1/race-keys?date=20260529");
  expect(rows).toStrictEqual([{ raceKey: "jra:2026:0529:05:01" }]);
});

it("fetchCatalogRows rejects non-success catalog responses", async () => {
  const fetch = vi.fn(async (): Promise<Response> => new Response(null, { status: 503 }));
  await expect(
    fetchCatalogRows({ fetch }, new URL("https://pc-keiba-r2-catalog/v1/race-features")),
  ).rejects.toThrowError("PC_KEIBA_R2_CATALOG /v1/race-features failed with HTTP 503");
});

it("fetchCatalogRows rejects object payloads", async () => {
  const fetch = vi.fn(async (): Promise<Response> => Response.json({ items: [] }));
  await expect(
    fetchCatalogRows({ fetch }, new URL("https://pc-keiba-r2-catalog/v1/race-keys")),
  ).rejects.toThrowError("PC_KEIBA_R2_CATALOG /v1/race-keys returned invalid rows");
});

it("fetchCatalogRows rejects primitive payloads", async () => {
  const fetch = vi.fn(async (): Promise<Response> => Response.json("rows"));
  await expect(
    fetchCatalogRows({ fetch }, new URL("https://pc-keiba-r2-catalog/v1/race-keys")),
  ).rejects.toThrowError("PC_KEIBA_R2_CATALOG /v1/race-keys returned invalid rows");
});

it("isRecord rejects primitive, null, and array values", () => {
  expect(isRecord("row")).toBe(false);
  expect(isRecord(null)).toBe(false);
  expect(isRecord([])).toBe(false);
  expect(isRecord({})).toBe(true);
});
