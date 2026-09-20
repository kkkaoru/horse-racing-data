// Run with bun via the package test scripts.
import { expect, test, vi } from "vitest";
vi.mock("server-only", () => ({}));
import { readCatalogRaceCourse } from "./race-course-catalog";
import type { CourseQuery } from "./race-course-catalog";

const query: CourseQuery = { keibajoCode: "05", kyori: "1600", trackCode: "11" };

test.each([" コース説明\n ", null])(
  "preserves course text and issues a bounded private request: %j",
  async (text) => {
    const fetch = vi.fn<(request: Request) => Promise<Response>>(async (_request) =>
      Response.json(
        { course: { courseKaishuNengappi: "20240106", courseSetsumei: text } },
        { headers: { "Cache-Control": "no-store" } },
      ),
    );
    expect(await readCatalogRaceCourse({ fetch }, query)).toMatchObject({
      courseKaishuNengappi: "20240106",
      courseSetsumei: text,
    });
    const request = fetch.mock.calls[0]?.[0];
    expect(request?.url).toBe(
      "https://pc-keiba-r2-catalog.internal/v1/race-course?keibajoCode=05&kyori=1600&trackCode=11",
    );
    expect(request?.method).toBe("GET");
    expect(request?.redirect).toBe("manual");
    expect(request?.signal).toBeInstanceOf(AbortSignal);
  },
);

test("distinguishes absent courses from missing bindings", async () => {
  await expect(
    readCatalogRaceCourse(
      {
        fetch: async () =>
          Response.json({ course: null }, { headers: { "Cache-Control": "no-store" } }),
      },
      query,
    ),
  ).resolves.toBeNull();
  await expect(readCatalogRaceCourse(undefined, query)).rejects.toThrow(
    "Catalog course unavailable",
  );
});

test.each([
  null,
  [],
  {},
  { course: {} },
  { course: { courseKaishuNengappi: 20240106, courseSetsumei: null } },
  { course: { courseKaishuNengappi: "20240106" } },
  { course: { courseKaishuNengappi: "20240106", courseSetsumei: 1 } },
])("rejects malformed payloads: %j", async (payload) => {
  await expect(
    readCatalogRaceCourse(
      { fetch: async () => Response.json(payload, { headers: { "Cache-Control": "no-store" } }) },
      query,
    ),
  ).rejects.toThrow("Catalog course unavailable");
});

test.each([
  { status: 503, body: "unavailable", cache: "no-store" },
  { status: 200, body: "{}", cache: "public" },
  { status: 503, body: null, cache: "no-store" },
  { status: 200, body: null, cache: "no-store" },
  { status: 200, body: "not JSON", cache: "no-store" },
  { status: 200, body: "x".repeat(65537), cache: "no-store" },
])("rejects failed or unsafe response bodies: $status/$cache", async ({ status, body, cache }) => {
  await expect(
    readCatalogRaceCourse(
      { fetch: async () => new Response(body, { status, headers: { "Cache-Control": cache } }) },
      query,
    ),
  ).rejects.toThrow("Catalog course unavailable");
});

test("sanitizes transport errors", async () => {
  await expect(
    readCatalogRaceCourse(
      {
        fetch: async () => {
          throw new Error("private upstream details");
        },
      },
      query,
    ),
  ).rejects.toThrow("Catalog course unavailable");
});
