// Run with bun (exercised via `bunx vitest run`).
import { expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const mocks = vi.hoisted(() => ({
  getRaceRunnersMock: vi.fn<(...args: never[]) => Promise<unknown[]>>(),
}));

vi.mock("../../../../../../../../../db/queries", () => ({
  getRaceRunners: mocks.getRaceRunnersMock,
}));

import { GET } from "./route";

const params = {
  day: "06",
  keibajoCode: "83",
  month: "09",
  raceNumber: "11",
  year: "2026",
};

it("returns compact Ban-ei weight-minus-futan rows", async () => {
  mocks.getRaceRunnersMock.mockResolvedValueOnce([
    { bataiju: "3E8", futanJuryo: "262", umaban: "01" },
  ]);
  const response = await GET(
    new Request("https://example.test/api/races/2026/09/06/83/11/weight-futan?source=nar"),
    { params: Promise.resolve(params) },
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ r: [{ d: 390, f: 610, u: "1", w: 1000 }] });
});

it("rejects an invalid source", async () => {
  const response = await GET(
    new Request("https://example.test/api/races/2026/09/06/83/11/weight-futan"),
    { params: Promise.resolve(params) },
  );
  expect(response.status).toBe(400);
  expect(await response.json()).toStrictEqual({ error: "invalid source" });
});

it("rejects invalid route params", async () => {
  const response = await GET(
    new Request("https://example.test/api/races/2026/09/06/83/11/weight-futan?source=nar"),
    { params: Promise.resolve({ ...params, year: "26" }) },
  );
  expect(response.status).toBe(400);
  expect(await response.json()).toStrictEqual({ error: "invalid_params" });
});
