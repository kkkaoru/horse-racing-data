// Run with bun. All service requests are mocked.
import { expect, it, vi } from "vitest";

import {
  readCatalogRaceDetail,
  type CatalogRaceDetailBinding,
  type CatalogRaceDetailQuery,
} from "./race-detail-catalog";
import type { RaceDetail } from "./race-types";

const query: CatalogRaceDetailQuery = {
  source: "jra",
  date: "20260816",
  keibajoCode: "A8",
  raceBango: "04",
};
const row: RaceDetail = {
  source: "jra",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0816",
  keibajoCode: "A8",
  raceBango: "04",
  kyosomeiHondai: "競走　 ",
  kyosomeiFukudai: null,
  gradeCode: " ",
  kyosoShubetsuCode: null,
  kyosoKigoCode: null,
  juryoShubetsuCode: null,
  kyosoJokenCode: null,
  kyosoJokenMeisho: null,
  kyori: "2000",
  trackCode: null,
  hassoJikoku: null,
  shussoTosu: null,
  kaisaiKai: null,
  kaisaiNichime: null,
  kyosomeiKakkonai: null,
  torokuTosu: null,
  tenkoCode: null,
  babajotaiCodeShiba: null,
  babajotaiCodeDirt: null,
};

it("preserves padded strings and nulls using only the private GET binding", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(Response.json({ row }, { headers: { "cache-control": "no-store" } }));
  const result = await readCatalogRaceDetail({ fetch }, query);
  expect(result?.kyosomeiHondai).toBe("競走　 ");
  expect(result?.gradeCode).toBe(" ");
  expect(result?.kyosomeiFukudai).toBe(null);
  expect(Object.keys(result ?? {})).toHaveLength(24);
  expect(fetch).toHaveBeenCalledTimes(1);
  const request = fetch.mock.calls[0]?.[0];
  expect(request?.url).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/race-detail?source=jra&date=20260816&keibajoCode=A8&raceBango=04",
  );
  expect(request?.method).toBe("GET");
  expect(request?.redirect).toBe("manual");
  expect(request?.headers.get("authorization")).toBe(null);
});
it("treats an explicit empty result as authoritative", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(Response.json({ row: null }, { headers: { "cache-control": "no-store" } }));
  await expect(readCatalogRaceDetail({ fetch }, query)).resolves.toBe(null);
  expect(fetch).toHaveBeenCalledTimes(1);
});
it("sanitizes truncated UTF-8 rejected by the shared bounded decoder", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(
      new Response(new Uint8Array([227, 129]), { headers: { "cache-control": "no-store" } }),
    );
  await expect(readCatalogRaceDetail({ fetch }, query)).rejects.toThrow(
    "Catalog race detail unavailable",
  );
  expect(fetch).toHaveBeenCalledTimes(1);
});
it("fails closed when the private binding is missing", async () => {
  await expect(readCatalogRaceDetail(undefined, query)).rejects.toThrow(
    "Catalog race detail unavailable",
  );
});
it("sanitizes provider failures without retries", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockRejectedValue(new Error("private token or endpoint"));
  await expect(readCatalogRaceDetail({ fetch }, query)).rejects.toThrow(
    "Catalog race detail unavailable",
  );
  expect(fetch).toHaveBeenCalledTimes(1);
});
it.each([401, 404, 500, 503, 301, 302, 303, 307, 308])(
  "does not interpret HTTP %i as absence",
  async (status) => {
    const fetch = vi
      .fn<CatalogRaceDetailBinding["fetch"]>()
      .mockResolvedValue(new Response(null, { status }));
    await expect(readCatalogRaceDetail({ fetch }, query)).rejects.toThrow(
      "Catalog race detail unavailable",
    );
  },
);
it("cancels an error response body", async () => {
  const cancel = vi.fn<() => void>();
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(new Response(new ReadableStream({ cancel }), { status: 503 }));
  await expect(readCatalogRaceDetail({ fetch }, query)).rejects.toThrow(
    "Catalog race detail unavailable",
  );
  expect(cancel).toHaveBeenCalledTimes(1);
});
it("rejects responses without the no-store contract", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(Response.json({ row: null }));
  await expect(readCatalogRaceDetail({ fetch }, query)).rejects.toThrow(
    "Catalog race detail unavailable",
  );
});
it.each([
  null,
  [],
  "invalid",
  {},
  { row: [] },
  { row: { ...row, source: "nar" } },
  { row: { ...row, kaisaiNen: "2025" } },
  { row: { ...row, kaisaiTsukihi: "0817" } },
  { row: { ...row, keibajoCode: "A6" } },
  { row: { ...row, raceBango: "05" } },
  { row: { ...row, kyori: 2000 } },
  { row: { ...row, extra: true } },
  { row: { ...row, kyori: undefined } },
])("rejects malformed or mismatched data %#", async (value) => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(Response.json(value, { headers: { "cache-control": "no-store" } }));
  await expect(readCatalogRaceDetail({ fetch }, query)).rejects.toThrow(
    "Catalog race detail unavailable",
  );
});
it("rejects missing response bodies", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(new Response(null, { headers: { "cache-control": "no-store" } }));
  await expect(readCatalogRaceDetail({ fetch }, query)).rejects.toThrow(
    "Catalog race detail unavailable",
  );
});
it("rejects invalid JSON", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(new Response("not json", { headers: { "cache-control": "no-store" } }));
  await expect(readCatalogRaceDetail({ fetch }, query)).rejects.toThrow(
    "Catalog race detail unavailable",
  );
});
it("accepts the exact byte limit", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(
      new Response('{"row":null}'.padEnd(65536, " "), { headers: { "cache-control": "no-store" } }),
    );
  await expect(readCatalogRaceDetail({ fetch }, query)).resolves.toBe(null);
});
it("stops and cancels the stream when the byte limit is exceeded", async () => {
  const cancel = vi.fn<() => void>();
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new Uint8Array(65537));
    },
    cancel,
  });
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(new Response(stream, { headers: { "cache-control": "no-store" } }));
  await expect(readCatalogRaceDetail({ fetch }, query)).rejects.toThrow(
    "Catalog race detail unavailable",
  );
  expect(cancel).toHaveBeenCalledTimes(1);
});
it("decodes Japanese text split across chunks", async () => {
  const bytes: Uint8Array = new TextEncoder().encode(JSON.stringify({ row }));
  const split: number = bytes.indexOf(0xe7) + 1;
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(bytes.slice(0, split));
      controller.enqueue(bytes.slice(split));
      controller.close();
    },
  });
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(new Response(stream, { headers: { "cache-control": "no-store" } }));
  expect((await readCatalogRaceDetail({ fetch }, query))?.kyosomeiHondai).toBe("競走　 ");
});
it("rejects invalid UTF-8 rather than changing data", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(
      new Response(new Uint8Array([255]), { headers: { "cache-control": "no-store" } }),
    );
  await expect(readCatalogRaceDetail({ fetch }, query)).rejects.toThrow(
    "Catalog race detail unavailable",
  );
});
