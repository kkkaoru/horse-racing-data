// Runs with bun via Vitest; real service-binding dispatch with all upstream network I/O mocked.
import { build } from "esbuild";
import { createFetchMock, Miniflare } from "miniflare";
import { afterAll, beforeAll, expect, it } from "vitest";
interface RuntimeState {
  worker: Miniflare | null;
}
const runtime: RuntimeState = { worker: null };
const upstream: ReturnType<typeof createFetchMock> = createFetchMock();
const endpoint: string = "/api/v1/accounts/account/r2-sql/query/catalog";
const instance = (): Miniflare => {
  if (runtime.worker === null) throw new Error("Native reader not started");
  return runtime.worker;
};
beforeAll(async () => {
  upstream.disableNetConnect();
  const gateway: string = new URL("./vector-gateway.ts", import.meta.url).pathname;
  const service: string = new URL("./race-detail-service.ts", import.meta.url).pathname;
  const consumer: string = new URL(
    "../../pc-keiba-viewer/src/lib/race-detail-catalog.ts",
    import.meta.url,
  ).pathname;
  const calendarConsumer: string = new URL(
    "../../pc-keiba-viewer/src/lib/race-calendar-catalog.ts",
    import.meta.url,
  ).pathname;
  const yearsConsumer: string = new URL(
    "../../pc-keiba-viewer/src/lib/race-years-catalog.ts",
    import.meta.url,
  ).pathname;
  const dayListConsumer: string = new URL(
    "../../pc-keiba-viewer/src/lib/race-day-list-catalog.ts",
    import.meta.url,
  ).pathname;
  const bundle = await build({
    stdin: {
      contents: `import gateway from ${JSON.stringify(gateway)};
import { readCatalogRaceDetail } from ${JSON.stringify(consumer)};
import { readCatalogRaceCalendar } from ${JSON.stringify(calendarConsumer)};
import { readCatalogRaceYears } from ${JSON.stringify(yearsConsumer)};
import { readCatalogRaceDayList, readCatalogRaceDayListWithJockeys } from ${JSON.stringify(dayListConsumer)};
export { RaceDetailReadService } from ${JSON.stringify(service)};
export default { async fetch(request, env) {
  const url = new URL(request.url);
  if (url.pathname === '/jockey-day-list-consumer') {
    try {
      return Response.json({ races: await readCatalogRaceDayListWithJockeys(env.READER, '20240229') });
    } catch (error) {
      return Response.json({ error: error.message }, { status: 503 });
    }
  }
  if (url.pathname === '/day-list-consumer') {
    try {
      return Response.json({ races: await readCatalogRaceDayList(env.READER, '20240229') });
    } catch (error) {
      return Response.json({ error: error.message }, { status: 503 });
    }
  }
  if (url.pathname === '/years-consumer') {
    try {
      return Response.json({ years: await readCatalogRaceYears(env.READER) });
    } catch (error) {
      return Response.json({ error: error.message }, { status: 503 });
    }
  }
  if (url.pathname === '/calendar-consumer') {
    try {
      return Response.json({ days: await readCatalogRaceCalendar(env.READER, '2026') });
    } catch (error) {
      return Response.json({ error: error.message }, { status: 503 });
    }
  }
  if (url.pathname === '/consumer') {
    try {
      return Response.json({ row: await readCatalogRaceDetail(env.READER, {
        source: 'jra', date: '20260816', keibajoCode: 'A8', raceBango: '04'
      }) });
    } catch (error) {
      return Response.json({ error: error.message }, { status: 503 });
    }
  }
  if (url.pathname.startsWith('/binding/')) {
    url.pathname = url.pathname.slice(8);
    return await env.READER.fetch(new Request(url, request));
  }
  return await gateway.fetch(request, env);
} };`,
      loader: "ts",
      resolveDir: process.cwd(),
    },
    bundle: true,
    format: "esm",
    platform: "neutral",
    conditions: ["react-server", "workerd", "worker", "browser"],
    mainFields: ["browser", "module", "main"],
    write: false,
    external: ["cloudflare:workers", "node:*"],
    // Next resolves this build-time guard; native tests use the Viewer's existing equivalent stub.
    alias: {
      "server-only": new URL("../../pc-keiba-viewer/src/test-stubs/server-only.ts", import.meta.url)
        .pathname,
    },
    logLevel: "silent",
  });
  const output = bundle.outputFiles[0];
  if (output === undefined) throw new Error("Missing native reader bundle");
  runtime.worker = new Miniflare({
    name: "race-reader-native",
    modules: true,
    script: output.text,
    // Match the repository's installed native runtime; production retains its own compatibility date.
    compatibilityDate: "2026-06-01",
    compatibilityFlags: ["nodejs_compat"],
    fetchMock: upstream,
    bindings: {
      R2_SQL_ACCOUNT_ID: "account",
      R2_SQL_BUCKET_NAME: "catalog",
      R2_SQL_NAMESPACE: "pc_keiba",
      R2_SQL_TOKEN: "test-provider-token",
    },
    serviceBindings: {
      READER: { name: "race-reader-native", entrypoint: "RaceDetailReadService" },
    },
  });
  await runtime.worker.ready;
}, 20000);
afterAll(async () => {
  await runtime.worker?.dispose();
  await upstream.close();
});
it("reads through the named binding without an audit token", async () => {
  const observed: { body: unknown; headers: unknown } = { body: null, headers: null };
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply((options) => {
      observed.body = options.body;
      observed.headers = options.headers;
      return { statusCode: 200, data: JSON.stringify({ success: true, result: { rows: [] } }) };
    });
  const response = await instance().dispatchFetch(
    "https://test/binding/v1/race-detail?source=jra&date=20260816&keibajoCode=A8&raceBango=04",
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({ row: null });
  if (!(observed.body instanceof ReadableStream))
    throw new Error("Expected a streamed SQL request");
  expect(await new Response(observed.body).text()).toMatch(/AND keibajo_code = 'A8'/u);
  expect(JSON.stringify(observed.headers).toLowerCase()).toMatch(
    /authorization.+bearer test-provider-token/u,
  );
  upstream.assertNoPendingInterceptors();
});
it("reads a year calendar across the native binding without administrator credentials", async () => {
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply(
      200,
      JSON.stringify({
        success: true,
        result: {
          rows: [{ kaisai_nen: "2026", kaisai_tsukihi: "0917", jra_count: "0", nar_count: 24 }],
        },
      }),
    );
  const response = await instance().dispatchFetch(
    "https://test/binding/v1/race-calendar?year=2026",
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({
    days: [{ year: "2026", month: "09", day: "17", jraCount: 0, narCount: 24 }],
  });
  upstream.assertNoPendingInterceptors();
});
it("executes the real Viewer calendar adapter through the native named service", async () => {
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply(
      200,
      JSON.stringify({
        success: true,
        result: {
          rows: [{ kaisai_nen: "2026", kaisai_tsukihi: "0917", jra_count: "0", nar_count: 24 }],
        },
      }),
    );
  const response = await instance().dispatchFetch("https://test/calendar-consumer");
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({
    days: [{ year: "2026", month: "09", day: "17", jraCount: 0, narCount: 24 }],
  });
  upstream.assertNoPendingInterceptors();
});
it("keeps the real Viewer calendar adapter fail-closed across the native boundary", async () => {
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply(503, "private provider failure");
  const response = await instance().dispatchFetch("https://test/calendar-consumer");
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({ error: "Catalog race calendar unavailable" });
  upstream.assertNoPendingInterceptors();
});
it("reads jockey names in UTF-8 order through the actual native binding", async () => {
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply(
      200,
      JSON.stringify({
        success: true,
        result: {
          rows: [
            {
              source: "jra",
              kaisai_nen: "2024",
              kaisai_tsukihi: "0229",
              keibajo_code: "05",
              race_bango: "01",
              kyosomei_hondai: "競走",
              kyosomei_fukudai: null,
              grade_code: null,
              kyoso_shubetsu_code: null,
              kyoso_kigo_code: null,
              juryo_shubetsu_code: null,
              kyoso_joken_code: null,
              kyoso_joken_meisho: null,
              kyori: "1600",
              track_code: "11",
              hasso_jikoku: "1000",
              shusso_tosu: "18",
              names: ["𐀀", "Ｚ", "A"],
            },
          ],
        },
      }),
    );
  const response = await instance().dispatchFetch(
    "https://test/binding/v1/race-day-list-with-jockeys?date=20240229",
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({
    races: [
      {
        source: "jra",
        kaisaiNen: "2024",
        kaisaiTsukihi: "0229",
        keibajoCode: "05",
        raceBango: "01",
        kyosomeiHondai: "競走",
        kyosomeiFukudai: null,
        gradeCode: null,
        kyosoShubetsuCode: null,
        kyosoKigoCode: null,
        juryoShubetsuCode: null,
        kyosoJokenCode: null,
        kyosoJokenMeisho: null,
        kyori: "1600",
        trackCode: "11",
        hassoJikoku: "1000",
        shussoTosu: "18",
        jockeyNames: ["A", "Ｚ", "𐀀"],
      },
    ],
  });
  upstream.assertNoPendingInterceptors();
});
it("runs the real jockey-aware Viewer adapter through the native service", async () => {
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply(
      200,
      JSON.stringify({
        success: true,
        result: {
          rows: [
            {
              source: "jra",
              kaisai_nen: "2024",
              kaisai_tsukihi: "0229",
              keibajo_code: "05",
              race_bango: "01",
              kyosomei_hondai: "競走",
              kyosomei_fukudai: null,
              grade_code: null,
              kyoso_shubetsu_code: null,
              kyoso_kigo_code: null,
              juryo_shubetsu_code: null,
              kyoso_joken_code: null,
              kyoso_joken_meisho: null,
              kyori: "1600",
              track_code: "11",
              hasso_jikoku: "1000",
              shusso_tosu: "18",
              names: ["𐀀", "Ｚ", "A"],
            },
          ],
        },
      }),
    );
  const response = await instance().dispatchFetch("https://test/jockey-day-list-consumer");
  expect(response.status).toBe(200);
  expect(await response.json()).toMatchObject({
    races: [{ source: "jra", jockeyNames: ["A", "Ｚ", "𐀀"] }],
  });
  upstream.assertNoPendingInterceptors();
});
it("passes an enriched day exceeding 64 KiB through the actual Viewer binding", async () => {
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply(
      200,
      JSON.stringify({
        success: true,
        result: {
          rows: Array.from({ length: 89 }, (_, index) => ({
            source: "jra",
            kaisai_nen: "2024",
            kaisai_tsukihi: "0229",
            keibajo_code: "05",
            race_bango: String(index + 1).padStart(2, "0"),
            kyosomei_hondai: "競走",
            kyosomei_fukudai: null,
            grade_code: null,
            kyoso_shubetsu_code: null,
            kyoso_kigo_code: null,
            juryo_shubetsu_code: null,
            kyoso_joken_code: null,
            kyoso_joken_meisho: null,
            kyori: "1600",
            track_code: "11",
            hasso_jikoku: "1000",
            shusso_tosu: "18",
            names: ["騎手名".repeat(80)],
          })),
        },
      }),
    );
  const response = await instance().dispatchFetch("https://test/jockey-day-list-consumer");
  expect(response.status).toBe(200);
  const body: string = await response.text();
  expect(new TextEncoder().encode(body).byteLength).toBeGreaterThan(65536);
  const payload: unknown = JSON.parse(body);
  expect(payload).toHaveProperty("races.length", 89);
  expect(payload).toHaveProperty("races.0.raceBango", "01");
  expect(payload).toHaveProperty("races.88.raceBango", "89");
  expect(payload).toHaveProperty("races.88.jockeyNames.0.length", 240);
  upstream.assertNoPendingInterceptors();
});
it("the real jockey-aware Viewer adapter fails closed through the native service", async () => {
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply(503, "private provider failure");
  const response = await instance().dispatchFetch("https://test/jockey-day-list-consumer");
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({ error: "Catalog race day list unavailable" });
  upstream.assertNoPendingInterceptors();
});
it("returns a genuine empty jockey-aware day across the native binding", async () => {
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply(200, JSON.stringify({ success: true, result: { rows: [] } }));
  const response = await instance().dispatchFetch(
    "https://test/binding/v1/race-day-list-with-jockeys?date=20240229",
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ races: [] });
  upstream.assertNoPendingInterceptors();
});
it("does not expose jockey-aware reads publicly or lend audit access to a binding", async () => {
  const publicResponse = await instance().dispatchFetch(
    "https://test/v1/race-day-list-with-jockeys?date=20240229",
  );
  expect(publicResponse.status).toBe(404);
  const auditResponse = await instance().dispatchFetch(
    "https://test/v1/internal/race-day-list-with-jockeys?date=20240229",
  );
  expect(auditResponse.status).toBe(401);
  const bindingAudit = await instance().dispatchFetch(
    "https://test/binding/v1/internal/race-day-list-with-jockeys?date=20240229",
  );
  expect(bindingAudit.status).toBe(404);
});
it("rejects jockey-aware mutations in the native runtime", async () => {
  const response = await instance().dispatchFetch(
    "https://test/binding/v1/race-day-list-with-jockeys?date=20240229",
    { method: "POST", body: "{}" },
  );
  expect(response.status).toBe(405);
});
it("does not convert native jockey query failure into an empty success", async () => {
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply(503, "private provider failure");
  const response = await instance().dispatchFetch(
    "https://test/binding/v1/race-day-list-with-jockeys?date=20240229",
  );
  expect(response.status).toBe(503);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({ error: "Catalog race day list unavailable" });
  upstream.assertNoPendingInterceptors();
});
it("reads day listings through the native private binding", async () => {
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply(200, JSON.stringify({ success: true, result: { rows: [] } }));
  const response = await instance().dispatchFetch(
    "https://test/binding/v1/race-day-list?date=20240229",
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({ races: [] });
  upstream.assertNoPendingInterceptors();
});
it("runs the real day-list Viewer adapter through the private binding", async () => {
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply(
      200,
      JSON.stringify({
        success: true,
        result: {
          rows: [
            {
              source: "nar",
              kaisai_nen: "2024",
              kaisai_tsukihi: "0229",
              keibajo_code: "36",
              race_bango: "01",
              kyosomei_hondai: "競走　 ",
              kyosomei_fukudai: null,
              grade_code: "",
              kyoso_shubetsu_code: "11",
              kyoso_kigo_code: null,
              juryo_shubetsu_code: "1",
              kyoso_joken_code: "005",
              kyoso_joken_meisho: null,
              kyori: "1600",
              track_code: "11",
              hasso_jikoku: "1000",
              shusso_tosu: "18",
            },
          ],
        },
      }),
    );
  const response = await instance().dispatchFetch("https://test/day-list-consumer");
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({
    races: [
      {
        source: "nar",
        kaisaiNen: "2024",
        kaisaiTsukihi: "0229",
        keibajoCode: "36",
        raceBango: "01",
        kyosomeiHondai: "競走　 ",
        kyosomeiFukudai: null,
        gradeCode: "",
        kyosoShubetsuCode: "11",
        kyosoKigoCode: null,
        juryoShubetsuCode: "1",
        jockeyNames: [],
        kyosoJokenCode: "005",
        kyosoJokenMeisho: null,
        kyori: "1600",
        trackCode: "11",
        hassoJikoku: "1000",
        shussoTosu: "18",
      },
    ],
  });
  upstream.assertNoPendingInterceptors();
});
it("propagates native day-list failure through the real Viewer adapter", async () => {
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply(503, "private provider failure");
  const response = await instance().dispatchFetch("https://test/day-list-consumer");
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({ error: "Catalog race day list unavailable" });
  upstream.assertNoPendingInterceptors();
});
it("keeps day listings private and audit authority separate", async () => {
  const publicResponse = await instance().dispatchFetch(
    "https://test/v1/race-day-list?date=20240229",
  );
  expect(publicResponse.status).toBe(404);
  const audit = await instance().dispatchFetch(
    "https://test/v1/internal/race-day-list?date=20240229",
  );
  expect(audit.status).toBe(401);
  const bindingAudit = await instance().dispatchFetch(
    "https://test/binding/v1/internal/race-day-list?date=20240229",
  );
  expect(bindingAudit.status).toBe(404);
});
it("rejects day-list mutations across the native binding", async () => {
  const response = await instance().dispatchFetch(
    "https://test/binding/v1/race-day-list?date=20240229",
    { method: "POST", body: "{}" },
  );
  expect(response.status).toBe(405);
});
it("does not convert native day-list failures into successful empty days", async () => {
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply(503, "private provider failure");
  const response = await instance().dispatchFetch(
    "https://test/binding/v1/race-day-list?date=20240229",
  );
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({ error: "Catalog race day list unavailable" });
  upstream.assertNoPendingInterceptors();
});
it("reads validated year summaries through the native private binding", async () => {
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply(
      200,
      JSON.stringify({
        success: true,
        result: { rows: [{ year: "2026", race_count: "500", day_count: "260" }] },
      }),
    );
  const response = await instance().dispatchFetch("https://test/binding/v1/race-years");
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({
    years: [{ year: "2026", raceCount: 500, dayCount: 260 }],
  });
  upstream.assertNoPendingInterceptors();
});
it("executes the real Viewer years adapter across the native service binding", async () => {
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply(
      200,
      JSON.stringify({
        success: true,
        result: { rows: [{ year: "2026", race_count: "500", day_count: "260" }] },
      }),
    );
  const response = await instance().dispatchFetch("https://test/years-consumer");
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({
    years: [{ year: "2026", raceCount: 500, dayCount: 260 }],
  });
  upstream.assertNoPendingInterceptors();
});
it("keeps the real Viewer years adapter fail-closed across the native service binding", async () => {
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply(503, "private provider failure");
  const response = await instance().dispatchFetch("https://test/years-consumer");
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({ error: "Catalog race years unavailable" });
  upstream.assertNoPendingInterceptors();
});
it("does not expose year summaries publicly or lend audit authority to the binding", async () => {
  const publicResponse = await instance().dispatchFetch("https://test/v1/race-years");
  expect(publicResponse.status).toBe(404);
  const audit = await instance().dispatchFetch("https://test/v1/internal/race-years");
  expect(audit.status).toBe(401);
  const bindingAudit = await instance().dispatchFetch(
    "https://test/binding/v1/internal/race-years",
  );
  expect(bindingAudit.status).toBe(404);
});
it("rejects year mutations across the native binding", async () => {
  const response = await instance().dispatchFetch("https://test/binding/v1/race-years", {
    method: "POST",
    body: "{}",
  });
  expect(response.status).toBe(405);
});
it("keeps native year-summary provider failure distinct from successful absence", async () => {
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply(503, "private provider failure");
  const response = await instance().dispatchFetch("https://test/binding/v1/race-years");
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({ error: "Catalog race years unavailable" });
  upstream.assertNoPendingInterceptors();
});
it("does not expose calendar reads on the default public handler", async () => {
  const response = await instance().dispatchFetch("https://test/v1/race-calendar?year=2026");
  expect(response.status).toBe(404);
  const audit = await instance().dispatchFetch("https://test/v1/internal/race-calendar?year=2026");
  expect(audit.status).toBe(401);
  const bindingAudit = await instance().dispatchFetch(
    "https://test/binding/v1/internal/race-calendar?year=2026",
  );
  expect(bindingAudit.status).toBe(404);
});
it("rejects calendar writes across the native binding", async () => {
  const response = await instance().dispatchFetch(
    "https://test/binding/v1/race-calendar?year=2026",
    { method: "POST", body: "{}" },
  );
  expect(response.status).toBe(405);
});
it("does not turn a failed native calendar query into an empty year", async () => {
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply(503, "private provider failure");
  const response = await instance().dispatchFetch(
    "https://test/binding/v1/race-calendar?year=2026",
  );
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({ error: "Catalog race calendar unavailable" });
  upstream.assertNoPendingInterceptors();
});
it("runs the real Viewer adapter through the named binding and preserves Japanese padding", async () => {
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply(
      200,
      JSON.stringify({
        success: true,
        result: {
          rows: [
            {
              kaisai_nen: "2026",
              kaisai_tsukihi: "0816",
              keibajo_code: "A8",
              race_bango: "04",
              kaisai_kai: null,
              kaisai_nichime: null,
              kyosomei_hondai: "競走　 ",
              kyosomei_fukudai: null,
              kyosomei_kakkonai: null,
              grade_code: " ",
              kyoso_shubetsu_code: null,
              kyoso_kigo_code: null,
              juryo_shubetsu_code: null,
              kyoso_joken_code: null,
              kyoso_joken_meisho: null,
              kyori: "2000",
              track_code: null,
              hasso_jikoku: null,
              toroku_tosu: null,
              shusso_tosu: null,
              tenko_code: null,
              babajotai_code_shiba: null,
              babajotai_code_dirt: null,
            },
          ],
        },
      }),
    );
  const response = await instance().dispatchFetch("https://test/consumer");
  expect(response.status).toBe(200);
  expect(await response.json()).toMatchObject({
    row: {
      source: "jra",
      keibajoCode: "A8",
      kyosomeiHondai: "競走　 ",
      kyori: "2000",
      gradeCode: " ",
      tenkoCode: null,
    },
  });
  upstream.assertNoPendingInterceptors();
});
it("propagates provider failure through the real Viewer adapter without fallback", async () => {
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply(503, "private provider failure");
  const response = await instance().dispatchFetch("https://test/consumer");
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({ error: "Catalog race detail unavailable" });
  upstream.assertNoPendingInterceptors();
});
it("does not expose the named reader through the default public handler", async () => {
  const response = await instance().dispatchFetch(
    "https://test/v1/race-detail?source=jra&date=20260816&keibajoCode=A8&raceBango=04",
  );
  expect(response.status).toBe(404);
  const audit = await instance().dispatchFetch(
    "https://test/v1/internal/race-detail?source=jra&date=20260816&keibajoCode=A8&raceBango=04",
  );
  expect(audit.status).toBe(401);
});
it("cannot use the named binding to access administrator capabilities", async () => {
  const response = await instance().dispatchFetch("https://test/binding/v1/internal/d1/audit");
  expect(response.status).toBe(404);
});
it("rejects writes across the native binding boundary", async () => {
  const response = await instance().dispatchFetch("https://test/binding/v1/race-detail", {
    method: "POST",
    body: "{}",
  });
  expect(response.status).toBe(405);
});
it("preserves provider failure as an uncached error rather than absence", async () => {
  upstream
    .get("https://api.sql.cloudflarestorage.com")
    .intercept({ path: endpoint, method: "POST" })
    .reply(503, "private provider failure");
  const response = await instance().dispatchFetch(
    "https://test/binding/v1/race-detail?source=nar&date=20260914&keibajoCode=83&raceBango=01",
  );
  expect(response.status).toBe(503);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({ error: "Catalog race detail unavailable" });
  upstream.assertNoPendingInterceptors();
});
