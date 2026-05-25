// run with: bun run test
import { afterEach, expect, it, vi } from "vitest";
import {
  buildPremiumUrl,
  discoverPremiumRaceLinks,
  extractPremiumSourceRaceId,
  fetchPremiumHtml,
  fetchPremiumHtmlAttempts,
  getPremiumRaceConfig,
  hasPremiumRaceFetchConfig,
  isPremiumRaceDataTarget,
  renderPremiumTemplate,
  summarizePremiumStableCommentHtml,
} from "./premium-race";

afterEach(() => {
  vi.restoreAllMocks();
});

it("isPremiumRaceDataTarget returns true for jra", () => {
  expect(isPremiumRaceDataTarget({ keibajoCode: "08", source: "jra" })).toBe(true);
});

it("isPremiumRaceDataTarget returns true for nar non-Ban-ei", () => {
  expect(isPremiumRaceDataTarget({ keibajoCode: "55", source: "nar" })).toBe(true);
});

it("isPremiumRaceDataTarget returns false for nar Ban-ei keibajo", () => {
  expect(isPremiumRaceDataTarget({ keibajoCode: "83", source: "nar" })).toBe(false);
});

it("getPremiumRaceConfig reads every env var when all are provided", () => {
  expect(
    getPremiumRaceConfig({
      PREMIUM_RACE_COMMENT_PATH_TEMPLATE: "/c",
      PREMIUM_RACE_COOKIE: "k",
      PREMIUM_RACE_DATA_TOP_PATH_TEMPLATE: "/d",
      PREMIUM_RACE_ENTRY_LINK_PATTERN: "p",
      PREMIUM_RACE_NAR_ORIGIN: "https://nar.example",
      PREMIUM_RACE_NAR_TOP_PATH_TEMPLATE: "/n",
      PREMIUM_RACE_ORIGIN: "https://x.test",
      PREMIUM_RACE_PADDOCK_PATH_TEMPLATE: "/p",
      PREMIUM_RACE_PROXY_BEARER: "tk",
      PREMIUM_RACE_PROXY_URL: "https://proxy.example",
      PREMIUM_RACE_PROXY_USER_ID: "u",
      PREMIUM_RACE_RESPONSE_CHARSET: "utf-8",
      PREMIUM_RACE_SOURCE_ID_QUERY_KEY: "rid",
      PREMIUM_RACE_TOP_PATH_TEMPLATE: "/t",
      PREMIUM_RACE_WORK_PATH_TEMPLATE: "/w",
    }),
  ).toStrictEqual({
    commentPathTemplate: "/c",
    cookie: "k",
    dataTopPathTemplate: "/d",
    entryLinkPattern: "p",
    narOrigin: "https://nar.example",
    narTopPathTemplate: "/n",
    origin: "https://x.test",
    paddockPathTemplate: "/p",
    proxyBearer: "tk",
    proxyUrl: "https://proxy.example",
    proxyUserId: "u",
    responseCharset: "utf-8",
    sourceIdQueryKey: "rid",
    topPathTemplate: "/t",
    workPathTemplate: "/w",
  });
});

it("getPremiumRaceConfig fills missing env values with null and the default NAR origin", () => {
  expect(getPremiumRaceConfig({})).toStrictEqual({
    commentPathTemplate: null,
    cookie: null,
    dataTopPathTemplate: null,
    entryLinkPattern: null,
    narOrigin: "https://nar.netkeiba.com",
    narTopPathTemplate: null,
    origin: null,
    paddockPathTemplate: null,
    proxyBearer: null,
    proxyUrl: null,
    proxyUserId: null,
    responseCharset: null,
    sourceIdQueryKey: "race_id",
    topPathTemplate: null,
    workPathTemplate: null,
  });
});

it("getPremiumRaceConfig propagates configured env variables", () => {
  const config = getPremiumRaceConfig({
    PREMIUM_RACE_NAR_ORIGIN: "https://configured.example",
    PREMIUM_RACE_ORIGIN: "https://primary.example",
    PREMIUM_RACE_SOURCE_ID_QUERY_KEY: "id",
  });
  expect(config.narOrigin).toBe("https://configured.example");
  expect(config.origin).toBe("https://primary.example");
  expect(config.sourceIdQueryKey).toBe("id");
});

it("hasPremiumRaceFetchConfig is true only when origin is set", () => {
  expect(hasPremiumRaceFetchConfig(getPremiumRaceConfig({}))).toBe(false);
  expect(hasPremiumRaceFetchConfig(getPremiumRaceConfig({ PREMIUM_RACE_ORIGIN: "https://x.test" }))).toBe(
    true,
  );
});

it("renderPremiumTemplate substitutes braces with values", () => {
  expect(renderPremiumTemplate("/{a}/{b}", { a: "x", b: "y" })).toBe("/x/y");
});

it("renderPremiumTemplate replaces repeated placeholders", () => {
  expect(renderPremiumTemplate("{a}-{a}", { a: "x" })).toBe("x-x");
});

it("buildPremiumUrl returns null when template is null", () => {
  expect(buildPremiumUrl(getPremiumRaceConfig({ PREMIUM_RACE_ORIGIN: "https://x.test" }), null, {})).toBeNull();
});

it("buildPremiumUrl returns null when origin is missing", () => {
  expect(buildPremiumUrl(getPremiumRaceConfig({}), "/race/{id}", { id: "1" })).toBeNull();
});

it("buildPremiumUrl uses narOrigin when source is nar", () => {
  expect(
    buildPremiumUrl(
      getPremiumRaceConfig({
        PREMIUM_RACE_NAR_ORIGIN: "https://nar.example",
        PREMIUM_RACE_ORIGIN: "https://primary.example",
      }),
      "/race/{id}",
      { id: "1" },
      { source: "nar" },
    ),
  ).toBe("https://nar.example/race/1");
});

it("buildPremiumUrl uses default origin when source is jra (or omitted)", () => {
  expect(
    buildPremiumUrl(
      getPremiumRaceConfig({
        PREMIUM_RACE_ORIGIN: "https://primary.example",
      }),
      "/race/{id}",
      { id: "1" },
    ),
  ).toBe("https://primary.example/race/1");
});

it("extractPremiumSourceRaceId returns the query value", () => {
  expect(extractPremiumSourceRaceId("/race?race_id=12345", "race_id")).toBe("12345");
});

it("extractPremiumSourceRaceId returns null when key absent", () => {
  expect(extractPremiumSourceRaceId("/race?other=1", "race_id")).toBeNull();
});

it("extractPremiumSourceRaceId returns null on malformed url", () => {
  expect(extractPremiumSourceRaceId(":::not-a-url", "race_id")).toBeNull();
});

it("fetchPremiumHtmlAttempts throws when config has no origin", async () => {
  await expect(
    fetchPremiumHtmlAttempts(getPremiumRaceConfig({}), "https://x.test/race"),
  ).rejects.toThrow("premium race fetch config is incomplete");
});

it("fetchPremiumHtmlAttempts returns direct attempt body on success", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(
    new Response("<html>data</html>", {
      headers: { "content-type": "text/html; charset=utf-8" },
      status: 200,
    }),
  );
  const config = getPremiumRaceConfig({ PREMIUM_RACE_ORIGIN: "https://x.test" });
  const attempts = await fetchPremiumHtmlAttempts(config, "https://x.test/race");
  expect(attempts.length).toBe(1);
  expect(attempts[0]!.mode).toBe("direct");
  expect(attempts[0]!.html).toBe("<html>data</html>");
});

it("fetchPremiumHtmlAttempts collects errors and throws when all attempts fail", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("err", { status: 500 }));
  const config = getPremiumRaceConfig({ PREMIUM_RACE_ORIGIN: "https://x.test" });
  await expect(fetchPremiumHtmlAttempts(config, "https://x.test/race")).rejects.toThrow(
    "premium race fetch failed",
  );
});

it("fetchPremiumHtmlAttempts uses proxy when proxyUrl/userId/bearer configured", async () => {
  const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(
    async (): Promise<Response> =>
      new Response("<html>via-proxy</html>", {
        headers: { "content-type": "text/html; charset=utf-8" },
        status: 200,
      }),
  );
  const config = getPremiumRaceConfig({
    PREMIUM_RACE_ORIGIN: "https://x.test",
    PREMIUM_RACE_PROXY_BEARER: "tk",
    PREMIUM_RACE_PROXY_URL: "https://proxy.example",
    PREMIUM_RACE_PROXY_USER_ID: "u1",
  });
  await fetchPremiumHtmlAttempts(config, "https://x.test/race");
  const proxyUrl = fetchSpy.mock.calls[0]![0] as string;
  expect(proxyUrl.startsWith("https://proxy.example")).toBe(true);
});

it("fetchPremiumHtmlAttempts uses cookie attempt when cookie is configured", async () => {
  const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(
    async (): Promise<Response> =>
      new Response("<html>via-cookie</html>", {
        headers: { "content-type": "text/html; charset=utf-8" },
        status: 200,
      }),
  );
  const config = getPremiumRaceConfig({
    PREMIUM_RACE_COOKIE: "session=abc",
    PREMIUM_RACE_ORIGIN: "https://x.test",
  });
  await fetchPremiumHtmlAttempts(config, "https://x.test/race");
  expect(fetchSpy.mock.calls[0]![1]).toBeDefined();
});

it("discoverPremiumRaceLinks extracts unique sourceRaceIds from anchor hrefs in HTML", () => {
  const config = getPremiumRaceConfig({
    PREMIUM_RACE_ORIGIN: "https://x.test",
  });
  const html =
    '<a href="/race?race_id=12345">A</a><a href="/race?race_id=12345">A-dup</a><a href="/race?race_id=67890">B</a>';
  const result = discoverPremiumRaceLinks(html, config);
  expect(result.length).toBe(2);
  expect(result[0]!.sourceRaceId).toBe("12345");
  expect(result[1]!.sourceRaceId).toBe("67890");
});

it("summarizePremiumStableCommentHtml returns cellRowCount and a text sample", () => {
  const html =
    '<table><tr><td>名前</td><td>所属</td></tr><tr><td>サンプル</td><td>東京</td></tr></table>';
  const summary = summarizePremiumStableCommentHtml(html);
  expect(summary.cellRowCount).toBe(2);
  expect(summary.textSample.length).toBeGreaterThan(0);
  expect(summary.samples.length).toBe(2);
});

it("summarizePremiumStableCommentHtml returns empty samples when no rows present", () => {
  const summary = summarizePremiumStableCommentHtml("<div>plain text</div>");
  expect(summary.cellRowCount).toBe(0);
  expect(summary.samples).toStrictEqual([]);
});

it("fetchPremiumHtml prefers the authenticated attempt over earlier ones", async () => {
  vi.spyOn(globalThis, "fetch").mockImplementation(
    async (input: RequestInfo | URL): Promise<Response> => {
      const url = String(input);
      if (url.startsWith("https://proxy.example")) {
        return new Response("<html>need login</html>", {
          headers: { "content-type": "text/html; charset=utf-8" },
          status: 200,
        });
      }
      return new Response('<html><div class="Icon_Account">user</div></html>', {
        headers: { "content-type": "text/html; charset=utf-8" },
        status: 200,
      });
    },
  );
  const config = getPremiumRaceConfig({
    PREMIUM_RACE_COOKIE: "s",
    PREMIUM_RACE_ORIGIN: "https://x.test",
    PREMIUM_RACE_PROXY_BEARER: "tk",
    PREMIUM_RACE_PROXY_URL: "https://proxy.example",
    PREMIUM_RACE_PROXY_USER_ID: "u1",
  });
  const html = await fetchPremiumHtml(config, "https://x.test/race");
  expect(html).toContain("Icon_Account");
});

it("fetchPremiumHtml returns the first attempt body when no attempt is authenticated", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(
    new Response("<html>guest</html>", {
      headers: { "content-type": "text/html; charset=utf-8" },
      status: 200,
    }),
  );
  const config = getPremiumRaceConfig({ PREMIUM_RACE_ORIGIN: "https://x.test" });
  const html = await fetchPremiumHtml(config, "https://x.test/race");
  expect(html).toBe("<html>guest</html>");
});

it("fetchPremiumHtmlAttempts records error from a rejected fetch and proceeds to next attempt", async () => {
  let call = 0;
  vi.spyOn(globalThis, "fetch").mockImplementation(async (): Promise<Response> => {
    call += 1;
    if (call === 1) {
      throw new Error("network down");
    }
    return new Response("<html>ok</html>", {
      headers: { "content-type": "text/html; charset=utf-8" },
      status: 200,
    });
  });
  const config = getPremiumRaceConfig({
    PREMIUM_RACE_COOKIE: "s",
    PREMIUM_RACE_ORIGIN: "https://x.test",
  });
  const attempts = await fetchPremiumHtmlAttempts(config, "https://x.test/race");
  expect(attempts).toHaveLength(1);
  expect(attempts[0]?.mode).toBe("direct");
});

it("fetchPremiumHtmlAttempts decodes EUC-JP body when responseCharset is set", async () => {
  const eucJp = new Uint8Array([0xc5, 0xec, 0xb5, 0xfe]);
  vi.spyOn(globalThis, "fetch").mockResolvedValue(
    new Response(eucJp.buffer, {
      headers: { "content-type": "text/html" },
      status: 200,
    }),
  );
  const config = getPremiumRaceConfig({
    PREMIUM_RACE_ORIGIN: "https://x.test",
    PREMIUM_RACE_RESPONSE_CHARSET: "euc-jp",
  });
  const attempts = await fetchPremiumHtmlAttempts(config, "https://x.test/race");
  expect(attempts[0]?.html).toBe("東京");
});
