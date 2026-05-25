// run with: bun run test
import { expect, it, vi } from "vitest";
import {
  fetchJraOddsWithPlaywright,
  fetchJraResultHtmlWithPlaywright,
  isJraScratchStatus,
  parseJraHorseWeights,
  parseJraRaceEntries,
  parseJraRaceResults,
} from "./jra";

interface LocatorMockOptions {
  count?: number;
  filterCount?: number;
  innerHtml?: () => string;
  text?: string | null;
}

const makeLocator = (options: LocatorMockOptions): Record<string, unknown> => {
  const locator: Record<string, unknown> = {};
  locator.count = vi.fn(async () => options.count ?? 0);
  locator.first = vi.fn(() => locator);
  locator.click = vi.fn(async () => undefined);
  locator.innerHTML = vi.fn(async () => options.innerHtml?.() ?? "");
  locator.filter = vi.fn(() => makeLocator({ ...options, count: options.filterCount }));
  locator.textContent = vi.fn(async () => options.text ?? null);
  locator.locator = vi.fn(() => makeLocator({}));
  return locator;
};

interface PageMockOptions {
  content?: string;
  locators?: Record<string, LocatorMockOptions>;
  textLocators?: Record<string, LocatorMockOptions>;
}

let urlCounter = 0;
let innerHtmlCounter = 0;

const makePage = (options: PageMockOptions = {}): Record<string, unknown> => {
  return {
    content: vi.fn(async () => options.content ?? "<html></html>"),
    getByText: vi.fn((text: string, _opt?: unknown) =>
      makeLocator(options.textLocators?.[text] ?? {}),
    ),
    goto: vi.fn(async () => undefined),
    locator: vi.fn((selector: string) => {
      const lo = options.locators?.[selector];
      return makeLocator({
        ...lo,
        innerHtml: () => {
          innerHtmlCounter += 1;
          return `${lo?.innerHtml?.() ?? "<x></x>"}${innerHtmlCounter}`;
        },
      });
    }),
    url: vi.fn(() => {
      urlCounter += 1;
      return `https://x.test/page/${urlCounter}`;
    }),
    waitForSelector: vi.fn(async () => undefined),
    waitForTimeout: vi.fn(async () => undefined),
  };
};

interface RouteRecord {
  handler: (route: {
    abort: ReturnType<typeof vi.fn>;
    continue: ReturnType<typeof vi.fn>;
    request: () => { resourceType: () => string };
  }) => Promise<void>;
  pattern: string;
}

interface BrowserMocks {
  close: ReturnType<typeof vi.fn>;
  newContext: ReturnType<typeof vi.fn>;
  page: Record<string, unknown>;
  routes: RouteRecord[];
}

const makeBrowser = (pageOptions: PageMockOptions): BrowserMocks => {
  const page = makePage(pageOptions);
  const close = vi.fn(async () => undefined);
  const routes: RouteRecord[] = [];
  const route = vi.fn(async (pattern: string, handler: RouteRecord["handler"]) => {
    routes.push({ handler, pattern });
  });
  const newContext = vi.fn(async () => ({
    newPage: vi.fn(async () => page),
    route,
  }));
  return { close, newContext, page, routes };
};

vi.mock("@cloudflare/playwright", () => ({ launch: vi.fn() }));

const setMockLaunch = async (browser: BrowserMocks) => {
  const mod = await import("@cloudflare/playwright");
  const launch = mod.launch as unknown as ReturnType<typeof vi.fn>;
  launch.mockReset();
  launch.mockResolvedValue({ close: browser.close, newContext: browser.newContext });
};

it("buildJraEntryUrlFromRace handles missing kaisai_kai/kaisai_nichime by falling back to null", async () => {
  const { buildJraEntryUrlFromRace } = await import("./jra");
  const result = buildJraEntryUrlFromRace({
    hasso_jikoku: "1500",
    kaisai_nen: "2026",
    kaisai_tsukihi: "0512",
    keibajo_code: "08",
    kyosomei_hondai: "T",
    race_bango: "1",
  });
  expect(result).toBeNull();
});

it("buildJraResultUrlFromRaceSource falls back to null when kaisaiKai/Nichime missing", async () => {
  const { buildJraResultUrlFromRaceSource } = await import("./jra");
  const result = buildJraResultUrlFromRaceSource({
    babaCode: "08",
    debaUrl: "u",
    kaisaiKai: null,
    kaisaiNen: "2026",
    kaisaiNichime: null,
    kaisaiTsukihi: "0512",
    keibajoCode: "08",
    lastOddsFetchAt: null,
    lastWeightFetchAt: null,
    oddsLinks: {},
    raceBango: "01",
    raceKey: "jra:2026:0512:08:01",
    raceName: null,
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
    source: "jra",
  });
  expect(result).toBeNull();
});

it("buildJraEntryUrlFromRace returns a URL when kaisai_kai/nichime are present", async () => {
  const { buildJraEntryUrlFromRace } = await import("./jra");
  const result = buildJraEntryUrlFromRace({
    hasso_jikoku: "1500",
    kaisai_kai: "02",
    kaisai_nen: "2026",
    kaisai_nichime: "06",
    kaisai_tsukihi: "0512",
    keibajo_code: "08",
    kyosomei_hondai: "T",
    race_bango: "1",
  });
  expect(result).not.toBeNull();
});

it("isJraScratchStatus returns true for known scratch statuses", () => {
  expect(isJraScratchStatus("除外")).toBe(true);
  expect(isJraScratchStatus("取消")).toBe(true);
});

it("isJraScratchStatus returns false for normal status text", () => {
  expect(isJraScratchStatus("出走")).toBe(false);
  expect(isJraScratchStatus("")).toBe(false);
});

it("fetchJraOddsWithPlaywright throws when browserBinding is undefined", async () => {
  await expect(fetchJraOddsWithPlaywright(undefined, "https://x.test/race")).rejects.toThrow(
    "JRA_BROWSER binding is required",
  );
});

it("fetchJraResultHtmlWithPlaywright throws when browserBinding is undefined", async () => {
  await expect(
    fetchJraResultHtmlWithPlaywright(undefined, "https://x.test/result"),
  ).rejects.toThrow("JRA_BROWSER binding is required");
});

it("fetchJraResultHtmlWithPlaywright returns page.content for the result page", async () => {
  urlCounter = 0;
  innerHtmlCounter = 0;
  const browser = makeBrowser({ content: "<html>result</html>" });
  await setMockLaunch(browser);
  const html = await fetchJraResultHtmlWithPlaywright({} as never, "https://x.test/result");
  expect(html).toBe("<html>result</html>");
  expect(browser.close).toHaveBeenCalledTimes(1);
});

it("fetchJraOddsWithPlaywright navigates to odds, iterates labels, returns entryHtml and latest", async () => {
  urlCounter = 0;
  innerHtmlCounter = 0;
  const browser = makeBrowser({
    content: "<html>entry</html>",
    locators: {
      "#odds_list": { innerHtml: () => "<table></table>" },
      "#race_related_link a": { count: 1 },
    },
  });
  await setMockLaunch(browser);
  const result = await fetchJraOddsWithPlaywright({} as never, "https://x.test/race");
  expect(result.entryHtml).toBe("<html>entry</html>");
  expect(Object.keys(result.latest).sort()).toStrictEqual([
    "3renpuku",
    "3rentan",
    "fukusho",
    "tansho",
    "umaren",
    "umatan",
    "wakuren",
    "wide",
  ]);
  expect(browser.close).toHaveBeenCalledTimes(1);
});

it("fetchJraOddsWithPlaywright continues iterating when a label click throws", async () => {
  urlCounter = 0;
  innerHtmlCounter = 0;
  const browser = makeBrowser({
    content: "<html>entry</html>",
    locators: {
      "#odds_list": { innerHtml: () => "<table></table>" },
      "#race_related_link a": { count: 1 },
    },
  });
  browser.page.getByText = vi.fn(() => ({
    click: vi.fn(async () => {
      throw new Error("boom");
    }),
    count: vi.fn(async () => 1),
    filter: vi.fn(),
    first: vi.fn(),
    innerHTML: vi.fn(async () => ""),
    locator: vi.fn(),
    textContent: vi.fn(async () => null),
  }));
  await setMockLaunch(browser);
  const result = await fetchJraOddsWithPlaywright({} as never, "https://x.test/race");
  expect(result.entryHtml).toBe("<html>entry</html>");
  expect(browser.close).toHaveBeenCalledTimes(1);
});

it("fetchJraOddsWithPlaywright's resource blocker aborts blocked types and continues others", async () => {
  urlCounter = 0;
  innerHtmlCounter = 0;
  const browser = makeBrowser({
    content: "<html>entry</html>",
    locators: {
      "#odds_list": { innerHtml: () => "<table></table>" },
      "#race_related_link a": { count: 1 },
    },
  });
  await setMockLaunch(browser);
  await fetchJraOddsWithPlaywright({} as never, "https://x.test/race");
  expect(browser.routes).toHaveLength(1);
  const handler = browser.routes[0]?.handler;
  const fontAbort = vi.fn(async () => undefined);
  const fontContinue = vi.fn(async () => undefined);
  await handler?.({
    abort: fontAbort,
    continue: fontContinue,
    request: () => ({ resourceType: () => "font" }),
  });
  expect(fontAbort).toHaveBeenCalledTimes(1);
  const docAbort = vi.fn(async () => undefined);
  const docContinue = vi.fn(async () => undefined);
  await handler?.({
    abort: docAbort,
    continue: docContinue,
    request: () => ({ resourceType: () => "document" }),
  });
  expect(docContinue).toHaveBeenCalledTimes(1);
});

it("fetchJraOddsWithPlaywright falls back to first related link when an exact オッズ link is missing", async () => {
  urlCounter = 0;
  innerHtmlCounter = 0;
  const browser = makeBrowser({
    content: "<html>entry</html>",
    locators: {
      "#odds_list": { innerHtml: () => "<table></table>" },
      "#race_related_link a": { count: 0 },
    },
  });
  // override locator to make filter().first() count() returns 0 (no exact オッズ)
  // but raw "#race_related_link a" first() returns count 1.
  browser.page.locator = vi.fn((selector: string) => {
    if (selector === "#race_related_link a") {
      return {
        click: vi.fn(async () => undefined),
        count: vi.fn(async () => 1),
        filter: vi.fn(() => ({
          click: vi.fn(async () => undefined),
          count: vi.fn(async () => 0),
          first: vi.fn(() => ({
            click: vi.fn(async () => undefined),
            count: vi.fn(async () => 0),
          })),
        })),
        first: vi.fn(() => ({
          click: vi.fn(async () => undefined),
          count: vi.fn(async () => 1),
        })),
        innerHTML: vi.fn(async () => ""),
        locator: vi.fn(),
        textContent: vi.fn(async () => null),
      };
    }
    if (selector === "#odds_list") {
      return makeLocator({ innerHtml: () => "<table></table>" });
    }
    return makeLocator({});
  });
  await setMockLaunch(browser);
  const result = await fetchJraOddsWithPlaywright({} as never, "https://x.test/race");
  expect(result.entryHtml).toBe("<html>entry</html>");
});

it("fetchJraOddsWithPlaywright falls back to getByText link when no related link found", async () => {
  urlCounter = 0;
  innerHtmlCounter = 0;
  const browser = makeBrowser({
    content: "<html>entry</html>",
    locators: {
      "#odds_list": { innerHtml: () => "<table></table>" },
    },
  });
  browser.page.locator = vi.fn((selector: string) => {
    if (selector === "#race_related_link a") {
      return {
        click: vi.fn(async () => undefined),
        count: vi.fn(async () => 0),
        filter: vi.fn(() => ({
          first: vi.fn(() => ({
            click: vi.fn(async () => undefined),
            count: vi.fn(async () => 0),
          })),
        })),
        first: vi.fn(() => ({
          click: vi.fn(async () => undefined),
          count: vi.fn(async () => 0),
        })),
        innerHTML: vi.fn(async () => ""),
        locator: vi.fn(),
        textContent: vi.fn(async () => null),
      };
    }
    if (selector === "#odds_list") {
      return makeLocator({ innerHtml: () => "<table></table>" });
    }
    return makeLocator({});
  });
  browser.page.getByText = vi.fn(() => ({
    click: vi.fn(async () => undefined),
    count: vi.fn(async () => 1),
    filter: vi.fn(),
    first: vi.fn(() => ({
      click: vi.fn(async () => undefined),
      count: vi.fn(async () => 1),
    })),
    innerHTML: vi.fn(async () => ""),
    locator: vi.fn(),
    textContent: vi.fn(async () => null),
  }));
  await setMockLaunch(browser);
  const result = await fetchJraOddsWithPlaywright({} as never, "https://x.test/race");
  expect(result.entryHtml).toBe("<html>entry</html>");
});

it("fetchJraOddsWithPlaywright throws when no odds link is found anywhere", async () => {
  urlCounter = 0;
  innerHtmlCounter = 0;
  const browser = makeBrowser({
    content: "<html>entry</html>",
    locators: {
      "#odds_list": { innerHtml: () => "<table></table>" },
    },
  });
  browser.page.locator = vi.fn(() => ({
    click: vi.fn(async () => undefined),
    count: vi.fn(async () => 0),
    filter: vi.fn(() => ({
      first: vi.fn(() => ({
        click: vi.fn(async () => undefined),
        count: vi.fn(async () => 0),
      })),
    })),
    first: vi.fn(() => ({
      click: vi.fn(async () => undefined),
      count: vi.fn(async () => 0),
    })),
    innerHTML: vi.fn(async () => ""),
    locator: vi.fn(),
    textContent: vi.fn(async () => null),
  }));
  browser.page.getByText = vi.fn(() => ({
    click: vi.fn(async () => undefined),
    count: vi.fn(async () => 0),
    filter: vi.fn(),
    first: vi.fn(() => ({
      click: vi.fn(async () => undefined),
      count: vi.fn(async () => 0),
    })),
    innerHTML: vi.fn(async () => ""),
    locator: vi.fn(),
    textContent: vi.fn(async () => null),
  }));
  await setMockLaunch(browser);
  await expect(fetchJraOddsWithPlaywright({} as never, "https://x.test/race")).rejects.toThrow(
    "JRA odds link was not found",
  );
});


it("parseJraRaceEntries skips rows without a horse number cell", () => {
  expect(parseJraRaceEntries(`<table><tr><td>no num</td></tr></table>`)).toStrictEqual([]);
});

it("parseJraRaceEntries returns null horseName/jockey when anchor not present and parses horse number", () => {
  const result = parseJraRaceEntries(
    `<table><tr><td class="num">5</td><td class="horse">x</td><td class="jockey">j</td></tr></table>`,
  );
  expect(result[0]?.horseNumber).toBe("5");
  expect(result[0]?.status).toBeNull();
});

it("parseJraHorseWeights returns empty when no weight cell present", () => {
  expect(
    parseJraHorseWeights(
      `<table><tr><td class="num">1</td><td class="horse">x</td></tr></table>`,
    ),
  ).toStrictEqual([]);
});

it("parseJraHorseWeights extracts weight + change when td.weight cell present", () => {
  const result = parseJraHorseWeights(
    `<table><tr><td class="num">3</td><td class="horse">x</td><td class="weight">510 (+2)</td></tr></table>`,
  );
  expect(result[0]?.weight).toBe(510);
  expect(result[0]?.changeSign).toBe("+");
  expect(result[0]?.changeAmount).toBe(2);
});

it("parseJraRaceResults skips rows without place cell", () => {
  expect(
    parseJraRaceResults(`<table><tr><td class="num">1</td></tr></table>`),
  ).toStrictEqual([]);
});

it("parseJraRaceResults returns rows with parsed finishPosition + horseNumber", () => {
  const result = parseJraRaceResults(
    `<table><tr><td class="place">2</td><td class="num">3</td><td class="time">1:23.4</td></tr></table>`,
  );
  expect(result).toStrictEqual([
    { finishPosition: "02", horseName: null, horseNumber: "3", time: "1:23.4" },
  ]);
});

it("parseJraRaceResults skips rows whose place cell matches an excluded status", () => {
  const result = parseJraRaceResults(
    `<table><tr><td class="place">取消</td><td class="num">2</td></tr></table>`,
  );
  expect(result).toStrictEqual([]);
});

it("parseJraRaceEntries returns 騎手変更 status when row contains the marker", () => {
  const result = parseJraRaceEntries(
    `<table>
      <tr>
        <td class="num">3</td>
        <td class="horse"><a>馬名</a></td>
        <td class="jockey"><a>騎手</a></td>
        <td class="info">騎手変更</td>
      </tr>
    </table>`,
  );
  expect(result[0]?.status).toBe("騎手変更");
});

it("parseJraRaceEntries returns extracted horseName when wrapped in name div + anchor", () => {
  const result = parseJraRaceEntries(
    `<table>
      <tr>
        <td class="num">4</td>
        <td class="horseName"><div class="name"><a>サンプル馬</a></div></td>
        <td class="jockey"><p class="jockey"><a>乗り役</a></p></td>
      </tr>
    </table>`,
  );
  expect(result[0]?.horseName).toBe("サンプル馬");
  expect(result[0]?.jockeyName).toBe("乗り役");
});

it("parseJraRaceEntries clears scratch status when weight match is present (sanitize)", () => {
  const result = parseJraRaceEntries(
    `<table>
      <tr>
        <td class="num">5</td>
        <td class="horse"><a>馬</a></td>
        <td class="status">除外</td>
        <td class="weight">500 (+2)</td>
      </tr>
    </table>`,
  );
  expect(result[0]?.status).toBeNull();
});

it("parseJraRaceResults extracts horseName from anchor inside class=horse cell", () => {
  const result = parseJraRaceResults(
    `<table>
      <tr>
        <td class="place">3</td>
        <td class="num">4</td>
        <td class="horse"><a>勝負馬</a></td>
        <td class="time">1:34.2</td>
      </tr>
    </table>`,
  );
  expect(result[0]?.horseName).toBe("勝負馬");
});

