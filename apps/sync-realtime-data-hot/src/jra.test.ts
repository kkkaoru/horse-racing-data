// run with: bun run test
import { expect, it, vi } from "vitest";
import {
  fetchJraOddsWithPlaywright,
  fetchJraResultHtmlWithPlaywright,
  isJraScratchStatus,
  parseJraHorseWeights,
  parseJraOddsByType,
  parseJraRaceEntries,
  parseJraRaceResultExcludedHorseNumbers,
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
    parseJraHorseWeights(`<table><tr><td class="num">1</td><td class="horse">x</td></tr></table>`),
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
  expect(parseJraRaceResults(`<table><tr><td class="num">1</td></tr></table>`)).toStrictEqual([]);
});

it("parseJraRaceResults returns rows with parsed finishPosition + horseNumber", () => {
  const result = parseJraRaceResults(
    `<table><tr><td class="place">2</td><td class="num">3</td><td class="time">1:23.4</td></tr></table>`,
  );
  expect(result).toStrictEqual([
    { finishPosition: "02", horseName: null, horseNumber: "3", time: "1:23.4" },
  ]);
});

it("parseJraHorseWeights returns null changeAmount/changeSign for weight without parens", () => {
  const result = parseJraHorseWeights(
    `<table><tr><td class="num">4</td><td class="horse"><a>馬</a></td><td class="weight">500</td></tr></table>`,
  );
  expect(result[0]?.changeAmount).toBeNull();
  expect(result[0]?.changeSign).toBeNull();
});

it("parseJraHorseWeights returns null horseName when no anchor present in horse cell", () => {
  const result = parseJraHorseWeights(
    `<table><tr><td class="num">5</td><td>raw horse</td><td class="weight">510 (+1)</td></tr></table>`,
  );
  expect(result[0]?.horseName).toBeNull();
});

it("parseJraRaceResults returns null finishPosition when place cell is not numeric", () => {
  expect(
    parseJraRaceResults(`<table><tr><td class="place">x</td><td class="num">3</td></tr></table>`),
  ).toStrictEqual([]);
});

it("parseJraRaceResults returns null time when no time cell present", () => {
  const result = parseJraRaceResults(
    `<table><tr><td class="place">2</td><td class="num">3</td></tr></table>`,
  );
  expect(result[0]?.time).toBeNull();
});

it("parseJraRaceResults returns null horseName when no anchor + non-empty horse cell", () => {
  const result = parseJraRaceResults(
    `<table><tr><td class="place">2</td><td class="num">3</td><td class="horse"> </td></tr></table>`,
  );
  expect(result[0]?.horseName).toBeNull();
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

it("parseJraRaceResultExcludedHorseNumbers skips excluded rows with an invalid horse number", () => {
  expect(
    parseJraRaceResultExcludedHorseNumbers(
      `<table><tr><td class="place">除外</td><td class="num">99</td></tr></table>`,
    ),
  ).toStrictEqual([]);
});

it("parseJraRaceEntries skips entry cells with empty stripped text in status detection", () => {
  const result = parseJraRaceEntries(
    `<table>
      <tr>
        <td class="num">7</td>
        <td class="horseName"><a>馬</a></td>
        <td class="jockey"><a>騎</a></td>
        <td>   </td>
      </tr>
    </table>`,
  );
  expect(result[0]?.status).toBeNull();
});

it("parseJraHorseWeights skips rows whose num cell is invalid", () => {
  expect(
    parseJraHorseWeights(
      `<table><tr><td class="num">99</td><td class="horse"><a>x</a></td><td class="weight">500 (+1)</td></tr></table>`,
    ),
  ).toStrictEqual([]);
});

it("parseJraOddsByType tansho skips cancelled rows and rows lacking odds/num", () => {
  const result = parseJraOddsByType(
    "tansho",
    `
      <table class="tanpuku">
        <tr><td class="odds_tan cancel"><strong>9.9</strong></td><td class="num">3</td></tr>
        <tr><td class="num"></td><td></td><td></td><td class="odds_tan"><strong>4.6</strong></td></tr>
        <tr><td class="num">2</td><td></td><td></td><td class="odds_tan"><strong>4.6</strong></td></tr>
      </table>
    `,
  );
  expect(result).toStrictEqual([{ combination: "2", odds: 4.6, rank: 1 }]);
});

it("parseJraOddsByType fukusho skips rows missing horseNumber, fukusho cell, or numeric values", () => {
  const result = parseJraOddsByType(
    "fukusho",
    `
      <table class="tanpuku">
        <tr><td class="num">99</td><td class="odds_fuku">1.5 - 2.5</td></tr>
        <tr><td class="num">1</td></tr>
        <tr><td class="num">2</td><td class="odds_fuku">no numbers</td></tr>
        <tr><td class="num">3</td><td class="odds_fuku">3.0 - 5.0</td></tr>
      </table>
    `,
  );
  expect(result).toStrictEqual([
    { averageOdds: 4, combination: "3", maxOdds: 5, minOdds: 3, rank: 1 },
  ]);
});

it("parseJraOddsByType umaren returns [] when caption is missing", () => {
  expect(
    parseJraOddsByType(
      "umaren",
      `
        <table class="umaren">
          <tr><th>2</th><td>5.5</td></tr>
        </table>
      `,
    ),
  ).toStrictEqual([]);
});

it("parseJraOddsByType umaren swaps left/right when caption number is greater than target", () => {
  const result = parseJraOddsByType(
    "umaren",
    `
      <table class="umaren">
        <caption>9</caption>
        <tr><th>1</th><td>12.5</td></tr>
      </table>
    `,
  );
  expect(result[0]?.combination).toBe("1-9");
});

it("parseJraOddsByType umaren skips rows missing target or odds text", () => {
  const result = parseJraOddsByType(
    "umaren",
    `
      <table class="umaren">
        <caption>1</caption>
        <tr><td>3.5</td></tr>
        <tr><th>2</th></tr>
        <tr><th>3</th><td>3.5</td></tr>
      </table>
    `,
  );
  expect(result).toStrictEqual([{ combination: "1-3", odds: 3.5, rank: 1 }]);
});

it("parseJraOddsByType wide returns [] when caption number is invalid", () => {
  expect(
    parseJraOddsByType(
      "wide",
      `
        <table class="wide">
          <caption>99</caption>
          <tr><th>1</th><td><span class="min">1.0</span>-<span class="max">2.0</span></td></tr>
        </table>
      `,
    ),
  ).toStrictEqual([]);
});

it("parseJraOddsByType wide skips rows lacking target or min/max or with invalid target", () => {
  const result = parseJraOddsByType(
    "wide",
    `
      <table class="wide">
        <caption>3</caption>
        <tr><th>99</th><td><span class="min">1.0</span>-<span class="max">2.0</span></td></tr>
        <tr><th>1</th></tr>
        <tr><th>5</th><td><span class="min">2.5</span>-<span class="max">3.5</span></td></tr>
      </table>
    `,
  );
  expect(result[0]?.combination).toBe("3-5");
});

it("parseJraOddsByType wide swaps left/right when caption is greater than target", () => {
  const result = parseJraOddsByType(
    "wide",
    `
      <table class="wide">
        <caption>9</caption>
        <tr><th>1</th><td><span class="min">5.0</span>-<span class="max">7.0</span></td></tr>
      </table>
    `,
  );
  expect(result[0]?.combination).toBe("1-9");
});

it("parseJraOddsByType 3renpuku returns [] when fuku3 table caption is missing", () => {
  expect(
    parseJraOddsByType(
      "3renpuku",
      `
        <table class="fuku3">
          <tr><th>3</th><td>15.5</td></tr>
        </table>
      `,
    ),
  ).toStrictEqual([]);
});

it("parseJraOddsByType 3renpuku skips rows lacking target/odds or with invalid horse number", () => {
  const result = parseJraOddsByType(
    "3renpuku",
    `
      <table class="fuku3">
        <caption>1-2</caption>
        <tr><th>99</th><td>15.5</td></tr>
        <tr><th>3</th></tr>
        <tr><td>15.5</td></tr>
        <tr><th>4</th><td>20.5</td></tr>
      </table>
    `,
  );
  expect(result[0]?.combination).toBe("1-2-4");
});

it("parseJraOddsByType 3rentan skips tan3_unit sections without first/second/third", () => {
  const result = parseJraOddsByType(
    "3rentan",
    `
      <div class="tan3_unit">
        <table><tr><th>9</th><td>10.0</td></tr></table>
      </div>
      <div class="tan3_unit">
        <span class="inner"><span class="num">1</span></span>
        <div class="cap"><span>2着</span></div>
        <div class="num"></div>
        <table><tr><th>9</th><td>10.0</td></tr></table>
      </div>
      <div class="tan3_unit">
        <span class="inner"><span class="num">1</span></span>
        <div class="cap"><span>2着</span></div>
        <div class="num">2</div>
        <table>
          <tr><td>10.0</td></tr>
          <tr><th>3</th></tr>
          <tr><th>99</th><td>15.0</td></tr>
          <tr><th>4</th><td>22.0</td></tr>
        </table>
      </div>
    `,
  );
  expect(result[0]?.combination).toBe("1-2-4");
});

it("parseJraOddsByType 3renpuku sorts horse numbers ascending when combination is unordered", () => {
  const result = parseJraOddsByType(
    "3renpuku",
    `
      <table class="fuku3">
        <caption>5-3</caption>
        <tr><th>1</th><td>10.5</td></tr>
      </table>
    `,
  );
  expect(result[0]?.combination).toBe("1-3-5");
});

it("fetchJraOddsWithPlaywright breaks out of the wait loop when probe.url changes", async () => {
  urlCounter = 100;
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
});

it("fetchJraOddsWithPlaywright waits and times out when both url and html stay stable", async () => {
  urlCounter = 0;
  innerHtmlCounter = 0;
  const browser = makeBrowser({
    content: "<html>entry</html>",
    locators: {
      "#race_related_link a": { count: 1 },
    },
  });
  browser.page.url = vi.fn(() => "https://x.test/race");
  const stableHtml = vi.fn(async () => "<table>stable</table>");
  browser.page.locator = vi.fn((selector: string) => {
    if (selector === "#odds_list") {
      return {
        click: vi.fn(async () => undefined),
        count: vi.fn(async () => 1),
        filter: vi.fn(() => ({
          first: vi.fn(() => ({
            click: vi.fn(async () => undefined),
            count: vi.fn(async () => 0),
          })),
        })),
        first: vi.fn(() => ({
          click: vi.fn(async () => undefined),
          count: vi.fn(async () => 1),
        })),
        innerHTML: stableHtml,
        locator: vi.fn(),
        textContent: vi.fn(async () => null),
      };
    }
    if (selector === "#race_related_link a") {
      return {
        click: vi.fn(async () => undefined),
        count: vi.fn(async () => 1),
        filter: vi.fn(() => ({
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
  });
  let elapsedTicks = 0;
  browser.page.waitForTimeout = vi.fn(async () => {
    elapsedTicks += 1;
    if (elapsedTicks >= 2) {
      vi.setSystemTime(new Date(Date.now() + 10_000));
    }
  });
  vi.useFakeTimers();
  vi.setSystemTime(new Date(0));
  await setMockLaunch(browser);
  const result = await fetchJraOddsWithPlaywright({} as never, "https://x.test/race");
  vi.useRealTimers();
  expect(result.entryHtml).toBe("<html>entry</html>");
});

it("fetchJraOddsWithPlaywright breaks the wait loop via html change when url is stable", async () => {
  urlCounter = 0;
  innerHtmlCounter = 0;
  const browser = makeBrowser({
    content: "<html>entry</html>",
    locators: {
      "#odds_list": { innerHtml: () => "<table></table>" },
      "#race_related_link a": { count: 1 },
    },
  });
  browser.page.url = vi.fn(() => "https://x.test/race");
  await setMockLaunch(browser);
  const result = await fetchJraOddsWithPlaywright({} as never, "https://x.test/race");
  expect(result.entryHtml).toBe("<html>entry</html>");
});

it("fetchJraOddsWithPlaywright clicks the exact オッズ filtered link when count > 0", async () => {
  urlCounter = 0;
  innerHtmlCounter = 0;
  const browser = makeBrowser({
    content: "<html>entry</html>",
    locators: {
      "#odds_list": { innerHtml: () => "<table></table>" },
      "#race_related_link a": { count: 1, filterCount: 1 },
    },
  });
  await setMockLaunch(browser);
  const result = await fetchJraOddsWithPlaywright({} as never, "https://x.test/race");
  expect(result.entryHtml).toBe("<html>entry</html>");
});

it("fetchJraOddsWithPlaywright makes a single Chromium attempt and surfaces its error", async () => {
  urlCounter = 0;
  innerHtmlCounter = 0;
  const mod = await import("@cloudflare/playwright");
  const launch = mod.launch as unknown as ReturnType<typeof vi.fn>;
  launch.mockReset();
  launch.mockRejectedValueOnce(new Error("first failure"));
  await expect(fetchJraOddsWithPlaywright({} as never, "https://x.test/race")).rejects.toThrow(
    "first failure",
  );
  expect(launch).toHaveBeenCalledTimes(1);
});

// Trimmed from live JRA HTML (2026-10-24 東京11R): deba `#race_related_link`
// odds link, the odds page tab nav, and the first `#odds_list` rows.
const HTTP_DEBA_HTML = `<div id="race_related_link"><ul>
<li class="odds"><a href="#" class="btn-def btn-sm blue btn-block" onclick="return doAction('/JRADB/accessO.html', 'pw151ouS305202604081120261024Z/CE');"><i class="fa fa-chevron-circle-right" aria-hidden="true"></i>オッズ</a></li></ul></div>`;
const HTTP_TAB_NAV = `<ul class="nav pills mt10">
<li><a href="#" onclick="return doAction('/JRADB/accessO.html', 'pw154ouS305202604081020261024Z/11');">馬連</a></li>
<li class="current"><a href="#" onclick="return doAction('/JRADB/accessO.html', 'pw151ouS305202604081120261024Z/CE');">単勝・複勝</a></li>
<li><a href="#" onclick="return doAction('/JRADB/accessO.html', 'pw153ouS305202604081120261024Z/D6');">枠連</a></li>
<li><a href="#" onclick="return doAction('/JRADB/accessO.html', 'pw154ouS305202604081120261024Z/5A');">馬連</a></li>
<li><a href="#" onclick="return doAction('/JRADB/accessO.html', 'pw155ouS305202604081120261024Z/DE');">ワイド</a></li>
<li><a href="#" onclick="return doAction('/JRADB/accessO.html', 'pw156ouS305202604081120261024Z/62');">馬単</a></li>
<li><a href="#" onclick="return doAction('/JRADB/accessO.html', 'pw157ouS305202604081120261024Z99/48');">3連複</a></li>
<li><a href="#" onclick="return doAction('/JRADB/accessO.html', 'pw158ouS305202604081120261024Z/6A');">3連単</a></li>
</ul>`;
const HTTP_WIN_HTML = `${HTTP_TAB_NAV}<div id="odds_list" class="mt25">
<table class="basic narrow-xy tanpuku"><tbody>
<tr><td class="waku" rowspan="1"><img src="/JRADB/img/waku/1.png" alt="枠1白" /></td><td class="num">1</td>
<td class="horse"><a href="/JRADB/accessU.html?CNAME=pw01dud002024100935/16">タプティ</a></td>
<td class="odds_tan">30.8</td><td class="odds_fuku"><span class="inner"><span class="min">3.7</span><span class="cap">-</span><span class="max">5.1</span></span></td></tr>
<tr><td class="waku" rowspan="1"><img src="/JRADB/img/waku/2.png" alt="枠2黒" /></td><td class="num">2</td>
<td class="horse"><a href="/JRADB/accessU.html?CNAME=pw01dud002024103955/03">マリアンデール</a></td>
<td class="odds_tan">15.1</td><td class="odds_fuku"><span class="inner"><span class="min">3.2</span><span class="cap">-</span><span class="max">4.4</span></span></td></tr>
</tbody></table></div>`;
const HTTP_UMAREN_HTML = `<div id="odds_list" class="mt25"><ul class="umaren_list mt15"><li>
<table class="basic narrow-xy umaren"><caption>1</caption><tbody>
<tr><th scope="row">2</th><td>200.5</td></tr>
</tbody></table></li></ul></div>`;

interface HttpFetchCall {
  body: string | null;
  method: string;
  url: string;
}

const stubJraHttp = (pages: Record<string, string | Uint8Array>, calls: HttpFetchCall[] = []) => {
  const fetchMock = vi.fn(async (url: string, init: RequestInit) => {
    const body = init.body instanceof URLSearchParams ? init.body.toString() : null;
    calls.push({ body, method: String(init.method), url });
    const key = body ?? url;
    const page = pages[key];
    return page === undefined ? new Response("missing", { status: 404 }) : new Response(page);
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
};

it("fetchJraOddsWithHttp walks deba -> win page -> tab pages and parses every bet type", async () => {
  const calls: HttpFetchCall[] = [];
  stubJraHttp(
    {
      "https://www.jra.go.jp/JRADB/accessD.html?CNAME=pw01dde": HTTP_DEBA_HTML,
      "cname=pw151ouS305202604081120261024Z%2FCE": HTTP_WIN_HTML,
      "cname=pw153ouS305202604081120261024Z%2FD6": '<div id="odds_list"></div>',
      "cname=pw154ouS305202604081120261024Z%2F5A": HTTP_UMAREN_HTML,
      "cname=pw155ouS305202604081120261024Z%2FDE": '<div id="odds_list"></div>',
      "cname=pw156ouS305202604081120261024Z%2F62": '<div id="odds_list"></div>',
      "cname=pw157ouS305202604081120261024Z99%2F48": '<div id="odds_list"></div>',
      "cname=pw158ouS305202604081120261024Z%2F6A": '<div id="odds_list"></div>',
    },
    calls,
  );
  const { fetchJraOddsWithHttp } = await import("./jra");
  const result = await fetchJraOddsWithHttp(
    "https://www.jra.go.jp/JRADB/accessD.html?CNAME=pw01dde",
  );
  expect(result.missingTypes).toStrictEqual([]);
  expect(result.latest.tansho).toStrictEqual([
    { combination: "2", odds: 15.1, rank: 1 },
    { combination: "1", odds: 30.8, rank: 2 },
  ]);
  expect(result.latest.umaren).toStrictEqual([{ combination: "1-2", odds: 200.5, rank: 1 }]);
  expect(result.latest.wakuren).toStrictEqual([]);
  expect(calls.length).toBe(8);
  expect(calls[0]).toStrictEqual({
    body: null,
    method: "GET",
    url: "https://www.jra.go.jp/JRADB/accessD.html?CNAME=pw01dde",
  });
  expect(calls[1]).toStrictEqual({
    body: "cname=pw151ouS305202604081120261024Z%2FCE",
    method: "POST",
    url: "https://www.jra.go.jp/JRADB/accessO.html",
  });
  vi.unstubAllGlobals();
});

it("fetchJraOddsWithHttp decodes Shift_JIS pages so Japanese tab labels match", async () => {
  // "馬連" in Shift_JIS is 94 6E 98 41; the rest of the page is ASCII.
  const sjisWinHtml = new Uint8Array([
    ...new TextEncoder().encode(
      `<a onclick="return doAction('/JRADB/accessO.html', 'pw154ouS305202604081120261024Z/5A');">`,
    ),
    0x94,
    0x6e,
    0x98,
    0x41,
    ...new TextEncoder().encode('</a><div id="odds_list"></div>'),
  ]);
  stubJraHttp({
    "https://x.test/deba": HTTP_DEBA_HTML,
    "cname=pw151ouS305202604081120261024Z%2FCE": sjisWinHtml,
    "cname=pw154ouS305202604081120261024Z%2F5A": HTTP_UMAREN_HTML,
  });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const { fetchJraOddsWithHttp } = await import("./jra");
  const result = await fetchJraOddsWithHttp("https://x.test/deba");
  expect(result.latest.umaren).toStrictEqual([{ combination: "1-2", odds: 200.5, rank: 1 }]);
  expect(result.missingTypes).toStrictEqual(["wakuren", "wide", "umatan", "3renpuku", "3rentan"]);
  warnSpy.mockRestore();
  vi.unstubAllGlobals();
});

it("fetchJraOddsWithHttp rejects when the deba page returns a non-OK status", async () => {
  stubJraHttp({});
  const { fetchJraOddsWithHttp } = await import("./jra");
  await expect(fetchJraOddsWithHttp("https://x.test/deba")).rejects.toThrow(
    "JRA HTTP 404 for https://x.test/deba",
  );
  vi.unstubAllGlobals();
});

it("fetchJraOddsWithHttp rejects when the deba page has no odds link", async () => {
  stubJraHttp({ "https://x.test/deba": "<html></html>" });
  const { fetchJraOddsWithHttp } = await import("./jra");
  await expect(fetchJraOddsWithHttp("https://x.test/deba")).rejects.toThrow(
    "JRA odds cname was not found on race entry page.",
  );
  vi.unstubAllGlobals();
});

it("fetchJraOddsWithHttp rejects when the win odds page is a JRA error page (no #odds_list)", async () => {
  stubJraHttp({
    "https://x.test/deba": HTTP_DEBA_HTML,
    "cname=pw151ouS305202604081120261024Z%2FCE": "<html>error013</html>",
  });
  const { fetchJraOddsWithHttp } = await import("./jra");
  await expect(fetchJraOddsWithHttp("https://x.test/deba")).rejects.toThrow(
    "JRA odds page pw151ouS305202604081120261024Z/CE has no #odds_list.",
  );
  vi.unstubAllGlobals();
});

it("fetchJraOddsWithHttp rejects when every tab page fails", async () => {
  stubJraHttp({
    "https://x.test/deba": HTTP_DEBA_HTML,
    "cname=pw151ouS305202604081120261024Z%2FCE": HTTP_WIN_HTML,
  });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const { fetchJraOddsWithHttp } = await import("./jra");
  await expect(fetchJraOddsWithHttp("https://x.test/deba")).rejects.toThrow(
    "All JRA odds tab pages failed over HTTP.",
  );
  warnSpy.mockRestore();
  vi.unstubAllGlobals();
});

it("fetchJraOdds serves from HTTP without launching a browser", async () => {
  stubJraHttp({
    "https://x.test/deba": HTTP_DEBA_HTML,
    "cname=pw151ouS305202604081120261024Z%2FCE": HTTP_WIN_HTML,
    "cname=pw154ouS305202604081120261024Z%2F5A": HTTP_UMAREN_HTML,
  });
  const mod = await import("@cloudflare/playwright");
  const launch = mod.launch as unknown as ReturnType<typeof vi.fn>;
  launch.mockReset();
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const { fetchJraOdds } = await import("./jra");
  const result = await fetchJraOdds({} as never, "https://x.test/deba");
  expect(result.latest.umaren).toStrictEqual([{ combination: "1-2", odds: 200.5, rank: 1 }]);
  expect(launch).not.toHaveBeenCalled();
  expect(logSpy).toHaveBeenCalledWith(
    "JRA odds served by engine=fetch missing=5 url=https://x.test/deba",
  );
  warnSpy.mockRestore();
  logSpy.mockRestore();
  vi.unstubAllGlobals();
});

it("fetchJraOdds falls back to one Playwright attempt when HTTP fails", async () => {
  urlCounter = 0;
  innerHtmlCounter = 0;
  stubJraHttp({});
  const browser = makeBrowser({
    content: "<html>entry</html>",
    locators: {
      "#odds_list": { innerHtml: () => "<table></table>" },
      "#race_related_link a": { count: 1 },
    },
  });
  await setMockLaunch(browser);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const { fetchJraOdds } = await import("./jra");
  const result = await fetchJraOdds({} as never, "https://x.test/deba");
  expect(result.entryHtml).toBe("<html>entry</html>");
  expect(browser.close).toHaveBeenCalledTimes(1);
  expect(logSpy).toHaveBeenCalledWith(
    "JRA odds served by engine=playwright missing=0 url=https://x.test/deba",
  );
  warnSpy.mockRestore();
  logSpy.mockRestore();
  vi.unstubAllGlobals();
});

it("waitForOddsListWithFallback falls back to broad selectors when #odds_list times out (F4 selector fallback)", async () => {
  // Drive `clickAndWaitForOdds` through `fetchJraOddsWithPlaywright`: the
  // first `waitForSelector('#odds_list', ...)` rejects, then the broad
  // selector probes succeed and the run continues without throwing.
  urlCounter = 0;
  innerHtmlCounter = 0;
  const browser = makeBrowser({
    content: "<html>entry</html>",
    locators: {
      "#odds_list": { innerHtml: () => "<table></table>" },
      "#race_related_link a": { count: 1 },
    },
  });
  const calls: string[] = [];
  const waitForSelectorMock = vi.fn(async (selector: string) => {
    calls.push(selector);
    if (selector === "#odds_list") {
      throw new Error("primary selector timeout");
    }
    return undefined;
  });
  browser.page.waitForSelector = waitForSelectorMock;
  await setMockLaunch(browser);
  const result = await fetchJraOddsWithPlaywright({} as never, "https://x.test/race");
  expect(result.entryHtml).toBe("<html>entry</html>");
  expect(calls).toContain("#odds_list");
  expect(calls).toContain("[id*='odds']");
  expect(calls).toContain("[class*='odds']");
});

it("waitForOddsListWithFallback rejects when both primary and broad fallback selectors time out", async () => {
  // Both attempts (retry + retry) hit primary + fallback rejections, so
  // the whole call eventually surfaces the timeout error.
  urlCounter = 0;
  innerHtmlCounter = 0;
  const browser = makeBrowser({
    content: "<html>entry</html>",
    locators: {
      "#odds_list": { innerHtml: () => "<table></table>" },
      "#race_related_link a": { count: 1 },
    },
  });
  browser.page.waitForSelector = vi.fn(async (selector: string) => {
    void selector;
    throw new Error("all selectors timed out");
  });
  await setMockLaunch(browser);
  const consoleSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await expect(fetchJraOddsWithPlaywright({} as never, "https://x.test/race")).rejects.toThrow();
  consoleSpy.mockRestore();
});

it("fetchJraOddsWithPlaywright returns empty missingTypes when every odds tab succeeds (K1-A)", async () => {
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
  expect(result.missingTypes).toStrictEqual([]);
});

it("fetchJraOddsWithPlaywright reports every non-tansho tab in missingTypes when each click throws (K1-A)", async () => {
  // tansho is read directly from `#odds_list` after the odds-page navigation
  // and never goes through `getByText(label).click`, so it stays in `latest`
  // while every other tab gets pushed into `missingTypes`.
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
  expect(result.missingTypes).toStrictEqual([
    "fukusho",
    "wakuren",
    "umaren",
    "wide",
    "umatan",
    "3renpuku",
    "3rentan",
  ]);
});
