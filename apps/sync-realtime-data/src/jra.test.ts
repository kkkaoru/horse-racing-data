// run with: bun run test
import { expect, it, vi } from "vitest";
import {
  fetchJraOddsWithPlaywright,
  fetchJraResultHtmlWithPlaywright,
  isJraScratchStatus,
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

interface BrowserMocks {
  close: ReturnType<typeof vi.fn>;
  newContext: ReturnType<typeof vi.fn>;
  page: Record<string, unknown>;
}

const makeBrowser = (pageOptions: PageMockOptions): BrowserMocks => {
  const page = makePage(pageOptions);
  const close = vi.fn(async () => undefined);
  const newContext = vi.fn(async () => ({
    newPage: vi.fn(async () => page),
    route: vi.fn(async () => undefined),
  }));
  return { close, newContext, page };
};

vi.mock("@cloudflare/playwright", () => ({ launch: vi.fn() }));

const setMockLaunch = async (browser: BrowserMocks) => {
  const mod = await import("@cloudflare/playwright");
  const launch = mod.launch as unknown as ReturnType<typeof vi.fn>;
  launch.mockReset();
  launch.mockResolvedValue({ close: browser.close, newContext: browser.newContext });
};

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
