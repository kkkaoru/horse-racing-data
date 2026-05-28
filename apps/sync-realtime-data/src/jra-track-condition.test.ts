// run with: bun run test
import { expect, it, vi } from "vitest";
import { fetchJraTrackConditionWithPlaywright } from "./jra-track-condition";

interface LocatorMockOptions {
  attribute?: string | null;
  count?: number;
  locators?: Record<string, LocatorMockOptions>;
  nth?: Record<number, LocatorMockOptions>;
  text?: string | null;
  textRejects?: boolean;
}

interface PageMockOptions {
  goto?: ReturnType<typeof vi.fn>;
  locators?: Record<string, LocatorMockOptions>;
  waitForSelector?: ReturnType<typeof vi.fn>;
}

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
  context: { newPage: ReturnType<typeof vi.fn>; route: ReturnType<typeof vi.fn> };
  page: unknown;
  routes: RouteRecord[];
}

const makeLocator = (options: LocatorMockOptions): Record<string, unknown> => {
  const locator: Record<string, unknown> = {};
  locator.count = vi.fn(async () => options.count ?? 0);
  locator.first = vi.fn(() => locator);
  locator.nth = vi.fn((index: number) => makeLocator(options.nth?.[index] ?? {}));
  locator.textContent = vi.fn(async () => {
    if (options.textRejects) throw new Error("textContent rejected");
    return options.text ?? null;
  });
  locator.getAttribute = vi.fn(async (_name: string) => options.attribute ?? null);
  locator.locator = vi.fn((selector: string) => makeLocator(options.locators?.[selector] ?? {}));
  return locator;
};

const makePage = (options: PageMockOptions = {}): unknown => {
  const locators = options.locators ?? {};
  return {
    goto: options.goto ?? vi.fn(async () => undefined),
    locator: vi.fn((selector: string) => makeLocator(locators[selector] ?? {})),
    waitForSelector: options.waitForSelector ?? vi.fn(async () => undefined),
  };
};

const makeBrowserMocks = (pageOptions: PageMockOptions): BrowserMocks => {
  const routes: RouteRecord[] = [];
  const close = vi.fn(async () => undefined);
  const route = vi.fn(async (pattern: string, handler: RouteRecord["handler"]) => {
    routes.push({ handler, pattern });
  });
  const page = makePage(pageOptions);
  const newPage = vi.fn(async () => page);
  return { close, context: { newPage, route }, page, routes };
};

vi.mock("@cloudflare/playwright", () => ({ launch: vi.fn() }));

const setMockLaunch = async (browser: {
  close: ReturnType<typeof vi.fn>;
  newContext: () => unknown;
}) => {
  const mod = await import("@cloudflare/playwright");
  const launch = mod.launch as unknown as ReturnType<typeof vi.fn>;
  launch.mockReset();
  launch.mockResolvedValue(browser);
};

it("fetchJraTrackConditionWithPlaywright throws when browserBinding is undefined", async () => {
  await expect(
    fetchJraTrackConditionWithPlaywright(undefined, {
      kaisaiNen: "2026",
      keibajoCode: "08",
    }),
  ).rejects.toThrow("JRA_BROWSER binding is required to fetch JRA track condition.");
});

it("fetchJraTrackConditionWithPlaywright throws on unsupported racecourse code", async () => {
  await expect(
    fetchJraTrackConditionWithPlaywright({} as never, {
      kaisaiNen: "2026",
      keibajoCode: "99",
    }),
  ).rejects.toThrow("unsupported JRA racecourse: 99");
});

it("fetchJraTrackConditionWithPlaywright throws when racecourse link is missing", async () => {
  const mocks = makeBrowserMocks({
    locators: {
      "#contentsBody .kaisai_tab a, .kaisai_tab a": {
        count: 1,
        nth: {
          0: { attribute: "/keiba/baba/sapporo.html", text: "札幌競馬場" },
        },
      },
    },
  });
  await setMockLaunch({
    close: mocks.close,
    newContext: async () => ({
      newPage: mocks.context.newPage,
      route: mocks.context.route,
    }),
  } as never);
  await expect(
    fetchJraTrackConditionWithPlaywright({} as never, {
      kaisaiNen: "2026",
      keibajoCode: "08",
    }),
  ).rejects.toThrow("JRA track condition link was not found: 京都");
  expect(mocks.close).toHaveBeenCalledTimes(1);
});

const buildUnitOption = (header: string, content: string): LocatorMockOptions => ({
  locators: {
    ".content p": { text: content },
    ".head": { text: header },
  },
});

it("fetchJraTrackConditionWithPlaywright extracts a full track condition payload", async () => {
  const dataListUnits: LocatorMockOptions = {
    count: 5,
    nth: {
      0: buildUnitOption("コース使用", "Aコース"),
      1: buildUnitOption("クッション値", "9.0"),
      2: buildUnitOption("芝", "良"),
      3: buildUnitOption("ダート", "重"),
      4: buildUnitOption("馬場の状態", "Bパート"),
    },
  };
  const mocks = makeBrowserMocks({
    locators: {
      "#contentsBody .kaisai_tab a, .kaisai_tab a": {
        count: 2,
        nth: {
          0: { attribute: "https://www.jra.go.jp/keiba/baba/sapporo.html", text: "札幌競馬場" },
          1: { attribute: "/keiba/baba/kyoto.html", text: "京都競馬場" },
        },
      },
      "#cushion_list option[selected]": { text: "1月3日 9時30分" },
      "#dirt_line .c4": { text: "12.5" },
      "#dirt_line .gm": { text: "11.0" },
      "#moist_list option[selected]": { text: "1月3日 10時00分" },
      "#turf_line .c4": { text: "10.0" },
      "#turf_line .gm": { text: "9.5" },
      ".condition .block_header .content .main h3 .time": { text: "1月3日" },
      ".data_list_unit": dataListUnits,
      ".turf_condition .content p": { text: "" },
      ".turf_length table tbody tr td": {
        nth: {
          1: { text: "9.0cm" },
          2: { text: "8.0cm" },
        },
      },
      ".weather strong": { text: "晴" },
    },
  });
  await setMockLaunch({
    close: mocks.close,
    newContext: async () => ({
      newPage: mocks.context.newPage,
      route: mocks.context.route,
    }),
  } as never);
  const result = await fetchJraTrackConditionWithPlaywright({} as never, {
    kaisaiNen: "2026",
    keibajoCode: "08",
  });
  expect(result.turf.condition).toBe("良");
  expect(result.turf.courseLayout).toBe("A");
  expect(result.turf.cushionValue).toBe("9.0");
  expect(result.turf.cushionMeasuredAt).toBe("2026-01-03T09:30:00+09:00");
  expect(result.turf.going).toBe("Bパート");
  expect(result.turf.height.japaneseZoysiaGrass).toBe("9.0cm");
  expect(result.turf.height.perennialRyegrass).toBe("8.0cm");
  expect(result.turf.measurementDate).toBe("2026-01-03");
  expect(result.turf.moisture.finalBend).toBe("10.0");
  expect(result.turf.moisture.finalFurlong).toBe("9.5");
  expect(result.turf.moisture.measuredAt).toBe("2026-01-03T10:00:00+09:00");
  expect(result.dirt.condition).toBe("重");
  expect(result.dirt.measurementDate).toBe("2026-01-03");
  expect(result.dirt.moisture.finalBend).toBe("12.5");
  expect(result.dirt.moisture.finalFurlong).toBe("11.0");
  expect(result.weather).toBe("晴");
  expect(typeof result.fetchedAt).toBe("string");
  expect(mocks.close).toHaveBeenCalledTimes(1);
});

it("fetchJraTrackConditionWithPlaywright defaults conditions to 良 and falls back to header weather", async () => {
  const dataListUnits: LocatorMockOptions = {
    count: 1,
    nth: {
      0: buildUnitOption("天候", "曇"),
    },
  };
  const mocks = makeBrowserMocks({
    locators: {
      "#contentsBody .kaisai_tab a, .kaisai_tab a": {
        count: 1,
        nth: {
          0: { attribute: "https://www.jra.go.jp/keiba/baba/tokyo.html", text: "東京競馬場" },
        },
      },
      ".data_list_unit": dataListUnits,
    },
  });
  await setMockLaunch({
    close: mocks.close,
    newContext: async () => ({
      newPage: mocks.context.newPage,
      route: mocks.context.route,
    }),
  } as never);
  const result = await fetchJraTrackConditionWithPlaywright({} as never, {
    kaisaiNen: "2026",
    keibajoCode: "05",
  });
  expect(result.turf.condition).toBe("良");
  expect(result.dirt.condition).toBe("良");
  expect(result.weather).toBe("曇");
  expect(result.turf.courseLayout).toBeNull();
  expect(result.turf.cushionValue).toBeNull();
  expect(result.turf.cushionMeasuredAt).toBeNull();
  expect(result.turf.going).toBeNull();
  expect(result.turf.height.japaneseZoysiaGrass).toBeNull();
  expect(result.turf.height.perennialRyegrass).toBeNull();
  expect(result.turf.measurementDate).toBeNull();
  expect(result.dirt.measurementDate).toBeNull();
  expect(result.dirt.moisture.measuredAt).toBeNull();
  expect(mocks.close).toHaveBeenCalledTimes(1);
});

it("fetchJraTrackConditionWithPlaywright skips anchors with null text or null href", async () => {
  const mocks = makeBrowserMocks({
    locators: {
      "#contentsBody .kaisai_tab a, .kaisai_tab a": {
        count: 3,
        nth: {
          0: { attribute: null, text: null },
          1: { attribute: null, text: "札幌競馬場" },
          2: { attribute: "https://www.jra.go.jp/keiba/baba/sapporo.html", text: "札幌競馬場" },
        },
      },
      ".data_list_unit": { count: 0 },
    },
  });
  await setMockLaunch({
    close: mocks.close,
    newContext: async () => ({
      newPage: mocks.context.newPage,
      route: mocks.context.route,
    }),
  } as never);
  const result = await fetchJraTrackConditionWithPlaywright({} as never, {
    kaisaiNen: "2026",
    keibajoCode: "01",
  });
  expect(result.turf.condition).toBe("良");
  expect(mocks.close).toHaveBeenCalledTimes(1);
});

it("fetchJraTrackConditionWithPlaywright tolerates rejected textContent in data_list and turf cells", async () => {
  const dataListUnits: LocatorMockOptions = {
    count: 1,
    nth: {
      0: {
        locators: {
          ".content p": { textRejects: true },
          ".head": { textRejects: true },
        },
      },
    },
  };
  const mocks = makeBrowserMocks({
    locators: {
      "#contentsBody .kaisai_tab a, .kaisai_tab a": {
        count: 1,
        nth: {
          0: { attribute: "https://www.jra.go.jp/keiba/baba/sapporo.html", text: "札幌競馬場" },
        },
      },
      ".condition .block_header .content .main h3 .time": { textRejects: true },
      ".data_list_unit": dataListUnits,
      ".turf_condition .content p": { textRejects: true },
      ".turf_length table tbody tr td": {
        nth: {
          1: { textRejects: true },
          2: { textRejects: true },
        },
      },
      ".weather strong": { textRejects: true },
    },
  });
  await setMockLaunch({
    close: mocks.close,
    newContext: async () => ({
      newPage: mocks.context.newPage,
      route: mocks.context.route,
    }),
  } as never);
  const result = await fetchJraTrackConditionWithPlaywright({} as never, {
    kaisaiNen: "2026",
    keibajoCode: "01",
  });
  expect(result.turf.condition).toBe("良");
  expect(result.dirt.condition).toBe("良");
  expect(result.turf.height.japaneseZoysiaGrass).toBeNull();
  expect(result.turf.height.perennialRyegrass).toBeNull();
  expect(result.weather).toBeNull();
  expect(mocks.close).toHaveBeenCalledTimes(1);
});

it("fetchJraTrackConditionWithPlaywright tolerates rejected textContent on matched header content cells", async () => {
  const dataListUnits: LocatorMockOptions = {
    count: 1,
    nth: {
      0: {
        locators: {
          ".content p": { textRejects: true },
          ".head": { text: "ダート" },
        },
      },
    },
  };
  const mocks = makeBrowserMocks({
    locators: {
      "#contentsBody .kaisai_tab a, .kaisai_tab a": {
        count: 1,
        nth: {
          0: { attribute: "https://www.jra.go.jp/keiba/baba/sapporo.html", text: "札幌競馬場" },
        },
      },
      ".data_list_unit": dataListUnits,
    },
  });
  await setMockLaunch({
    close: mocks.close,
    newContext: async () => ({
      newPage: mocks.context.newPage,
      route: mocks.context.route,
    }),
  } as never);
  const result = await fetchJraTrackConditionWithPlaywright({} as never, {
    kaisaiNen: "2026",
    keibajoCode: "01",
  });
  expect(result.dirt.condition).toBe("良");
  expect(mocks.close).toHaveBeenCalledTimes(1);
});

it("setupResourceBlocker aborts blocked resource types and continues others", async () => {
  const mocks = makeBrowserMocks({
    locators: {
      "#contentsBody .kaisai_tab a, .kaisai_tab a": {
        count: 1,
        nth: {
          0: { attribute: "https://www.jra.go.jp/keiba/baba/sapporo.html", text: "札幌競馬場" },
        },
      },
      ".data_list_unit": { count: 0 },
    },
  });
  await setMockLaunch({
    close: mocks.close,
    newContext: async () => ({
      newPage: mocks.context.newPage,
      route: mocks.context.route,
    }),
  } as never);
  await fetchJraTrackConditionWithPlaywright({} as never, {
    kaisaiNen: "2026",
    keibajoCode: "01",
  });
  expect(mocks.routes).toHaveLength(1);
  const handler = mocks.routes[0]?.handler;
  const fontAbort = vi.fn(async () => undefined);
  const fontContinue = vi.fn(async () => undefined);
  await handler?.({
    abort: fontAbort,
    continue: fontContinue,
    request: () => ({ resourceType: () => "font" }),
  });
  expect(fontAbort).toHaveBeenCalledTimes(1);
  expect(fontContinue).not.toHaveBeenCalled();
  const docAbort = vi.fn(async () => undefined);
  const docContinue = vi.fn(async () => undefined);
  await handler?.({
    abort: docAbort,
    continue: docContinue,
    request: () => ({ resourceType: () => "document" }),
  });
  expect(docContinue).toHaveBeenCalledTimes(1);
  expect(docAbort).not.toHaveBeenCalled();
});
