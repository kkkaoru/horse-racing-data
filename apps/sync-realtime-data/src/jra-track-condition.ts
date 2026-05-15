import type { Browser, BrowserContext, BrowserWorker, Page, Route } from "@cloudflare/playwright";

import type { TrackCondition } from "./types";

const JRA_TRACK_CONDITION_URL = "https://www.jra.go.jp/keiba/baba/index.html";
const JRA_TRACK_CONDITION_BASE_URL = "https://www.jra.go.jp/keiba/baba/";
const NAVIGATION_TIMEOUT_MS = 20_000;
const SELECTOR_TIMEOUT_MS = 10_000;
const BLOCKED_RESOURCE_TYPES = new Set(["font", "image", "media", "stylesheet"]);

const JRA_KEIBAJO_NAMES: Record<string, string> = {
  "01": "札幌",
  "02": "函館",
  "03": "福島",
  "04": "新潟",
  "05": "東京",
  "06": "中山",
  "07": "中京",
  "08": "京都",
  "09": "阪神",
  "10": "小倉",
};

interface RacecourseLink {
  name: string;
  url: string;
}

interface ExtractedTrackCondition {
  courseUsage: string;
  cushionMeasurementText: string;
  cushionValue: string;
  dirtCondition: string;
  dirtMoistureCorner: string;
  dirtMoistureGoal: string;
  measurementDateText: string;
  moistureMeasurementText: string;
  turfCondition: string;
  turfGoing: string;
  turfHeightForeign: string;
  turfHeightNative: string;
  turfMoistureCorner: string;
  turfMoistureGoal: string;
  weather: string;
}

const setupResourceBlocker = async (context: BrowserContext): Promise<void> => {
  await context.route("**/*", async (route: Route) => {
    if (BLOCKED_RESOURCE_TYPES.has(route.request().resourceType())) {
      await route.abort();
      return;
    }
    await route.continue();
  });
};

const normalizeText = (value: string | null | undefined): string | null => {
  const cleaned = value?.replace(/\s+/gu, " ").trim() ?? "";
  return cleaned.length > 0 ? cleaned : null;
};

const normalizeMeasurementValue = (value: string): string | null => {
  const cleaned = value
    .replace(/[％%]/gu, "")
    .replace(/\s+/gu, " ")
    .trim();
  return cleaned.length > 0 ? cleaned : null;
};

const pad = (value: string): string => value.padStart(2, "0");

const convertJapaneseDateToIso = (text: string, year: string): string | null => {
  const match = text.match(/(\d{1,2})月(\d{1,2})日/u);
  if (!match?.[1] || !match[2]) {
    return null;
  }
  return `${year}-${pad(match[1])}-${pad(match[2])}`;
};

const convertJapaneseDateTimeToIso = (text: string, year: string): string | null => {
  const date = text.match(/(\d{1,2})月(\d{1,2})日/u);
  const time = text.match(/(\d{1,2})時(\d{1,2})分/u);
  if (!date?.[1] || !date[2] || !time?.[1] || !time[2]) {
    return null;
  }
  return `${year}-${pad(date[1])}-${pad(date[2])}T${pad(time[1])}:${pad(time[2])}:00+09:00`;
};

const extractCourseLayout = (value: string): string | null =>
  normalizeText(value.match(/([A-Z])コース/u)?.[1]);

const findRacecourseLinks = async (page: Page): Promise<RacecourseLink[]> => {
  await page.goto(JRA_TRACK_CONDITION_URL, {
    timeout: NAVIGATION_TIMEOUT_MS,
    waitUntil: "domcontentloaded",
  });
  await page.waitForSelector("#contentsBody .kaisai_tab, .kaisai_tab", {
    timeout: SELECTOR_TIMEOUT_MS,
  });
  const anchors = page.locator("#contentsBody .kaisai_tab a, .kaisai_tab a");
  const links: RacecourseLink[] = [];
  const count = await anchors.count();
  for (let index = 0; index < count; index += 1) {
    const anchor = anchors.nth(index);
    const name = (await anchor.textContent())?.replace(/競馬場/g, "").trim() ?? "";
    const href = (await anchor.getAttribute("href")) ?? "";
    if (name && href) {
      links.push({
        name,
        url: href.startsWith("http")
          ? href
          : new URL(href, JRA_TRACK_CONDITION_BASE_URL).toString(),
      });
    }
  }
  return links;
};

const extractTrackConditionFromPage = async (page: Page): Promise<ExtractedTrackCondition> => {
  await page.waitForSelector(".data_list_unit, .condition", { timeout: SELECTOR_TIMEOUT_MS });
  const text = async (selector: string): Promise<string> =>
    (
      (await page
        .locator(selector)
        .first()
        .textContent()
        .catch(() => null)) ?? ""
    )
      .replace(/\s+/g, " ")
      .trim();
  const dataByHeader = async (header: string): Promise<string> => {
    const units = page.locator(".data_list_unit");
    const count = await units.count();
    for (let index = 0; index < count; index += 1) {
      const unit = units.nth(index);
      const title = (
        (await unit
          .locator(".head")
          .textContent()
          .catch(() => null)) ?? ""
      ).trim();
      if (title === header) {
        return (
          (await unit
            .locator(".content p")
            .first()
            .textContent()
            .catch(() => null)) ?? ""
        )
          .replace(/\s+/g, " ")
          .trim();
      }
    }
    return "";
  };
  const turfLengthCells = page.locator(".turf_length table tbody tr td");
  return {
    courseUsage: await dataByHeader("コース使用"),
    cushionMeasurementText: await text("#cushion_list option[selected]"),
    cushionValue: (await dataByHeader("クッション値")).replace(/[^0-9.]/g, ""),
    dirtCondition: (await dataByHeader("ダート")) || "良",
    dirtMoistureCorner: await text("#dirt_line .c4"),
    dirtMoistureGoal: await text("#dirt_line .gm"),
    measurementDateText: await text(".condition .block_header .content .main h3 .time"),
    moistureMeasurementText: await text("#moist_list option[selected]"),
    turfCondition: (await dataByHeader("芝")) || "良",
    turfGoing: (await text(".turf_condition .content p")) || (await dataByHeader("馬場の状態")),
    turfHeightForeign: (
      (await turfLengthCells
        .nth(2)
        .textContent()
        .catch(() => null)) ?? ""
    )
      .replace(/\s+/g, " ")
      .trim(),
    turfHeightNative: (
      (await turfLengthCells
        .nth(1)
        .textContent()
        .catch(() => null)) ?? ""
    )
      .replace(/\s+/g, " ")
      .trim(),
    turfMoistureCorner: await text("#turf_line .c4"),
    turfMoistureGoal: await text("#turf_line .gm"),
    weather: (await text(".weather strong")) || (await dataByHeader("天候")),
  };
};

export const fetchJraTrackConditionWithPlaywright = async (
  browserBinding: BrowserWorker | undefined,
  params: {
    kaisaiNen: string;
    keibajoCode: string;
  },
): Promise<TrackCondition> => {
  if (!browserBinding) {
    throw new Error("JRA_BROWSER binding is required to fetch JRA track condition.");
  }
  const racecourseName = JRA_KEIBAJO_NAMES[params.keibajoCode];
  if (!racecourseName) {
    throw new Error(`unsupported JRA racecourse: ${params.keibajoCode}`);
  }

  const { launch } = await import("@cloudflare/playwright");
  let browser: Browser | null = null;
  try {
    browser = await launch(browserBinding);
    const context = await browser.newContext();
    await setupResourceBlocker(context);
    const page = await context.newPage();
    const links = await findRacecourseLinks(page);
    const link = links.find((item) => item.name.includes(racecourseName));
    if (!link) {
      throw new Error(`JRA track condition link was not found: ${racecourseName}`);
    }

    await page.goto(link.url, { timeout: NAVIGATION_TIMEOUT_MS, waitUntil: "domcontentloaded" });
    const data = await extractTrackConditionFromPage(page);
    const measurementDate = convertJapaneseDateToIso(data.measurementDateText, params.kaisaiNen);
    const moistureMeasuredAt = convertJapaneseDateTimeToIso(
      data.moistureMeasurementText,
      params.kaisaiNen,
    );
    return {
      dirt: {
        condition: normalizeText(data.dirtCondition),
        measurementDate,
        moisture: {
          finalBend: normalizeMeasurementValue(data.dirtMoistureCorner),
          finalFurlong: normalizeMeasurementValue(data.dirtMoistureGoal),
          measuredAt: moistureMeasuredAt,
        },
      },
      fetchedAt: new Date().toISOString(),
      sourceUpdatedAt: null,
      turf: {
        condition: normalizeText(data.turfCondition),
        courseLayout: extractCourseLayout(data.courseUsage),
        cushionMeasuredAt: convertJapaneseDateTimeToIso(
          data.cushionMeasurementText,
          params.kaisaiNen,
        ),
        cushionValue: normalizeText(data.cushionValue),
        going: normalizeText(data.turfGoing),
        height: {
          japaneseZoysiaGrass: normalizeText(data.turfHeightNative),
          perennialRyegrass: normalizeText(data.turfHeightForeign),
        },
        measurementDate,
        moisture: {
          finalBend: normalizeMeasurementValue(data.turfMoistureCorner),
          finalFurlong: normalizeMeasurementValue(data.turfMoistureGoal),
          measuredAt: moistureMeasuredAt,
        },
      },
      weather: normalizeText(data.weather),
    };
  } finally {
    await browser?.close();
  }
};
