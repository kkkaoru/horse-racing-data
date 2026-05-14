import { buildNarRaceKey, NAR_BABA_CODE_TO_LOCAL_KEIBAJO } from "horse-racing-realtime/nar";
import type { OddsData, OddsType, RaceEntry, RaceResult } from "./types";

const KEIBA_GO_ORIGIN = "https://www.keiba.go.jp";
const TOP_PAGE_URL = `${KEIBA_GO_ORIGIN}/KeibaWeb/TodayRaceInfo/TodayRaceInfoTop`;
const KEIBA_GO_BASE_URL = `${KEIBA_GO_ORIGIN}/KeibaWeb/TodayRaceInfo`;
const USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 sync-realtime-data/1.0";

export const BABA_CODE_TO_LOCAL_KEIBAJO = NAR_BABA_CODE_TO_LOCAL_KEIBAJO;

const RACE_LIST_LINK_PATTERN =
  /href="(\/KeibaWeb\/TodayRaceInfo\/RaceList\?k_raceDate=[^&"]+&k_babaCode=\d+)"/gu;
const TODAY_RACE_ARTICLE_PATTERN = /<article class="todayRace">([\s\S]*?)<\/article>/u;
const RACE_LINK_PATTERN =
  /href="[^"]*DebaTable\?k_raceDate=([^&]+)&k_raceNo=(\d+)&k_babaCode=(\d+)"/gu;
const LINK_TEXT_MAP: Record<string, OddsType> = {
  "単・複": "tansho",
  三連単: "3rentan",
  三連複: "3renpuku",
  ワイド: "wide",
  枠連: "wakuren",
  馬連単: "umatan",
  馬連複: "umaren",
};
const TARGET_ODDS_NAV_DIV_INDEX = 3;
const ODDS_TYPES: OddsType[] = [
  "tansho",
  "wakuren",
  "umaren",
  "umatan",
  "wide",
  "3renpuku",
  "3rentan",
];

export interface RaceListUrl {
  babaCode: string;
  url: string;
}

export interface KeibaGoRaceLink {
  babaCode: string;
  raceNumber: string;
  url: string;
}

export interface KeibaGoRaceMetadata {
  raceName: string | null;
  startTime: string | null;
}

export const buildRaceKey = (
  year: string,
  monthDay: string,
  keibajoCode: string,
  raceNumber: string,
): string => buildNarRaceKey(year, monthDay, keibajoCode, raceNumber);

export const buildRaceListUrl = (targetDate: string, babaCode: string): RaceListUrl => {
  const raceDate = `${targetDate.slice(0, 4)}%2F${targetDate.slice(4, 6)}%2F${targetDate.slice(6, 8)}`;
  return {
    babaCode,
    url: `${KEIBA_GO_BASE_URL}/RaceList?k_raceDate=${raceDate}&k_babaCode=${babaCode}`,
  };
};

const fetchHtml = async (url: string): Promise<string> => {
  const response = await fetch(url, {
    headers: {
      Accept: "text/html,application/xhtml+xml",
      "User-Agent": USER_AGENT,
    },
  });
  if (!response.ok) {
    throw new Error(`Failed to fetch ${url}: ${response.status}`);
  }
  return response.text();
};

const stripHtmlTags = (text: string): string =>
  text
    .replace(/<[^>]*>/g, "")
    .replace(/&nbsp;/g, " ")
    .trim();

const normalizeText = (text: string): string => stripHtmlTags(text).replace(/\s+/gu, " ").trim();

const dedupe = <T>(values: readonly T[]): T[] => Array.from(new Set(values));

const toFullKeibaGoUrl = (path: string): string =>
  path.startsWith("http") ? path : `${KEIBA_GO_ORIGIN}${path}`;

const extractBabaCode = (url: string): string | null => {
  const code = new URL(url).searchParams.get("k_babaCode");
  return code ? code.padStart(2, "0") : null;
};

export const fetchTodayRaceListUrls = async (targetDate: string): Promise<RaceListUrl[]> => {
  const html = await fetchHtml(TOP_PAGE_URL);
  const article = html.match(TODAY_RACE_ARTICLE_PATTERN)?.[1] ?? html;
  const target = `${targetDate.slice(0, 4)}/${targetDate.slice(4, 6)}/${targetDate.slice(6, 8)}`;
  const paths = dedupe(
    Array.from(article.matchAll(RACE_LIST_LINK_PATTERN))
      .map((match) => match[1])
      .filter((path): path is string => typeof path === "string"),
  );

  return paths
    .map(toFullKeibaGoUrl)
    .filter((url) => new URL(url).searchParams.get("k_raceDate") === target)
    .map((url) => ({ babaCode: extractBabaCode(url) ?? "", url }))
    .filter((item) => item.babaCode in BABA_CODE_TO_LOCAL_KEIBAJO);
};

export const fetchRaceLinksFromRaceList = async (
  raceListUrl: string,
): Promise<KeibaGoRaceLink[]> => {
  const url = new URL(raceListUrl);
  const raceDate = url.searchParams.get("k_raceDate");
  const babaCode = url.searchParams.get("k_babaCode")?.padStart(2, "0");
  if (!raceDate || !babaCode) {
    return [];
  }

  const html = await fetchHtml(raceListUrl);
  const seen = new Set<string>();
  const links: KeibaGoRaceLink[] = [];

  for (const match of html.matchAll(RACE_LINK_PATTERN)) {
    const linkDate = match[1]!;
    const raceNo = match[2]!;
    const linkBabaCode = match[3]!.padStart(2, "0");
    if (decodeURIComponent(linkDate) !== raceDate || linkBabaCode !== babaCode) {
      continue;
    }
    const raceNumber = raceNo.padStart(2, "0");
    if (seen.has(raceNumber)) {
      continue;
    }
    seen.add(raceNumber);
    links.push({
      babaCode,
      raceNumber,
      url: `${KEIBA_GO_BASE_URL}/DebaTable?k_raceDate=${linkDate}&k_raceNo=${Number(raceNo)}&k_babaCode=${babaCode}`,
    });
  }

  return links.sort((left, right) => Number(left.raceNumber) - Number(right.raceNumber));
};

const collectOddsLinksFromNav = (nav: string): Partial<Record<OddsType, string>> => {
  const oddsLinks: Partial<Record<OddsType, string>> = {};
  const divMatches = nav.matchAll(/<div[^>]*>([\s\S]*?)<\/div>/gi);
  let divIndex = 0;
  let targetDiv = "";
  for (const divMatch of divMatches) {
    divIndex += 1;
    if (divIndex === TARGET_ODDS_NAV_DIV_INDEX) {
      targetDiv = divMatch[1]!;
      break;
    }
  }

  for (const linkMatch of targetDiv.matchAll(
    /<a[^>]*href=["']([^"']*)["'][^>]*>([\s\S]*?)<\/a>/gi,
  )) {
    const href = linkMatch[1];
    const text = linkMatch[2] ? stripHtmlTags(linkMatch[2]) : "";
    const oddsType = LINK_TEXT_MAP[text];
    if (href && oddsType) {
      oddsLinks[oddsType] = href;
    }
  }

  return oddsLinks;
};

export const extractOddsLinks = (
  html: string,
  baseUrl: string,
): Partial<Record<OddsType, string>> => {
  for (const navMatch of html.matchAll(/<nav[^>]*>[\s\S]*?<\/nav>/gi)) {
    const rawLinks = collectOddsLinksFromNav(navMatch[0]);
    const links = Object.fromEntries(
      Object.entries(rawLinks).map(([type, href]) => [
        type,
        href ? convertToAbsoluteKeibaGoUrl(href, baseUrl) : href,
      ]),
    ) as Partial<Record<OddsType, string>>;
    if (Object.keys(links).length > 0) {
      return links;
    }
  }
  return {};
};

export const parseRaceMetadata = (html: string): KeibaGoRaceMetadata => {
  const heading = html.match(/<h4[^>]*>([\s\S]*?)<\/h4>/iu)?.[1] ?? "";
  const startTime = normalizeText(heading).match(/(\d{1,2}):(\d{2})発走/u);
  const titleSection = html.match(
    /<section[^>]*class=["'][^"']*raceTitle[^"']*["'][^>]*>([\s\S]*?)<\/section>/iu,
  )?.[1];
  const raceName = titleSection
    ? normalizeText(titleSection.match(/<h3[^>]*>([\s\S]*?)<\/h3>/iu)?.[1] ?? "")
    : "";

  return {
    raceName: raceName.length > 0 ? raceName : null,
    startTime:
      startTime?.[1] && startTime[2] ? `${startTime[1].padStart(2, "0")}${startTime[2]}` : null,
  };
};

export const fetchRacePage = fetchHtml;

export const buildRaceResultUrl = (debaUrl: string): string =>
  debaUrl.replace("/DebaTable?", "/RaceMarkTable?");

export const convertToAbsoluteKeibaGoUrl = (oddsPath: string, baseUrl: string): string => {
  if (oddsPath.startsWith("http")) {
    return oddsPath;
  }
  const isIpatVersion = baseUrl.includes("/KeibaWeb_IPAT/");
  const basePathPrefix = isIpatVersion ? "/KeibaWeb_IPAT/" : "/KeibaWeb/";
  if (oddsPath.startsWith("../")) {
    return `${KEIBA_GO_ORIGIN}${oddsPath.replace(/^\.\.\//, basePathPrefix)}`;
  }
  if (oddsPath.startsWith("./")) {
    return `${KEIBA_GO_ORIGIN}${oddsPath.replace(/^\.\//, basePathPrefix)}`;
  }
  if (oddsPath.startsWith("/")) {
    return `${KEIBA_GO_ORIGIN}${oddsPath}`;
  }
  return `${KEIBA_GO_ORIGIN}${basePathPrefix}${oddsPath}`;
};

const isValidHorseNum = (horseNum: string): boolean => {
  const parsed = Number(horseNum);
  return parsed >= 1 && parsed <= 18;
};

const isValidFrameNum = (frameNum: string): boolean => {
  const parsed = Number(frameNum);
  return parsed >= 1 && parsed <= 8;
};

const roundOdds = (value: number): number => Math.round(value * 100) / 100;

const addRank = (item: OddsData, index: number): OddsData => ({ ...item, rank: index + 1 });

const sortByOdds = (left: OddsData, right: OddsData): number =>
  (left.odds ?? left.averageOdds ?? 0) - (right.odds ?? right.averageOdds ?? 0);

const parseTanshoOdds = (html: string): OddsData[] => {
  const tbody = html.match(/<tbody>([\s\S]*?)<\/tbody>/i)?.[1];
  if (!tbody) {
    return [];
  }
  return Array.from(tbody.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi))
    .map((row) =>
      Array.from(row[1]!.matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)).map((cell) =>
        stripHtmlTags(cell[1]!),
      ),
    )
    .map((cells): OddsData | null => {
      const horseNumber = cells[1];
      const oddsText = cells[3];
      if (!horseNumber || !isValidHorseNum(horseNumber) || !oddsText) {
        return null;
      }
      const odds = Number(oddsText);
      return Number.isFinite(odds) ? { combination: horseNumber, odds: roundOdds(odds) } : null;
    })
    .filter((item): item is OddsData => item !== null)
    .sort(sortByOdds)
    .map(addRank);
};

const extractRankingRows = (html: string): string[] =>
  Array.from(
    html.matchAll(/<table[^>]*class="odd_ranking_table"[^>]*>([\s\S]*?)<\/table>/gi),
  ).flatMap((table) =>
    Array.from(table[1]!.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)).map((row) => row[1]!),
  );

const parsePairOddsRow = (row: string, ordered: boolean): OddsData | null => {
  const cells = Array.from(row.matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)).map((cell) =>
    stripHtmlTags(cell[1]!),
  );
  const combination = cells[0];
  const oddsText = cells[1];
  const match = combination?.match(/^(\d+)-(\d+)$/u);
  if (!match?.[1] || !match[2] || !oddsText) {
    return null;
  }
  const odds = Number(oddsText);
  if (!Number.isFinite(odds)) {
    return null;
  }
  const left = Number(match[1]);
  const right = Number(match[2]);
  const normalized = ordered || left <= right ? `${left}-${right}` : `${right}-${left}`;
  return { combination: normalized, odds: roundOdds(odds) };
};

const parseRankingPairOdds = (html: string, ordered: boolean): OddsData[] =>
  Array.from(
    new Map(
      extractRankingRows(html)
        .map((row) => parsePairOddsRow(row, ordered))
        .filter((item): item is OddsData => item !== null)
        .map((item) => [item.combination, item] as const),
    ).values(),
  )
    .sort(sortByOdds)
    .map(addRank);

const parseWideOdds = (html: string): OddsData[] =>
  Array.from(
    new Map(
      extractRankingRows(html)
        .map((row): OddsData | null => {
          const combination = row.match(/<td>\s*(\d+)-(\d+)\s*<\/td>/s);
          const oddsRange = row.match(
            /<td[^>]*>\s*([0-9]+\.?[0-9]*)<\/br>\s*-\s*([0-9]+\.?[0-9]*)\s*<\/td>/s,
          );
          if (!combination?.[1] || !combination[2] || !oddsRange?.[1] || !oddsRange[2]) {
            return null;
          }
          const left = Number(combination[1]);
          const right = Number(combination[2]);
          const minOdds = roundOdds(Number(oddsRange[1]));
          const maxOdds = roundOdds(Number(oddsRange[2]));
          return {
            averageOdds: roundOdds((minOdds + maxOdds) / 2),
            combination: left <= right ? `${left}-${right}` : `${right}-${left}`,
            maxOdds,
            minOdds,
          };
        })
        .filter((item): item is OddsData => item !== null)
        .map((item) => [item.combination, item] as const),
    ).values(),
  )
    .sort(sortByOdds)
    .map(addRank);

const parseWakurenOdds = (html: string): OddsData[] => {
  const list = html.match(/<ul[^>]*class="odd_horse_number_list"[^>]*>([\s\S]*?)<\/ul>/i)?.[1];
  if (!list) {
    return [];
  }
  const values = Array.from(list.matchAll(/<table[^>]*>([\s\S]*?)<\/table>/gi)).flatMap((table) => {
    const tableHtml = table[1]!;
    const base = tableHtml.match(/<th[^>]*class="odd_post\d+"[^>]*>\s*(\d+)\s*<\/th>/i)?.[1];
    if (!base || !isValidFrameNum(base)) {
      return [];
    }
    return Array.from(
      tableHtml.matchAll(
        /<tr>\s*<td>\s*(\d+)\s*<\/td>\s*<td[^>]*>\s*([0-9]+\.?[0-9]*)\s*<\/td>\s*<\/tr>/gi,
      ),
    )
      .map((row): OddsData | null => {
        const target = row[1];
        const oddsText = row[2];
        if (!target || !isValidFrameNum(target) || !oddsText) {
          return null;
        }
        const left = Number(base);
        const right = Number(target);
        return {
          combination: left <= right ? `${left}-${right}` : `${right}-${left}`,
          odds: roundOdds(Number(oddsText)),
        };
      })
      .filter((item): item is OddsData => item !== null);
  });

  return Array.from(new Map(values.map((item) => [item.combination, item] as const)).values())
    .sort(sortByOdds)
    .map(addRank);
};

const parseTripleOdds = (html: string, ordered: boolean): OddsData[] => {
  const patterns = [
    /(\d+)-(\d+)-(\d+)[^0-9]*?([0-9]+\.?[0-9]*)/g,
    /(\d+)\s*→\s*(\d+)\s*→\s*(\d+)[^0-9]*?([0-9]+\.?[0-9]*)/g,
    /(\d+)\s*[-ー]\s*(\d+)\s*[-ー]\s*(\d+)[^0-9]*?([0-9]+\.?[0-9]*)/g,
  ];
  const odds = patterns.flatMap((pattern) =>
    Array.from(html.matchAll(pattern)).map((match): OddsData | null => {
      const horses = [match[1], match[2], match[3]];
      const oddsText = match[4];
      if (horses.some((horse) => !horse || !isValidHorseNum(horse)) || !oddsText) {
        return null;
      }
      const normalized = ordered
        ? horses.join("-")
        : horses
            .map((horse) => Number(horse))
            .toSorted((left, right) => left - right)
            .join("-");
      return { combination: normalized, odds: roundOdds(Number(oddsText)) };
    }),
  );
  return Array.from(
    new Map(
      odds
        .filter((item): item is OddsData => item !== null)
        .map((item) => [item.combination, item]),
    ).values(),
  )
    .sort(sortByOdds)
    .map(addRank);
};

const parseOddsByType = (type: OddsType, html: string): OddsData[] => {
  switch (type) {
    case "3renpuku":
      return parseTripleOdds(html, false);
    case "3rentan":
      return parseTripleOdds(html, true);
    case "tansho":
      return parseTanshoOdds(html);
    case "umaren":
      return parseRankingPairOdds(html, false);
    case "umatan":
      return parseRankingPairOdds(html, true);
    case "wakuren":
      return parseWakurenOdds(html);
    case "wide":
      return parseWideOdds(html);
  }
};

export const fetchOdds = async (
  baseUrl: string,
  oddsLinks: Partial<Record<OddsType, string>>,
): Promise<Partial<Record<OddsType, OddsData[]>>> => {
  const entries = await Promise.all(
    ODDS_TYPES.map(async (type): Promise<[OddsType, OddsData[]] | null> => {
      const url = oddsLinks[type];
      if (!url) {
        return null;
      }
      try {
        const html = await fetchHtml(convertToAbsoluteKeibaGoUrl(url, baseUrl));
        return [type, parseOddsByType(type, html)];
      } catch (error) {
        console.warn(`Failed to fetch ${type} odds`, error);
        return null;
      }
    }),
  );
  return Object.fromEntries(
    entries.filter((entry): entry is [OddsType, OddsData[]] => entry !== null),
  );
};

export const parseHorseWeights = (html: string) => {
  const horseBlocks = html.split(/<tr[^>]*class=["'][^"']*tBorder[^"']*["'][^>]*>/giu).slice(1);
  return horseBlocks
    .map((block) => {
      const horseNumber = block.match(
        /class=["'][^"']*horseNum[^"']*["'][^>]*>\s*(\d{1,2})\s*</iu,
      )?.[1];
      const horseName = block.match(
        /class=["'][^"']*horseName[^"']*["'][^>]*>([\s\S]*?)<\/a>/iu,
      )?.[1];
      const oddsWeightHtml =
        block.match(/<td[^>]*class=["'][^"']*odds_weight[^"']*["'][^>]*>([\s\S]*?)<\/td>/iu)?.[1] ??
        "";
      const oddsWeightText = stripHtmlTags(oddsWeightHtml.replace(/<br\s*\/?>/giu, " ")).replace(
        /\s+/g,
        " ",
      );
      const match = oddsWeightText.match(
        /(?:^|[^0-9.])([3-9]\d{2}|1[0-3]\d{2})(?:\s*\(([+-]?)(\d+)\))?(?![0-9.])/u,
      );
      if (!horseNumber || !isValidHorseNum(horseNumber) || !match?.[1]) {
        return null;
      }
      return {
        changeAmount: match[3] ? Number(match[3]) : null,
        changeSign: match[2] || null,
        horseName: horseName ? stripHtmlTags(horseName) : null,
        horseNumber,
        weight: Number(match[1]),
      };
    })
    .filter((item): item is NonNullable<typeof item> => item !== null);
};

const normalizeRaceResultCell = (cell: string): string =>
  stripHtmlTags(cell.replace(/<br\s*\/?>/giu, " ")).replace(/\s+/g, " ");

const ENTRY_STATUS_LABELS = ["出場停止", "出走取消", "取消", "競走除外", "除外"] as const;

const normalizeEntryStatus = (block: string): string | null => {
  const currentInfoCells = block.matchAll(
    /<td[^>]*class=["'][^"']*\binfo\b[^"']*["'][^>]*>([\s\S]*?)<\/td>/giu,
  );
  for (const cell of currentInfoCells) {
    const normalized = normalizeRaceResultCell(cell[1] ?? "");
    const status = ENTRY_STATUS_LABELS.find((label) => normalized.includes(label));
    if (status) {
      return status;
    }
  }
  return null;
};

export const parseRaceEntries = (html: string): Omit<RaceEntry, "fetchedAt">[] =>
  html
    .split(/<tr[^>]*class=["'][^"']*tBorder[^"']*["'][^>]*>/giu)
    .slice(1)
    .map((block) => {
      const horseNumber = block.match(
        /class=["'][^"']*horseNum[^"']*["'][^>]*>\s*(\d{1,2})\s*</iu,
      )?.[1];
      const horseName = block.match(
        /class=["'][^"']*horseName[^"']*["'][^>]*>([\s\S]*?)<\/a>/iu,
      )?.[1];
      const jockeyName = block.match(
        /class=["'][^"']*jockeyName[^"']*["'][^>]*>([\s\S]*?)<\/a>/iu,
      )?.[1];
      if (!horseNumber || !isValidHorseNum(horseNumber)) {
        return null;
      }
      return {
        horseName: horseName ? normalizeRaceResultCell(horseName) : null,
        horseNumber,
        jockeyName: jockeyName
          ? normalizeRaceResultCell(jockeyName).replace(/（.*?）/gu, "")
          : null,
        status: normalizeEntryStatus(block),
      };
    })
    .filter((entry): entry is Omit<RaceEntry, "fetchedAt"> => entry !== null);

export const parseRaceEntryHorseNumbers = (html: string): string[] =>
  parseRaceEntries(html)
    .map((entry) => entry.horseNumber)
    .filter((horseNumber, index, values) => values.indexOf(horseNumber) === index)
    .toSorted((left, right) => Number(left) - Number(right));

export const parseRaceResultHorseWeights = (html: string) =>
  Array.from(html.matchAll(/<tr[^>]*bgcolor=["']#FFFFFF["'][^>]*>([\s\S]*?)<\/tr>/giu))
    .map((row) => {
      const cells = Array.from((row[1] ?? "").matchAll(/<td[^>]*>([\s\S]*?)<\/td>/giu)).map(
        (cell) => normalizeRaceResultCell(cell[1] ?? ""),
      );
      const horseNumber = cells[2];
      const horseName = cells[3];
      const weight = cells[9];
      const changeAmount = cells[10];
      if (
        !horseNumber ||
        !isValidHorseNum(horseNumber) ||
        !weight ||
        !/^(?:[3-9]\d{2}|1[0-3]\d{2})$/u.test(weight)
      ) {
        return null;
      }
      return {
        changeAmount:
          changeAmount && /^-?\d+$/u.test(changeAmount) ? Math.abs(Number(changeAmount)) : null,
        changeSign: changeAmount?.startsWith("-") ? "-" : changeAmount ? "+" : null,
        horseName: horseName || null,
        horseNumber,
        weight: Number(weight),
      };
    })
    .filter((item): item is NonNullable<typeof item> => item !== null);

const RESULT_EXCLUDED_STATUSES = new Set<string>(ENTRY_STATUS_LABELS);

const isResultExcludedStatus = (status: string): boolean => RESULT_EXCLUDED_STATUSES.has(status);

export const parseRaceResults = (html: string): Omit<RaceResult, "fetchedAt">[] =>
  Array.from(html.matchAll(/<tr[^>]*bgcolor=["']#FFFFFF["'][^>]*>([\s\S]*?)<\/tr>/giu))
    .map((row) => {
      const cells = Array.from((row[1] ?? "").matchAll(/<td[^>]*>([\s\S]*?)<\/td>/giu)).map(
        (cell) => normalizeRaceResultCell(cell[1] ?? ""),
      );
      const finishPosition = cells[0];
      const horseNumber = cells[2];
      const horseName = cells[3];
      const time = cells[11];
      if (
        !finishPosition ||
        isResultExcludedStatus(finishPosition) ||
        !horseNumber ||
        !isValidHorseNum(horseNumber)
      ) {
        return null;
      }
      return {
        finishPosition: /^\d+$/u.test(finishPosition)
          ? finishPosition.padStart(2, "0")
          : finishPosition,
        horseName: horseName || null,
        horseNumber,
        time: time || null,
      };
    })
    .filter((item): item is Omit<RaceResult, "fetchedAt"> => item !== null)
    .toSorted(
      (left, right) =>
        (Number(left.finishPosition) || 999) - (Number(right.finishPosition) || 999) ||
        Number(left.horseNumber) - Number(right.horseNumber),
    );

export const parseRaceResultExcludedHorseNumbers = (html: string): string[] =>
  Array.from(html.matchAll(/<tr[^>]*bgcolor=["']#FFFFFF["'][^>]*>([\s\S]*?)<\/tr>/giu))
    .map((row) => {
      const cells = Array.from((row[1] ?? "").matchAll(/<td[^>]*>([\s\S]*?)<\/td>/giu)).map(
        (cell) => normalizeRaceResultCell(cell[1] ?? ""),
      );
      const finishPosition = cells[0];
      const horseNumber = cells[2];
      if (
        !finishPosition ||
        !isResultExcludedStatus(finishPosition) ||
        !horseNumber ||
        !isValidHorseNum(horseNumber)
      ) {
        return null;
      }
      return horseNumber;
    })
    .filter((horseNumber): horseNumber is string => horseNumber !== null)
    .filter((horseNumber, index, values) => values.indexOf(horseNumber) === index)
    .toSorted((left, right) => Number(left) - Number(right));
