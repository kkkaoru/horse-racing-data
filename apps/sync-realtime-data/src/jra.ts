import type { Browser, BrowserContext, BrowserWorker, Page, Route } from "@cloudflare/playwright";

import { buildJraRaceEntryUrl, buildJraRaceResultUrl } from "../../pc-keiba-viewer/src/lib/jra-url";
import type { LocalRaceRow } from "./storage";
import type {
  HorseWeight,
  NarRaceSource,
  OddsData,
  OddsType,
  RaceEntry,
  RaceResult,
} from "./types";

const ODDS_LIST_SELECTOR = "#odds_list";
const NAVIGATION_TIMEOUT_MS = 15_000;
const CLICK_TIMEOUT_MS = 8_000;
const CHANGE_TIMEOUT_MS = 3_000;
const CONTENT_PROBE_LENGTH = 128;

const ODDS_PAGE_LABELS: ReadonlyArray<{ label: string; type: OddsType }> = [
  { label: "単勝・複勝", type: "tansho" },
  { label: "単勝・複勝", type: "fukusho" },
  { label: "枠連", type: "wakuren" },
  { label: "馬連", type: "umaren" },
  { label: "ワイド", type: "wide" },
  { label: "馬単", type: "umatan" },
  { label: "3連複", type: "3renpuku" },
  { label: "3連単", type: "3rentan" },
];

const BLOCKED_RESOURCE_TYPES = new Set(["font", "image", "media", "stylesheet"]);
const ENTRY_SCRATCH_STATUS_LABELS = ["出場停止", "出走取消", "取消", "競走除外", "除外"] as const;
const RESULT_EXCLUDED_STATUS_LABELS = [
  "出走取消",
  "取消",
  "競走除外",
  "除外",
  "競走中止",
  "競争中止",
  "中止",
] as const;

export const buildJraEntryUrlFromRace = (race: LocalRaceRow): string | null =>
  buildJraRaceEntryUrl({
    kaisaiKai: race.kaisai_kai ?? null,
    kaisaiNen: race.kaisai_nen,
    kaisaiNichime: race.kaisai_nichime ?? null,
    kaisaiTsukihi: race.kaisai_tsukihi,
    keibajoCode: race.keibajo_code,
    raceBango: race.race_bango.padStart(2, "0"),
    source: "jra",
  });

export const buildJraResultUrlFromRaceSource = (race: NarRaceSource): string | null =>
  buildJraRaceResultUrl({
    kaisaiKai: race.kaisaiKai ?? null,
    kaisaiNen: race.kaisaiNen,
    kaisaiNichime: race.kaisaiNichime ?? null,
    kaisaiTsukihi: race.kaisaiTsukihi,
    keibajoCode: race.keibajoCode,
    raceBango: race.raceBango.padStart(2, "0"),
    source: race.source,
  });

const stripHtmlTags = (text: string): string =>
  text
    .replace(/<br\s*\/?>/giu, " ")
    .replace(/<[^>]*>/g, "")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/\s+/gu, " ")
    .trim();

const normalizeJockeyName = (text: string): string =>
  stripHtmlTags(text)
    .replace(/[△▲☆★◇◆□■▽▼]/gu, "")
    .replace(/[\s\p{Separator}\u200B-\u200D\uFEFF]+/gu, "");

const normalizeHorseNumber = (value: string): string => String(Number(value));

const isValidHorseNumber = (value: string): boolean => {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed >= 1 && parsed <= 18;
};

const isValidFrameNumber = (value: string): boolean => {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed >= 1 && parsed <= 8;
};

const roundOdds = (value: number): number => Math.round(value * 100) / 100;

const addRank = (item: OddsData, index: number): OddsData => ({ ...item, rank: index + 1 });

const sortByOdds = (left: OddsData, right: OddsData): number =>
  (left.odds ?? left.averageOdds ?? 0) - (right.odds ?? right.averageOdds ?? 0);

const uniqueByCombination = (rows: OddsData[]): OddsData[] =>
  Array.from(new Map(rows.map((row) => [row.combination, row])).values());

const extractRows = (html: string): string[] =>
  Array.from(html.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/giu)).map((match) => match[1] ?? "");

const extractClassCell = (row: string, className: string): string | null =>
  row.match(
    new RegExp(
      `<td[^>]*class=["'][^"']*\\b${className}\\b[^"']*["'][^>]*>([\\s\\S]*?)<\\/td>`,
      "iu",
    ),
  )?.[1] ?? null;

const extractFirstAnchorText = (html: string | null): string | null =>
  html?.match(/<a\b[^>]*>([\s\S]*?)<\/a>/iu)?.[1] ?? null;

const extractCells = (row: string): { attrs: string; html: string }[] =>
  Array.from(row.matchAll(/<td\b([^>]*)>([\s\S]*?)<\/td>/giu)).map((match) => ({
    attrs: match[1] ?? "",
    html: match[2] ?? "",
  }));

const extractJraHorseNumber = (row: string): string | null =>
  row.match(/<td[^>]*class=["'][^"']*\bnum\b[^"']*["'][^>]*>\s*(\d{1,2})\b/iu)?.[1] ??
  row.match(/class=["'][^"']*horseNum[^"']*["'][^>]*>\s*(\d{1,2})\s*</iu)?.[1] ??
  null;

const hasClassLike = (html: string, pattern: RegExp): boolean =>
  Array.from(html.matchAll(/class=["']([^"']*)["']/giu)).some((match) =>
    pattern.test(match[1] ?? ""),
  );

const isPastEntryStatusCell = (attrs: string, html: string): boolean =>
  hasClassLike(`${attrs} ${html}`, /past|history|recent|previous/iu);

const extractJraEntryStatus = (row: string): string | null => {
  let hasCurrentJockeyChange = false;
  for (const { attrs, html } of extractCells(row)) {
    if (isPastEntryStatusCell(attrs, html)) {
      continue;
    }
    const text = stripHtmlTags(html);
    if (!text) {
      continue;
    }
    if (text.includes("騎手変更")) {
      hasCurrentJockeyChange = true;
      continue;
    }
    const scratchStatus = ENTRY_SCRATCH_STATUS_LABELS.find((label) =>
      label.length > 2 ? text === label || text.startsWith(label) : text === label,
    );
    if (scratchStatus) {
      return scratchStatus;
    }
  }
  return hasCurrentJockeyChange ? "騎手変更" : null;
};

const extractJraHorseName = (row: string): string | null => {
  const horseNameCell = extractClassCell(row, "horseName") ?? extractClassCell(row, "horse") ?? row;
  return (
    extractFirstAnchorText(
      horseNameCell.match(/class=["'][^"']*\bname\b[^"']*["'][^>]*>([\s\S]*?)<\/div>/iu)?.[1] ??
        horseNameCell,
    ) ?? null
  );
};

const extractJraJockeyName = (row: string): string | null => {
  const jockeyCell = extractClassCell(row, "jockey");
  const currentJockey =
    jockeyCell?.match(/<p[^>]*class=["'][^"']*\bjockey\b[^"']*["'][^>]*>([\s\S]*?)<\/p>/iu)?.[1] ??
    jockeyCell;
  return extractFirstAnchorText(currentJockey) ?? currentJockey ?? null;
};

const extractJraHorseWeightMatch = (row: string): RegExpMatchArray | null => {
  const weightCell =
    row.match(
      /<div[^>]*class=["'][^"']*\bcell\b[^"']*\bweight\b[^"']*["'][^>]*>([\s\S]*?)<\/div>/iu,
    )?.[1] ??
    row.match(
      /<td[^>]*class=["'][^"']*(?:weight|odds_weight)[^"']*["'][^>]*>([\s\S]*?)<\/td>/iu,
    )?.[1];
  if (!weightCell) {
    return null;
  }
  const weightText = stripHtmlTags(weightCell);
  return weightText.match(/(?:^|[^0-9.])([3-9]\d{2})\s*(?:kg)?(?:\s*\(([+-]?)(\d+)\))?(?![0-9.])/u);
};

export const isJraScratchStatus = (status: string): boolean =>
  (ENTRY_SCRATCH_STATUS_LABELS as readonly string[]).includes(status);

export const sanitizeJraRaceEntriesWithOdds = (
  entries: Omit<RaceEntry, "fetchedAt">[],
  latest: Partial<Record<OddsType, OddsData[]>> | null | undefined,
): Omit<RaceEntry, "fetchedAt">[] => {
  const activeHorseNumbers = new Set(
    (latest?.tansho ?? [])
      .map((odds) => normalizeHorseNumber(odds.combination))
      .filter((combination) => isValidHorseNumber(combination)),
  );
  if (activeHorseNumbers.size === 0) {
    return entries;
  }
  return entries.map((entry) =>
    entry.status && isJraScratchStatus(entry.status) && activeHorseNumbers.has(entry.horseNumber)
      ? { ...entry, status: null }
      : entry,
  );
};

export const parseJraRaceEntries = (html: string): Omit<RaceEntry, "fetchedAt">[] =>
  extractRows(html)
    .map((row) => {
      const horseNumber = extractJraHorseNumber(row);
      if (!horseNumber || !isValidHorseNumber(horseNumber)) {
        return null;
      }
      const horseName = extractJraHorseName(row);
      const jockeyName = extractJraJockeyName(row);
      const rowStatus = extractJraEntryStatus(row);
      const status =
        rowStatus && isJraScratchStatus(rowStatus) && extractJraHorseWeightMatch(row)
          ? null
          : rowStatus;
      return {
        horseName: horseName ? stripHtmlTags(horseName) : null,
        horseNumber: normalizeHorseNumber(horseNumber),
        jockeyName: jockeyName ? normalizeJockeyName(jockeyName) : null,
        status,
      };
    })
    .filter((entry): entry is NonNullable<typeof entry> => entry !== null);

export const parseJraHorseWeights = (html: string): HorseWeight[] =>
  extractRows(html)
    .map((row) => {
      const horseNumber = extractJraHorseNumber(row);
      if (!horseNumber || !isValidHorseNumber(horseNumber)) {
        return null;
      }
      const horseName = extractJraHorseName(row);
      const match = extractJraHorseWeightMatch(row);
      if (!match?.[1]) {
        return null;
      }
      const weight: HorseWeight = {
        changeAmount: match[3] ? Number(match[3]) : null,
        changeSign: match[2] || null,
        horseName: horseName ? stripHtmlTags(horseName) : null,
        horseNumber: normalizeHorseNumber(horseNumber),
        weight: Number(match[1]),
      };
      return weight;
    })
    .filter((weight): weight is NonNullable<typeof weight> => weight !== null);

const normalizeFinishPosition = (value: string): string | null => {
  const cleaned = stripHtmlTags(value);
  if (/^\d{1,2}$/u.test(cleaned)) {
    return cleaned.padStart(2, "0");
  }
  return null;
};

const isJraResultExcludedStatus = (value: string): boolean => {
  const cleaned = stripHtmlTags(value);
  return (RESULT_EXCLUDED_STATUS_LABELS as readonly string[]).some(
    (label) => cleaned === label || cleaned.startsWith(label),
  );
};

const extractJraResultHorseName = (row: string): string | null => {
  const horseCell = extractClassCell(row, "horse");
  if (!horseCell) {
    return null;
  }
  const anchorText = extractFirstAnchorText(horseCell);
  const horseName = stripHtmlTags(anchorText ?? horseCell);
  return horseName || null;
};

export const parseJraRaceResults = (html: string): Omit<RaceResult, "fetchedAt">[] =>
  extractRows(html)
    .map((row): Omit<RaceResult, "fetchedAt"> | null => {
      const placeCell = extractClassCell(row, "place");
      const horseNumberCell = extractClassCell(row, "num");
      if (!placeCell || !horseNumberCell) {
        return null;
      }
      if (isJraResultExcludedStatus(placeCell)) {
        return null;
      }
      const finishPosition = normalizeFinishPosition(placeCell);
      const horseNumber = stripHtmlTags(horseNumberCell);
      if (!finishPosition || !isValidHorseNumber(horseNumber)) {
        return null;
      }
      const time = stripHtmlTags(extractClassCell(row, "time") ?? "");
      return {
        finishPosition,
        horseName: extractJraResultHorseName(row),
        horseNumber: normalizeHorseNumber(horseNumber),
        time: time || null,
      };
    })
    .filter((result): result is Omit<RaceResult, "fetchedAt"> => result !== null)
    .toSorted((left, right) => Number(left.finishPosition) - Number(right.finishPosition));

export const parseJraRaceResultExcludedHorseNumbers = (html: string): string[] =>
  extractRows(html)
    .map((row): string | null => {
      const placeCell = extractClassCell(row, "place");
      const horseNumberCell = extractClassCell(row, "num");
      if (!placeCell || !horseNumberCell || !isJraResultExcludedStatus(placeCell)) {
        return null;
      }
      const horseNumber = stripHtmlTags(horseNumberCell);
      return isValidHorseNumber(horseNumber) ? normalizeHorseNumber(horseNumber) : null;
    })
    .filter((horseNumber): horseNumber is string => horseNumber !== null);

const parseTanshoOdds = (html: string): OddsData[] =>
  Array.from(
    html.matchAll(/<table[^>]*class=["'][^"']*tanpuku[^"']*["'][^>]*>([\s\S]*?)<\/table>/giu),
  )
    .flatMap((table) =>
      extractRows(table[1] ?? "").map((row): OddsData | null => {
        if (/class=["'][^"']*odds_tan\s+cancel[^"']*["']/iu.test(row)) {
          return null;
        }
        const horseNumber = row.match(
          /<td[^>]*class=["']num["'][^>]*>\s*(\d{1,2})\s*<\/td>/iu,
        )?.[1];
        const oddsText = row.match(
          /<td[^>]*class=["']odds_tan["'][^>]*>(?:<strong[^>]*>)?([\d.]+)/iu,
        )?.[1];
        if (!horseNumber || !oddsText || !isValidHorseNumber(horseNumber)) {
          return null;
        }
        const odds = Number(oddsText);
        return Number.isFinite(odds)
          ? { combination: normalizeHorseNumber(horseNumber), odds: roundOdds(odds) }
          : null;
      }),
    )
    .filter((row): row is OddsData => row !== null)
    .sort(sortByOdds)
    .map(addRank);

const parseFukushoOdds = (html: string): OddsData[] =>
  Array.from(
    html.matchAll(/<table[^>]*class=["'][^"']*tanpuku[^"']*["'][^>]*>([\s\S]*?)<\/table>/giu),
  )
    .flatMap((table) =>
      extractRows(table[1] ?? "").map((row): OddsData | null => {
        const horseNumber = row.match(
          /<td[^>]*class=["']num["'][^>]*>\s*(\d{1,2})\s*<\/td>/iu,
        )?.[1];
        const fukushoCell = row.match(
          /<td[^>]*class=["'][^"']*odds_fuku[^"']*["'][^>]*>([\s\S]*?)<\/td>/iu,
        )?.[1];
        if (!horseNumber || !fukushoCell || !isValidHorseNumber(horseNumber)) {
          return null;
        }
        const values = Array.from(fukushoCell.matchAll(/[\d]+(?:\.[\d]+)?/gu))
          .map((match) => Number(match[0]))
          .filter((value) => Number.isFinite(value));
        if (values.length === 0) {
          return null;
        }
        const minOdds = Math.min(...values);
        const maxOdds = Math.max(...values);
        return {
          averageOdds: roundOdds((minOdds + maxOdds) / 2),
          combination: normalizeHorseNumber(horseNumber),
          maxOdds: roundOdds(maxOdds),
          minOdds: roundOdds(minOdds),
        };
      }),
    )
    .filter((row): row is OddsData => row !== null)
    .sort(sortByOdds)
    .map(addRank);

const parsePairTables = (
  html: string,
  tableClass: string,
  captionPattern: RegExp,
  ordered: boolean,
): OddsData[] =>
  uniqueByCombination(
    Array.from(
      html.matchAll(
        new RegExp(
          `<table[^>]*class=["'][^"']*${tableClass}[^"']*["'][^>]*>([\\s\\S]*?)<\\/table>`,
          "giu",
        ),
      ),
    ).flatMap((table): OddsData[] => {
      const tableHtml = table[1] ?? "";
      const base = tableHtml.match(captionPattern)?.[1];
      if (!base) {
        return [];
      }
      return extractRows(tableHtml)
        .map((row): OddsData | null => {
          const target = row.match(/<th[^>]*[^>]*>\s*(\d{1,2})\s*<\/th>/iu)?.[1];
          const oddsText = row.match(
            /<td[^>]*>(?:<strong[^>]*>)?([\d.]+)(?:<\/strong>)?\s*<\/td>/iu,
          )?.[1];
          if (!target || !oddsText) {
            return null;
          }
          const left = Number(base);
          const right = Number(target);
          if (!Number.isFinite(left) || !Number.isFinite(right)) {
            return null;
          }
          const combination = ordered || left <= right ? `${left}-${right}` : `${right}-${left}`;
          return { combination, odds: roundOdds(Number(oddsText)) };
        })
        .filter((row): row is OddsData => row !== null);
    }),
  )
    .sort(sortByOdds)
    .map(addRank);

const parseWideOdds = (html: string): OddsData[] =>
  uniqueByCombination(
    Array.from(
      html.matchAll(/<table[^>]*class=["'][^"']*wide[^"']*["'][^>]*>([\s\S]*?)<\/table>/giu),
    ).flatMap((table): OddsData[] => {
      const tableHtml = table[1] ?? "";
      const base = tableHtml.match(/<caption[^>]*>\s*(\d{1,2})\s*<\/caption>/iu)?.[1];
      if (!base || !isValidHorseNumber(base)) {
        return [];
      }
      return extractRows(tableHtml)
        .map((row): OddsData | null => {
          const target = row.match(/<th[^>]*>\s*(\d{1,2})\s*<\/th>/iu)?.[1];
          const minOdds = row.match(/<span\s+class=["']min["']>\s*([\d.]+)\s*<\/span>/iu)?.[1];
          const maxOdds = row.match(/<span\s+class=["']max["']>\s*([\d.]+)\s*<\/span>/iu)?.[1];
          if (!target || !minOdds || !maxOdds || !isValidHorseNumber(target)) {
            return null;
          }
          const left = Number(base);
          const right = Number(target);
          const min = roundOdds(Number(minOdds));
          const max = roundOdds(Number(maxOdds));
          return {
            averageOdds: roundOdds((min + max) / 2),
            combination: left <= right ? `${left}-${right}` : `${right}-${left}`,
            maxOdds: max,
            minOdds: min,
          };
        })
        .filter((row): row is OddsData => row !== null);
    }),
  )
    .sort(sortByOdds)
    .map(addRank);

const parseTripleOdds = (html: string, ordered: boolean): OddsData[] =>
  uniqueByCombination(
    Array.from(
      html.matchAll(/<table[^>]*class=["'][^"']*fuku3[^"']*["'][^>]*>([\s\S]*?)<\/table>/giu),
    )
      .flatMap((table): OddsData[] => {
        const tableHtml = table[1] ?? "";
        const caption = stripHtmlTags(
          tableHtml.match(/<caption[^>]*>([\s\S]*?)<\/caption>/iu)?.[1] ?? "",
        );
        const base = caption.match(/^(\d{1,2})-(\d{1,2})$/u);
        if (!base?.[1] || !base[2]) {
          return [];
        }
        return extractRows(tableHtml)
          .map((row): OddsData | null => {
            const target = row.match(/<th[^>]*>\s*(\d{1,2})\s*<\/th>/iu)?.[1];
            const oddsText = row.match(
              /<td[^>]*>(?:<strong[^>]*>)?([\d.]+)(?:<\/strong>)?\s*<\/td>/iu,
            )?.[1];
            if (!target || !oddsText) {
              return null;
            }
            const nums = [Number(base[1]), Number(base[2]), Number(target)];
            if (nums.some((num) => !isValidHorseNumber(String(num)))) {
              return null;
            }
            return {
              combination: nums.toSorted((left, right) => left - right).join("-"),
              odds: roundOdds(Number(oddsText)),
            };
          })
          .filter((row): row is OddsData => row !== null);
      })
      .concat(
        Array.from(
          html.matchAll(
            /<div\s+class=["']tan3_unit[^"']*["'][^>]*>([\s\S]*?)(?=<div\s+class=["']tan3_unit|$)/giu,
          ),
        ).flatMap((unit): OddsData[] => {
          const unitHtml = unit[1] ?? "";
          const first = unitHtml.match(
            /<span class=["']inner["']>\s*<span class=["']num["']>\s*(\d{1,2})\s*<\/span>/iu,
          )?.[1];
          if (!first) {
            return [];
          }
          return Array.from(
            unitHtml.matchAll(
              /<div class=["']cap["']><span>2着<\/span><\/div>\s*<div class=["']num["']>(\d{1,2})<\/div>[\s\S]*?<table[^>]*>([\s\S]*?)<\/table>/giu,
            ),
          ).flatMap((section): OddsData[] => {
            const second = section[1];
            const tableHtml = section[2] ?? "";
            if (!second) {
              return [];
            }
            return extractRows(tableHtml)
              .map((row): OddsData | null => {
                const third = row.match(/<th[^>]*>\s*(\d{1,2})\s*<\/th>/iu)?.[1];
                const oddsText = row.match(
                  /<td[^>]*>(?:<strong[^>]*>)?([\d.]+)(?:<\/strong>)?\s*<\/td>/iu,
                )?.[1];
                if (!third || !oddsText) {
                  return null;
                }
                const nums = [first, second, third];
                if (nums.some((num) => !isValidHorseNumber(num))) {
                  return null;
                }
                return {
                  combination: ordered
                    ? nums.map((num) => Number(num)).join("-")
                    : nums
                        .map((num) => Number(num))
                        .toSorted((left, right) => left - right)
                        .join("-"),
                  odds: roundOdds(Number(oddsText)),
                };
              })
              .filter((row): row is OddsData => row !== null);
          });
        }),
      ),
  )
    .sort(sortByOdds)
    .map(addRank);

export const parseJraOddsByType = (type: OddsType, html: string): OddsData[] => {
  switch (type) {
    case "3renpuku":
      return parseTripleOdds(html, false);
    case "3rentan":
      return parseTripleOdds(html, true);
    case "fukusho":
      return parseFukushoOdds(html);
    case "tansho":
      return parseTanshoOdds(html);
    case "umaren":
      return parsePairTables(html, "umaren", /<caption>\s*(\d{1,2})\s*<\/caption>/iu, false);
    case "umatan":
      return parsePairTables(html, "umatan", /<caption>\s*(\d{1,2})\s*<\/caption>/iu, true);
    case "wakuren":
      return parsePairTables(
        html,
        "waku",
        /<caption[^>]*class=["']waku(\d)["'][^>]*>/iu,
        false,
      ).filter((row) => row.combination.split("-").every(isValidFrameNumber));
    case "wide":
      return parseWideOdds(html);
  }
};

const setupResourceBlocker = async (context: BrowserContext): Promise<void> => {
  await context.route("**/*", async (route: Route) => {
    if (BLOCKED_RESOURCE_TYPES.has(route.request().resourceType())) {
      await route.abort();
      return;
    }
    await route.continue();
  });
};

const captureProbe = async (page: Page): Promise<{ html: string; url: string }> => ({
  html: (
    await page
      .locator(ODDS_LIST_SELECTOR)
      .innerHTML({ timeout: 500 })
      .catch(() => "")
  ).slice(0, CONTENT_PROBE_LENGTH),
  url: page.url(),
});

const clickAndWaitForOdds = async (page: Page, clickAction: () => Promise<void>): Promise<void> => {
  const probe = await captureProbe(page);
  await clickAction();
  const startedAt = Date.now();
  while (Date.now() - startedAt < CHANGE_TIMEOUT_MS) {
    if (page.url() !== probe.url) {
      break;
    }
    const html = (await page.locator(ODDS_LIST_SELECTOR).innerHTML({ timeout: 500 })).slice(
      0,
      CONTENT_PROBE_LENGTH,
    );
    if (html.length > 0 && html !== probe.html) {
      break;
    }
    await page.waitForTimeout(100);
  }
  await page.waitForSelector(ODDS_LIST_SELECTOR, {
    state: "attached",
    timeout: NAVIGATION_TIMEOUT_MS,
  });
};

const navigateFromRacePageToOdds = async (page: Page): Promise<void> => {
  await clickAndWaitForOdds(page, async () => {
    const oddsLink = page.locator("#race_related_link a").filter({ hasText: "オッズ" }).first();
    if ((await oddsLink.count()) > 0) {
      await oddsLink.click({ timeout: CLICK_TIMEOUT_MS });
      return;
    }
    const firstRelatedLink = page.locator("#race_related_link a").first();
    if ((await firstRelatedLink.count()) > 0) {
      await firstRelatedLink.click({ timeout: CLICK_TIMEOUT_MS });
      return;
    }
    const textLink = page.getByText("オッズ", { exact: false }).first();
    if ((await textLink.count()) > 0) {
      await textLink.click({ timeout: CLICK_TIMEOUT_MS });
      return;
    }
    {
      throw new Error("JRA odds link was not found on race entry page.");
    }
  });
};

const getOddsListHtml = async (page: Page): Promise<string> => {
  const innerHtml = await page.locator(ODDS_LIST_SELECTOR).innerHTML();
  const html = `<div id="odds_list">${innerHtml}</div>`;
  if (!html) {
    throw new Error("JRA odds list DOM was not found.");
  }
  return html;
};

export const fetchJraOddsWithPlaywright = async (
  browserBinding: BrowserWorker | undefined,
  entryUrl: string,
): Promise<{
  entryHtml: string;
  latest: Partial<Record<OddsType, OddsData[]>>;
}> => {
  if (!browserBinding) {
    throw new Error("JRA_BROWSER binding is required to fetch JRA odds.");
  }
  const { launch } = await import("@cloudflare/playwright");
  let browser: Browser | null = null;
  try {
    browser = await launch(browserBinding);
    const context = await browser.newContext();
    await setupResourceBlocker(context);
    const page = await context.newPage();
    await page.goto(entryUrl, { timeout: NAVIGATION_TIMEOUT_MS, waitUntil: "domcontentloaded" });
    const entryHtml = await page.content();
    await navigateFromRacePageToOdds(page);
    const latest: Partial<Record<OddsType, OddsData[]>> = {};
    for (const { label, type } of ODDS_PAGE_LABELS) {
      try {
        if (type !== "tansho") {
          await clickAndWaitForOdds(page, () =>
            page.getByText(label, { exact: true }).click({ timeout: CLICK_TIMEOUT_MS }),
          );
        }
        const html = await getOddsListHtml(page);
        latest[type] = parseJraOddsByType(type, html);
      } catch (error) {
        console.warn(`Failed to fetch JRA ${type} odds`, error);
      }
    }
    return { entryHtml, latest };
  } finally {
    await browser?.close();
  }
};

export const fetchJraResultHtmlWithPlaywright = async (
  browserBinding: BrowserWorker | undefined,
  resultUrl: string,
): Promise<string> => {
  if (!browserBinding) {
    throw new Error("JRA_BROWSER binding is required to fetch JRA results.");
  }
  const { launch } = await import("@cloudflare/playwright");
  let browser: Browser | null = null;
  try {
    browser = await launch(browserBinding);
    const context = await browser.newContext();
    await setupResourceBlocker(context);
    const page = await context.newPage();
    await page.goto(resultUrl, { timeout: NAVIGATION_TIMEOUT_MS, waitUntil: "domcontentloaded" });
    return await page.content();
  } finally {
    await browser?.close();
  }
};
