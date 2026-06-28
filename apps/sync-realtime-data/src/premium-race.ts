import { formatError } from "./format-error";
import type { NarRaceSource } from "./types";

export interface PremiumRaceConfig {
  commentPathTemplate: string | null;
  cookie: string | null;
  dataTopPathTemplate: string | null;
  entryLinkPattern: string | null;
  narOrigin: string | null;
  narTopPathTemplate: string | null;
  origin: string | null;
  paddockPathTemplate: string | null;
  proxyBearer: string | null;
  proxyUrl: string | null;
  proxyUserId: string | null;
  responseCharset: string | null;
  sourceIdQueryKey: string;
  topPathTemplate: string | null;
  workPathTemplate: string | null;
}

export interface PremiumFetchAttempt {
  html: string;
  mode: "cookie" | "direct" | "proxy";
}

export interface PremiumRaceLink {
  entryUrl: string;
  sourceRaceId: string;
}

export interface PremiumTrainingReview {
  commentText: string | null;
  evaluationGrade: string | null;
  evaluationText: string | null;
  horseName: string | null;
  horseNumber: string;
  riderName: string | null;
  trainingDate: string;
}

export interface PremiumStableComment {
  commentText: string;
  evaluationGrade: number | null;
  evaluationText: string | null;
  frameNumber: string | null;
  horseName: string | null;
  horseNumber: string;
}

export interface PremiumPaddockBulletin {
  commentText: string | null;
  evaluationText: string | null;
  frameNumber: string | null;
  groupKey: "favorite" | "value";
  horseName: string | null;
  horseNumber: string;
}

export interface PremiumDataTopHorse {
  horseName: string | null;
  horseNumber: string;
  rank: number;
  reasons: string[];
}

export interface PremiumPaddockParseResult {
  authRequired: boolean;
  bulletins: PremiumPaddockBulletin[];
  pending: boolean;
  unavailable: boolean;
}

type EnvLike = {
  PREMIUM_RACE_COMMENT_PATH_TEMPLATE?: string;
  PREMIUM_RACE_COOKIE?: string;
  PREMIUM_RACE_DATA_TOP_AREA_CLASS?: string;
  PREMIUM_RACE_DATA_TOP_HORSE_LINK_CLASS?: string;
  PREMIUM_RACE_DATA_TOP_HORSE_NUMBER_CLASS?: string;
  PREMIUM_RACE_DATA_TOP_PATH_TEMPLATE?: string;
  PREMIUM_RACE_DATA_TOP_REASON_LIST_CLASS?: string;
  PREMIUM_RACE_ENTRY_LINK_PATTERN?: string;
  PREMIUM_RACE_NAR_ORIGIN?: string;
  PREMIUM_RACE_NAR_TOP_PATH_TEMPLATE?: string;
  PREMIUM_RACE_ORIGIN?: string;
  PREMIUM_RACE_PADDOCK_PATH_TEMPLATE?: string;
  PREMIUM_RACE_PROXY_BEARER?: string;
  PREMIUM_RACE_PROXY_URL?: string;
  PREMIUM_RACE_PROXY_USER_ID?: string;
  PREMIUM_RACE_RESPONSE_CHARSET?: string;
  PREMIUM_RACE_SOURCE_ID_QUERY_KEY?: string;
  PREMIUM_RACE_TOP_PATH_TEMPLATE?: string;
  PREMIUM_RACE_WORK_PATH_TEMPLATE?: string;
};

const HTML_ENTITY_MAP: Record<string, string> = {
  amp: "&",
  gt: ">",
  lt: "<",
  nbsp: " ",
  quot: '"',
};

export const BAN_EI_KEIBAJO_CODE = "83";

// 2026-06-28: restricted from "jra or non-Ban-ei nar" to "jra only".
// NAR premium scraping returns no data from netkeiba and the failing
// fetches clog the main jobs queue, blocking fetch-results / fetch-weights
// for hours during race-day. Same JRA-only gate as fetch-premium-paddock.
export const isPremiumRaceDataTarget = (
  race: Pick<NarRaceSource, "keibajoCode" | "source">,
): boolean => race.source === "jra";

const DEFAULT_NAR_PREMIUM_ORIGIN = "https://nar.netkeiba.com";

export const getPremiumRaceConfig = (env: EnvLike): PremiumRaceConfig => ({
  commentPathTemplate: env.PREMIUM_RACE_COMMENT_PATH_TEMPLATE ?? null,
  cookie: env.PREMIUM_RACE_COOKIE ?? null,
  dataTopPathTemplate: env.PREMIUM_RACE_DATA_TOP_PATH_TEMPLATE ?? null,
  entryLinkPattern: env.PREMIUM_RACE_ENTRY_LINK_PATTERN ?? null,
  narOrigin: env.PREMIUM_RACE_NAR_ORIGIN ?? DEFAULT_NAR_PREMIUM_ORIGIN,
  narTopPathTemplate: env.PREMIUM_RACE_NAR_TOP_PATH_TEMPLATE ?? null,
  origin: env.PREMIUM_RACE_ORIGIN ?? null,
  paddockPathTemplate: env.PREMIUM_RACE_PADDOCK_PATH_TEMPLATE ?? null,
  proxyBearer: env.PREMIUM_RACE_PROXY_BEARER ?? null,
  proxyUrl: env.PREMIUM_RACE_PROXY_URL ?? null,
  proxyUserId: env.PREMIUM_RACE_PROXY_USER_ID ?? null,
  responseCharset: env.PREMIUM_RACE_RESPONSE_CHARSET ?? null,
  sourceIdQueryKey: env.PREMIUM_RACE_SOURCE_ID_QUERY_KEY ?? "race_id",
  topPathTemplate: env.PREMIUM_RACE_TOP_PATH_TEMPLATE ?? null,
  workPathTemplate: env.PREMIUM_RACE_WORK_PATH_TEMPLATE ?? null,
});

export const hasPremiumRaceFetchConfig = (config: PremiumRaceConfig): boolean =>
  Boolean(config.origin);

export const renderPremiumTemplate = (template: string, params: Record<string, string>): string =>
  Object.entries(params).reduce(
    (current, [key, value]) => current.replaceAll(`{${key}}`, value),
    template,
  );

export const buildPremiumUrl = (
  config: PremiumRaceConfig,
  template: string | null,
  params: Record<string, string>,
  options: { source?: "jra" | "nar" } = {},
): string | null => {
  if (!template) {
    return null;
  }
  const origin = options.source === "nar" ? config.narOrigin : config.origin;
  if (!origin) {
    return null;
  }
  const path = renderPremiumTemplate(template, params);
  return new URL(path, origin).toString();
};

// netkeiba renders the `Icon_Account` class only when the visitor is
// signed in. Cookie-mode and proxy-mode both produce a "200 OK + valid
// HTML" response even when the session was rejected, so we pick the
// attempt whose body proves authentication instead of blindly trusting
// the first non-error attempt.
const PREMIUM_HTML_AUTHENTICATED_MARKER = "Icon_Account";

export const fetchPremiumHtml = async (
  config: PremiumRaceConfig,
  targetUrl: string,
): Promise<string> => {
  const attempts = await fetchPremiumHtmlAttempts(config, targetUrl);
  const authenticated = attempts.find((attempt) =>
    attempt.html.includes(PREMIUM_HTML_AUTHENTICATED_MARKER),
  );
  // fetchPremiumHtmlAttempts throws when no attempt succeeds, so attempts[0] is always defined here.
  return (authenticated ?? attempts[0]!).html;
};

export const fetchPremiumHtmlAttempts = async (
  config: PremiumRaceConfig,
  targetUrl: string,
): Promise<PremiumFetchAttempt[]> => {
  if (!hasPremiumRaceFetchConfig(config)) {
    throw new Error("premium race fetch config is incomplete");
  }
  const hasCookie = Boolean(config.cookie);
  const hasProxy = Boolean(config.proxyUrl && config.proxyUserId && config.proxyBearer);
  const attempts: Array<{
    headers: Record<string, string>;
    mode: PremiumFetchAttempt["mode"];
    url: string;
  }> = [];
  if (hasProxy) {
    const requestUrl = new URL(config.proxyUrl as string);
    requestUrl.searchParams.set("url", targetUrl);
    requestUrl.searchParams.set("user_id", config.proxyUserId as string);
    requestUrl.searchParams.set("cache", "0");
    attempts.push({
      headers: { Authorization: `Bearer ${config.proxyBearer}` },
      mode: "proxy",
      url: requestUrl.toString(),
    });
  }
  if (hasCookie) {
    attempts.push({
      headers: { Cookie: config.cookie as string },
      mode: "cookie",
      url: targetUrl,
    });
  }
  attempts.push({ headers: {}, mode: "direct", url: targetUrl });

  const results: PremiumFetchAttempt[] = [];
  const errors: string[] = [];
  for (const attempt of attempts) {
    const response = await fetch(attempt.url, {
      headers: {
        ...attempt.headers,
        "User-Agent":
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36",
      },
    }).catch((error: unknown) => {
      errors.push(`${attempt.mode}: ${formatError(error)}`);
      return null;
    });
    if (!response?.ok) {
      errors.push(`${attempt.mode}: ${response ? response.status : "fetch_failed"}`);
      continue;
    }
    const buffer = await response.arrayBuffer();
    const charset = config.responseCharset ?? detectHtmlCharset(buffer) ?? "utf-8";
    results.push({ html: new TextDecoder(charset).decode(buffer), mode: attempt.mode });
  }
  if (results.length === 0) {
    throw new Error(`premium race fetch failed: ${errors.join("; ")}`);
  }
  return results;
};

const detectHtmlCharset = (buffer: ArrayBuffer): string | null => {
  const head = String.fromCharCode(...new Uint8Array(buffer.slice(0, 1024)));
  const charset =
    head.match(/charset=["']?([a-z0-9_-]+)/iu)?.[1] ??
    head.match(/content=["'][^"']*charset=([a-z0-9_-]+)/iu)?.[1] ??
    null;
  return charset ? charset.toLowerCase() : null;
};

export const extractPremiumSourceRaceId = (
  value: string,
  sourceIdQueryKey: string,
): string | null => {
  try {
    return new URL(value, "https://local.invalid").searchParams.get(sourceIdQueryKey);
  } catch {
    return null;
  }
};

const cleanText = (value: string | null | undefined): string =>
  (value ?? "")
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/giu, " ")
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/giu, " ")
    .replace(/<!--[\s\S]*?-->/gu, " ")
    .replace(/<br\s*\/?>/giu, " ")
    .replace(/<[^>]*>/gu, "")
    .replace(/&([a-z]+);/giu, (_, key: string) => HTML_ENTITY_MAP[key.toLowerCase()] ?? "")
    .replace(/&#(\d+);/gu, (_, key: string) => String.fromCodePoint(Number(key)))
    .replace(/\s+/gu, " ")
    .trim();

const normalizeHorseNumber = (value: string | null | undefined): string | null => {
  const text = cleanText(value).replace(/[^\d]/gu, "");
  if (!text) {
    return null;
  }
  const parsed = Number(text);
  return Number.isInteger(parsed) && parsed > 0 ? String(parsed) : null;
};

const escapeRegExp = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");

const stripHtmlComments = (value: string): string => value.replace(/<!--[\s\S]*?-->/gu, " ");

const extractClassCell = (row: string, className: string | null | undefined): string | null => {
  if (!className) {
    return null;
  }
  const classPattern = className.endsWith("*")
    ? `${escapeRegExp(className.slice(0, -1))}[^"']*`
    : `\\b${escapeRegExp(className)}\\b`;
  return (
    row.match(
      new RegExp(
        `<(?:td|th|div|span|p)[^>]*class=["'][^"']*${classPattern}[^"']*["'][^>]*>([\\s\\S]*?)<\\/(?:td|th|div|span|p)>`,
        "iu",
      ),
    )?.[1] ?? null
  );
};

const extractRowsByClass = (html: string, className: string | null | undefined): string[] => {
  if (className === "*") {
    return Array.from(html.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/giu)).map((match) => match[1]!);
  }
  if (!className) {
    return [];
  }
  const escaped = escapeRegExp(className);
  return Array.from(
    html.matchAll(
      new RegExp(
        `<tr[^>]*class=["'][^"']*\\b${escaped}\\b[^"']*["'][^>]*>([\\s\\S]*?)<\\/tr>`,
        "giu",
      ),
    ),
  ).map((match) => match[1]!);
};

const extractTableCells = (row: string): string[] =>
  Array.from(row.matchAll(/<td\b[^>]*>([\s\S]*?)<\/td>/giu)).map((match) => match[0]!);

const findCellIndexByClass = (cells: string[], className: string | null | undefined): number => {
  if (!className) {
    return -1;
  }
  const classPattern = className.endsWith("*")
    ? `${escapeRegExp(className.slice(0, -1))}[^"']*`
    : `\\b${escapeRegExp(className)}\\b`;
  return cells.findIndex((cell) =>
    new RegExp(`class=["'][^"']*${classPattern}[^"']*["']`, "iu").test(cell),
  );
};

const extractRelativeCellText = (
  row: string,
  anchorClassName: string | null | undefined,
  offset: number,
): string => {
  const cells = extractTableCells(row);
  const anchorIndex = findCellIndexByClass(cells, anchorClassName);
  if (anchorIndex < 0) {
    return "";
  }
  return cleanText(cells[anchorIndex + offset] ?? null);
};

const extractTablesByClass = (
  html: string,
  className: string | null | undefined,
): { before: string; fullHtml: string; html: string }[] => {
  if (!className) {
    return [];
  }
  const escaped = escapeRegExp(className);
  const matches = Array.from(
    html.matchAll(
      new RegExp(
        `<table[^>]*class=["'][^"']*\\b${escaped}\\b[^"']*["'][^>]*>([\\s\\S]*?)<\\/table>`,
        "giu",
      ),
    ),
  );
  return matches.map((match, index) => {
    const startIndex = match.index!;
    const previousEnd =
      index === 0
        ? Math.max(0, startIndex - 1200)
        : matches[index - 1]!.index! + matches[index - 1]![0].length;
    return {
      before: html.slice(previousEnd, startIndex),
      fullHtml: match[0]!,
      html: match[1]!,
    };
  });
};

const extractTableCellRows = (html: string): string[][] =>
  Array.from(html.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/giu))
    .map((rowMatch) =>
      Array.from(rowMatch[1]!.matchAll(/<(?:td|th)[^>]*>([\s\S]*?)<\/(?:td|th)>/giu))
        .map((cellMatch) => cleanText(cellMatch[1]))
        .filter((cell) => cell !== ""),
    )
    .filter((cells) => cells.length > 0);

const extractRawTableCellRows = (html: string): { html: string; text: string }[][] =>
  Array.from(html.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/giu))
    .map((rowMatch) =>
      Array.from(rowMatch[1]!.matchAll(/<(?:td|th)[^>]*>([\s\S]*?)<\/(?:td|th)>/giu)).map(
        (cellMatch) => ({
          html: cellMatch[1]!,
          text: cleanText(cellMatch[1]),
        }),
      ),
    )
    .filter((cells) => cells.some((cell) => cell.text || cell.html));

const extractStableEvaluationGrade = (
  row: string,
  className: string | null | undefined,
): number | null => {
  const evaluationCell = extractClassCell(row, className);
  const mark = evaluationCell?.match(/Icon_Mark_0([123])\b/u)?.[1] ?? null;
  return mark ? Number(mark) : null;
};

const extractStableEvaluationGradeFromCells = (
  cells: { html: string; text: string }[],
): number | null => {
  const mark = cells
    .map((cell) => cell.html)
    .join(" ")
    .match(/Icon_Mark_0([123])\b/u)?.[1];
  return mark ? Number(mark) : null;
};

export const discoverPremiumRaceLinks = (
  html: string,
  config: PremiumRaceConfig,
): PremiumRaceLink[] => {
  const pattern =
    config.entryLinkPattern ??
    `href=["']([^"']*\\?(?:[^"']*&)?${escapeRegExp(config.sourceIdQueryKey)}=\\d+[^"']*)["']`;
  const links = Array.from(html.matchAll(new RegExp(pattern, "giu")))
    .map((match) => match[1])
    .filter((value): value is string => Boolean(value))
    .map((href) => {
      const entryUrl = config.origin ? new URL(href, config.origin).toString() : href;
      const sourceRaceId = extractPremiumSourceRaceId(entryUrl, config.sourceIdQueryKey);
      return sourceRaceId ? { entryUrl, sourceRaceId } : null;
    })
    .filter((link): link is PremiumRaceLink => link !== null);
  return Array.from(new Map(links.map((link) => [link.sourceRaceId, link])).values());
};

export const buildJraPremiumSourceRaceId = (
  race: Pick<
    NarRaceSource,
    "kaisaiKai" | "kaisaiNichime" | "kaisaiNen" | "keibajoCode" | "raceBango" | "source"
  >,
): string | null => {
  if (race.source !== "jra" || !race.kaisaiKai || !race.kaisaiNichime) {
    return null;
  }
  return `${race.kaisaiNen}${race.keibajoCode}${race.kaisaiKai}${race.kaisaiNichime}${race.raceBango.padStart(2, "0")}`;
};

// netkeiba NAR `race_id` is `YYYY` + venue(2) + `MMDD`(4) + race(2) — 12 digits.
// Sample observed: `202554110906` = 2025 / 高知(54) / 11-09 / R06. Ban-ei
// (keibajo 83) deliberately stays out: the user excludes it from data-top
// coverage.
export const buildNarPremiumSourceRaceId = (
  race: Pick<NarRaceSource, "kaisaiNen" | "kaisaiTsukihi" | "keibajoCode" | "raceBango" | "source">,
): string | null => {
  if (race.source !== "nar" || race.keibajoCode === BAN_EI_KEIBAJO_CODE) {
    return null;
  }
  if (!/^\d{4}$/.test(race.kaisaiTsukihi)) {
    return null;
  }
  return `${race.kaisaiNen}${race.keibajoCode}${race.kaisaiTsukihi}${race.raceBango.padStart(2, "0")}`;
};

export const buildPremiumRaceLinkFromRace = (
  race: NarRaceSource,
  config: PremiumRaceConfig,
): PremiumRaceLink | null => {
  const sourceRaceId = buildJraPremiumSourceRaceId(race) ?? buildNarPremiumSourceRaceId(race);
  if (!sourceRaceId) {
    return null;
  }
  const entryUrl =
    buildPremiumUrl(
      config,
      config.dataTopPathTemplate,
      { sourceRaceId },
      { source: race.source },
    ) ??
    buildPremiumUrl(config, config.workPathTemplate, { sourceRaceId }, { source: race.source }) ??
    `${config.sourceIdQueryKey}=${sourceRaceId}`;
  return { entryUrl, sourceRaceId };
};

export const sourceRaceIdCandidates = (race: NarRaceSource): string[] => {
  const jraId = buildJraPremiumSourceRaceId(race);
  const narId = buildNarPremiumSourceRaceId(race);
  return [
    ...(jraId ? [jraId] : []),
    ...(narId ? [narId] : []),
    `${race.kaisaiNen}${race.keibajoCode}${race.raceBango}`,
    `${race.kaisaiNen}${race.keibajoCode}${race.raceBango.replace(/^0+/u, "")}`,
  ];
};

export const matchPremiumLinkToRace = (
  links: PremiumRaceLink[],
  race: NarRaceSource,
): PremiumRaceLink | null => {
  const raceNumber = race.raceBango.replace(/^0+/u, "");
  return (
    links.find((link) =>
      sourceRaceIdCandidates(race).some((item) => link.sourceRaceId.endsWith(item)),
    ) ??
    links.find((link) => link.sourceRaceId.endsWith(race.raceBango)) ??
    links.find((link) => link.sourceRaceId.endsWith(raceNumber)) ??
    null
  );
};

export const parsePremiumTrainingReviews = (
  html: string,
  env: {
    PREMIUM_RACE_WORK_COMMENT_CLASS?: string;
    PREMIUM_RACE_WORK_DATE_CLASS?: string;
    PREMIUM_RACE_WORK_GRADE_CLASS?: string;
    PREMIUM_RACE_WORK_HORSE_NAME_CLASS?: string;
    PREMIUM_RACE_WORK_HORSE_NUMBER_CLASS?: string;
    PREMIUM_RACE_WORK_RIDER_CLASS?: string;
    PREMIUM_RACE_WORK_ROW_CLASS?: string;
    PREMIUM_RACE_WORK_TEXT_CLASS?: string;
  },
): PremiumTrainingReview[] => {
  const rows = extractRowsByClass(html, env.PREMIUM_RACE_WORK_ROW_CLASS);
  const reviews: PremiumTrainingReview[] = [];
  let currentHorse: {
    actionComment: string | null;
    horseName: string | null;
    horseNumber: string;
  } | null = null;

  for (const row of rows) {
    const rowHorseNumber = normalizeHorseNumber(
      extractClassCell(row, env.PREMIUM_RACE_WORK_HORSE_NUMBER_CLASS),
    );
    if (rowHorseNumber) {
      currentHorse = {
        actionComment:
          cleanText(extractClassCell(row, env.PREMIUM_RACE_WORK_COMMENT_CLASS)) || null,
        horseName: cleanText(extractClassCell(row, env.PREMIUM_RACE_WORK_HORSE_NAME_CLASS)) || null,
        horseNumber: rowHorseNumber,
      };
    }

    const horseNumber = rowHorseNumber ?? currentHorse?.horseNumber ?? null;
    const trainingDate = cleanText(extractClassCell(row, env.PREMIUM_RACE_WORK_DATE_CLASS));
    const evaluationText = cleanText(extractClassCell(row, env.PREMIUM_RACE_WORK_TEXT_CLASS));
    const evaluationGrade = cleanText(extractClassCell(row, env.PREMIUM_RACE_WORK_GRADE_CLASS));
    const riderName =
      cleanText(extractClassCell(row, env.PREMIUM_RACE_WORK_RIDER_CLASS)) ||
      extractRelativeCellText(row, env.PREMIUM_RACE_WORK_DATE_CLASS, 3);
    if (!horseNumber || (!trainingDate && !evaluationText && !evaluationGrade && !riderName)) {
      continue;
    }
    reviews.push({
      commentText:
        cleanText(extractClassCell(row, env.PREMIUM_RACE_WORK_COMMENT_CLASS)) ||
        currentHorse?.actionComment ||
        null,
      evaluationGrade: evaluationGrade || null,
      evaluationText: evaluationText || null,
      horseName:
        cleanText(extractClassCell(row, env.PREMIUM_RACE_WORK_HORSE_NAME_CLASS)) ||
        currentHorse?.horseName ||
        null,
      horseNumber,
      riderName: riderName || null,
      trainingDate,
    });
  }

  return reviews;
};

export const parsePremiumStableComments = (
  html: string,
  env: {
    PREMIUM_RACE_COMMENT_LABEL_EVALUATION?: string;
    PREMIUM_RACE_COMMENT_LABEL_FRAME?: string;
    PREMIUM_RACE_COMMENT_LABEL_HORSE_NAME?: string;
    PREMIUM_RACE_COMMENT_LABEL_HORSE_NUMBER?: string;
    PREMIUM_RACE_COMMENT_LABEL_TEXT?: string;
    PREMIUM_RACE_COMMENT_ROW_CLASS?: string;
  },
): PremiumStableComment[] => {
  const classBasedRows = extractRowsByClass(html, env.PREMIUM_RACE_COMMENT_ROW_CLASS)
    .map((row): PremiumStableComment | null => {
      const horseNumber = normalizeHorseNumber(
        extractClassCell(row, env.PREMIUM_RACE_COMMENT_LABEL_HORSE_NUMBER),
      );
      const commentText = cleanText(extractClassCell(row, env.PREMIUM_RACE_COMMENT_LABEL_TEXT));
      if (!horseNumber || !commentText) {
        return null;
      }
      return {
        commentText,
        evaluationGrade: extractStableEvaluationGrade(
          row,
          env.PREMIUM_RACE_COMMENT_LABEL_EVALUATION,
        ),
        evaluationText:
          cleanText(extractClassCell(row, env.PREMIUM_RACE_COMMENT_LABEL_EVALUATION)) || null,
        frameNumber: normalizeHorseNumber(
          extractClassCell(row, env.PREMIUM_RACE_COMMENT_LABEL_FRAME),
        ),
        horseName:
          cleanText(extractClassCell(row, env.PREMIUM_RACE_COMMENT_LABEL_HORSE_NAME)) || null,
        horseNumber,
      };
    })
    .filter((row): row is PremiumStableComment => row !== null);
  if (classBasedRows.length > 0) {
    return classBasedRows;
  }

  return extractRawTableCellRows(html)
    .map((cells): PremiumStableComment | null => {
      const textCells = cells.map((cell) => cell.text);
      const nonEmptyTextCells = textCells.filter((cell) => cell !== "");
      const horseNumber =
        normalizeHorseNumber(textCells[1]) ??
        normalizeHorseNumber(textCells[2]) ??
        normalizeHorseNumber(nonEmptyTextCells[1]);
      const commentText =
        cleanText(textCells[3]) || cleanText(textCells[4]) || cleanText(nonEmptyTextCells[3]);
      if (!horseNumber || !commentText || commentText === "コメント") {
        return null;
      }
      return {
        commentText,
        evaluationGrade: extractStableEvaluationGradeFromCells(cells),
        evaluationText: cleanText(textCells[4]) || null,
        frameNumber:
          normalizeHorseNumber(textCells[0]) ?? normalizeHorseNumber(nonEmptyTextCells[0]),
        horseName: cleanText(textCells[2]) || cleanText(nonEmptyTextCells[2]) || null,
        horseNumber,
      };
    })
    .filter((row): row is PremiumStableComment => row !== null);
};

const PREMIUM_STABLE_COMMENT_FULL_TABLE_CLASS = "Comment_Table_Show_All";

export const isPremiumStableCommentHtmlAuthorized = (html: string): boolean =>
  html.includes(PREMIUM_STABLE_COMMENT_FULL_TABLE_CLASS);

// netkeiba renders this gate text whenever the upstream session is unauthenticated.
// Production verified 2026-06-20: the proxy intermittently returns HTTP 200 with the
// subscription-prompt body, which we used to accept as a "successful" fetch and write
// `status='ok'` with zero stable comments. We keep two substrings so the heuristic
// stays specific (an authenticated detail page can mention "登録" in other contexts).
const PREMIUM_LOGIN_PROMPT_MARKER_PRIMARY = "プレミアムサービス";
const PREMIUM_LOGIN_PROMPT_MARKER_SECONDARY = "登録でご覧になれます";

export const detectPremiumLoginPrompt = (html: string): boolean =>
  html.includes(PREMIUM_LOGIN_PROMPT_MARKER_PRIMARY) &&
  html.includes(PREMIUM_LOGIN_PROMPT_MARKER_SECONDARY);

// Worker reads the previous fetch state's message JSON to lift the auth-retry
// counter so we cap how many times we re-queue the same race while the proxy
// session is broken. Keep the parse defensive so legacy non-JSON messages
// (older error strings) do not crash the handler.
interface PremiumStateMessageShape {
  authRetryCount: number;
}

export const parsePremiumStateMessage = (message: string | null): PremiumStateMessageShape => {
  if (!message) {
    return { authRetryCount: 0 };
  }
  const parsed = safeParseJson(message);
  if (!parsed || typeof parsed !== "object") {
    return { authRetryCount: 0 };
  }
  const candidate = (parsed as Record<string, unknown>).authRetryCount;
  return {
    authRetryCount: typeof candidate === "number" && Number.isFinite(candidate) ? candidate : 0,
  };
};

const safeParseJson = (value: string): unknown => {
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
};

export const summarizePremiumStableCommentHtml = (
  html: string,
): {
  cellRowCount: number;
  textSample: string;
  samples: string[][];
} => {
  const cellRows = extractTableCellRows(html);
  return {
    cellRowCount: cellRows.length,
    textSample: cleanText(html).slice(0, 1200),
    samples: cellRows
      .filter((cells) => cells.some((cell) => cell.length > 0))
      .slice(0, 12)
      .map((cells) => cells.slice(0, 8).map((cell) => cell.slice(0, 80))),
  };
};

// className is always supplied with a non-empty fallback at the call site, so accepting null/undefined here would be dead defensive code.
const extractAreaHtml = (html: string, className: string): string | null => {
  const escaped = escapeRegExp(className);
  return (
    html.match(
      new RegExp(
        `<div[^>]*class=["'][^"']*\\b${escaped}\\b[^"']*["'][^>]*>([\\s\\S]*?)<\\/div>`,
        "iu",
      ),
    )?.[1] ?? null
  );
};

export const parsePremiumDataTopHorses = (
  html: string,
  env: {
    PREMIUM_RACE_DATA_TOP_AREA_CLASS?: string;
    PREMIUM_RACE_DATA_TOP_HORSE_LINK_CLASS?: string;
    PREMIUM_RACE_DATA_TOP_HORSE_NUMBER_CLASS?: string;
    PREMIUM_RACE_DATA_TOP_REASON_LIST_CLASS?: string;
  },
): PremiumDataTopHorse[] => {
  const areaHtml = extractAreaHtml(
    html,
    env.PREMIUM_RACE_DATA_TOP_AREA_CLASS ?? "DataPickupHorseArea",
  );
  if (!areaHtml) {
    return [];
  }
  const horseNumberClass = env.PREMIUM_RACE_DATA_TOP_HORSE_NUMBER_CLASS ?? "Umaban_Num";
  const horseLinkClass = env.PREMIUM_RACE_DATA_TOP_HORSE_LINK_CLASS ?? "data_top_horse_link";
  const reasonListClass = env.PREMIUM_RACE_DATA_TOP_REASON_LIST_CLASS ?? "PickupDataBox";
  const horseNumberPattern = escapeRegExp(horseNumberClass);
  const horseLinkPattern = escapeRegExp(horseLinkClass);
  const reasonListPattern = escapeRegExp(reasonListClass);

  return Array.from(areaHtml.matchAll(/<dl\b[^>]*>([\s\S]*?)<\/dl>/giu))
    .map((match, index): PremiumDataTopHorse | null => {
      const block = match[1]!;
      const horseNumber = normalizeHorseNumber(
        block.match(
          new RegExp(
            `<(?:span|td|div)[^>]*class=["'][^"']*\\b${horseNumberPattern}\\b[^"']*["'][^>]*>([\\s\\S]*?)<\\/(?:span|td|div)>`,
            "iu",
          ),
        )?.[1],
      );
      const horseName =
        cleanText(
          block.match(
            new RegExp(
              `<a[^>]*class=["'][^"']*\\b${horseLinkPattern}\\b[^"']*["'][^>]*>([\\s\\S]*?)<\\/a>`,
              "iu",
            ),
          )?.[1],
        ) || null;
      const reasonBlock =
        block.match(
          new RegExp(
            `<dd[^>]*class=["'][^"']*\\b${reasonListPattern}\\b[^"']*["'][^>]*>([\\s\\S]*?)<\\/dd>`,
            "iu",
          ),
        )?.[1] ?? "";
      const reasons = Array.from(reasonBlock.matchAll(/<li\b[^>]*>([\s\S]*?)<\/li>/giu))
        .map((reasonMatch) => cleanText(reasonMatch[1]))
        .filter((reason) => reason.length > 0);
      if (!horseNumber || reasons.length === 0) {
        return null;
      }
      return {
        horseName,
        horseNumber,
        rank: index + 1,
        reasons,
      };
    })
    .filter((row): row is PremiumDataTopHorse => row !== null);
};

export const parsePremiumPaddockBulletins = (
  html: string,
  env: {
    PREMIUM_RACE_PADDOCK_GROUP_FAVORITE_LABEL?: string;
    PREMIUM_RACE_PADDOCK_GROUP_VALUE_LABEL?: string;
    PREMIUM_RACE_PADDOCK_LABEL_COMMENT?: string;
    PREMIUM_RACE_PADDOCK_LABEL_EVALUATION?: string;
    PREMIUM_RACE_PADDOCK_LABEL_FRAME?: string;
    PREMIUM_RACE_PADDOCK_LABEL_HORSE_NAME?: string;
    PREMIUM_RACE_PADDOCK_LABEL_HORSE_NUMBER?: string;
    PREMIUM_RACE_PADDOCK_PENDING_TEXT?: string;
    PREMIUM_RACE_PADDOCK_ROW_CLASS?: string;
    PREMIUM_RACE_PADDOCK_TABLE_CLASS?: string;
    PREMIUM_RACE_PADDOCK_UNAVAILABLE_TEXT?: string;
  },
): PremiumPaddockParseResult => {
  const activeHtml = stripHtmlComments(html);
  const pageText = cleanText(activeHtml);
  const unavailableText = env.PREMIUM_RACE_PADDOCK_UNAVAILABLE_TEXT;
  const pendingText = env.PREMIUM_RACE_PADDOCK_PENDING_TEXT;
  const unavailable = unavailableText ? pageText.includes(unavailableText) : false;
  const authRequired =
    /Premium_Regist_Box|Premium_Regist_Box02|Premium_Regist_Btn/u.test(activeHtml) ||
    /登録して続きを見る|登録済みの方はこちらからログイン|すでに登録済みの方はここからログイン|ログイン/u.test(
      pageText,
    );
  const pending =
    (pendingText ? pageText.includes(pendingText) : false) ||
    authRequired ||
    /PaddockDummy|SampleDummy/u.test(activeHtml);
  const tableSections = extractTablesByClass(activeHtml, env.PREMIUM_RACE_PADDOCK_TABLE_CLASS);
  const rowGroups =
    tableSections.length > 0
      ? tableSections.flatMap((table) => {
          if (/PaddockDummy|SampleDummy/u.test(table.fullHtml)) {
            return [];
          }
          const heading = cleanText(table.before);
          const groupKey: "favorite" | "value" =
            env.PREMIUM_RACE_PADDOCK_GROUP_VALUE_LABEL &&
            heading.includes(env.PREMIUM_RACE_PADDOCK_GROUP_VALUE_LABEL)
              ? "value"
              : "favorite";
          return extractRowsByClass(table.html, "*").map((row) => ({ groupKey, row }));
        })
      : extractRowsByClass(activeHtml, env.PREMIUM_RACE_PADDOCK_ROW_CLASS).map(
          (row, index, rows) => ({
            groupKey: (index < Math.ceil(rows.length / 2) ? "favorite" : "value") as
              | "favorite"
              | "value",
            row,
          }),
        );
  const bulletins = rowGroups
    .map(({ groupKey, row }): PremiumPaddockBulletin | null => {
      const horseNumber = normalizeHorseNumber(
        extractClassCell(row, env.PREMIUM_RACE_PADDOCK_LABEL_HORSE_NUMBER),
      );
      if (!horseNumber) {
        return null;
      }
      return {
        commentText:
          cleanText(extractClassCell(row, env.PREMIUM_RACE_PADDOCK_LABEL_COMMENT)) || null,
        evaluationText:
          cleanText(extractClassCell(row, env.PREMIUM_RACE_PADDOCK_LABEL_EVALUATION)) || null,
        frameNumber: normalizeHorseNumber(
          extractClassCell(row, env.PREMIUM_RACE_PADDOCK_LABEL_FRAME),
        ),
        groupKey,
        horseName:
          cleanText(extractClassCell(row, env.PREMIUM_RACE_PADDOCK_LABEL_HORSE_NAME)) || null,
        horseNumber,
      };
    })
    .filter((row): row is PremiumPaddockBulletin => row !== null);
  return {
    authRequired: bulletins.length === 0 && authRequired,
    bulletins,
    pending: bulletins.length === 0 && pending,
    unavailable: bulletins.length === 0 && unavailable && !pending,
  };
};
