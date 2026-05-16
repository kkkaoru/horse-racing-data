import type { RaceSource } from "./codes";

export interface NetkeibaRaceIdInput {
  kaisaiKai: string | null;
  kaisaiNen: string;
  kaisaiNichime: string | null;
  keibajoCode: string;
  raceBango: string;
  source: RaceSource;
}

export interface NetkeibaTrainingReview {
  commentText: string | null;
  evaluationGrade: string | null;
  evaluationText: string | null;
  horseName: string | null;
  horseNumber: string;
  riderName: string | null;
  trainingDate: string;
}

const HTML_ENTITY_MAP: Record<string, string> = {
  amp: "&",
  gt: ">",
  lt: "<",
  nbsp: " ",
  quot: '"',
};

const cleanHtmlText = (value: string | null | undefined): string =>
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

const escapeRegExp = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");

const extractClassCell = (row: string, className: string): string | null =>
  row.match(
    new RegExp(
      `<(?:td|th|div|span)[^>]*class=["'][^"']*\\b${escapeRegExp(className)}\\b[^"']*["'][^>]*>([\\s\\S]*?)<\\/(?:td|th|div|span)>`,
      "iu",
    ),
  )?.[1] ?? null;

const normalizeHorseNumber = (value: string | null | undefined): string | null => {
  const text = cleanHtmlText(value).replace(/[^\d]/gu, "");
  if (!text) {
    return null;
  }
  const parsed = Number(text);
  return Number.isInteger(parsed) && parsed > 0 ? String(parsed) : null;
};

export const buildNetkeibaRaceId = (input: NetkeibaRaceIdInput): string | null => {
  if (input.source !== "jra" || !input.kaisaiKai || !input.kaisaiNichime) {
    return null;
  }
  return `${input.kaisaiNen}${input.keibajoCode}${input.kaisaiKai}${input.kaisaiNichime}${input.raceBango.padStart(2, "0")}`;
};

export const parseNetkeibaTrainingReviews = (html: string): NetkeibaTrainingReview[] =>
  Array.from(
    html.matchAll(
      /<tr\b[^>]*class\s*=\s*["'][^"']*\bOikiriDataHead\d+\b[^"']*["'][^>]*>([\s\S]*?)<\/tr>/giu,
    ),
  )
    .map((match): NetkeibaTrainingReview | null => {
      const row = match[1] ?? "";
      const horseNumber = normalizeHorseNumber(extractClassCell(row, "Umaban"));
      if (!horseNumber) {
        return null;
      }
      const evaluationText = cleanHtmlText(extractClassCell(row, "Training_Critic"));
      const evaluationGrade =
        cleanHtmlText(
          row.match(/<td\b[^>]*class=["'][^"']*\bRank_[^"']*["'][^>]*>([\s\S]*?)<\/td>/iu)?.[1],
        ) || null;
      if (!evaluationText && !evaluationGrade) {
        return null;
      }
      return {
        commentText: null,
        evaluationGrade,
        evaluationText,
        horseName: cleanHtmlText(extractClassCell(row, "Horse_Name")) || null,
        horseNumber,
        riderName: null,
        trainingDate: "",
      };
    })
    .filter((review): review is NetkeibaTrainingReview => review !== null);
