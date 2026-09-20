// This file runs with Bun. All provider-specific patterns come from a private profile.
import {
  parseSecondaryHorseResults,
  parseSecondaryPersonResults,
  type SecondaryHorseResult,
  type SecondaryPersonKind,
  type SecondaryPersonResult,
  type SecondaryResultMarkupProfile,
} from "./secondary-result-parser";

export type HistorySourceKind = "horse" | SecondaryPersonKind;
export type HistorySourceRow = SecondaryHorseResult | SecondaryPersonResult;

export interface HistorySourcePageProfile {
  readonly markup: SecondaryResultMarkupProfile;
  readonly populationPattern: string;
  readonly nextLabels: readonly string[];
  readonly emptyMarker: string;
}

export interface HistorySourcePageInput {
  readonly html: string;
  readonly url: string;
  readonly kind: HistorySourceKind;
  readonly sourceId: string;
  readonly profile: HistorySourcePageProfile;
}

export interface ParsedHistorySourcePage {
  readonly rows: readonly HistorySourceRow[];
  readonly nextUrl: string | null;
  readonly publishedCount: number;
}

const LINK_PATTERN: RegExp = /<a\b[^>]*href\s*=\s*["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/giu;
const TAG_PATTERN: RegExp = /<[^>]+>/gu;
const INTEGER_PATTERN: RegExp = /^\d+$/u;

const plainText = (text: string): string =>
  text.replace(TAG_PATTERN, " ").replaceAll("&nbsp;", " ").replace(/\s+/gu, " ").trim();

const population = (input: HistorySourcePageInput): number => {
  const captured: string | undefined = new RegExp(input.profile.populationPattern, "u").exec(
    plainText(input.html),
  )?.[1];
  const text: string = captured?.replaceAll(",", "") ?? "";
  const count: number = Number(text);
  if (!INTEGER_PATTERN.test(text) || !Number.isSafeInteger(count)) {
    throw new Error("Published history population could not be verified.");
  }
  return count;
};

const nextPage = (input: HistorySourcePageInput): string | null => {
  const urls: readonly string[] = [
    ...new Set(
      Array.from(input.html.matchAll(LINK_PATTERN))
        .filter((match: RegExpMatchArray): boolean =>
          input.profile.nextLabels.includes(plainText(match[2] ?? "")),
        )
        .map(
          (match: RegExpMatchArray): string =>
            new URL((match[1] ?? "").replaceAll("&amp;", "&"), input.url).href,
        ),
    ),
  ];
  if (urls.length > 1) throw new Error("Published history next-page links disagree.");
  return urls[0] ?? null;
};

export const parseHistorySourcePage = (input: HistorySourcePageInput): ParsedHistorySourcePage => {
  if (!/<\/html>/iu.test(input.html)) throw new Error("History source document is incomplete.");
  const publishedCount: number = population(input);
  const nextUrl: string | null = nextPage(input);
  if (
    publishedCount === 0 &&
    input.profile.emptyMarker !== "" &&
    input.html.includes(input.profile.emptyMarker)
  ) {
    if (nextUrl !== null) throw new Error("Empty published history unexpectedly has pagination.");
    return { rows: [], nextUrl, publishedCount };
  }
  const rows: readonly HistorySourceRow[] =
    input.kind === "horse"
      ? parseSecondaryHorseResults(input.html, input.sourceId, input.profile.markup)
      : parseSecondaryPersonResults(input.html, input.kind, input.sourceId, input.profile.markup);
  if (rows.length === 0 || rows.length > publishedCount) {
    throw new Error("Parsed history rows disagree with the published population.");
  }
  return { rows, nextUrl, publishedCount };
};
