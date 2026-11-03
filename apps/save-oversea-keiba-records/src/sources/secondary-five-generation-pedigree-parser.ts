// This file runs with Bun. Pure parser for a cached secondary-source five-generation pedigree page.
// It yields the two-generation pedigree of the horse's sire, dam and dam-sire, which lets the
// deeper ancestors (父父父, 父母父, 母父父, 母母父) be read through their parents' rows.
// The table marker and horse route prefix come from an operator-owned private profile.
import type {
  SecondaryHorsePedigree,
  SecondaryPedigreeAncestor,
} from "./secondary-pedigree-parser";

export interface SecondaryFiveGenerationProfile {
  readonly tableMarker: string;
  readonly horsePathPrefix: string;
}
interface PedigreeCell {
  readonly generation: number;
  readonly ancestor: SecondaryPedigreeAncestor | null;
}

const CELL_PATTERN: RegExp = /<td\b([^>]*)>([\s\S]*?)<\/td>/giu;
const ROWSPAN_PATTERN: RegExp = /rowspan=["']?(\d+)/iu;
const TAG_PATTERN: RegExp = /<[^>]+>/gu;
const SPACE_PATTERN: RegExp = /\s+/gu;
// The five-generation page prints a breeding country after the name ("Montjeu (愛)"); the
// two-generation horse page does not, so it is dropped to keep one spelling per ancestor.
const COUNTRY_SUFFIX_PATTERN: RegExp = /\s*\([^()]*\)$/u;
const ID_PATTERN: RegExp = /^[0-9a-z]{10}$/u;
// rowspan of a cell in a 32-row, five-generation table: 16 → 1st generation … none → 5th.
const GENERATION_BY_ROWSPAN: ReadonlyMap<string, number> = new Map([
  ["16", 1],
  ["8", 2],
  ["4", 3],
  ["2", 4],
]);
const LAST_GENERATION: number = 5;
const GENERATION_SIZES: readonly number[] = [2, 4, 8, 16];

const clean = (value: string): string =>
  value
    .replaceAll("&amp;", "&")
    .replaceAll("&nbsp;", " ")
    .replace(TAG_PATTERN, " ")
    .replace(SPACE_PATTERN, " ")
    .trim()
    .replace(COUNTRY_SUFFIX_PATTERN, "");

const escapeRegExp = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");

const cellAncestor = (html: string, linkPattern: RegExp): SecondaryPedigreeAncestor | null => {
  const match: RegExpExecArray | null = linkPattern.exec(html);
  const id: string = match?.[1] ?? "";
  const name: string = clean(match?.[2] ?? "");
  return ID_PATTERN.test(id) && name !== "" ? { sourceHorseId: id, name } : null;
};

const tableCells = (html: string, profile: SecondaryFiveGenerationProfile): PedigreeCell[] => {
  const start: number = html.indexOf(profile.tableMarker);
  const end: number = start < 0 ? -1 : html.indexOf("</table>", start);
  if (end < 0) return [];
  const linkPattern: RegExp = new RegExp(
    `href=["'][^"']*${escapeRegExp(profile.horsePathPrefix)}([0-9a-z]+)\\/?["'][^>]*>([\\s\\S]*?)<\\/a>`,
    "iu",
  );
  return Array.from(html.slice(start, end).matchAll(CELL_PATTERN), (cell) => ({
    generation:
      GENERATION_BY_ROWSPAN.get(ROWSPAN_PATTERN.exec(cell[1] ?? "")?.[1] ?? "") ?? LAST_GENERATION,
    ancestor: cellAncestor(cell[2] ?? "", linkPattern),
  }));
};

// Cells are listed depth-first (sire before dam), so each generation keeps that order.
const generations = (cells: readonly PedigreeCell[]): (SecondaryPedigreeAncestor | null)[][] =>
  GENERATION_SIZES.map((_, index) =>
    cells.filter((cell) => cell.generation === index + 1).map((cell) => cell.ancestor),
  );

const parentRow = (input: {
  readonly horse: SecondaryPedigreeAncestor | null | undefined;
  readonly parents: readonly (SecondaryPedigreeAncestor | null | undefined)[];
  readonly sourceUrl: string;
}): SecondaryHorsePedigree[] => {
  const [sire, sireSire, dam, damSire] = input.parents;
  return input.horse && sire && sireSire && dam && damSire
    ? [
        {
          sourceHorseId: input.horse.sourceHorseId,
          sire,
          sireSire,
          dam,
          damSire,
          sourceUrl: input.sourceUrl,
        },
      ]
    : [];
};

// Returns the rows of the sire, the dam and the dam-sire; an ancestor without complete links is skipped.
export const parseSecondaryAncestorPedigrees = (
  html: string,
  profile: SecondaryFiveGenerationProfile,
  sourceUrl: string,
): SecondaryHorsePedigree[] => {
  const cells: PedigreeCell[] = tableCells(html, profile);
  const levels: (SecondaryPedigreeAncestor | null)[][] = generations(cells);
  const valid: boolean = GENERATION_SIZES.every((size, index) => levels[index]?.length === size);
  if (!valid) return [];
  const [first = [], second = [], third = [], fourth = []] = levels;
  return [
    ...parentRow({
      horse: first[0],
      parents: [second[0], third[0], second[1], third[2]],
      sourceUrl,
    }),
    ...parentRow({
      horse: first[1],
      parents: [second[2], third[4], second[3], third[6]],
      sourceUrl,
    }),
    ...parentRow({
      horse: second[2],
      parents: [third[4], fourth[8], third[5], fourth[10]],
      sourceUrl,
    }),
  ];
};
