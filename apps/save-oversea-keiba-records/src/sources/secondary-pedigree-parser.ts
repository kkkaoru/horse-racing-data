// Pure parser for cached secondary-source pedigree AJAX responses.
// Source-specific table and route markers are supplied by an operator-owned profile.

export interface SecondaryPedigreeMarkupProfile {
  readonly tableMarker: string;
  readonly horsePathPrefix: string;
  readonly sourceUrlTemplate: string;
}

export interface SecondaryPedigreeAncestor {
  readonly sourceHorseId: string;
  readonly name: string;
}

export interface SecondaryHorsePedigree {
  readonly sourceHorseId: string;
  readonly sire: SecondaryPedigreeAncestor;
  readonly sireSire: SecondaryPedigreeAncestor;
  readonly dam: SecondaryPedigreeAncestor;
  readonly damSire: SecondaryPedigreeAncestor;
  readonly sourceUrl: string;
}

const CELL_PATTERN = /<td\b[^>]*>([\s\S]*?)<\/td>/giu;
const TAG_PATTERN = /<[^>]+>/gu;
const SPACE_PATTERN = /\s+/gu;
const EXPECTED_ANCESTOR_COUNT = 6;
const SIRE_INDEX = 0;
const SIRE_SIRE_INDEX = 1;
const DAM_INDEX = 3;
const DAM_SIRE_INDEX = 4;

const clean = (value: string): string =>
  value
    .replaceAll("&amp;", "&")
    .replaceAll("&nbsp;", " ")
    .replace(TAG_PATTERN, " ")
    .replace(SPACE_PATTERN, " ")
    .trim();

const escapeRegExp = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");

const parseResponse = (json: string): string => {
  const parsed: unknown = JSON.parse(json);
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error("Secondary pedigree response must be an object.");
  }
  if (!("status" in parsed) || !("data" in parsed)) {
    throw new Error("Secondary pedigree response is missing status or data.");
  }
  if (parsed.status !== "OK" || typeof parsed.data !== "string") {
    throw new Error("Secondary pedigree response is not successful.");
  }
  return parsed.data;
};

const parseAncestors = (
  html: string,
  profile: SecondaryPedigreeMarkupProfile,
): SecondaryPedigreeAncestor[] => {
  const markerIndex = html.indexOf(profile.tableMarker);
  if (markerIndex < 0) throw new Error("Secondary pedigree table marker was not found.");
  const tableEnd = html.indexOf("</table>", markerIndex);
  if (tableEnd < 0) throw new Error("Secondary pedigree table is not closed.");
  const pathPattern = new RegExp(
    `href=["'][^"']*${escapeRegExp(profile.horsePathPrefix)}([^/"'?#\\s]+)\\/?["'][^>]*>([\\s\\S]*?)<\\/a>`,
    "iu",
  );
  return Array.from(html.slice(markerIndex, tableEnd).matchAll(CELL_PATTERN), (cell) => {
    const match = pathPattern.exec(cell[0]);
    if (!match?.[1] || !match[2]) {
      throw new Error("Secondary pedigree cell is missing an ancestor link.");
    }
    const name = clean(match[2]);
    if (name === "") throw new Error("Secondary pedigree ancestor name is empty.");
    return { sourceHorseId: match[1], name };
  });
};

type CompleteSecondaryPedigree = readonly [
  SecondaryPedigreeAncestor,
  SecondaryPedigreeAncestor,
  SecondaryPedigreeAncestor,
  SecondaryPedigreeAncestor,
  SecondaryPedigreeAncestor,
  SecondaryPedigreeAncestor,
];

const isCompletePedigree = (
  ancestors: readonly SecondaryPedigreeAncestor[],
): ancestors is CompleteSecondaryPedigree => ancestors.length === EXPECTED_ANCESTOR_COUNT;

export const parseSecondaryHorsePedigree = (
  json: string,
  sourceHorseId: string,
  profile: SecondaryPedigreeMarkupProfile,
): SecondaryHorsePedigree => {
  const ancestors = parseAncestors(parseResponse(json), profile);
  if (!isCompletePedigree(ancestors)) {
    throw new Error(`Secondary pedigree has ${String(ancestors.length)} ancestors.`);
  }
  return {
    sourceHorseId,
    sire: ancestors[SIRE_INDEX],
    sireSire: ancestors[SIRE_SIRE_INDEX],
    dam: ancestors[DAM_INDEX],
    damSire: ancestors[DAM_SIRE_INDEX],
    sourceUrl: profile.sourceUrlTemplate.replaceAll("{HORSE_ID}", sourceHorseId),
  };
};
