// This file runs with Bun.
//
// Pure secondary-source racecard parser.
// Input: HTML string for one overseas racecard table from the secondary source,
//        plus an operator-supplied markup profile that describes how to locate
//        horse-number cells, gate cells, entity links, and affiliation labels.
// Output: runners keyed/sorted by horse number, plus non-throwing parse issues.
//
// The committed code is intentionally selector-agnostic. Live class tokens and
// identity-route prefixes are never hardcoded here: the operator supplies them
// via a local JSON profile (see OVERSEA_SECONDARY_MARKUP_PROFILE_PATH). The
// secondary source restricts automated access, so its markup structure is not
// published in this repository.
//
// Result shape:
//   SecondarySourceParseResult {
//     runners: SecondarySourceRunner[]  // complete enough to join by horseNumber
//     issues: SecondarySourceParseIssue[]  // missing/invalid fields, duplicates, empty docs
//   }
//
// SecondarySourceRunner fields:
//   horseNumber, gate, horseName, horseId, jockeyId, trainerId, trainerAffiliation
//
// Incomplete rows still surface as issues. A row without a horse-number cell does not
// become a runner. Identity fields that fail to parse become null and emit an issue.
// The pure parser never performs network, filesystem, or database I/O. The profile
// loader is the only I/O surface, and tests inject a fake file reader.

export type SecondarySourceParseIssueCode =
  | "duplicate_horse_number"
  | "invalid_gate"
  | "invalid_horse_number"
  | "missing_gate"
  | "missing_horse_id"
  | "missing_horse_name"
  | "missing_horse_number"
  | "missing_jockey_id"
  | "missing_trainer_affiliation"
  | "missing_trainer_id"
  | "no_runner_rows";

export interface SecondarySourceRunner {
  horseNumber: number;
  gate: number | null;
  horseName: string | null;
  horseId: string | null;
  jockeyId: string | null;
  trainerId: string | null;
  trainerAffiliation: string | null;
}

export interface SecondarySourceParseIssue {
  code: SecondarySourceParseIssueCode;
  message: string;
  rowIndex: number;
  horseNumber: number | null;
}

export interface SecondarySourceParseResult {
  runners: readonly SecondarySourceRunner[];
  issues: readonly SecondarySourceParseIssue[];
}

/**
 * Operator-supplied markup profile for one secondary-source document shape.
 * Tokens describe how to locate runner fields; they are not committed defaults.
 */
export interface SecondarySourceMarkupProfile {
  readonly horseNumberClassToken: string;
  readonly gateClassToken: string;
  readonly horsePathSegment: string;
  readonly jockeyPathPrefix: string;
  readonly trainerPathPrefix: string;
  readonly affiliationLabels: readonly string[];
}

export interface ParseSecondarySourceRacecardInput {
  readonly html: string;
  readonly profile: SecondarySourceMarkupProfile;
}

export interface LoadSecondarySourceMarkupProfileInput {
  readonly env: Readonly<Record<string, string | undefined>>;
  readonly readTextFile: (path: string) => string;
}

interface CaptureMatchInput {
  html: string;
  pattern: RegExp;
}

interface OptionalNumberParseResult {
  value: number | null;
  invalid: boolean;
  present: boolean;
}

interface RowFieldExtraction {
  horseNumberRaw: string | null;
  gateRaw: string | null;
  horseId: string | null;
  horseName: string | null;
  jockeyId: string | null;
  trainerId: string | null;
  trainerAffiliation: string | null;
}

interface CompiledMarkupPatterns {
  readonly horseNumberCellPattern: RegExp;
  readonly gateCellPattern: RegExp;
  readonly horseIdPattern: RegExp;
  readonly horseAnchorPattern: RegExp;
  readonly jockeyIdPattern: RegExp;
  readonly trainerIdPattern: RegExp;
  readonly trainerAffiliationPattern: RegExp;
  readonly horsePathSegment: string;
}

interface RowBuildInput {
  rowHtml: string;
  rowIndex: number;
  patterns: CompiledMarkupPatterns;
}

interface RowBuildResult {
  runner: SecondarySourceRunner | null;
  issues: readonly SecondarySourceParseIssue[];
}

interface IssueBuildInput {
  code: SecondarySourceParseIssueCode;
  message: string;
  rowIndex: number;
  horseNumber: number | null;
}

interface CandidateRow {
  readonly rowHtml: string;
  readonly rowIndex: number;
}

export const OVERSEA_SECONDARY_MARKUP_PROFILE_PATH: string =
  "OVERSEA_SECONDARY_MARKUP_PROFILE_PATH";

const ROW_PATTERN: RegExp = /<tr\b[^>]*>([\s\S]*?)<\/tr>/gi;
const TAG_PATTERN: RegExp = /<[^>]+>/g;
const WHITESPACE_PATTERN: RegExp = /\s+/g;
const DIGITS_ONLY_PATTERN: RegExp = /^\d+$/;
const HORSE_TITLE_PATTERN: RegExp = /\btitle="([^"]*)"/i;
const REGEXP_ESCAPE_PATTERN: RegExp = /[.*+?^${}()|[\]\\]/g;

const FIRST_CAPTURE_GROUP_INDEX: number = 1;
const NO_ROW_INDEX: number = -1;

type ProfileStringKey =
  | "horseNumberClassToken"
  | "gateClassToken"
  | "horsePathSegment"
  | "jockeyPathPrefix"
  | "trainerPathPrefix";

const isJsonObject = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);

const isNonEmptyString = (value: unknown): value is string =>
  typeof value === "string" && value.length > 0;

const isNonEmptyStringArray = (value: unknown): value is string[] =>
  Array.isArray(value) &&
  value.length > 0 &&
  value.every((item: unknown): boolean => isNonEmptyString(item));

const escapeRegExp = (value: string): string => value.replace(REGEXP_ESCAPE_PATTERN, "\\$&");

const requireProfileString = (record: Record<string, unknown>, key: ProfileStringKey): string => {
  const value: unknown = record[key];
  if (!isNonEmptyString(value)) {
    throw new Error(`Secondary-source markup profile field "${key}" must be a non-empty string.`);
  }
  return value;
};

/**
 * Parse and validate a JSON markup-profile document into a typed profile.
 * Throws English errors when the document is not a usable profile object.
 */
export const parseSecondarySourceMarkupProfileJson = (
  rawJson: string,
): SecondarySourceMarkupProfile => {
  const parsed: unknown = JSON.parse(rawJson);
  if (!isJsonObject(parsed)) {
    throw new Error("Secondary-source markup profile must be a JSON object.");
  }

  const affiliationLabelsRaw: unknown = parsed.affiliationLabels;
  if (!isNonEmptyStringArray(affiliationLabelsRaw)) {
    throw new Error(
      'Secondary-source markup profile field "affiliationLabels" must be a non-empty array of non-empty strings.',
    );
  }

  return {
    horseNumberClassToken: requireProfileString(parsed, "horseNumberClassToken"),
    gateClassToken: requireProfileString(parsed, "gateClassToken"),
    horsePathSegment: requireProfileString(parsed, "horsePathSegment"),
    jockeyPathPrefix: requireProfileString(parsed, "jockeyPathPrefix"),
    trainerPathPrefix: requireProfileString(parsed, "trainerPathPrefix"),
    affiliationLabels: affiliationLabelsRaw.slice(),
  };
};

/**
 * Load the operator-supplied markup profile from the path in
 * OVERSEA_SECONDARY_MARKUP_PROFILE_PATH. The reader is injected so tests never
 * touch the filesystem. The path itself must stay outside version control.
 */
export const loadSecondarySourceMarkupProfile = ({
  env,
  readTextFile,
}: LoadSecondarySourceMarkupProfileInput): SecondarySourceMarkupProfile => {
  const profilePath: string | undefined = env[OVERSEA_SECONDARY_MARKUP_PROFILE_PATH];
  if (profilePath === undefined || profilePath.length === 0) {
    throw new Error(
      `Set ${OVERSEA_SECONDARY_MARKUP_PROFILE_PATH} to the absolute path of your local secondary-source markup profile JSON file. The profile is operator-supplied and intentionally not version-controlled.`,
    );
  }
  return parseSecondarySourceMarkupProfileJson(readTextFile(profilePath));
};

const compileMarkupPatterns = (profile: SecondarySourceMarkupProfile): CompiledMarkupPatterns => {
  const horseNumberToken: string = escapeRegExp(profile.horseNumberClassToken);
  const gateToken: string = escapeRegExp(profile.gateClassToken);
  const horsePath: string = escapeRegExp(profile.horsePathSegment);
  const jockeyPath: string = escapeRegExp(profile.jockeyPathPrefix);
  const trainerPath: string = escapeRegExp(profile.trainerPathPrefix);
  const affiliationAlternation: string = profile.affiliationLabels
    .map((label: string): string => escapeRegExp(label))
    .join("|");

  return {
    // Matching is prefix/token based so cosmetic class suffixes do not matter.
    horseNumberCellPattern: new RegExp(
      `<td\\s+class="${horseNumberToken}[^"]*"[^>]*>([\\s\\S]*?)<\\/td>`,
      "i",
    ),
    // Gate cells use gateClassToken + digit (e.g. color band). Exclude longer
    // tokens that only share the same alphabetic prefix.
    gateCellPattern: new RegExp(
      `<td\\s+class="${gateToken}\\d+[^"]*"[^>]*>([\\s\\S]*?)<\\/td>`,
      "i",
    ),
    horseIdPattern: new RegExp(`href="[^"]*${horsePath}([^/"'?#\\s]+)`, "i"),
    horseAnchorPattern: new RegExp(
      `<a\\b[^>]*href="[^"]*${horsePath}[^"]*"[^>]*>([\\s\\S]*?)<\\/a>`,
      "i",
    ),
    jockeyIdPattern: new RegExp(`href="[^"]*${jockeyPath}([^/"'?#\\s]+)`, "i"),
    trainerIdPattern: new RegExp(`href="[^"]*${trainerPath}([^/"'?#\\s]+)`, "i"),
    trainerAffiliationPattern: new RegExp(`(${affiliationAlternation})`),
    horsePathSegment: profile.horsePathSegment,
  };
};

const decodeHtml = (value: string): string =>
  value
    .replaceAll("&amp;", "&")
    .replaceAll("&quot;", '"')
    .replaceAll("&#39;", "'")
    .replaceAll("&nbsp;", " ");

const cleanText = (value: string): string =>
  decodeHtml(value.replace(TAG_PATTERN, " ").replace(WHITESPACE_PATTERN, " ").trim());

const firstCapture = ({ html, pattern }: CaptureMatchInput): string | null => {
  const matched: RegExpExecArray | null = pattern.exec(html);
  const value: string | undefined = matched?.[FIRST_CAPTURE_GROUP_INDEX];
  return value === undefined ? null : value;
};

const parseOptionalDigits = (raw: string | null): OptionalNumberParseResult => {
  if (raw === null) {
    return { value: null, invalid: false, present: false };
  }
  const cleaned: string = cleanText(raw);
  if (cleaned.length === 0) {
    return { value: null, invalid: false, present: false };
  }
  if (!DIGITS_ONLY_PATTERN.test(cleaned)) {
    return { value: null, invalid: true, present: true };
  }
  return { value: Number(cleaned), invalid: false, present: true };
};

const extractHorseName = (anchorInnerHtml: string, anchorTagHtml: string): string | null => {
  const title: string | null = firstCapture({ html: anchorTagHtml, pattern: HORSE_TITLE_PATTERN });
  if (title !== null) {
    const cleanedTitle: string = cleanText(title);
    if (cleanedTitle.length > 0) {
      return cleanedTitle;
    }
  }
  const cleanedInner: string = cleanText(anchorInnerHtml);
  return cleanedInner.length === 0 ? null : cleanedInner;
};

const extractHorseIdentity = (
  rowHtml: string,
  patterns: CompiledMarkupPatterns,
): { horseId: string | null; horseName: string | null } => {
  const horseId: string | null = firstCapture({ html: rowHtml, pattern: patterns.horseIdPattern });
  if (horseId === null) {
    return { horseId: null, horseName: null };
  }
  const matched: RegExpExecArray | null = patterns.horseAnchorPattern.exec(rowHtml);
  if (matched === null) {
    return { horseId, horseName: null };
  }
  const anchorInner: string = matched[FIRST_CAPTURE_GROUP_INDEX] ?? "";
  return {
    horseId,
    horseName: extractHorseName(anchorInner, matched[0]),
  };
};

const extractRowFields = (
  rowHtml: string,
  patterns: CompiledMarkupPatterns,
): RowFieldExtraction => {
  const horseIdentity = extractHorseIdentity(rowHtml, patterns);
  return {
    horseNumberRaw: firstCapture({ html: rowHtml, pattern: patterns.horseNumberCellPattern }),
    gateRaw: firstCapture({ html: rowHtml, pattern: patterns.gateCellPattern }),
    horseId: horseIdentity.horseId,
    horseName: horseIdentity.horseName,
    jockeyId: firstCapture({ html: rowHtml, pattern: patterns.jockeyIdPattern }),
    trainerId: firstCapture({ html: rowHtml, pattern: patterns.trainerIdPattern }),
    trainerAffiliation: firstCapture({
      html: rowHtml,
      pattern: patterns.trainerAffiliationPattern,
    }),
  };
};

const buildIssue = ({
  code,
  message,
  rowIndex,
  horseNumber,
}: IssueBuildInput): SecondarySourceParseIssue => ({
  code,
  message,
  rowIndex,
  horseNumber,
});

const buildRowResult = ({ rowHtml, rowIndex, patterns }: RowBuildInput): RowBuildResult => {
  const fields: RowFieldExtraction = extractRowFields(rowHtml, patterns);
  const horseNumberParsed: OptionalNumberParseResult = parseOptionalDigits(fields.horseNumberRaw);
  const gateParsed: OptionalNumberParseResult = parseOptionalDigits(fields.gateRaw);

  if (!horseNumberParsed.present) {
    return {
      runner: null,
      issues: [
        buildIssue({
          code: "missing_horse_number",
          message: "Runner row is missing a horse-number cell.",
          rowIndex,
          horseNumber: null,
        }),
      ],
    };
  }

  const horseNumber: number | null = horseNumberParsed.value;
  if (horseNumber === null) {
    return {
      runner: null,
      issues: [
        buildIssue({
          code: "invalid_horse_number",
          message: "Runner row has a non-numeric horse number.",
          rowIndex,
          horseNumber: null,
        }),
      ],
    };
  }

  const issues: SecondarySourceParseIssue[] = [];

  if (!gateParsed.present) {
    issues.push(
      buildIssue({
        code: "missing_gate",
        message: "Runner row is missing a gate cell.",
        rowIndex,
        horseNumber,
      }),
    );
  } else if (gateParsed.invalid) {
    issues.push(
      buildIssue({
        code: "invalid_gate",
        message: "Runner row has a non-numeric gate.",
        rowIndex,
        horseNumber,
      }),
    );
  }

  if (fields.horseId === null) {
    issues.push(
      buildIssue({
        code: "missing_horse_id",
        message: "Runner row is missing a horse identity path.",
        rowIndex,
        horseNumber,
      }),
    );
  }
  if (fields.horseName === null) {
    issues.push(
      buildIssue({
        code: "missing_horse_name",
        message: "Runner row is missing a horse name.",
        rowIndex,
        horseNumber,
      }),
    );
  }
  if (fields.jockeyId === null) {
    issues.push(
      buildIssue({
        code: "missing_jockey_id",
        message: "Runner row is missing a jockey identity path.",
        rowIndex,
        horseNumber,
      }),
    );
  }
  if (fields.trainerId === null) {
    issues.push(
      buildIssue({
        code: "missing_trainer_id",
        message: "Runner row is missing a trainer identity path.",
        rowIndex,
        horseNumber,
      }),
    );
  }
  if (fields.trainerAffiliation === null) {
    issues.push(
      buildIssue({
        code: "missing_trainer_affiliation",
        message: "Runner row is missing a trainer affiliation label.",
        rowIndex,
        horseNumber,
      }),
    );
  }

  return {
    runner: {
      horseNumber,
      gate: gateParsed.invalid ? null : gateParsed.value,
      horseName: fields.horseName,
      horseId: fields.horseId,
      jockeyId: fields.jockeyId,
      trainerId: fields.trainerId,
      trainerAffiliation: fields.trainerAffiliation,
    },
    issues,
  };
};

const isCandidateRunnerRow = (rowHtml: string, patterns: CompiledMarkupPatterns): boolean =>
  // Require runner-body cells (td), not header labels (th) that reuse class tokens.
  rowHtml.includes(patterns.horsePathSegment) ||
  patterns.horseNumberCellPattern.test(rowHtml) ||
  patterns.gateCellPattern.test(rowHtml);

const compareByHorseNumber = (left: SecondarySourceRunner, right: SecondarySourceRunner): number =>
  left.horseNumber - right.horseNumber;

const collectDuplicateIssues = (
  runners: readonly SecondarySourceRunner[],
): readonly SecondarySourceParseIssue[] => {
  const counts: Map<number, number> = runners.reduce(
    (acc: Map<number, number>, runner: SecondarySourceRunner): Map<number, number> => {
      const next: Map<number, number> = new Map(acc);
      next.set(runner.horseNumber, (next.get(runner.horseNumber) ?? 0) + 1);
      return next;
    },
    new Map<number, number>(),
  );

  return Array.from(counts.entries())
    .filter((entry: readonly [number, number]): boolean => entry[1] > 1)
    .map(
      ([horseNumber]: readonly [number, number]): SecondarySourceParseIssue =>
        buildIssue({
          code: "duplicate_horse_number",
          message: "Multiple runner rows share the same horse number.",
          rowIndex: NO_ROW_INDEX,
          horseNumber,
        }),
    );
};

/**
 * Parse a secondary-source overseas racecard HTML document into runners + issues.
 * Rows are discovered from table body markup; horse numbers come from the number
 * cell (never from row order, because sources may list rows by gate order).
 * All markup selectors come from the supplied profile.
 */
export const parseSecondarySourceRacecard = ({
  html,
  profile,
}: ParseSecondarySourceRacecardInput): SecondarySourceParseResult => {
  const patterns: CompiledMarkupPatterns = compileMarkupPatterns(profile);
  const rowMatches: RegExpMatchArray[] = Array.from(html.matchAll(ROW_PATTERN));
  const candidateRows: CandidateRow[] = rowMatches
    .map(
      (matched: RegExpMatchArray, index: number): CandidateRow => ({
        rowHtml: matched[FIRST_CAPTURE_GROUP_INDEX] ?? "",
        rowIndex: index,
      }),
    )
    .filter(
      (row: CandidateRow): boolean =>
        row.rowHtml.length > 0 && isCandidateRunnerRow(row.rowHtml, patterns),
    );

  if (candidateRows.length === 0) {
    return {
      runners: [],
      issues: [
        buildIssue({
          code: "no_runner_rows",
          message: "Secondary source document has no runner rows.",
          rowIndex: NO_ROW_INDEX,
          horseNumber: null,
        }),
      ],
    };
  }

  const rowResults: RowBuildResult[] = candidateRows.map(
    ({ rowHtml, rowIndex }: CandidateRow): RowBuildResult =>
      buildRowResult({ rowHtml, rowIndex, patterns }),
  );

  const runners: SecondarySourceRunner[] = rowResults
    .map((result: RowBuildResult): SecondarySourceRunner | null => result.runner)
    .filter(
      (runner: SecondarySourceRunner | null): runner is SecondarySourceRunner => runner !== null,
    )
    .slice()
    .sort(compareByHorseNumber);

  const issues: SecondarySourceParseIssue[] = [
    ...rowResults.flatMap(
      (result: RowBuildResult): readonly SecondarySourceParseIssue[] => result.issues,
    ),
    ...collectDuplicateIssues(runners),
  ];

  return { runners, issues };
};
