// Run with bun. Exact observed event titles; venue codes do not prove physical venue identity.
interface ArcContext {
  source: string;
  date: string;
  keibajoCode: string;
  raceBango: string;
}
interface EventDayContext {
  date: string;
  keibajoCode: string;
  raceBango?: string;
}
export type EventScope = "arc-cohort-policy-v2" | `overseas-event-v1:${string}`;
export const ARC_EVENT_SCOPE: EventScope = "arc-cohort-policy-v2";
const OVERSEAS_EVENT_SCOPE_PREFIX: string = "overseas-event-v1:";
const OVERSEAS_KEIBAJO_PATTERN: RegExp = /^[A-Z][0-9A-Z]$/u;
const IDENTIFIER_PATTERN: RegExp = /^[A-Za-z_][A-Za-z0-9_]*$/u;
const FULL_WIDTH_SPACE: string = "　";
const GRADE_SUFFIX_PATTERN: RegExp = /\((?:GI{1,3}|G[123])\)$/u;
const ARC_TITLE: string = "凱旋門賞";
export const isCurrentArc = (input: ArcContext): boolean =>
  input.source === "jra" &&
  input.date === "20261004" &&
  input.keibajoCode === "A8" &&
  input.raceBango === "05";
// Any JRA-sold overseas race is an event whose cohort is its own past editions.
export const isOverseasEvent = (input: ArcContext): boolean =>
  input.source === "jra" && OVERSEAS_KEIBAJO_PATTERN.test(input.keibajoCode);
export const normalizeEventTitle = (title: string): string =>
  title.replaceAll(FULL_WIDTH_SPACE, "").trim().replace(GRADE_SUFFIX_PATTERN, "");
const sqlLiteral = (value: string): string => `'${value.replaceAll("'", "''")}'`;
// Matches JV titles with internal full-width spaces and an optional (GI) suffix.
export const eventTitlesSql = (column: string, titles: readonly string[]): string =>
  `btrim(replace(${column}, chr(12288), '')) IN (${titles.flatMap((title) => [sqlLiteral(title), sqlLiteral(`${title}(GI)`)]).join(", ")})`;
export const arcTitleSql = (column: string): string => eventTitlesSql(column, [ARC_TITLE]);
export const overseasEventScope = (title: string): EventScope => `overseas-event-v1:${title}`;
export const eventScopeTitleSql = (column: string, scope: EventScope): string =>
  scope === ARC_EVENT_SCOPE
    ? arcTitleSql(column)
    : eventTitlesSql(column, [scope.slice(OVERSEAS_EVENT_SCOPE_PREFIX.length)]);
// Without a race number, every title of that day and venue code (revision scope).
export const buildEventTitlesQuery = (namespace: string, input: EventDayContext): string => {
  if (!IDENTIFIER_PATTERN.test(namespace)) throw new Error("Invalid event namespace");
  return `SELECT DISTINCT kyosomei_hondai FROM ${namespace}.jvd_ra WHERE kaisai_nen = '${input.date.slice(0, 4)}' AND kaisai_tsukihi = '${input.date.slice(4)}' AND keibajo_code = '${input.keibajoCode}'${input.raceBango === undefined ? "" : ` AND race_bango = '${input.raceBango}'`} ORDER BY kyosomei_hondai LIMIT 100`;
};
// Distinct normalized titles; rows without a title cannot identify an event.
export const eventTitlesFromRows = (rows: readonly Record<string, unknown>[]): string[] =>
  [
    ...new Set(
      rows.flatMap((row) =>
        typeof row.kyosomei_hondai === "string" ? [normalizeEventTitle(row.kyosomei_hondai)] : [],
      ),
    ),
  ]
    .filter((title) => title !== "")
    .toSorted();
