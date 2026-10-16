// Run with bun. Exact observed event titles; venue codes do not prove physical venue identity.
interface ArcContext {
  source: string;
  date: string;
  keibajoCode: string;
  raceBango: string;
}
export const isCurrentArc = (input: ArcContext): boolean =>
  input.source === "jra" &&
  input.date === "20261004" &&
  input.keibajoCode === "A8" &&
  input.raceBango === "05";
export const arcTitleSql = (column: string): string =>
  `btrim(replace(${column}, chr(12288), '')) IN ('凱旋門賞', '凱旋門賞(GI)')`;
