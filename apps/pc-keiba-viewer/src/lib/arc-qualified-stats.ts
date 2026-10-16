// Run with bun. Wire contract mirrors Catalog's standalone arc-qualified-people.ts.
import type { WinRateHeatmapCell, WinRateHeatmapMetricKey } from "./win-rate-heatmap";
export interface ArcPeople {
  horse: string | null;
  jockey: string | null;
  trainer: string | null;
  owner: string | null;
}
export interface ArcCountRow {
  umaban: string;
  kind: string;
  source: "netkeiba";
  people: ArcPeople;
  scope: "arc-series" | "arc-parislongchamp-turf";
  since: string;
  before: string;
  gate: number | null;
  status: "observed" | "missing-identity" | "unobserved";
  starts: number;
  wins: number;
  top2: number;
  top3: number;
  evidenceKeys: string[];
  unknownKeys: string[];
  withdrawnKeys: string[];
  unverifiedGateKeys: string[];
}
export interface ArcQualifiedStats {
  rows: ArcCountRow[];
  coverage: { complete: false; rawRows: number; invalidHorseRows: number };
}
interface Cohort {
  kind: string;
  cell: WinRateHeatmapMetricKey;
  roles: (keyof ArcPeople)[];
  since: string;
  venue: boolean;
  gate: boolean;
}
const COHORTS: Cohort[] = [
  {
    kind: "jockey",
    cell: "jockey",
    roles: ["jockey"],
    since: "2016-10-04",
    venue: false,
    gate: false,
  },
  {
    kind: "trainer",
    cell: "trainer",
    roles: ["trainer"],
    since: "2016-10-04",
    venue: false,
    gate: false,
  },
  {
    kind: "owner",
    cell: "owner",
    roles: ["owner"],
    since: "2016-10-04",
    venue: false,
    gate: false,
  },
  {
    kind: "horse10Y",
    cell: "horse",
    roles: ["horse"],
    since: "2016-10-04",
    venue: false,
    gate: false,
  },
  {
    kind: "horseJockey30Y",
    cell: "horseJockey",
    roles: ["horse", "jockey"],
    since: "1996-10-04",
    venue: false,
    gate: false,
  },
  {
    kind: "jockeyVenue3Y",
    cell: "jockeyVenue",
    roles: ["jockey"],
    since: "2023-10-04",
    venue: true,
    gate: false,
  },
  {
    kind: "jockeyTrainerVenue10Y",
    cell: "jockeyTrainerVenue",
    roles: ["jockey", "trainer"],
    since: "2016-10-04",
    venue: true,
    gate: false,
  },
  {
    kind: "ownerVenue30Y",
    cell: "ownerVenue",
    roles: ["owner"],
    since: "1996-10-04",
    venue: true,
    gate: false,
  },
  {
    kind: "triple30Y",
    cell: "jockeyTrainerOwner",
    roles: ["jockey", "trainer", "owner"],
    since: "1996-10-04",
    venue: false,
    gate: false,
  },
  {
    kind: "jockeyFrame10Y",
    cell: "jockeyFrame",
    roles: ["jockey"],
    since: "2016-10-04",
    venue: true,
    gate: true,
  },
];
const ROLES: (keyof ArcPeople)[] = ["horse", "jockey", "trainer", "owner"];
const WIDTHS: Record<keyof ArcPeople, number> = { horse: 10, jockey: 5, trainer: 5, owner: 6 };
const BEFORE: string = "2026-10-04";
const record = (v: unknown): v is Record<string, unknown> =>
  typeof v === "object" && v !== null && !Array.isArray(v);
const count = (v: unknown): v is number =>
  typeof v === "number" && Number.isSafeInteger(v) && v >= 0;
const sourceId = (v: unknown, role: keyof ArcPeople): boolean =>
  typeof v === "string" &&
  new RegExp(`^[0-9a-z]{${WIDTHS[role]}}$`, "u").test(v) &&
  /[1-9a-z]/u.test(v);
const keys = (v: unknown, since: string): v is string[] =>
  Array.isArray(v) &&
  v.every((key: unknown) => {
    if (typeof key !== "string" || !/^\d{4}-\d{2}-\d{2}:[0-9a-z]{10}$/u.test(key)) return false;
    const date: string = key.slice(0, 10);
    return (
      Number.isFinite(Date.parse(date)) &&
      new Date(date).toISOString().slice(0, 10) === date &&
      date >= since &&
      date < BEFORE &&
      sourceId(key.slice(11), "horse")
    );
  }) &&
  new Set(v).size === v.length;
const validRow = (v: unknown): v is ArcCountRow => {
  if (!record(v) || !record(v.people)) return false;
  const c: Cohort | undefined = COHORTS.find((item) => item.kind === v.kind);
  const p: Record<string, unknown> = v.people;
  if (
    !c ||
    typeof v.umaban !== "string" ||
    !/^(0[1-9]|1[0-6])$/u.test(v.umaban) ||
    v.source !== "netkeiba" ||
    v.since !== c.since ||
    v.before !== BEFORE ||
    v.scope !== (c.venue ? "arc-parislongchamp-turf" : "arc-series") ||
    !ROLES.every((role) => p[role] === null || sourceId(p[role], role)) ||
    (v.umaban === "13" && p.owner !== null)
  )
    return false;
  if (
    c.gate ? !(v.gate === null || (count(v.gate) && v.gate > 0 && v.gate <= 24)) : v.gate !== null
  )
    return false;
  if (
    !count(v.starts) ||
    !count(v.wins) ||
    !count(v.top2) ||
    !count(v.top3) ||
    v.wins > v.top2 ||
    v.top2 > v.top3 ||
    v.top3 > v.starts
  )
    return false;
  if (
    !keys(v.evidenceKeys, c.since) ||
    !keys(v.unknownKeys, c.since) ||
    !keys(v.withdrawnKeys, c.since) ||
    !keys(v.unverifiedGateKeys, c.since)
  )
    return false;
  const all: string[] = [
    ...v.evidenceKeys,
    ...v.unknownKeys,
    ...v.withdrawnKeys,
    ...v.unverifiedGateKeys,
  ];
  const known: boolean = c.roles.every((role) => p[role] !== null) && (!c.gate || v.gate !== null);
  const status: string = v.starts > 0 ? "observed" : "unobserved";
  return (
    v.status === (known ? status : "missing-identity") &&
    v.evidenceKeys.length === v.starts &&
    new Set(all).size === all.length &&
    (c.gate || v.unverifiedGateKeys.length === 0) &&
    (known || all.length === 0) &&
    (!c.roles.includes("horse") || all.every((key) => key.slice(11) === p.horse))
  );
};
export const isArcQualifiedStats = (v: unknown): v is ArcQualifiedStats => {
  if (
    !record(v) ||
    !record(v.coverage) ||
    v.coverage.complete !== false ||
    !count(v.coverage.rawRows) ||
    !count(v.coverage.invalidHorseRows) ||
    v.coverage.invalidHorseRows > v.coverage.rawRows ||
    !Array.isArray(v.rows) ||
    !v.rows.every(validRow)
  )
    return false;
  const rows: ArcCountRow[] = v.rows;
  return (
    new Set(rows.map((r) => `${r.umaban}:${r.kind}`)).size === rows.length &&
    rows.every((row) =>
      rows.every(
        (other) =>
          row.umaban !== other.umaban ||
          ROLES.every((role) => row.people[role] === other.people[role]),
      ),
    ) &&
    rows.every((row) =>
      rows.every(
        (other) =>
          row.umaban === other.umaban ||
          row.people.horse === null ||
          row.people.horse !== other.people.horse,
      ),
    )
  );
};
export const requireArcQualifiedStats = (v: unknown): ArcQualifiedStats => {
  if (!isArcQualifiedStats(v)) throw new Error("Arc qualified statistics are missing or malformed");
  return v;
};
export const arcQualifiedCells = (
  stats: ArcQualifiedStats | undefined,
  horseNumber: string,
): Partial<Record<WinRateHeatmapMetricKey, WinRateHeatmapCell>> =>
  Object.fromEntries(
    COHORTS.map((c) => {
      const row: ArcCountRow | undefined = stats?.rows.find(
        (r) => Number(r.umaban) === Number(horseNumber) && r.kind === c.kind,
      );
      const observed: boolean = row?.status === "observed" && row.starts > 0;
      const rate = (value: number): number | null =>
        observed && row ? Math.round((value * 1000) / row.starts) / 10 : null;
      return [
        c.cell,
        {
          name: row
            ? c.roles.map((role) => `netkeiba:${role}:${row.people[role] ?? "unknown"}`).join(" × ")
            : "未取得",
          provenance: `${c.since}以上〜${BEFORE}未満（固定rolling${2026 - Number(c.since.slice(0, 4))}年・選択条件期間とは別）。${c.venue ? "実開催地パリロンシャン・芝の凱旋門賞のみ" : "凱旋門賞シリーズ全開催地"}。部分収録・網羅未確認。${row?.status ?? "unobserved"}。枠未検証除外${row?.unverifiedGateKeys.length ?? 0}件・着順不明${row?.unknownKeys.length ?? 0}件・取消${row?.withdrawnKeys.length ?? 0}件。`,
          starts: observed && row ? row.starts : null,
          winCount: observed && row ? row.wins : null,
          quinellaCount: observed && row ? row.top2 : null,
          showCount: observed && row ? row.top3 : null,
          winRate: rate(row?.wins ?? 0),
          quinellaRate: rate(row?.top2 ?? 0),
          showRate: rate(row?.top3 ?? 0),
        },
      ];
    }),
  );
