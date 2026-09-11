// Run with bun. Produces bounded, race-scoped fingerprints for the day-stable
// Catalog inputs from which a Worker-materialized race foundation was built.

import type { PredictCategory } from "./types";

const CATALOG_ORIGIN: string = "https://pc-keiba-r2-catalog.internal";
const MAX_CATALOG_ROWS: number = 1_024;
const MAX_ROWS_PER_RACE: number = 32;
const PAD_WIDTH: number = 2;
const encoder: TextEncoder = new TextEncoder();

// These fields can affect the day-stable feature pipeline. Live odds, weight,
// weight delta, and settled result fields are intentionally absent: the
// rescore overlay owns those values and must not invalidate stable history.
const STABLE_SOURCE_FIELDS: ReadonlyArray<string> = [
  "babajotai_code_dirt",
  "babajotai_code_shiba",
  "bamei",
  "banushimei",
  "barei",
  "chokyoshimei_ryakusho",
  "futan_juryo",
  "grade_code",
  "hasso_jikoku",
  "juryo_shubetsu_code",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "ketto_toroku_bango",
  "kishumei_ryakusho",
  "kyori",
  "kyoso_joken_code",
  "kyoso_shubetsu_code",
  "race_bango",
  "race_date",
  "race_name",
  "seibetsu_code",
  "shusso_tosu",
  "source",
  "track_code",
  "umaban",
  "wakuban",
];

export interface RaceSourceCatalog {
  fetch: (request: Request) => Promise<Response>;
}

interface RaceSourceSnapshotParams {
  catalog: RaceSourceCatalog;
  category: PredictCategory;
  raceBango?: string;
  runYmd: string;
  venueCode?: string;
}

interface CatalogPayload {
  rows: unknown[];
}

interface ParsedSourceRow {
  canonical: string;
  entryToken: string;
  raceId: string;
}

interface RaceSourceGroup {
  canonicalRows: string[];
  entryTokens: string[];
  raceId: string;
}

export interface CatalogRaceSourceSnapshot {
  entrySetHash: string;
  raceId: string;
  rowCount: number;
  stableSourceHash: string;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const hex = (bytes: ArrayBuffer): string =>
  [...new Uint8Array(bytes)].map((value) => value.toString(16).padStart(2, "0")).join("");

const sha256 = async (value: string): Promise<string> =>
  hex(await crypto.subtle.digest("SHA-256", encoder.encode(value)));

const sourceFor = (category: PredictCategory): string => (category === "jra" ? "jra" : "nar");

const catalogSourceFor = (category: PredictCategory): string => category;

const positiveIntegerText = (value: unknown): string | null => {
  if (typeof value !== "number" && typeof value !== "string") return null;
  const text: string = String(value).trim();
  if (!/^\d+$/.test(text)) return null;
  const parsed: number = Number(text);
  return Number.isSafeInteger(parsed) && parsed > 0 ? String(parsed) : null;
};

const paddedCode = (value: unknown): string | null => {
  const text: string | null = positiveIntegerText(value);
  return text === null ? null : text.padStart(PAD_WIDTH, "0");
};

const primitiveCell = (value: unknown): boolean | number | string | null | undefined =>
  value === null ||
  typeof value === "boolean" ||
  typeof value === "number" ||
  typeof value === "string"
    ? value
    : undefined;

const canonicalStableRow = (row: Record<string, unknown>): string | null => {
  const values: Array<boolean | number | string | null | undefined> = STABLE_SOURCE_FIELDS.map(
    (field) => primitiveCell(row[field]),
  );
  return values.some((value) => value === undefined) ? null : JSON.stringify(values);
};

const parseSourceRow = (
  value: unknown,
  category: PredictCategory,
  runYmd: string,
): ParsedSourceRow | null => {
  if (!isRecord(value)) return null;
  const source: unknown = value.source;
  const year: unknown = value.kaisai_nen;
  const monthDay: unknown = value.kaisai_tsukihi;
  const venueCode: string | null = paddedCode(value.keibajo_code);
  const raceBango: string | null = paddedCode(value.race_bango);
  const ketto: unknown = value.ketto_toroku_bango;
  const umaban: string | null = positiveIntegerText(value.umaban);
  const canonical: string | null = canonicalStableRow(value);
  if (
    source !== sourceFor(category) ||
    typeof year !== "string" ||
    typeof monthDay !== "string" ||
    `${year}${monthDay}` !== runYmd ||
    venueCode === null ||
    raceBango === null ||
    typeof ketto !== "string" ||
    ketto.trim() === "" ||
    umaban === null ||
    canonical === null ||
    (category === "ban-ei" && venueCode !== "83") ||
    (category !== "ban-ei" && venueCode === "83")
  ) {
    return null;
  }
  return {
    canonical,
    entryToken: `${ketto.trim()}:${umaban}`,
    raceId: `${source}:${year}:${monthDay}:${venueCode}:${raceBango}`,
  };
};

const groupSourceRows = (
  rows: unknown[],
  category: PredictCategory,
  runYmd: string,
): RaceSourceGroup[] => {
  if (rows.length === 0 || rows.length > MAX_CATALOG_ROWS) throw new Error("catalog-row-limit");
  const parsed: Array<ParsedSourceRow | null> = rows.map((row) =>
    parseSourceRow(row, category, runYmd),
  );
  if (parsed.some((row) => row === null)) throw new Error("catalog-row-invalid");
  const groups = new Map<string, RaceSourceGroup>();
  parsed
    .filter((row): row is ParsedSourceRow => row !== null)
    .forEach((row) => {
      const current: RaceSourceGroup | undefined = groups.get(row.raceId);
      if (current === undefined) {
        groups.set(row.raceId, {
          canonicalRows: [row.canonical],
          entryTokens: [row.entryToken],
          raceId: row.raceId,
        });
        return;
      }
      current.canonicalRows.push(row.canonical);
      current.entryTokens.push(row.entryToken);
    });
  const values: RaceSourceGroup[] = [...groups.values()];
  if (
    values.length === 0 ||
    values.some(
      (group) =>
        group.entryTokens.length === 0 ||
        group.entryTokens.length > MAX_ROWS_PER_RACE ||
        new Set(group.entryTokens).size !== group.entryTokens.length,
    )
  ) {
    throw new Error("catalog-race-invalid");
  }
  return values;
};

const buildCatalogUrl = (params: RaceSourceSnapshotParams): URL => {
  const url = new URL("/v1/race-features", CATALOG_ORIGIN);
  url.searchParams.set("date", params.runYmd);
  url.searchParams.set("source", catalogSourceFor(params.category));
  if (params.venueCode !== undefined) url.searchParams.set("keibajoCode", params.venueCode);
  if (params.raceBango !== undefined) url.searchParams.set("raceBango", params.raceBango);
  return url;
};

const parsePayload = async (response: Response): Promise<CatalogPayload> => {
  if (!response.ok)
    throw new Error(`Catalog race source snapshot failed with HTTP ${response.status}`);
  const payload: unknown = await response.json();
  if (!isRecord(payload) || !Array.isArray(payload.rows)) {
    throw new Error("Catalog race source snapshot returned invalid rows");
  }
  return { rows: payload.rows };
};

const snapshotForGroup = async (group: RaceSourceGroup): Promise<CatalogRaceSourceSnapshot> => ({
  entrySetHash: await sha256(group.entryTokens.toSorted().join("\n")),
  raceId: group.raceId,
  rowCount: group.entryTokens.length,
  stableSourceHash: await sha256(group.canonicalRows.toSorted().join("\n")),
});

export const fetchCatalogRaceSourceSnapshots = async (
  params: RaceSourceSnapshotParams,
): Promise<ReadonlyMap<string, CatalogRaceSourceSnapshot>> => {
  if (!/^\d{8}$/.test(params.runYmd)) throw new Error("invalid-run-ymd");
  if ((params.venueCode === undefined) !== (params.raceBango === undefined)) {
    throw new Error("incomplete-race-scope");
  }
  const payload: CatalogPayload = await parsePayload(
    await params.catalog.fetch(new Request(buildCatalogUrl(params))),
  );
  const snapshots: CatalogRaceSourceSnapshot[] = await Promise.all(
    groupSourceRows(payload.rows, params.category, params.runYmd).map(snapshotForGroup),
  );
  if (params.venueCode !== undefined && snapshots.length !== 1) {
    throw new Error("catalog-race-scope-mismatch");
  }
  return new Map(snapshots.map((snapshot) => [snapshot.raceId, snapshot]));
};
