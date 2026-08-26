// Run with bun. Fixed-contract client for running-style inputs derived by the
// catalog Worker exclusively from local-PostgreSQL-sourced raw Iceberg tables.

import { buildRunningStyleRaceKey, type RunningStyleRaceParams } from "./running-style-features";
import { isRunningStyleDerivedFieldFeature } from "./running-style-field-features";
import type { RaceHorseFeatureRow } from "./running-style-r2";
import type { CatalogServiceBinding } from "./types";

const CATALOG_ORIGIN = "https://pc-keiba-r2-catalog.internal";
export const RUNNING_STYLE_CATALOG_GENERATION = "raw-iceberg-v1";
const CATALOG_HTTP_5XX_PATTERN = /PC_KEIBA_R2_CATALOG \S+ failed with HTTP 5\d\d/;
// A Catalog race-wide query normally finishes within the original 45s
// budget. When R2 SQL returns execution-resource code 70200, the Catalog
// Worker retries the exact race in bounded per-horse batches. Keep the client
// deadline below the queue's 150s lease so an abandoned request is aborted
// before the lease can be reclaimed and redelivered.
// Keep the Catalog request plus the bounded PostgreSQL fallback below the
// Queue lease so a slow R2 query cannot outlive the message and redeliver the
// same inference while its first attempt is still running.
const RUNNING_STYLE_CATALOG_TIMEOUT_MS = 60_000;
// Bounded slice of a failing Catalog response body appended to the thrown error so
// the operator-visible D1 state carries the Catalog `code`/`detail` instead of a bare
// HTTP status. Never echoes request headers or env values.
const CATALOG_ERROR_DETAIL_MAX_CHARS = 500;
const CATALOG_ERROR_DETAIL_ELLIPSIS = "...";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const requireString = (value: unknown, field: string): string => {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`catalog running-style row has invalid ${field}`);
  }
  return value;
};

const optionalString = (value: unknown, field: string): string | null => {
  if (value === null || value === undefined) return null;
  return requireString(value, field);
};

const requireNumber = (value: unknown, field: string): number => {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`catalog running-style row has invalid ${field}`);
  }
  return value;
};

const optionalNumber = (value: unknown, field: string): number | null => {
  if (value === null || value === undefined) return null;
  return requireNumber(value, field);
};

const parseNumberMap = (value: unknown, field: string): Record<string, number | null> => {
  if (!isRecord(value)) throw new Error(`catalog running-style row has invalid ${field}`);
  return Object.fromEntries(
    Object.entries(value).map(([name, cell]) => [name, optionalNumber(cell, `${field}.${name}`)]),
  );
};

const parsePeerInputs = (value: unknown): RaceHorseFeatureRow["peerInputs"] => {
  if (!isRecord(value)) throw new Error("catalog running-style row has invalid peerInputs");
  return {
    careerWinRate: optionalNumber(value.careerWinRate, "peerInputs.careerWinRate"),
    kohan3fAvg5: optionalNumber(value.kohan3fAvg5, "peerInputs.kohan3fAvg5"),
    pastCorner1NormAvg5: optionalNumber(
      value.pastCorner1NormAvg5,
      "peerInputs.pastCorner1NormAvg5",
    ),
    pastFirst3fAvg5: optionalNumber(value.pastFirst3fAvg5, "peerInputs.pastFirst3fAvg5"),
    pastNigeRate: optionalNumber(value.pastNigeRate, "peerInputs.pastNigeRate"),
    pastOikomiRate: optionalNumber(value.pastOikomiRate, "peerInputs.pastOikomiRate"),
    pastSashiRate: optionalNumber(value.pastSashiRate, "peerInputs.pastSashiRate"),
    pastSenkouRate: optionalNumber(value.pastSenkouRate, "peerInputs.pastSenkouRate"),
    speedIndexAvg5: optionalNumber(value.speedIndexAvg5, "peerInputs.speedIndexAvg5"),
    speedIndexBest5: optionalNumber(value.speedIndexBest5, "peerInputs.speedIndexBest5"),
  };
};

const parseFeatureRow = (value: unknown): RaceHorseFeatureRow => {
  if (!isRecord(value)) throw new Error("catalog running-style response has invalid row");
  const source = requireString(value.source, "source");
  if (source !== "jra" && source !== "nar") {
    throw new Error("catalog running-style row has invalid source");
  }
  return {
    bamei: optionalString(value.bamei, "bamei"),
    category: requireString(value.category, "category"),
    gradeCode: optionalString(value.gradeCode, "gradeCode"),
    kaisaiNen: requireString(value.kaisaiNen, "kaisaiNen"),
    kaisaiTsukihi: requireString(value.kaisaiTsukihi, "kaisaiTsukihi"),
    keibajoCode: requireString(value.keibajoCode, "keibajoCode"),
    kettoTorokuBango: requireString(value.kettoTorokuBango, "kettoTorokuBango"),
    kyori: optionalNumber(value.kyori, "kyori"),
    kyosoJokenCode: optionalString(value.kyosoJokenCode, "kyosoJokenCode"),
    narSubClass: optionalString(value.narSubClass, "narSubClass"),
    peerInputs: parsePeerInputs(value.peerInputs),
    perHorseFeatures: parseNumberMap(value.perHorseFeatures, "perHorseFeatures"),
    raceBango: requireString(value.raceBango, "raceBango"),
    raceKey: requireString(value.raceKey, "raceKey"),
    shussoTosu: optionalNumber(value.shussoTosu, "shussoTosu"),
    source,
    trackCode: optionalString(value.trackCode, "trackCode"),
    umaban: requireNumber(value.umaban, "umaban"),
  };
};

const parseJsonOrNull = (text: string): unknown => {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
};

const formatErrorCode = (code: unknown): string =>
  typeof code === "string" || typeof code === "number" ? `code=${String(code)}` : "";

const structuredErrorParts = (payload: Record<string, unknown>): string[] =>
  [
    typeof payload.error === "string" ? payload.error : "",
    formatErrorCode(payload.code),
    typeof payload.detail === "string" ? payload.detail : "",
  ].filter((part) => part.length > 0);

const truncateErrorDetail = (detail: string): string =>
  detail.length > CATALOG_ERROR_DETAIL_MAX_CHARS
    ? `${detail.slice(0, CATALOG_ERROR_DETAIL_MAX_CHARS)}${CATALOG_ERROR_DETAIL_ELLIPSIS}`
    : detail;

const catalogErrorDetail = (body: string): string => {
  const parsed = parseJsonOrNull(body);
  const parts = isRecord(parsed) ? structuredErrorParts(parsed) : [];
  return truncateErrorDetail(parts.length > 0 ? parts.join(" ") : body.trim());
};

export const isCatalogUnavailableError = (error: unknown): boolean => {
  const message = error instanceof Error ? error.message : String(error);
  return (
    message.includes("r2_sql_unavailable") ||
    message.includes("running-style Catalog request timed out") ||
    CATALOG_HTTP_5XX_PATTERN.test(message)
  );
};

const fetchCatalogJson = async (
  catalog: CatalogServiceBinding,
  url: URL,
  timeoutMs?: number,
): Promise<unknown> => {
  const controller = new AbortController();
  const timeoutId =
    timeoutMs === undefined ? undefined : setTimeout(() => controller.abort(), timeoutMs);
  const response = await catalog
    .fetch(new Request(url, { method: "GET", signal: controller.signal }))
    .catch((error: unknown) => {
      if (controller.signal.aborted && timeoutMs !== undefined) {
        throw new Error(`running-style Catalog request timed out after ${String(timeoutMs)}ms`);
      }
      throw error;
    })
    .finally(() => {
      if (timeoutId !== undefined) clearTimeout(timeoutId);
    });
  if (!response.ok) {
    // Safe: the ok path below returns before this branch, so the body stream is
    // read at most once. A body that cannot be read degrades to the bare message.
    const detail = catalogErrorDetail(await response.text().catch(() => ""));
    const message = `PC_KEIBA_R2_CATALOG ${url.pathname} failed with HTTP ${response.status}`;
    throw new Error(detail.length > 0 ? `${message}: ${detail}` : message);
  }
  return response.json();
};

const requireRowsEnvelope = (payload: unknown, label: string): unknown[] => {
  if (!isRecord(payload) || !Array.isArray(payload.rows)) {
    throw new Error(`${label} has invalid rows`);
  }
  return payload.rows;
};

export const fetchRunningStyleFeaturesFromCatalog = async (
  catalog: CatalogServiceBinding,
  race: RunningStyleRaceParams,
  featureNames: ReadonlyArray<string>,
): Promise<ReadonlyArray<RaceHorseFeatureRow>> => {
  const url = new URL("/v1/running-style-features", CATALOG_ORIGIN);
  url.searchParams.set("date", `${race.kaisaiNen}${race.kaisaiTsukihi}`);
  const keibajoCode = race.keibajoCode.padStart(2, "0");
  const catalogSource = race.source === "nar" && keibajoCode === "83" ? "ban-ei" : race.source;
  url.searchParams.set("source", catalogSource);
  url.searchParams.set("keibajoCode", keibajoCode);
  url.searchParams.set("raceBango", race.raceBango.padStart(2, "0"));
  if (race.gradeCode !== undefined && race.gradeCode !== null) {
    url.searchParams.set("gradeCode", race.gradeCode);
  }
  const payload = await fetchCatalogJson(catalog, url, RUNNING_STYLE_CATALOG_TIMEOUT_MS);
  if (!isRecord(payload) || payload.generation !== RUNNING_STYLE_CATALOG_GENERATION) {
    throw new Error("catalog running-style response has invalid generation");
  }
  const responseFeatureNames = payload.featureNames;
  if (
    !Array.isArray(responseFeatureNames) ||
    !responseFeatureNames.every((x) => typeof x === "string")
  ) {
    throw new Error("catalog running-style response has invalid featureNames");
  }
  const rawFeatureNames = featureNames.filter((name) => !isRunningStyleDerivedFieldFeature(name));
  if (!rawFeatureNames.every((name) => responseFeatureNames.includes(name))) {
    throw new Error("catalog running-style response is missing requested model features");
  }
  if (!Array.isArray(payload.rows)) {
    throw new Error("catalog running-style response has invalid rows");
  }
  const rows = payload.rows.map(parseFeatureRow);
  const raceKey = buildRunningStyleRaceKey(race);
  if (rows.some((row) => row.raceKey !== raceKey)) {
    throw new Error(`catalog running-style response contains another race for ${raceKey}`);
  }
  return rows;
};

export interface CatalogRaceKeyRow {
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  source: "jra" | "nar";
  grade_code?: string | null;
}

const parseRaceKeyRow = (value: unknown): CatalogRaceKeyRow => {
  if (!isRecord(value)) throw new Error("catalog race-key response has invalid row");
  const sourceValue = requireString(value.source, "source");
  if (sourceValue !== "jra" && sourceValue !== "nar") {
    throw new Error("catalog race-key response has invalid source");
  }
  const gradeCode = optionalString(value.grade_code, "grade_code");
  return {
    kaisai_nen: requireString(value.kaisai_nen, "kaisai_nen"),
    kaisai_tsukihi: requireString(value.kaisai_tsukihi, "kaisai_tsukihi"),
    keibajo_code: requireString(value.keibajo_code, "keibajo_code"),
    race_bango: requireString(value.race_bango, "race_bango"),
    source: sourceValue,
    ...(gradeCode === null ? {} : { grade_code: gradeCode }),
  };
};

export const fetchRunningStyleRaceKeysFromCatalog = async (
  catalog: CatalogServiceBinding,
  date: string,
): Promise<CatalogRaceKeyRow[]> => {
  const url = new URL("/v1/race-keys", CATALOG_ORIGIN);
  url.searchParams.set("date", date);
  const payload = await fetchCatalogJson(catalog, url);
  return requireRowsEnvelope(payload, "catalog race-key response").map(parseRaceKeyRow);
};

const raceKeyFromRawFeature = (value: unknown): string => {
  if (!isRecord(value)) throw new Error("catalog race-feature response has invalid row");
  return `${requireString(value.source, "source")}:${requireString(value.kaisai_nen, "kaisai_nen")}${requireString(value.kaisai_tsukihi, "kaisai_tsukihi")}:${requireString(value.keibajo_code, "keibajo_code")}:${requireString(value.race_bango, "race_bango")}`;
};

export const fetchRunningStyleFeatureCountsFromCatalog = async (
  catalog: CatalogServiceBinding,
  date: string,
): Promise<Map<string, number>> => {
  const url = new URL("/v1/race-features", CATALOG_ORIGIN);
  url.searchParams.set("date", date);
  url.searchParams.set("source", "all");
  const payload = await fetchCatalogJson(catalog, url);
  return requireRowsEnvelope(payload, "catalog race-feature response").reduce<Map<string, number>>(
    (counts, row) => {
      const raceKey = raceKeyFromRawFeature(row);
      counts.set(raceKey, (counts.get(raceKey) ?? 0) + 1);
      return counts;
    },
    new Map<string, number>(),
  );
};
