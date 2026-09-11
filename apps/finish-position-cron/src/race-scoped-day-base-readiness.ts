// Run with bun. Allows an unchanged race to use its attested per-race foundation
// while another race makes the category-wide running-style watermark stale.

import {
  buildDayBaseRaceFoundationKey,
  getDayBaseRaceFoundationReadiness,
} from "./day-base-race-materializer";
import { fetchCatalogRaceSourceSnapshots } from "./race-source-snapshot";
import { getRunningStyleRaceReadiness } from "./running-style-readiness";
import type { Env, PredictCategory } from "./types";

const READY_REASON = "ready";
const SHA_256_HEX_LENGTH = 64;

interface RaceScopedDayBaseReadinessParams {
  category: PredictCategory;
  env: Env;
  keibajoCode: string;
  raceBango: string;
  requireStableSourceHash?: boolean;
  runYmd: string;
}

interface RunningStyleFeatureRow {
  kettoTorokuBango: string;
  pNige: number;
  pOikomi: number;
  pSashi: number;
  pSenkou: number;
  predictedClass: number;
  umaban: number;
}

interface RunningStyleDatabaseRow {
  ketto_toroku_bango: string;
  p_nige: number;
  p_oikomi: number;
  p_sashi: number;
  p_senkou: number;
  predicted_class: number | null;
  umaban: number;
}

export interface RaceScopedDayBaseReadiness {
  ready: boolean;
  reason: string;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const metadataString = (object: R2Object | null, key: string): string | null => {
  const value = object?.customMetadata?.[key]?.trim();
  return value === undefined || value.length === 0 ? null : value;
};

const metadataCount = (object: R2Object | null, key: string): number | null => {
  const value = metadataString(object, key);
  if (value === null) return null;
  const count = Number(value);
  return Number.isSafeInteger(count) && count > 0 ? count : null;
};

const raceSource = (category: PredictCategory): string => (category === "jra" ? "jra" : "nar");

const runningStyleRaceKey = (params: RaceScopedDayBaseReadinessParams): string =>
  `${raceSource(params.category)}:${params.runYmd}:${params.keibajoCode}:${params.raceBango}`;

const catalogRaceId = (params: RaceScopedDayBaseReadinessParams): string =>
  `${raceSource(params.category)}:${params.runYmd.slice(0, 4)}:${params.runYmd.slice(4)}:${params.keibajoCode.padStart(2, "0")}:${params.raceBango.padStart(2, "0")}`;

const finiteNumber = (value: unknown): number | null => {
  const numberValue = Number(value);
  return Number.isFinite(numberValue) ? numberValue : null;
};

const parseRunningStyleFeatureRow = (value: unknown): RunningStyleFeatureRow | null => {
  if (!isRecord(value) || typeof value.ketto_toroku_bango !== "string") return null;
  const kettoTorokuBango = value.ketto_toroku_bango.trim();
  const pNige = finiteNumber(value.rs_p_nige);
  const pOikomi = finiteNumber(value.rs_p_oikomi);
  const pSashi = finiteNumber(value.rs_p_sashi);
  const pSenkou = finiteNumber(value.rs_p_senkou);
  const predictedClass = Number(value.rs_predicted_class);
  const umaban = Number(value.umaban);
  if (
    kettoTorokuBango.length === 0 ||
    pNige === null ||
    pOikomi === null ||
    pSashi === null ||
    pSenkou === null ||
    !Number.isSafeInteger(predictedClass) ||
    predictedClass < 0 ||
    predictedClass > 3 ||
    !Number.isSafeInteger(umaban) ||
    umaban <= 0
  ) {
    return null;
  }
  return {
    kettoTorokuBango,
    pNige,
    pOikomi,
    pSashi,
    pSenkou,
    predictedClass,
    umaban,
  };
};

const parseFoundationRunningStyles = async (
  foundation: R2ObjectBody,
): Promise<RunningStyleFeatureRow[] | null> => {
  const envelope: unknown = await foundation.json();
  if (!isRecord(envelope) || !Array.isArray(envelope.rows)) return null;
  const rows = envelope.rows.map(parseRunningStyleFeatureRow);
  return rows.length > 0 && rows.every((row) => row !== null)
    ? rows.filter((row): row is RunningStyleFeatureRow => row !== null)
    : null;
};

const readLiveRunningStyles = async (
  params: RaceScopedDayBaseReadinessParams,
): Promise<RunningStyleFeatureRow[] | null> => {
  const result = await params.env.REALTIME_DB.prepare(
    `select ketto_toroku_bango, horse_number as umaban,
            p_nige, p_senkou, p_sashi, p_oikomi,
            case predicted_label
              when 'nige' then 0 when 'senkou' then 1
              when 'sashi' then 2 when 'oikomi' then 3
              else null
            end as predicted_class
       from race_running_styles
      where race_key = ?1
      order by horse_number`,
  )
    .bind(runningStyleRaceKey(params))
    .all<RunningStyleDatabaseRow>();
  const rows = result.results.map((row): RunningStyleFeatureRow | null =>
    parseRunningStyleFeatureRow({
      ketto_toroku_bango: row.ketto_toroku_bango,
      rs_p_nige: row.p_nige,
      rs_p_oikomi: row.p_oikomi,
      rs_p_sashi: row.p_sashi,
      rs_p_senkou: row.p_senkou,
      rs_predicted_class: row.predicted_class,
      umaban: row.umaban,
    }),
  );
  return rows.length > 0 && rows.every((row) => row !== null)
    ? rows.filter((row): row is RunningStyleFeatureRow => row !== null)
    : null;
};

const compareRunningStyleRows = (
  left: RunningStyleFeatureRow,
  right: RunningStyleFeatureRow,
): number =>
  left.umaban - right.umaban || left.kettoTorokuBango.localeCompare(right.kettoTorokuBango);

const sameRunningStyles = (
  foundation: readonly RunningStyleFeatureRow[],
  live: readonly RunningStyleFeatureRow[],
): boolean =>
  foundation.length === live.length &&
  JSON.stringify([...foundation].sort(compareRunningStyleRows)) ===
    JSON.stringify([...live].sort(compareRunningStyleRows));

const checkRunningStyleFreshness = async (
  params: RaceScopedDayBaseReadinessParams,
  foundation: R2ObjectBody,
  foundationRowCount: number,
): Promise<RaceScopedDayBaseReadiness> => {
  if (params.category === "ban-ei") return { ready: true, reason: READY_REASON };
  const readiness = await getRunningStyleRaceReadiness({
    category: params.category,
    db: params.env.REALTIME_DB,
    races: [
      {
        category: params.category,
        keibajoCode: params.keibajoCode,
        raceBango: params.raceBango,
      },
    ],
    runYmd: params.runYmd,
  });
  const raceReadiness = readiness[0];
  if (raceReadiness === undefined) {
    return { ready: false, reason: "race-running-style-state-missing" };
  }
  if (raceReadiness.reason !== null) {
    return { ready: false, reason: `race-running-style-${raceReadiness.reason}` };
  }
  const [foundationRows, liveRows] = await Promise.all([
    parseFoundationRunningStyles(foundation),
    readLiveRunningStyles(params),
  ]);
  if (foundationRows === null || liveRows === null) {
    return { ready: false, reason: "race-running-style-features-invalid" };
  }
  if (foundationRows.length !== foundationRowCount || liveRows.length !== foundationRowCount) {
    return {
      ready: false,
      reason: `race-rs-row-count-${String(liveRows.length)}-of-${String(foundationRowCount)}`,
    };
  }
  return sameRunningStyles(foundationRows, liveRows)
    ? { ready: true, reason: READY_REASON }
    : { ready: false, reason: "race-running-style-features-mismatch" };
};

export const getRaceScopedDayBaseReadiness = async (
  params: RaceScopedDayBaseReadinessParams,
): Promise<RaceScopedDayBaseReadiness> => {
  const attestation = await getDayBaseRaceFoundationReadiness({
    category: params.category,
    env: params.env,
    raceNumber: params.raceBango,
    runYmd: params.runYmd,
    venueCode: params.keibajoCode,
  });
  if (!attestation.ready) return attestation;
  const foundationKey = buildDayBaseRaceFoundationKey(
    params.category,
    params.runYmd,
    params.keibajoCode,
    params.raceBango,
  );
  if (params.env.PC_KEIBA_R2_CATALOG === undefined) {
    throw new Error("PC_KEIBA_R2_CATALOG binding is unavailable");
  }
  const [foundation, snapshots] = await Promise.all([
    params.env.FEATURES_CACHE.get(foundationKey),
    fetchCatalogRaceSourceSnapshots({
      catalog: params.env.PC_KEIBA_R2_CATALOG,
      category: params.category,
      raceBango: params.raceBango,
      runYmd: params.runYmd,
      venueCode: params.keibajoCode,
    }),
  ]);
  if (foundation === null) {
    return { ready: false, reason: "race-foundation-disappeared" };
  }
  const snapshot = snapshots.get(catalogRaceId(params));
  if (snapshot === undefined) {
    return { ready: false, reason: "race-catalog-source-snapshot-missing" };
  }
  const foundationEntrySetHash = metadataString(foundation, "entry-set-hash");
  const foundationRowCount = metadataCount(foundation, "row-count");
  const foundationStableSourceHash = metadataString(foundation, "catalog-source-hash");
  if (
    foundationEntrySetHash === null ||
    foundationEntrySetHash.length !== SHA_256_HEX_LENGTH ||
    foundationRowCount === null
  ) {
    return { ready: false, reason: "race-foundation-metadata-invalid" };
  }
  if (
    foundationRowCount !== snapshot.rowCount ||
    foundationEntrySetHash !== snapshot.entrySetHash
  ) {
    return { ready: false, reason: "race-entry-set-mismatch" };
  }
  if (params.requireStableSourceHash === true && foundationStableSourceHash === null) {
    return { ready: false, reason: "race-catalog-source-hash-missing" };
  }
  if (
    foundationStableSourceHash !== null &&
    foundationStableSourceHash !== snapshot.stableSourceHash
  ) {
    return { ready: false, reason: "race-catalog-source-mismatch" };
  }
  return checkRunningStyleFreshness(params, foundation, foundationRowCount);
};
