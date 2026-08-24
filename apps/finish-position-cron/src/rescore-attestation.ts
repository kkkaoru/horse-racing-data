// Build a fresh, race-scoped attestation immediately before a rescore Container
// call. The Queue body deliberately does not carry this evidence: every retry
// must re-read the live entry set and the exact R2 objects it is about to use.

import { buildDayBaseObjectKey } from "./day-base-object-key";
import { buildDayBaseRaceFoundationKey } from "./day-base-race-materializer";
import { buildPerRaceFeatCacheKey } from "./scoring/feature-cache";
import type { CatalogServiceBinding, PredictCategory } from "./types";

const CATALOG_ORIGIN = "https://pc-keiba-r2-catalog";
const FRESH_ENTRIES_PATH = "/v1/internal/fresh-race-entries";
// R2 SQL-backed catalog requests can legitimately take several seconds when
// the warehouse is waking up. Five seconds was shorter than the observed
// cold-path latency (83:01 timed out at exactly this boundary), which turned
// a transient Service Binding delay into a Queue redelivery. Keep the request
// bounded, but leave enough wall time for one cold query to complete. The
// attestation remains fail-closed: a timeout still aborts rather than using
// stale or cached entrant data.
const ATTESTATION_TIMEOUT_MS = 15_000;
const HASH_PATTERN = /^[0-9a-f]{64}$/u;
const POSITIVE_INTEGER_PATTERN = /^[1-9][0-9]*$/u;
const encoder = new TextEncoder();

interface RescoreAttestationParams {
  category: PredictCategory;
  env: RescoreAttestationEnv;
  keibajoCode: string;
  raceBango: string;
  runYmd: string;
}

interface AttestationHeadObject {
  customMetadata?: Record<string, string>;
  etag: string;
  version?: string;
}

interface RescoreAttestationEnv {
  FEATURES_CACHE: {
    head(key: string): Promise<AttestationHeadObject | null>;
  };
  FINISH_POSITION_ATTESTATION_TOKEN?: string;
  PC_KEIBA_R2_CATALOG?: CatalogServiceBinding;
}

interface FreshEntry {
  kettoTorokuBango: string;
  umaban: number;
}

interface FreshEntriesResponse {
  date: string;
  entries: FreshEntry[];
  keibajoCode: string;
  raceBango: string;
  source: PredictCategory;
}

export interface RescoreAttestation {
  attestationIssuedAtMs: number;
  entryCount: number;
  entrySetHash: string;
  featureCacheEtag: string;
  featureCacheVersion: string;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const requireScopeCode = (value: string, name: string): string => {
  const normalized = value.trim().padStart(2, "0");
  if (!/^[0-9]{2}$/u.test(normalized)) throw new Error(`invalid attestation ${name}`);
  return normalized;
};

const parseFreshEntry = (value: unknown): FreshEntry => {
  if (!isRecord(value)) throw new Error("invalid attestation entry");
  const ketto = value.kettoTorokuBango;
  const umaban = value.umaban;
  if (typeof ketto !== "string" || ketto.trim() === "") {
    throw new Error("invalid attestation kettoTorokuBango");
  }
  if (typeof umaban !== "number" || !Number.isSafeInteger(umaban) || umaban <= 0) {
    throw new Error("invalid attestation umaban");
  }
  return { kettoTorokuBango: ketto.trim(), umaban };
};

const parseFreshEntriesResponse = (
  value: unknown,
  expected: Omit<FreshEntriesResponse, "entries">,
): FreshEntriesResponse => {
  if (!isRecord(value) || !Array.isArray(value.entries) || value.entries.length === 0) {
    throw new Error("empty or invalid fresh race entries response");
  }
  if (
    value.source !== expected.source ||
    value.date !== expected.date ||
    value.keibajoCode !== expected.keibajoCode ||
    value.raceBango !== expected.raceBango
  ) {
    throw new Error("fresh race entries scope mismatch");
  }
  const entries = value.entries.map(parseFreshEntry);
  const tokens = entries.map((entry) => `${entry.kettoTorokuBango}:${String(entry.umaban)}`);
  if (new Set(tokens).size !== tokens.length) throw new Error("duplicate fresh race entry");
  return { ...expected, entries };
};

const hex = (bytes: ArrayBuffer): string =>
  [...new Uint8Array(bytes)].map((value) => value.toString(16).padStart(2, "0")).join("");

const hashEntries = async (entries: readonly FreshEntry[]): Promise<string> => {
  const contract = entries
    .map((entry) => `${entry.kettoTorokuBango}:${String(entry.umaban)}`)
    .sort()
    .join("\n");
  return hex(await crypto.subtle.digest("SHA-256", encoder.encode(contract)));
};

const requiredText = (value: string | undefined, name: string): string => {
  if (value === undefined || value.trim() === "") throw new Error(`missing ${name}`);
  return value;
};

const fetchFreshEntries = async (
  params: RescoreAttestationParams,
  keibajoCode: string,
  raceBango: string,
): Promise<FreshEntriesResponse> => {
  const catalog = params.env.PC_KEIBA_R2_CATALOG;
  const token = params.env.FINISH_POSITION_ATTESTATION_TOKEN;
  if (catalog === undefined) throw new Error("PC_KEIBA_R2_CATALOG binding is required");
  if (token === undefined || token.trim() === "") {
    throw new Error("FINISH_POSITION_ATTESTATION_TOKEN is required");
  }
  const url = new URL(FRESH_ENTRIES_PATH, CATALOG_ORIGIN);
  url.search = new URLSearchParams({
    date: params.runYmd,
    keibajoCode,
    raceBango,
    source: params.category,
  }).toString();
  const response = await catalog.fetch(
    new Request(url, {
      headers: {
        Authorization: `Bearer ${token}`,
        "Cache-Control": "no-store",
        Pragma: "no-cache",
      },
      signal: AbortSignal.timeout(ATTESTATION_TIMEOUT_MS),
    }),
  );
  if (!response.ok) throw new Error(`fresh race entries failed with HTTP ${response.status}`);
  return parseFreshEntriesResponse(await response.json(), {
    date: params.runYmd,
    keibajoCode,
    raceBango,
    source: params.category,
  });
};

export const createRescoreAttestation = async (
  params: RescoreAttestationParams,
): Promise<RescoreAttestation> => {
  const keibajoCode = requireScopeCode(params.keibajoCode, "keibajoCode");
  const raceBango = requireScopeCode(params.raceBango, "raceBango");
  const featureKey = buildPerRaceFeatCacheKey(
    params.category,
    params.runYmd,
    keibajoCode,
    raceBango,
  );
  const foundationKey = buildDayBaseRaceFoundationKey(
    params.category,
    params.runYmd,
    keibajoCode,
    raceBango,
  );
  const sourceKey = buildDayBaseObjectKey(params);
  const [fresh, featureCache, foundation, sourceDayBase] = await Promise.all([
    fetchFreshEntries(params, keibajoCode, raceBango),
    params.env.FEATURES_CACHE.head(featureKey),
    params.env.FEATURES_CACHE.head(foundationKey),
    params.env.FEATURES_CACHE.head(sourceKey),
  ]);
  if (featureCache === null) throw new Error("missing per-race feature cache");
  if (foundation === null) throw new Error("missing per-race foundation");
  if (sourceDayBase === null) throw new Error("missing source day-base cache");

  const entrySetHash = await hashEntries(fresh.entries);
  const metadata = foundation.customMetadata;
  if (metadata?.["entry-set-hash"] !== entrySetHash) {
    throw new Error("foundation entry-set hash mismatch");
  }
  if (metadata["row-count"] !== String(fresh.entries.length)) {
    throw new Error("foundation entry count mismatch");
  }
  const sourceEtag = requiredText(sourceDayBase.etag, "source day-base etag");
  if (metadata["source-etag"] !== sourceEtag) {
    throw new Error("foundation source etag mismatch");
  }
  const featureCacheEtag = requiredText(featureCache.etag, "feature cache etag");
  const featureCacheVersion = requiredText(featureCache.version, "feature cache version");

  return {
    attestationIssuedAtMs: Date.now(),
    entryCount: fresh.entries.length,
    entrySetHash,
    featureCacheEtag,
    featureCacheVersion,
  };
};

export const addRescoreAttestationToUrl = (
  predictUrl: string,
  attestation: RescoreAttestation,
): string => {
  if (
    !HASH_PATTERN.test(attestation.entrySetHash) ||
    !Number.isSafeInteger(attestation.entryCount) ||
    attestation.entryCount <= 0 ||
    !POSITIVE_INTEGER_PATTERN.test(String(attestation.attestationIssuedAtMs))
  ) {
    throw new Error("invalid rescore attestation");
  }
  const url = new URL(predictUrl);
  url.searchParams.set("entrySetHash", attestation.entrySetHash);
  url.searchParams.set("entryCount", String(attestation.entryCount));
  url.searchParams.set(
    "featureCacheEtag",
    requiredText(attestation.featureCacheEtag, "feature cache etag"),
  );
  url.searchParams.set(
    "featureCacheVersion",
    requiredText(attestation.featureCacheVersion, "feature cache version"),
  );
  url.searchParams.set("attestationIssuedAtMs", String(attestation.attestationIssuedAtMs));
  return url.toString();
};
