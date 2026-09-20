// Run with bun. Private Catalog reader; failures never become absence or PostgreSQL fallback.
import "server-only";
import { readBoundedCatalogBody } from "./catalog-read-body";
import type { RaceSource } from "./codes";
import type { Runner } from "./race-types";

export interface CatalogRaceRunnersQuery {
  source: RaceSource;
  date: string;
  keibajoCode: string;
  raceBango: string;
}
export interface CatalogRaceRunnersBinding {
  fetch(request: Request): Promise<Response>;
}
interface CatalogRunnerIdentity {
  umaban: string;
  identitySource: string | null;
  sourceHorseId: string | null;
  sourceUrl: string | null;
  horseNameFull: string | null;
  jockeyNameFull: string | null;
  trainerNameFull: string | null;
  ownerNameFull: string | null;
}

const RUNNER_FIELDS: readonly string[] = [
  "wakuban",
  "umaban",
  "kettoTorokuBango",
  "bamei",
  "moshokuCode",
  "seibetsuCode",
  "barei",
  "futanJuryo",
  "kishumeiRyakusho",
  "chokyoshimeiRyakusho",
  "banushimei",
  "bataiju",
  "zogenFugo",
  "zogenSa",
  "kakuteiChakujun",
  "tanshoOdds",
  "tanshoNinkijun",
  "sohaTime",
  "timeSa",
  "corner1",
  "corner2",
  "corner3",
  "corner4",
  "kohan3f",
  "blinkerShiyoKubun",
  "sireName",
  "sireSireName",
  "damSireName",
];
const IDENTITY_FIELDS: readonly string[] = [
  "umaban",
  "identitySource",
  "sourceHorseId",
  "sourceUrl",
  "horseNameFull",
  "jockeyNameFull",
  "trainerNameFull",
  "ownerNameFull",
];
const TIMEOUT_MS: number = 35_000;
const ENDPOINT: string = "https://pc-keiba-r2-catalog.internal/v1/race-runners";
const FAILURE: string = "Catalog race runners unavailable";
const UMABAN: RegExp = /^(0[1-9]|1[0-8])$/u;
const MAX_RUNNERS: number = 18;
const MAX_IDENTITIES: number = 19;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const hasNullableStringFields = (
  value: unknown,
  fields: readonly string[],
): value is Record<string, unknown> =>
  isRecord(value) &&
  Object.keys(value).length === fields.length &&
  fields.every((field) => {
    const item: unknown = value[field];
    return item === null || typeof item === "string";
  });

const isRunner = (value: unknown): value is Runner => {
  if (!hasNullableStringFields(value, RUNNER_FIELDS)) return false;
  const umaban: unknown = value.umaban;
  const ketto: unknown = value.kettoTorokuBango;
  return (
    typeof umaban === "string" && UMABAN.test(umaban) && typeof ketto === "string" && ketto !== ""
  );
};

const isIdentity = (value: unknown): value is CatalogRunnerIdentity => {
  if (!hasNullableStringFields(value, IDENTITY_FIELDS)) return false;
  const umaban: unknown = value.umaban;
  return typeof umaban === "string" && UMABAN.test(umaban);
};

const parseRunners = (value: unknown[]): Runner[] => {
  if (value.length > MAX_RUNNERS) throw new Error(FAILURE);
  if (!value.every((row): row is Runner => isRunner(row))) throw new Error(FAILURE);
  return value;
};

const parseIdentities = (value: unknown[]): CatalogRunnerIdentity[] => {
  if (value.length > MAX_IDENTITIES) throw new Error(FAILURE);
  if (!value.every((row): row is CatalogRunnerIdentity => isIdentity(row)))
    throw new Error(FAILURE);
  const seen: Set<string> = new Set();
  for (const identity of value) {
    if (seen.has(identity.umaban)) throw new Error(FAILURE);
    seen.add(identity.umaban);
  }
  return value;
};

const mergeIdentities = (runners: Runner[], identities: CatalogRunnerIdentity[]): Runner[] => {
  if (identities.length === 0) return runners;
  const byUmaban = new Map<string, CatalogRunnerIdentity>(
    identities.map((identity) => [identity.umaban, identity]),
  );
  return runners.map((runner) => {
    const identity: CatalogRunnerIdentity | undefined =
      runner.umaban === null ? undefined : byUmaban.get(runner.umaban);
    if (identity === undefined) return runner;
    return {
      ...runner,
      identitySource: identity.identitySource,
      sourceHorseId: identity.sourceHorseId,
      sourceUrl: identity.sourceUrl,
      horseNameFull: identity.horseNameFull,
      jockeyNameFull: identity.jockeyNameFull,
      trainerNameFull: identity.trainerNameFull,
      ownerNameFull: identity.ownerNameFull,
    };
  });
};

export const readCatalogRaceRunners = async (
  binding: CatalogRaceRunnersBinding | undefined,
  query: CatalogRaceRunnersQuery,
): Promise<Runner[]> => {
  if (binding === undefined) throw new Error(FAILURE);
  const url: URL = new URL(ENDPOINT);
  url.search = new URLSearchParams({
    source: query.source,
    date: query.date,
    keibajoCode: query.keibajoCode,
    raceBango: query.raceBango,
  }).toString();
  try {
    const response: Response = await binding.fetch(
      new Request(url, {
        method: "GET",
        // Workers supports manual/follow, not error; non-200 responses are rejected below.
        redirect: "manual",
        signal: AbortSignal.timeout(TIMEOUT_MS),
      }),
    );
    if (response.status !== 200 || response.headers.get("cache-control") !== "no-store") {
      await response.body?.cancel();
      throw new Error(FAILURE);
    }
    const value: unknown = await readBoundedCatalogBody(response);
    if (!isRecord(value) || !Array.isArray(value.runners) || !Array.isArray(value.identities))
      throw new Error(FAILURE);
    const runners: Runner[] = parseRunners(value.runners);
    const identities: CatalogRunnerIdentity[] = parseIdentities(value.identities);
    return mergeIdentities(runners, identities);
  } catch {
    throw new Error(FAILURE);
  }
};
