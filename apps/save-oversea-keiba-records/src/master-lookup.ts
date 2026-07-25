// This file runs with Bun.
// Owner names use exact NFKC-and-space-normalized matching for Japanese names. European names
// ignore case and punctuation, then remove the known card suffixes "ET AL" and "SCEA". A European
// card name may be a prefix of the master name after that normalization. A match is accepted only
// when exactly one master row matches and supplies an owner code; zero or multiple rows are absent.

export interface MasterLookupStatement {
  readonly text: string;
  readonly values: readonly (readonly string[])[];
}

export interface MasterLookupRow extends Readonly<Record<string, string | undefined>> {
  readonly code?: string;
  readonly canonical_name?: string;
  readonly tozai_shozoku_code?: string;
  readonly banushi_code?: string;
  readonly banushimei?: string;
  readonly banushimei_hojinkaku?: string;
  readonly banushimei_eur?: string;
}

export interface MasterLookupResult {
  readonly rows: readonly MasterLookupRow[];
}

export interface MasterLookupQueryRunner {
  (statement: MasterLookupStatement): Promise<MasterLookupResult>;
}

export interface MasterLookupPrefetchRequest {
  readonly horseRegistrationNumbers: readonly string[];
  readonly jockeyCodes: readonly string[];
  readonly trainerCodes: readonly string[];
  readonly ownerNames: readonly string[];
}

export interface MasterEntityRecord {
  readonly exists: boolean;
  readonly canonicalName: string | null;
}

export interface TrainerMasterRecord extends MasterEntityRecord {
  readonly tozaiShozokuCode: string | null;
}

export interface OwnerMasterRecord {
  readonly code: string | null;
  readonly canonicalName: string | null;
}

export interface MasterLookupPort {
  readonly findHorse: (kettoTorokuBango: string) => Promise<MasterEntityRecord>;
  readonly findJockey: (kishuCode: string) => Promise<MasterEntityRecord>;
  readonly findTrainer: (chokyoshiCode: string) => Promise<TrainerMasterRecord>;
  readonly findOwnerByName: (ownerName: string) => Promise<OwnerMasterRecord>;
  readonly prefetch: (request: MasterLookupPrefetchRequest) => Promise<void>;
}

interface EntityLookupParameters {
  readonly code: string;
  readonly cache: Map<string, MasterEntityRecord>;
  readonly buildStatement: CodeQueryBuilder;
  readonly runner: MasterLookupQueryRunner;
}

interface TrainerLookupParameters {
  readonly code: string;
  readonly cache: Map<string, TrainerMasterRecord>;
  readonly runner: MasterLookupQueryRunner;
}

interface OwnerLookupParameters {
  readonly ownerName: string;
  readonly cache: Map<string, OwnerMasterRecord>;
  readonly runner: MasterLookupQueryRunner;
}

interface EntityBatchParameters {
  readonly codes: readonly string[];
  readonly buildStatement: CodeQueryBuilder;
  readonly runner: MasterLookupQueryRunner;
}

interface PrefetchResults {
  readonly horses: ReadonlyMap<string, string | null>;
  readonly jockeys: ReadonlyMap<string, string | null>;
  readonly trainers: ReadonlyMap<string, TrainerMasterRecord>;
  readonly owners: ReadonlyMap<string, OwnerMasterRecord>;
}

interface LookupCaches {
  readonly horses: Map<string, MasterEntityRecord>;
  readonly jockeys: Map<string, MasterEntityRecord>;
  readonly trainers: Map<string, TrainerMasterRecord>;
  readonly owners: Map<string, OwnerMasterRecord>;
}

interface PrefetchParameters {
  readonly request: MasterLookupPrefetchRequest;
  readonly caches: LookupCaches;
  readonly runner: MasterLookupQueryRunner;
}

interface CodeQueryBuilder {
  (codes: readonly string[]): MasterLookupStatement;
}

const HORSE_LOOKUP_SQL: string = `SELECT
  trim(ketto_toroku_bango) AS code,
  trim(bamei) AS canonical_name
FROM jvd_um
WHERE ketto_toroku_bango = ANY($1::text[])`;
const JOCKEY_LOOKUP_SQL: string = `SELECT
  trim(kishu_code) AS code,
  trim(kishumei_ryakusho) AS canonical_name
FROM jvd_ks
WHERE kishu_code = ANY($1::text[])`;
const TRAINER_LOOKUP_SQL: string = `SELECT
  trim(chokyoshi_code) AS code,
  trim(chokyoshimei_ryakusho) AS canonical_name,
  trim(tozai_shozoku_code) AS tozai_shozoku_code
FROM jvd_ch
WHERE chokyoshi_code = ANY($1::text[])`;
const OWNER_LOOKUP_SQL: string = `SELECT
  trim(banushi_code) AS banushi_code,
  trim(banushimei) AS banushimei,
  trim(banushimei_hojinkaku) AS banushimei_hojinkaku,
  trim(banushimei_eur) AS banushimei_eur
FROM jvd_bn
WHERE
  regexp_replace(replace(trim(banushimei), '　', ''), '[[:space:]]', '', 'g') = ANY($1::text[])
  OR regexp_replace(replace(trim(banushimei_hojinkaku), '　', ''), '[[:space:]]', '', 'g') = ANY($1::text[])
  OR EXISTS (
    SELECT 1
    FROM unnest($2::text[]) AS requested(root)
    WHERE upper(regexp_replace(trim(banushimei_eur), '[^A-Za-z0-9]', '', 'g')) LIKE requested.root || '%'
  )`;
const OWNER_SUFFIXES: readonly string[] = ["ETAL", "SCEA"];
const MINIMUM_OWNER_ROOT_LENGTH: number = 6;
const ENTITY_ABSENT: MasterEntityRecord = { exists: false, canonicalName: null };
const TRAINER_ABSENT: TrainerMasterRecord = {
  exists: false,
  canonicalName: null,
  tozaiShozokuCode: null,
};
const OWNER_ABSENT: OwnerMasterRecord = { code: null, canonicalName: null };

export const buildHorseLookupQuery = (codes: readonly string[]): MasterLookupStatement => ({
  text: HORSE_LOOKUP_SQL,
  values: [codes],
});

export const buildJockeyLookupQuery = (codes: readonly string[]): MasterLookupStatement => ({
  text: JOCKEY_LOOKUP_SQL,
  values: [codes],
});

export const buildTrainerLookupQuery = (codes: readonly string[]): MasterLookupStatement => ({
  text: TRAINER_LOOKUP_SQL,
  values: [codes],
});

export const normalizeOwnerName = (ownerName: string): string =>
  ownerName.normalize("NFKC").replaceAll(/\s/gu, "");

export const normalizeEuropeanOwnerName = (ownerName: string): string => {
  const normalizedName: string = ownerName
    .normalize("NFKC")
    .toLocaleUpperCase("en-US")
    .replaceAll(/[^\p{L}\p{N}]/gu, "");
  return OWNER_SUFFIXES.reduce(
    (currentName: string, suffix: string): string =>
      currentName.endsWith(suffix) ? currentName.slice(0, -suffix.length) : currentName,
    normalizedName,
  );
};

export const buildOwnerLookupQuery = (ownerNames: readonly string[]): MasterLookupStatement => {
  const normalizedNames: readonly string[] = ownerNames.map(normalizeOwnerName);
  const europeanRoots: readonly string[] = ownerNames
    .map(normalizeEuropeanOwnerName)
    .filter((ownerRoot: string): boolean => ownerRoot.length >= MINIMUM_OWNER_ROOT_LENGTH);
  return {
    text: OWNER_LOOKUP_SQL,
    values: [normalizedNames, europeanRoots],
  };
};

const normalizeMasterValue = (value: string | undefined): string | null => {
  if (value === undefined) {
    return null;
  }
  const trimmed: string = value.trim();
  return trimmed.length === 0 ? null : trimmed;
};

export const shapeExistingCodes = (rows: readonly MasterLookupRow[]): ReadonlySet<string> =>
  new Set(
    rows
      .map((row: MasterLookupRow): string | undefined => row.code)
      .filter((code: string | undefined): code is string => code !== undefined),
  );

export const shapeEntityRecords = (
  rows: readonly MasterLookupRow[],
): ReadonlyMap<string, string | null> =>
  new Map(
    rows
      .map((row: MasterLookupRow): readonly [string, string | null] | null =>
        row.code === undefined ? null : [row.code, normalizeMasterValue(row.canonical_name)],
      )
      .filter(
        (
          entry: readonly [string, string | null] | null,
        ): entry is readonly [string, string | null] => entry !== null,
      ),
  );

export const shapeTrainerRecords = (
  rows: readonly MasterLookupRow[],
): ReadonlyMap<string, TrainerMasterRecord> =>
  new Map(
    rows
      .map((row: MasterLookupRow): readonly [string, TrainerMasterRecord] | null => {
        if (row.code === undefined) {
          return null;
        }
        return [
          row.code,
          {
            exists: true,
            canonicalName: normalizeMasterValue(row.canonical_name),
            tozaiShozokuCode: normalizeMasterValue(row.tozai_shozoku_code),
          },
        ];
      })
      .filter(
        (
          entry: readonly [string, TrainerMasterRecord] | null,
        ): entry is readonly [string, TrainerMasterRecord] => entry !== null,
      ),
  );

const rowMatchesOwner = (ownerName: string, row: MasterLookupRow): boolean => {
  const normalizedName: string = normalizeOwnerName(ownerName);
  const japaneseMatches: boolean = [row.banushimei, row.banushimei_hojinkaku]
    .filter((name: string | undefined): name is string => name !== undefined)
    .map(normalizeOwnerName)
    .some((name: string): boolean => name === normalizedName);
  if (japaneseMatches) {
    return true;
  }

  const ownerRoot: string = normalizeEuropeanOwnerName(ownerName);
  if (ownerRoot.length < MINIMUM_OWNER_ROOT_LENGTH || row.banushimei_eur === undefined) {
    return false;
  }
  return normalizeEuropeanOwnerName(row.banushimei_eur).startsWith(ownerRoot);
};

export const pickOwnerRecord = (
  ownerName: string,
  rows: readonly MasterLookupRow[],
): OwnerMasterRecord => {
  const matchingRows: readonly MasterLookupRow[] = rows.filter((row: MasterLookupRow): boolean =>
    rowMatchesOwner(ownerName, row),
  );
  if (matchingRows.length !== 1) {
    return OWNER_ABSENT;
  }
  const matchingRow: MasterLookupRow | undefined = matchingRows[0];
  const code: string | null = normalizeMasterValue(matchingRow?.banushi_code);
  return code === null
    ? OWNER_ABSENT
    : { code, canonicalName: normalizeMasterValue(matchingRow?.banushimei) };
};

export const pickOwnerCode = (ownerName: string, rows: readonly MasterLookupRow[]): string | null =>
  pickOwnerRecord(ownerName, rows).code;

export const shapeOwnerMatches = (
  ownerNames: readonly string[],
  rows: readonly MasterLookupRow[],
): ReadonlyMap<string, OwnerMasterRecord> =>
  new Map(
    ownerNames.map((ownerName: string): readonly [string, OwnerMasterRecord] => [
      ownerName,
      pickOwnerRecord(ownerName, rows),
    ]),
  );

const runEntityBatch = async ({
  codes,
  buildStatement,
  runner,
}: EntityBatchParameters): Promise<ReadonlyMap<string, string | null>> => {
  if (codes.length === 0) {
    return new Map<string, string | null>();
  }
  const result: MasterLookupResult = await runner(buildStatement(codes));
  return shapeEntityRecords(result.rows);
};

const runTrainerBatch = async (
  codes: readonly string[],
  runner: MasterLookupQueryRunner,
): Promise<ReadonlyMap<string, TrainerMasterRecord>> => {
  if (codes.length === 0) {
    return new Map<string, TrainerMasterRecord>();
  }
  const result: MasterLookupResult = await runner(buildTrainerLookupQuery(codes));
  return shapeTrainerRecords(result.rows);
};

const runOwnerBatch = async (
  ownerNames: readonly string[],
  runner: MasterLookupQueryRunner,
): Promise<ReadonlyMap<string, OwnerMasterRecord>> => {
  if (ownerNames.length === 0) {
    return new Map<string, OwnerMasterRecord>();
  }
  const result: MasterLookupResult = await runner(buildOwnerLookupQuery(ownerNames));
  return shapeOwnerMatches(ownerNames, result.rows);
};

const findEntity = async ({
  code,
  cache,
  buildStatement,
  runner,
}: EntityLookupParameters): Promise<MasterEntityRecord> => {
  const cachedValue: MasterEntityRecord | undefined = cache.get(code);
  if (cachedValue !== undefined) {
    return cachedValue;
  }
  const records: ReadonlyMap<string, string | null> = await runEntityBatch({
    codes: [code],
    buildStatement,
    runner,
  });
  const record: MasterEntityRecord = records.has(code)
    ? { exists: true, canonicalName: records.get(code) ?? null }
    : ENTITY_ABSENT;
  cache.set(code, record);
  return record;
};

const loadTrainerRecord = async ({
  code,
  cache,
  runner,
}: TrainerLookupParameters): Promise<TrainerMasterRecord> => {
  const cachedValue: TrainerMasterRecord | undefined = cache.get(code);
  if (cachedValue !== undefined) {
    return cachedValue;
  }
  const records: ReadonlyMap<string, TrainerMasterRecord> = await runTrainerBatch([code], runner);
  const record: TrainerMasterRecord = records.get(code) ?? TRAINER_ABSENT;
  cache.set(code, record);
  return record;
};

const findOwner = async ({
  ownerName,
  cache,
  runner,
}: OwnerLookupParameters): Promise<OwnerMasterRecord> => {
  const cachedValue: OwnerMasterRecord | undefined = cache.get(ownerName);
  if (cachedValue !== undefined) {
    return cachedValue;
  }
  const matches: ReadonlyMap<string, OwnerMasterRecord> = await runOwnerBatch([ownerName], runner);
  const record: OwnerMasterRecord = matches.get(ownerName) ?? OWNER_ABSENT;
  cache.set(ownerName, record);
  return record;
};

const cacheEntityResults = (
  codes: readonly string[],
  records: ReadonlyMap<string, string | null>,
  cache: Map<string, MasterEntityRecord>,
): void => {
  codes.forEach((code: string): void => {
    cache.set(
      code,
      records.has(code)
        ? { exists: true, canonicalName: records.get(code) ?? null }
        : ENTITY_ABSENT,
    );
  });
};

const cacheTrainerResults = (
  codes: readonly string[],
  records: ReadonlyMap<string, TrainerMasterRecord>,
  cache: Map<string, TrainerMasterRecord>,
): void => {
  codes.forEach((code: string): void => {
    cache.set(code, records.get(code) ?? TRAINER_ABSENT);
  });
};

const loadPrefetchResults = async (
  request: MasterLookupPrefetchRequest,
  runner: MasterLookupQueryRunner,
): Promise<PrefetchResults> => {
  const [horses, jockeys, trainers, owners]: readonly [
    ReadonlyMap<string, string | null>,
    ReadonlyMap<string, string | null>,
    ReadonlyMap<string, TrainerMasterRecord>,
    ReadonlyMap<string, OwnerMasterRecord>,
  ] = await Promise.all([
    runEntityBatch({
      codes: request.horseRegistrationNumbers,
      buildStatement: buildHorseLookupQuery,
      runner,
    }),
    runEntityBatch({
      codes: request.jockeyCodes,
      buildStatement: buildJockeyLookupQuery,
      runner,
    }),
    runTrainerBatch(request.trainerCodes, runner),
    runOwnerBatch(request.ownerNames, runner),
  ]);
  return { horses, jockeys, trainers, owners };
};

const prefetchLookups = async ({ request, caches, runner }: PrefetchParameters): Promise<void> => {
  const results: PrefetchResults = await loadPrefetchResults(request, runner);
  cacheEntityResults(request.horseRegistrationNumbers, results.horses, caches.horses);
  cacheEntityResults(request.jockeyCodes, results.jockeys, caches.jockeys);
  cacheTrainerResults(request.trainerCodes, results.trainers, caches.trainers);
  results.owners.forEach((record: OwnerMasterRecord, ownerName: string): void => {
    caches.owners.set(ownerName, record);
  });
};

export const createMasterLookupPort = (runner: MasterLookupQueryRunner): MasterLookupPort => {
  const caches: LookupCaches = {
    horses: new Map<string, MasterEntityRecord>(),
    jockeys: new Map<string, MasterEntityRecord>(),
    trainers: new Map<string, TrainerMasterRecord>(),
    owners: new Map<string, OwnerMasterRecord>(),
  };
  return {
    findHorse: (kettoTorokuBango: string): Promise<MasterEntityRecord> =>
      findEntity({
        code: kettoTorokuBango,
        cache: caches.horses,
        buildStatement: buildHorseLookupQuery,
        runner,
      }),
    findJockey: (kishuCode: string): Promise<MasterEntityRecord> =>
      findEntity({
        code: kishuCode,
        cache: caches.jockeys,
        buildStatement: buildJockeyLookupQuery,
        runner,
      }),
    findTrainer: (chokyoshiCode: string): Promise<TrainerMasterRecord> =>
      loadTrainerRecord({ code: chokyoshiCode, cache: caches.trainers, runner }),
    findOwnerByName: (ownerName: string): Promise<OwnerMasterRecord> =>
      findOwner({ ownerName, cache: caches.owners, runner }),
    prefetch: (request: MasterLookupPrefetchRequest): Promise<void> =>
      prefetchLookups({ request, caches, runner }),
  };
};
