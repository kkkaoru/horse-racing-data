// This file runs with Bun.
//
// Orchestration for the overseas racecard save CLI. All business flow lives here so
// unit tests can inject fakes for network, filesystem, master lookup, and SQL.
// The thin main.ts shell only builds real ports and forwards argv/env.

import {
  formatDryRunDiffReport,
  runDryRunDiffGate,
  type DryRunDiffResult,
  type DryRunRaceKey,
  type DiffableRow,
} from "./storage/dry-run-diff";
import {
  resolveMasterVerifiedEntityCodes,
  type MasterLookupPort as EntityMasterLookupPort,
} from "./domain/entity-resolver";
import {
  createBackfillAwareLookup,
  formatMasterBackfillReport,
  planMasterBackfill,
  type MasterBackfillCandidate,
} from "./domain/master-backfill";
import { buildMasterRows } from "./domain/master-row-builder";
import {
  writeJvdSeRunnersIdempotently,
  type IdentityConflict,
  type WriteSummary,
  type JvdRaceKey,
  type SqlExecutor as IdempotentSqlExecutor,
} from "./storage/idempotent-write";
import { parseJraCard } from "./sources/jra-card-parser";
import { mapJvdRows } from "./domain/jvd-mapper";
import {
  createMasterLookupPort,
  type MasterLookupPort,
  type MasterLookupQueryRunner,
  type MasterLookupStatement,
} from "./storage/master-lookup";
import { buildMasterInsertStatements } from "./storage/master-upsert-sql";
import {
  reconcileRunners,
  type JraReconcileRunner,
  type SecondaryReconcileRunner,
  type ReconcileResult,
} from "./domain/reconcile";
import {
  OVERSEA_SECONDARY_MARKUP_PROFILE_PATH,
  parseSecondarySourceMarkupProfileJson,
  parseSecondarySourceRacecard,
  type SecondarySourceMarkupProfile,
  type SecondarySourceParseIssue,
  type SecondarySourceRunner,
} from "./sources/secondary-source-parser";
import {
  buildJraCardUrl,
  buildSecondaryCardUrl,
  loadRaceSources,
  OVERSEA_SECONDARY_CARD_URL_TEMPLATE,
  type FileReadPort,
  type HtmlFetchPort,
  type LoadedSources,
} from "./sources/source-loader";
import type {
  JvdRaRow,
  JvdRows,
  JvdSeRow,
  ParsedJraRace,
  ResolvedEntityCodes,
  RaceStorageIdentity,
  SqlStatement,
} from "./types";
import { buildJvdRaUpsert } from "./storage/upsert-sql";

export type SaveExitCode = 0 | 1;

export const EXIT_SUCCESS: SaveExitCode = 0;
export const EXIT_FAILURE: SaveExitCode = 1;

export const USAGE_MESSAGE: string =
  "Usage: bun run src/main.ts <jra-racecard-id> <secondary-race-id> [options]\n" +
  "\n" +
  "Positional arguments:\n" +
  "  <jra-racecard-id>      JRA overseas racecard CNAME identifier\n" +
  "  <secondary-race-id>    Secondary-source race identifier\n" +
  "\n" +
  "Options:\n" +
  "  --apply                  Write after a safe dry-run diff (default is dry-run only)\n" +
  "  --dry-run                Diff only; do not write (default)\n" +
  "  --jra-file <path>        Read JRA card HTML from a local file (skips JRA HTTP)\n" +
  "  --secondary-file <path>  Read secondary card HTML from a local file (skips secondary HTTP)\n" +
  "  --venue-code <code>      JV venue code for storage (required, e.g. A6)\n" +
  "  --race-number <number>   JV race number for storage (required, e.g. 05)\n" +
  "\n" +
  "Environment:\n" +
  `  ${OVERSEA_SECONDARY_CARD_URL_TEMPLATE}  Secondary card URL template with {RACE_ID}\n` +
  `  ${OVERSEA_SECONDARY_MARKUP_PROFILE_PATH}  Operator-supplied markup profile JSON path\n` +
  "  POSTGRES_DB / POSTGRES_USER / POSTGRES_PASSWORD  Database connection (names only; never log values)\n" +
  "  POSTGRES_HOST / POSTGRES_PORT                     Optional host/port overrides";

const FLAG_APPLY: string = "--apply";
const FLAG_DRY_RUN: string = "--dry-run";
const FLAG_JRA_FILE: string = "--jra-file";
const FLAG_SECONDARY_FILE: string = "--secondary-file";
const FLAG_VENUE_CODE: string = "--venue-code";
const FLAG_RACE_NUMBER: string = "--race-number";

const FLAG_JRA_FILE_PREFIX: string = "--jra-file=";
const FLAG_SECONDARY_FILE_PREFIX: string = "--secondary-file=";
const FLAG_VENUE_CODE_PREFIX: string = "--venue-code=";
const FLAG_RACE_NUMBER_PREFIX: string = "--race-number=";

const REQUIRED_POSITIONAL_COUNT: number = 2;
const EMPTY_NAME: string = "";

export interface LoggerPort {
  readonly info: (message: string) => void;
  readonly error: (message: string) => void;
}

export type SaveQueryParameter = string | readonly string[];

export interface SaveSqlStatement {
  readonly text: string;
  readonly values: readonly SaveQueryParameter[];
}

export interface SaveQueryOutcome {
  readonly rowCount: number;
  readonly rows: readonly Readonly<Record<string, string>>[];
}

export interface SaveSqlExecutor {
  readonly execute: (statement: SaveSqlStatement) => Promise<SaveQueryOutcome>;
}

export interface TransactionRunner {
  <T>(callback: (executor: SaveSqlExecutor) => Promise<T>): Promise<T>;
}

export interface RunSavePorts {
  readonly fetchPort: HtmlFetchPort;
  readonly fileReadPort: FileReadPort;
  readonly secondarySourceMarkupProfile?: SecondarySourceMarkupProfile;
  readonly masterLookupRunner: MasterLookupQueryRunner;
  readonly executor: SaveSqlExecutor;
  readonly withTransaction: TransactionRunner;
  readonly logger: LoggerPort;
}

export interface RunSaveInput {
  readonly argv: readonly string[];
  readonly env: Record<string, string | undefined>;
  readonly ports: RunSavePorts;
}

export interface RunSaveResult {
  readonly exitCode: SaveExitCode;
  readonly wrote: boolean;
  readonly dryRunVerdict: "safe" | "blocked" | null;
  readonly writeSummary: WriteSummary | null;
  readonly networkRequestCount: number;
}

export interface ParsedSaveCliArgs {
  readonly jraRacecardId: string;
  readonly secondaryRaceId: string;
  readonly apply: boolean;
  readonly jraCachePath: string | null;
  readonly secondaryCachePath: string | null;
  readonly venueCode: string;
  readonly raceNumber: string;
}

export type ParseSaveCliArgsResult =
  | { readonly ok: true; readonly args: ParsedSaveCliArgs }
  | { readonly ok: false; readonly message: string };

interface ArgParseState {
  readonly positionals: readonly string[];
  readonly apply: boolean;
  readonly jraFile: string | null;
  readonly secondaryFile: string | null;
  readonly venueCode: string | null;
  readonly raceNumber: string | null;
  readonly error: string | null;
}

interface FlagValueConsumeInput {
  readonly state: ArgParseState;
  readonly argv: readonly string[];
  readonly index: number;
  readonly flagName: string;
  readonly assign: (state: ArgParseState, value: string) => ArgParseState;
}

interface FlagValueConsumeResult {
  readonly state: ArgParseState;
  readonly nextIndex: number;
}

interface AdaptedSecondaryRunners {
  readonly runners: readonly SecondaryReconcileRunner[];
  readonly skippedIncompleteCount: number;
}

interface ResolvedRunnerCodes {
  readonly horseNumber: number;
  readonly codes: ResolvedEntityCodes;
}

interface BuildWritePathInput {
  readonly rows: JvdRows;
  readonly raceKey: JvdRaceKey;
  readonly masterCandidates: readonly MasterBackfillCandidate[];
  readonly withTransaction: TransactionRunner;
}

interface LogReportInput {
  readonly logger: LoggerPort;
  readonly diffResult: DryRunDiffResult;
  readonly reconcileResult: ReconcileResult;
  readonly masterCandidates: readonly MasterBackfillCandidate[];
  readonly secondaryIssues: readonly SecondarySourceParseIssue[];
  readonly networkRequestCount: number;
  readonly skippedIncompleteSecondary: number;
}

const INITIAL_ARG_STATE: ArgParseState = {
  positionals: [],
  apply: false,
  jraFile: null,
  secondaryFile: null,
  venueCode: null,
  raceNumber: null,
  error: null,
};

const isFlagToken = (token: string): boolean => token.startsWith("--");

const consumeFlagValue = ({
  state,
  argv,
  index,
  flagName,
  assign,
}: FlagValueConsumeInput): FlagValueConsumeResult => {
  const value: string | undefined = argv[index + 1];
  if (value === undefined || isFlagToken(value)) {
    return {
      state: {
        ...state,
        error: `Missing value for ${flagName}.`,
      },
      nextIndex: index + 1,
    };
  }
  return {
    state: assign(state, value),
    nextIndex: index + 2,
  };
};

const consumeArgToken = (
  argv: readonly string[],
  index: number,
  state: ArgParseState,
): { readonly state: ArgParseState; readonly nextIndex: number } => {
  const token: string | undefined = argv[index];
  if (token === undefined) {
    return { state, nextIndex: index + 1 };
  }

  if (token === FLAG_APPLY) {
    return { state: { ...state, apply: true }, nextIndex: index + 1 };
  }
  if (token === FLAG_DRY_RUN) {
    return { state: { ...state, apply: false }, nextIndex: index + 1 };
  }

  if (token === FLAG_JRA_FILE) {
    return consumeFlagValue({
      state,
      argv,
      index,
      flagName: FLAG_JRA_FILE,
      assign: (current: ArgParseState, value: string): ArgParseState => ({
        ...current,
        jraFile: value,
      }),
    });
  }
  if (token.startsWith(FLAG_JRA_FILE_PREFIX)) {
    return {
      state: { ...state, jraFile: token.slice(FLAG_JRA_FILE_PREFIX.length) },
      nextIndex: index + 1,
    };
  }

  if (token === FLAG_SECONDARY_FILE) {
    return consumeFlagValue({
      state,
      argv,
      index,
      flagName: FLAG_SECONDARY_FILE,
      assign: (current: ArgParseState, value: string): ArgParseState => ({
        ...current,
        secondaryFile: value,
      }),
    });
  }
  if (token.startsWith(FLAG_SECONDARY_FILE_PREFIX)) {
    return {
      state: { ...state, secondaryFile: token.slice(FLAG_SECONDARY_FILE_PREFIX.length) },
      nextIndex: index + 1,
    };
  }

  if (token === FLAG_VENUE_CODE) {
    return consumeFlagValue({
      state,
      argv,
      index,
      flagName: FLAG_VENUE_CODE,
      assign: (current: ArgParseState, value: string): ArgParseState => ({
        ...current,
        venueCode: value,
      }),
    });
  }
  if (token.startsWith(FLAG_VENUE_CODE_PREFIX)) {
    return {
      state: { ...state, venueCode: token.slice(FLAG_VENUE_CODE_PREFIX.length) },
      nextIndex: index + 1,
    };
  }

  if (token === FLAG_RACE_NUMBER) {
    return consumeFlagValue({
      state,
      argv,
      index,
      flagName: FLAG_RACE_NUMBER,
      assign: (current: ArgParseState, value: string): ArgParseState => ({
        ...current,
        raceNumber: value,
      }),
    });
  }
  if (token.startsWith(FLAG_RACE_NUMBER_PREFIX)) {
    return {
      state: { ...state, raceNumber: token.slice(FLAG_RACE_NUMBER_PREFIX.length) },
      nextIndex: index + 1,
    };
  }

  if (isFlagToken(token)) {
    return {
      state: { ...state, error: `Unknown option: ${token}` },
      nextIndex: index + 1,
    };
  }

  return {
    state: { ...state, positionals: [...state.positionals, token] },
    nextIndex: index + 1,
  };
};

const reduceArgTokens = (
  argv: readonly string[],
  index: number,
  state: ArgParseState,
): ArgParseState => {
  if (index >= argv.length) {
    return state;
  }
  if (state.error !== null) {
    return state;
  }
  const step = consumeArgToken(argv, index, state);
  return reduceArgTokens(argv, step.nextIndex, step.state);
};

export const parseSaveCliArgs = (argv: readonly string[]): ParseSaveCliArgsResult => {
  const state: ArgParseState = reduceArgTokens(argv, 0, INITIAL_ARG_STATE);
  if (state.error !== null) {
    return { ok: false, message: `${state.error}\n${USAGE_MESSAGE}` };
  }

  if (state.positionals.length !== REQUIRED_POSITIONAL_COUNT) {
    return {
      ok: false,
      message: `Expected exactly ${String(REQUIRED_POSITIONAL_COUNT)} positional arguments, got ${String(state.positionals.length)}.\n${USAGE_MESSAGE}`,
    };
  }

  const jraRacecardId: string | undefined = state.positionals[0];
  const secondaryRaceId: string | undefined = state.positionals[1];
  if (jraRacecardId === undefined || secondaryRaceId === undefined) {
    return { ok: false, message: USAGE_MESSAGE };
  }
  if (jraRacecardId.length === 0 || secondaryRaceId.length === 0) {
    return {
      ok: false,
      message: `JRA racecard id and secondary race id must be non-empty.\n${USAGE_MESSAGE}`,
    };
  }

  if (state.venueCode === null || state.venueCode.length === 0) {
    return {
      ok: false,
      message: `Missing required option ${FLAG_VENUE_CODE}.\n${USAGE_MESSAGE}`,
    };
  }
  if (state.raceNumber === null || state.raceNumber.length === 0) {
    return {
      ok: false,
      message: `Missing required option ${FLAG_RACE_NUMBER}.\n${USAGE_MESSAGE}`,
    };
  }

  return {
    ok: true,
    args: {
      jraRacecardId,
      secondaryRaceId,
      apply: state.apply,
      jraCachePath: state.jraFile,
      secondaryCachePath: state.secondaryFile,
      venueCode: state.venueCode,
      raceNumber: state.raceNumber,
    },
  };
};

const toJraReconcileRunner = (runner: ParsedJraRace["runners"][number]): JraReconcileRunner => ({
  horseNumber: runner.horseNumber,
  gate: runner.gate,
  horseName: runner.horseName,
  sex: runner.sex,
  age: runner.age,
  coatColour: runner.coatColour,
  weightCarriedKg: runner.weightCarriedKg,
  jockeyAbbrev: runner.jockeyAbbrev,
  trainerAbbrev: runner.trainerAbbrev,
  trainerCountry: runner.trainerCountry,
  owner: runner.owner,
  winOdds: runner.winOdds,
  popularity: runner.popularity,
  formRecord: runner.formRecord,
  sire: runner.sire,
  dam: runner.dam,
  damsire: runner.damsire,
});

const toSecondaryReconcileRunner = (
  runner: SecondarySourceRunner,
): SecondaryReconcileRunner | null => {
  if (runner.gate === null || runner.horseName === null) {
    return null;
  }
  return {
    horseNumber: runner.horseNumber,
    gate: runner.gate,
    horseName: runner.horseName,
    jockeyName: EMPTY_NAME,
    trainerName: EMPTY_NAME,
    horseId: runner.horseId,
    jockeyId: runner.jockeyId,
    trainerId: runner.trainerId,
    affiliationLabel: runner.trainerAffiliation ?? EMPTY_NAME,
  };
};

const adaptSecondaryRunners = (
  runners: readonly SecondarySourceRunner[],
): AdaptedSecondaryRunners => {
  const adapted: readonly (SecondaryReconcileRunner | null)[] = runners.map(
    toSecondaryReconcileRunner,
  );
  const complete: readonly SecondaryReconcileRunner[] = adapted.filter(
    (runner: SecondaryReconcileRunner | null): runner is SecondaryReconcileRunner =>
      runner !== null,
  );
  return {
    runners: complete,
    skippedIncompleteCount: adapted.length - complete.length,
  };
};

const nonNullIds = (values: readonly (string | null)[]): readonly string[] =>
  values.filter((value: string | null): value is string => value !== null && value.length > 0);

const loadSecondaryMarkupProfile = async (input: {
  readonly env: Record<string, string | undefined>;
  readonly ports: RunSavePorts;
}): Promise<SecondarySourceMarkupProfile> => {
  if (input.ports.secondarySourceMarkupProfile !== undefined) {
    return input.ports.secondarySourceMarkupProfile;
  }
  const profilePath: string | undefined = input.env[OVERSEA_SECONDARY_MARKUP_PROFILE_PATH];
  if (profilePath === undefined || profilePath.length === 0) {
    throw new Error(
      `Set ${OVERSEA_SECONDARY_MARKUP_PROFILE_PATH} to the path of the operator-supplied secondary-source markup profile JSON file.`,
    );
  }
  const profileJson: string = await input.ports.fileReadPort.readFile(profilePath);
  return parseSecondarySourceMarkupProfileJson(profileJson);
};

const prefetchMasterLookup = async (input: {
  readonly reconcileResult: ReconcileResult;
  readonly lookup: MasterLookupPort;
}): Promise<void> => {
  const merged = input.reconcileResult.mergedRunners;
  await input.lookup.prefetch({
    horseRegistrationNumbers: nonNullIds(merged.map((runner) => runner.secondaryHorseId)),
    jockeyCodes: nonNullIds(merged.map((runner) => runner.secondaryJockeyId)),
    trainerCodes: nonNullIds(merged.map((runner) => runner.secondaryTrainerId)),
    ownerNames: merged.map((runner) => runner.owner),
  });
};

const resolveAllEntityCodes = async (input: {
  readonly reconcileResult: ReconcileResult;
  readonly entityLookup: EntityMasterLookupPort;
}): Promise<ReadonlyMap<number, ResolvedEntityCodes>> => {
  const merged = input.reconcileResult.mergedRunners;
  const resolved: readonly ResolvedRunnerCodes[] = await Promise.all(
    merged.map(async (runner): Promise<ResolvedRunnerCodes> => {
      const resolution = await resolveMasterVerifiedEntityCodes({
        identity: {
          horseId: runner.secondaryHorseId,
          jockeyId: runner.secondaryJockeyId,
          trainerId: runner.secondaryTrainerId,
          ownerName: runner.owner,
        },
        lookup: input.entityLookup,
      });
      return { horseNumber: runner.horseNumber, codes: resolution.codes };
    }),
  );

  return new Map(
    resolved.map((entry: ResolvedRunnerCodes): readonly [number, ResolvedEntityCodes] => [
      entry.horseNumber,
      entry.codes,
    ]),
  );
};

const compactRaceDate = (date: string): string => date.replaceAll("-", "");

const toDiffableRows = (runners: readonly JvdSeRow[]): readonly DiffableRow[] =>
  runners.map(
    (runner: JvdSeRow): DiffableRow => ({
      ...runner,
      umaban: runner.umaban,
    }),
  );

const toRaceKey = (race: JvdRaRow): DryRunRaceKey & JvdRaceKey => ({
  kaisai_nen: race.kaisai_nen,
  kaisai_tsukihi: race.kaisai_tsukihi,
  keibajo_code: race.keibajo_code,
  race_bango: race.race_bango,
});

const toIdempotentExecutor = (executor: SaveSqlExecutor): IdempotentSqlExecutor => ({
  execute: async (statement: SqlStatement) => {
    const outcome: SaveQueryOutcome = await executor.execute(statement);
    // Forward rows: identity lookups need ketto_toroku_bango cells. Dropping them
    // left rowCount>0 with rows===undefined and fail-closed on every existing runner.
    return { rowCount: outcome.rowCount, rows: outcome.rows };
  },
});

const applyWrites = async ({
  rows,
  raceKey,
  masterCandidates,
  withTransaction,
}: BuildWritePathInput): Promise<WriteSummary> =>
  withTransaction(async (txExecutor: SaveSqlExecutor): Promise<WriteSummary> => {
    // Insert shape-valid missing masters first so SE entity codes resolve against them.
    // INSERT ... ON CONFLICT DO NOTHING only; never UPDATE/DELETE masters.
    const masterStatements: readonly SqlStatement[] = buildMasterInsertStatements(
      buildMasterRows(masterCandidates),
    );
    for (const statement of masterStatements) {
      await txExecutor.execute(statement);
    }
    await txExecutor.execute(buildJvdRaUpsert(rows.race));
    return writeJvdSeRunnersIdempotently({
      raceKey,
      runners: rows.runners,
      executor: toIdempotentExecutor(txExecutor),
    });
  });

const formatWriteSummary = (summary: WriteSummary): string =>
  `Write summary: migrated=${String(summary.migrated)} inserted=${String(summary.inserted)} updated=${String(summary.updated)} skipped=${String(summary.skipped)} conflicts=${String(summary.conflicts.length)}`;

const formatRaceKey = (raceKey: JvdRaceKey): string =>
  `${raceKey.kaisai_nen}/${raceKey.kaisai_tsukihi}/${raceKey.keibajo_code}/${raceKey.race_bango}`;

const formatIdentityConflict = (conflict: IdentityConflict): string =>
  `IDENTITY CONFLICT: race=${formatRaceKey(conflict.raceKey)} umaban=${conflict.umaban} stored_ketto=${conflict.storedKettoTorokuBango} incoming_ketto=${conflict.incomingKettoTorokuBango}. This runner was NOT written.`;

const logIdentityConflicts = (logger: LoggerPort, conflicts: readonly IdentityConflict[]): void => {
  logger.error(
    `IDENTITY CONFLICTS: ${String(conflicts.length)} runner(s) refused due to ketto_toroku_bango mismatch. No automatic merge or delete was attempted; human ops must decide.`,
  );
  conflicts.forEach((conflict: IdentityConflict): void => {
    logger.error(formatIdentityConflict(conflict));
  });
  logger.error(
    "Apply finished with identity conflicts. Non-conflicting runners in the same batch may still have been written; conflicting runners were skipped.",
  );
};

const logReport = ({
  logger,
  diffResult,
  reconcileResult,
  masterCandidates,
  secondaryIssues,
  networkRequestCount,
  skippedIncompleteSecondary,
}: LogReportInput): void => {
  logger.info("=== Dry-run diff report ===");
  formatDryRunDiffReport(diffResult).forEach((line: string): void => {
    logger.info(line);
  });

  formatMasterBackfillReport(masterCandidates).forEach((line: string): void => {
    logger.info(line);
  });

  logger.info("=== Reconciliation ===");
  logger.info(`Gate disagreements: ${String(reconcileResult.report.gateDisagreements.length)}`);
  reconcileResult.report.gateDisagreements.forEach((warning): void => {
    logger.info(warning.message);
  });
  logger.info(
    `Unmatched JRA horse numbers: ${reconcileResult.report.unmatchedJraHorseNumbers.join(",") || "(none)"}`,
  );
  logger.info(
    `Unmatched secondary horse numbers: ${
      reconcileResult.report.unmatchedSecondaryRunners
        .map((runner) => String(runner.horseNumber))
        .join(",") || "(none)"
    }`,
  );
  logger.info(
    `Secondary runners skipped (incomplete gate/name): ${String(skippedIncompleteSecondary)}`,
  );

  logger.info("=== Secondary source parse issues ===");
  if (secondaryIssues.length === 0) {
    logger.info("(none)");
  }
  secondaryIssues.forEach((issue: SecondarySourceParseIssue): void => {
    logger.info(
      `[${issue.code}] row=${String(issue.rowIndex)} horseNumber=${String(issue.horseNumber)} ${issue.message}`,
    );
  });

  logger.info(`HTTP requests made: ${String(networkRequestCount)}`);
};

const failureResult = (networkRequestCount: number): RunSaveResult => ({
  exitCode: EXIT_FAILURE,
  wrote: false,
  dryRunVerdict: null,
  writeSummary: null,
  networkRequestCount,
});

const successDryRunResult = (input: {
  readonly networkRequestCount: number;
  readonly verdict: "safe" | "blocked";
}): RunSaveResult => ({
  exitCode: input.verdict === "blocked" ? EXIT_FAILURE : EXIT_SUCCESS,
  wrote: false,
  dryRunVerdict: input.verdict,
  writeSummary: null,
  networkRequestCount: input.networkRequestCount,
});

export const runSave = async (input: RunSaveInput): Promise<RunSaveResult> => {
  const { ports, env, argv } = input;
  const parsed: ParseSaveCliArgsResult = parseSaveCliArgs(argv);
  if (!parsed.ok) {
    ports.logger.error(parsed.message);
    return failureResult(0);
  }

  const args: ParsedSaveCliArgs = parsed.args;

  // Always validate the JRA id shape. Secondary URL is only required when the network path is used.
  const jraCardUrl: string = buildJraCardUrl(args.jraRacecardId);
  const secondaryCardUrl: string =
    args.secondaryCachePath === null
      ? buildSecondaryCardUrl(args.secondaryRaceId, env[OVERSEA_SECONDARY_CARD_URL_TEMPLATE])
      : `cache://secondary/${args.secondaryRaceId}`;

  const loaded: LoadedSources = await loadRaceSources({
    jraCardUrl,
    secondaryCardUrl,
    jraCachePath: args.jraCachePath,
    secondaryCachePath: args.secondaryCachePath,
    fetchPort: ports.fetchPort,
    fileReadPort: ports.fileReadPort,
  });

  const jraRace: ParsedJraRace = parseJraCard(loaded.jraHtml);
  const secondaryProfile: SecondarySourceMarkupProfile = await loadSecondaryMarkupProfile({
    env,
    ports,
  });
  const secondaryParsed = parseSecondarySourceRacecard({
    html: loaded.secondaryHtml,
    profile: secondaryProfile,
  });
  const adaptedSecondary: AdaptedSecondaryRunners = adaptSecondaryRunners(secondaryParsed.runners);

  const reconcileResult: ReconcileResult = reconcileRunners({
    jraRunners: jraRace.runners.map(toJraReconcileRunner),
    secondaryRunners: adaptedSecondary.runners,
  });

  const lookup: MasterLookupPort = createMasterLookupPort(ports.masterLookupRunner);
  await prefetchMasterLookup({ reconcileResult, lookup });

  const masterCandidates: readonly MasterBackfillCandidate[] = await planMasterBackfill({
    mergedRunners: reconcileResult.mergedRunners,
    lookup,
    raceDateCompact: compactRaceDate(jraRace.date),
  });

  // Treat planned numeric-only inserts as present so SE mapping uses real codes on apply.
  const entityLookup: EntityMasterLookupPort = createBackfillAwareLookup(lookup, masterCandidates);
  const resolvedCodes: ReadonlyMap<number, ResolvedEntityCodes> = await resolveAllEntityCodes({
    reconcileResult,
    entityLookup,
  });

  const storageIdentity: RaceStorageIdentity = {
    venueCode: args.venueCode,
    raceNumber: args.raceNumber,
  };

  const jvdRows: JvdRows = mapJvdRows({
    race: jraRace,
    storageIdentity,
    resolvedCodes,
  });

  const raceKey = toRaceKey(jvdRows.race);
  const diffResult: DryRunDiffResult = await runDryRunDiffGate({
    raceKey,
    incomingRows: toDiffableRows(jvdRows.runners),
    executor: {
      execute: async (statement: SqlStatement) => {
        const outcome: SaveQueryOutcome = await ports.executor.execute(statement);
        return { rows: outcome.rows };
      },
    },
  });

  logReport({
    logger: ports.logger,
    diffResult,
    reconcileResult,
    masterCandidates,
    secondaryIssues: secondaryParsed.issues,
    networkRequestCount: loaded.networkRequestCount,
    skippedIncompleteSecondary: adaptedSecondary.skippedIncompleteCount,
  });

  if (diffResult.hasRegression || diffResult.verdict === "blocked") {
    ports.logger.error(
      "BLOCKED: dry-run reported one or more REGRESSION columns (a stored real value would be replaced by a placeholder). Aborting without write.",
    );
    return successDryRunResult({
      networkRequestCount: loaded.networkRequestCount,
      verdict: "blocked",
    });
  }

  if (!args.apply) {
    ports.logger.info("Dry-run complete (no write). Pass --apply to write after a safe verdict.");
    ports.logger.info(
      `Would write jvd_ra + ${String(jvdRows.runners.length)} jvd_se runners for race key ${raceKey.kaisai_nen}/${raceKey.kaisai_tsukihi}/${raceKey.keibajo_code}/${raceKey.race_bango}.`,
    );
    if (masterCandidates.length > 0) {
      ports.logger.info(
        `Would insert ${String(masterCandidates.length)} numeric-only master row(s) (ON CONFLICT DO NOTHING).`,
      );
    }
    return successDryRunResult({
      networkRequestCount: loaded.networkRequestCount,
      verdict: "safe",
    });
  }

  const writeSummary: WriteSummary = await applyWrites({
    rows: jvdRows,
    raceKey,
    masterCandidates,
    withTransaction: ports.withTransaction,
  });

  ports.logger.info(formatWriteSummary(writeSummary));

  if (writeSummary.conflicts.length > 0) {
    logIdentityConflicts(ports.logger, writeSummary.conflicts);
    return {
      exitCode: EXIT_FAILURE,
      wrote: true,
      dryRunVerdict: "safe",
      writeSummary,
      networkRequestCount: loaded.networkRequestCount,
    };
  }

  ports.logger.info("Apply complete.");

  return {
    exitCode: EXIT_SUCCESS,
    wrote: true,
    dryRunVerdict: "safe",
    writeSummary,
    networkRequestCount: loaded.networkRequestCount,
  };
};

// Exported for main.ts bridging of MasterLookupStatement without coupling tests to pg.
export const createMasterLookupRunnerFromExecutor = (
  executor: SaveSqlExecutor,
): MasterLookupQueryRunner => {
  return async (statement: MasterLookupStatement) => {
    const outcome: SaveQueryOutcome = await executor.execute({
      text: statement.text,
      values: statement.values,
    });
    return {
      rows: outcome.rows.map((row: Readonly<Record<string, string>>) => ({
        code: row.code,
        canonical_name: row.canonical_name,
        tozai_shozoku_code: row.tozai_shozoku_code,
        banushi_code: row.banushi_code,
        banushimei: row.banushimei,
        banushimei_hojinkaku: row.banushimei_hojinkaku,
        banushimei_eur: row.banushimei_eur,
      })),
    };
  };
};
