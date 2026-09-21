// Runs with bun; read-only Catalog runner projection preserving absence versus provider failure.
export interface RaceRunnersReadInput {
  namespace: string;
  source: "jra" | "nar";
  date: string;
  keibajoCode: string;
  raceBango: string;
}
export interface RaceRunnersReadOptions {
  input: RaceRunnersReadInput;
  query: (sql: string) => Promise<unknown[]>;
}
export interface RaceRunnersIdentityRow {
  umaban: string;
  identitySource: string | null;
  sourceHorseId: string | null;
  sourceUrl: string | null;
  horseNameFull: string | null;
  jockeyNameFull: string | null;
  trainerNameFull: string | null;
  ownerNameFull: string | null;
}
export interface RaceRunnersResult {
  runners: Record<string, string | null>[];
  identities: RaceRunnersIdentityRow[];
}

interface RunnerColumn {
  column: string;
  property: string;
}
interface IdentityColumn {
  column: string;
  property: keyof Omit<RaceRunnersIdentityRow, "umaban">;
}

// Raw JV/NV values, never normalised: the viewer formats these itself and the
// incumbent PostgreSQL projection returns the same unpadded strings.
const RUNNER_COLUMNS: readonly RunnerColumn[] = [
  { column: "wakuban", property: "wakuban" },
  { column: "umaban", property: "umaban" },
  { column: "ketto_toroku_bango", property: "kettoTorokuBango" },
  { column: "bamei", property: "bamei" },
  { column: "moshoku_code", property: "moshokuCode" },
  { column: "seibetsu_code", property: "seibetsuCode" },
  { column: "barei", property: "barei" },
  { column: "futan_juryo", property: "futanJuryo" },
  { column: "kishumei_ryakusho", property: "kishumeiRyakusho" },
  { column: "chokyoshimei_ryakusho", property: "chokyoshimeiRyakusho" },
  { column: "banushimei", property: "banushimei" },
  { column: "bataiju", property: "bataiju" },
  { column: "zogen_fugo", property: "zogenFugo" },
  { column: "zogen_sa", property: "zogenSa" },
  { column: "kakutei_chakujun", property: "kakuteiChakujun" },
  { column: "tansho_odds", property: "tanshoOdds" },
  { column: "tansho_ninkijun", property: "tanshoNinkijun" },
  { column: "soha_time", property: "sohaTime" },
  { column: "time_sa", property: "timeSa" },
  { column: "corner_1", property: "corner1" },
  { column: "corner_2", property: "corner2" },
  { column: "corner_3", property: "corner3" },
  { column: "corner_4", property: "corner4" },
  { column: "kohan_3f", property: "kohan3f" },
  { column: "blinker_shiyo_kubun", property: "blinkerShiyoKubun" },
  { column: "sire_name", property: "sireName" },
  { column: "sire_sire_name", property: "sireSireName" },
  { column: "dam_sire_name", property: "damSireName" },
];
const IDENTITY_COLUMNS: readonly IdentityColumn[] = [
  { column: "source", property: "identitySource" },
  { column: "source_horse_id", property: "sourceHorseId" },
  { column: "source_url", property: "sourceUrl" },
  { column: "horse_name_full", property: "horseNameFull" },
  { column: "jockey_name_full", property: "jockeyNameFull" },
  { column: "trainer_name_full", property: "trainerNameFull" },
  { column: "owner_name_full", property: "ownerNameFull" },
];
const IDENTIFIER: RegExp = /^[A-Za-z_][A-Za-z0-9_]*$/u;
const DATE: RegExp = /^\d{8}$/u;
const VENUE_CODE: RegExp = /^[0-9A-Z]{2}$/u;
const RACE_NUMBER: RegExp = /^\d{2}$/u;
const UMABAN: RegExp = /^(0[1-9]|1[0-8])$/u;
const IDENTITY_COLUMN_COUNT: number = 8;
const RUNNER_COLUMN_COUNT: number = 28;
const ASCII_EDGE_SPACES: RegExp = /^ +| +$/gu;
const MAX_RUNNERS: number = 18;
const MAX_IDENTITIES: number = 19;

const validDate = (date: string): boolean => {
  if (!DATE.test(date)) return false;
  const iso: string = `${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6, 8)}`;
  const parsed: Date = new Date(`${iso}T00:00:00.000Z`);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === iso;
};

const requireInput = (input: RaceRunnersReadInput): void => {
  if (
    !IDENTIFIER.test(input.namespace) ||
    !validDate(input.date) ||
    !VENUE_CODE.test(input.keibajoCode) ||
    !RACE_NUMBER.test(input.raceBango) ||
    (input.source !== "jra" && input.source !== "nar")
  )
    throw new Error("Invalid race runners input");
};

// The incumbent viewer query trims ASCII edge spaces from the overseas
// identity names and reports an empty value as null (PostgreSQL `btrim` with
// the default space character). Reproduce that exactly. Bloodline names are
// NOT trimmed: PostgreSQL's `[[:space:]　]` edge trim is a no-op for the
// padded `ketto_joho_*` values, so the raw mirror bytes are already the
// incumbent output.
const trimmedIdentityName = (value: unknown): string | null => {
  if (typeof value !== "string") return null;
  const trimmed: string = value.replace(ASCII_EDGE_SPACES, "");
  return trimmed === "" ? null : trimmed;
};

export const buildRaceRunnersReadSql = (input: RaceRunnersReadInput): string => {
  requireInput(input);
  const runnerTable: string = input.source === "jra" ? "jvd_se" : "nvd_se";
  const masterTable: string = input.source === "jra" ? "jvd_um" : "nvd_um";
  // Provisional JV rows coexist with confirmed rows for the same umaban. The
  // catalog mirror carries the same flag as ijo_kubun_code, and R2 SQL has no
  // `~` operator, so the 01-18 window uses a numeric cast instead of a regex.
  const provisionalFilter: string =
    input.source === "jra"
      ? "\n  AND coalesce(btrim(se.ijo_kubun_code), '0') NOT IN ('1', '2')"
      : "";
  // JV/NV `se` snapshots carry no bloodline columns, so the last three runner
  // projections come from the master join instead of `se` itself.
  const bloodlineColumns: readonly string[] = ["sire_name", "sire_sire_name", "dam_sire_name"];
  const runnerColumns: string = RUNNER_COLUMNS.filter(
    ({ column }) => !bloodlineColumns.includes(column),
  )
    .map(({ column }) => `se.${column}`)
    .join(", ");
  const bloodline: string =
    input.source === "jra"
      ? `um.ketto_joho_01b AS sire_name, um.ketto_joho_03b AS sire_sire_name, um.ketto_joho_05b AS dam_sire_name`
      : `NULL AS sire_name, NULL AS sire_sire_name, NULL AS dam_sire_name`;
  const masterJoin: string =
    input.source === "jra"
      ? `\nLEFT JOIN ${input.namespace}.${masterTable} um ON um.ketto_toroku_bango = se.ketto_toroku_bango`
      : "";
  return `SELECT ${runnerColumns}, ${bloodline}
FROM ${input.namespace}.${runnerTable} se${masterJoin}
WHERE se.kaisai_nen = '${input.date.slice(0, 4)}'
  AND se.kaisai_tsukihi = '${input.date.slice(4)}'
  AND se.keibajo_code = '${input.keibajoCode}'
  AND se.race_bango = '${input.raceBango}'${provisionalFilter}
  AND try_cast(nullif(btrim(coalesce(se.umaban, '')), '') AS INT) BETWEEN 1 AND 18
  AND nullif(btrim(coalesce(se.ketto_toroku_bango, '')), '') IS NOT NULL
ORDER BY try_cast(nullif(se.umaban, '') AS INT) ASC, se.ketto_toroku_bango ASC
LIMIT ${MAX_RUNNERS + 1}`;
};

// Overseas identities live in their own table; joining it into the runner
// statement exceeded R2 SQL expression depth (error 40018), so it is read
// separately and merged by umaban.
export const buildRaceRunnersIdentitySql = (input: RaceRunnersReadInput): string => {
  requireInput(input);
  if (input.source !== "jra") throw new Error("Overseas identities exist for JRA races only");
  return `SELECT umaban, ${IDENTITY_COLUMNS.map(({ column }) => column).join(", ")}
FROM ${input.namespace}.oversea_runner_identity
WHERE race_source = 'jra'
  AND kaisai_nen = '${input.date.slice(0, 4)}'
  AND kaisai_tsukihi = '${input.date.slice(4)}'
  AND keibajo_code = '${input.keibajoCode}'
  AND race_bango = '${input.raceBango}'
ORDER BY umaban ASC
LIMIT ${MAX_IDENTITIES}`;
};

const parseRunnerRow = (value: unknown): Record<string, string | null> => {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new Error("Invalid race runner row");
  if (Object.keys(value).length !== RUNNER_COLUMN_COUNT)
    throw new Error("Missing or invalid race runner field");
  const row: Record<string, string | null> = {};
  for (const { column, property } of RUNNER_COLUMNS) {
    const field: unknown = Reflect.get(value, column);
    if (field !== null && typeof field !== "string")
      throw new Error("Missing or invalid race runner field");
    row[property] = field;
  }
  return row;
};

const parseIdentityRow = (value: unknown): RaceRunnersIdentityRow => {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new Error("Invalid overseas identity row");
  if (Object.keys(value).length !== IDENTITY_COLUMN_COUNT)
    throw new Error("Missing or invalid overseas identity field");
  const umaban: unknown = Reflect.get(value, "umaban");
  if (typeof umaban !== "string" || !UMABAN.test(umaban))
    throw new Error("Invalid overseas identity umaban");
  const identity: RaceRunnersIdentityRow = {
    umaban,
    identitySource: null,
    sourceHorseId: null,
    sourceUrl: null,
    horseNameFull: null,
    jockeyNameFull: null,
    trainerNameFull: null,
    ownerNameFull: null,
  };
  for (const { column, property } of IDENTITY_COLUMNS) {
    const field: unknown = Reflect.get(value, column);
    if (field !== null && typeof field !== "string")
      throw new Error("Missing or invalid overseas identity field");
    identity[property] = trimmedIdentityName(field);
  }
  return identity;
};

const parseRunnerRows = (
  rows: unknown[],
  input: RaceRunnersReadInput,
): Record<string, string | null>[] => {
  if (rows.length > MAX_RUNNERS) throw new Error("Too many race runners");
  const runners: Record<string, string | null>[] = rows.map((row) => parseRunnerRow(row));
  const identities: Set<string> = new Set();
  for (const [index, runner] of runners.entries()) {
    const umaban: string | null = runner.umaban ?? null;
    const ketto: string | null = runner.kettoTorokuBango ?? null;
    if (umaban === null || !UMABAN.test(umaban)) throw new Error("Invalid race runner umaban");
    if (ketto === null || ketto === "") throw new Error("Invalid race runner horse identity");
    const key: string = `${umaban}/${ketto}`;
    if (identities.has(key)) throw new Error("Duplicate race runner identity");
    identities.add(key);
    const previous: Record<string, string | null> | undefined = runners[index - 1];
    if (previous !== undefined && (previous.umaban ?? "") > umaban)
      throw new Error("Unordered race runners");
  }
  if (input.source === "nar" && runners.some((runner) => runner.sireName !== null))
    throw new Error("NAR runners must not carry bloodline names");
  return runners;
};

const parseIdentityRows = (rows: unknown[]): RaceRunnersIdentityRow[] => {
  if (rows.length > MAX_IDENTITIES) throw new Error("Too many overseas identities");
  const identities: RaceRunnersIdentityRow[] = rows.map((row) => parseIdentityRow(row));
  const seen: Set<string> = new Set();
  for (const identity of identities) {
    if (seen.has(identity.umaban)) throw new Error("Duplicate overseas identity");
    seen.add(identity.umaban);
  }
  return identities;
};

export const readRaceRunners = async (
  options: RaceRunnersReadOptions,
): Promise<RaceRunnersResult> => {
  const runners: Record<string, string | null>[] = parseRunnerRows(
    await options.query(buildRaceRunnersReadSql(options.input)),
    options.input,
  );
  if (options.input.source !== "jra") return { runners, identities: [] };
  return {
    runners,
    identities: parseIdentityRows(await options.query(buildRaceRunnersIdentitySql(options.input))),
  };
};
