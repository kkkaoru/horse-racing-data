// This test runs with Bun and Vitest.
import { expect, test } from "vitest";
import type {
  MasterLookupQueryRunner,
  MasterLookupResult,
  MasterLookupStatement,
} from "./master-lookup";
import {
  createMasterLookupRunnerFromExecutor,
  parseSaveCliArgs,
  runSave,
  type LoggerPort,
  type RunSavePorts,
  type SaveQueryOutcome,
  type SaveSqlExecutor,
  type SaveSqlStatement,
} from "./run-save";
import {
  OVERSEA_SECONDARY_MARKUP_PROFILE_PATH,
  type SecondarySourceMarkupProfile,
} from "./secondary-source-parser";
import type { FileReadPort, HtmlFetchPort } from "./source-loader";

const JRA_RACECARD_ID: string = "pk01dde0110420260101051";
const SECONDARY_RACE_ID: string = "20260725A605";
const VENUE_CODE: string = "A6";
const RACE_NUMBER: string = "05";
const SECONDARY_TEMPLATE: string = "https://example.invalid/card/{RACE_ID}";
const TEST_SECONDARY_PROFILE: SecondarySourceMarkupProfile = {
  horseNumberClassToken: "RunnerNumber",
  gateClassToken: "StartStall",
  horsePathSegment: "/entity-a/",
  jockeyPathPrefix: "/entity-b/",
  trainerPathPrefix: "/entity-c/",
  affiliationLabels: ["StableHome", "StableAway", "ForeignYard"],
};
const TEST_SECONDARY_PROFILE_JSON: string = JSON.stringify(TEST_SECONDARY_PROFILE);

const JRA_HTML: string = `
<div class="cell date">2026年7月25日（土曜） アスコット競馬場<span class="country">（イギリス）</span></div>
<span class="race_name">テストステークス &amp; カップ (G1)</span>
<div class="cell course"><span class="cap">コース：</span>2,390<span class="unit">メートル</span><span class="detail">（芝・右）</span></div>
<div class="cell time">発走時刻：<strong>23時35分</strong><span class="local_time">（現地時間：7月25日 15時35分）</span></div>
<table><tbody>
<tr>
<td class="num">1</td>
<td class="horse"><div class="name"><div class="line"><div class="txt">テストホース</div></div></div>
<div class="odds"><strong class="red">1.6</strong></div><span class="pop_rank">(1<span>番人気</span>)</span>
<div class="cell result">(10.5.1.1)</div><p class="owner">TEST OWNER</p>
<p class="trainer">F.トレーナー<span class="division">(FR)</span></p>
<ul><li class="sire"><span>父：</span>Test Sire</li><li class="mare"><span>母：</span>Test Dam<span class="bloodmare">(母の父：Test Damsire)</span></li></ul></td>
<td class="jockey"><p class="age">せん5/鹿</p><p class="weight">61.0<span>kg</span></p><p class="jockey">M.ジョッキー</p></td>
<td class="waku">7</td>
</tr>
<tr>
<td class="num">2</td>
<td class="horse"><div class="name"><div class="line"><div class="txt">サンプルホース</div></div>
<div class="cell result">(2.3.4.5)</div><p class="owner">SAMPLE OWNER</p>
<p class="trainer">サンプル師</p>
<ul><li class="sire"><span>父：</span>Sample Sire</li><li class="mare"><span>母：</span>Sample Dam<span class="bloodmare">(母の父：Sample Damsire)</span></li></ul></td>
<td class="jockey"><p class="age">牝4/黒鹿</p><p class="weight">56.5<span>kg</span></p><p class="jockey"><a>R.サンプル</a></p></td>
<td class="waku">3</td>
</tr>
</tbody></table>`;

// Horse 1 gate intentionally differs from JRA (7) so gate disagreement is reported.
// Horse 2 matches JRA gate 3. Includes one extra secondary-only horse number 9.
const SECONDARY_HTML: string = `
<table><tbody>
<tr class="RunnerList">
  <td class="StartStall1 Txt_C "><span>1</span></td>
  <td class="RunnerNumber1 Txt_C">1</td>
  <td class="RunnerInfo">
    <span class="RunnerName">
      <a href="https://example.com/db/entity-a/2021190001" target="_blank" title="テストホース">
        テストホース
      </a>
    </span>
  </td>
  <td class="Pilot">
    <a href="https://example.com/db/entity-b/05504/" title="Pilot A">
      <span>Pilot A</span>
    </a>
  </td>
  <td class="Yard">
    <span class="YardLabel">ForeignYard</span>
    <a href="https://example.com/db/entity-c/05701/" title="Yard A">
      <span>Yard A</span>
    </a>
  </td>
</tr>
<tr class="RunnerList">
  <td class="StartStall3 Txt_C "><span>3</span></td>
  <td class="RunnerNumber2 Txt_C">2</td>
  <td class="RunnerInfo">
    <span class="RunnerName">
      <a href="https://example.com/db/entity-a/2020190005" target="_blank" title="サンプルホース">
        サンプルホース
      </a>
    </span>
  </td>
  <td class="Pilot">
    <a href="https://example.com/db/entity-b/05271/" title="Pilot B">
      <span>Pilot B</span>
    </a>
  </td>
  <td class="Yard">
    <span class="YardLabel">StableHome</span>
    <a href="https://example.com/db/entity-c/01038/" title="Yard B">
      <span>Yard B</span>
    </a>
  </td>
</tr>
<tr class="RunnerList">
  <td class="StartStall4 Txt_C "><span>4</span></td>
  <td class="RunnerNumber9 Txt_C">9</td>
  <td class="RunnerInfo">
    <span class="RunnerName">
      <a href="https://example.com/db/entity-a/9999999999" target="_blank" title="EXTRA">
        EXTRA
      </a>
    </span>
  </td>
  <td class="Pilot">
    <a href="https://example.com/db/entity-b/00001/" title="Pilot X">
      <span>Pilot X</span>
    </a>
  </td>
  <td class="Yard">
    <span class="YardLabel">StableAway</span>
    <a href="https://example.com/db/entity-c/00002/" title="Yard X">
      <span>Yard X</span>
    </a>
  </td>
</tr>
</tbody></table>
`;

// Missing affiliation on one row surfaces a secondary parse issue without dropping the runner.
const SECONDARY_HTML_WITH_ISSUE: string = `
<table><tbody>
<tr class="RunnerList">
  <td class="StartStall7 Txt_C "><span>7</span></td>
  <td class="RunnerNumber1 Txt_C">1</td>
  <td class="RunnerInfo">
    <span class="RunnerName">
      <a href="https://example.com/db/entity-a/2021190001" target="_blank" title="テストホース">
        テストホース
      </a>
    </span>
  </td>
  <td class="Pilot">
    <a href="https://example.com/db/entity-b/05504/" title="Pilot A">
      <span>Pilot A</span>
    </a>
  </td>
  <td class="Yard">
    <a href="https://example.com/db/entity-c/05701/" title="Yard A">
      <span>Yard A</span>
    </a>
  </td>
</tr>
<tr class="RunnerList">
  <td class="StartStall3 Txt_C "><span>3</span></td>
  <td class="RunnerNumber2 Txt_C">2</td>
  <td class="RunnerInfo">
    <span class="RunnerName">
      <a href="https://example.com/db/entity-a/2020190005" target="_blank" title="サンプルホース">
        サンプルホース
      </a>
    </span>
  </td>
  <td class="Pilot">
    <a href="https://example.com/db/entity-b/05271/" title="Pilot B">
      <span>Pilot B</span>
    </a>
  </td>
  <td class="Yard">
    <span class="YardLabel">StableHome</span>
    <a href="https://example.com/db/entity-c/01038/" title="Yard B">
      <span>Yard B</span>
    </a>
  </td>
</tr>
</tbody></table>
`;

interface FakeLogger {
  readonly port: LoggerPort;
  readonly infos: string[];
  readonly errors: string[];
}

interface FetchCallLog {
  readonly urls: string[];
}

interface FileReadCallLog {
  readonly paths: string[];
}

interface StatementLog {
  readonly statements: SaveSqlStatement[];
}

const createFakeLogger = (): FakeLogger => {
  const infos: string[] = [];
  const errors: string[] = [];
  return {
    infos,
    errors,
    port: {
      info: (message: string): void => {
        infos.push(message);
      },
      error: (message: string): void => {
        errors.push(message);
      },
    },
  };
};

const createFetchPort = (
  log: FetchCallLog,
  htmlByUrl: ReadonlyMap<string, string>,
): HtmlFetchPort => ({
  fetchHtml: async (url: string): Promise<string> => {
    log.urls.push(url);
    const html: string | undefined = htmlByUrl.get(url);
    if (html === undefined) {
      throw new Error(`Unexpected fetch URL in test: ${url}`);
    }
    return html;
  },
});

const createFileReadPort = (
  log: FileReadCallLog,
  htmlByPath: ReadonlyMap<string, string>,
): FileReadPort => ({
  readFile: async (path: string): Promise<string> => {
    log.paths.push(path);
    const html: string | undefined = htmlByPath.get(path);
    if (html === undefined) {
      throw new Error(`Unexpected file path in test: ${path}`);
    }
    return html;
  },
});

const emptyMasterLookupRunner: MasterLookupQueryRunner = async (): Promise<MasterLookupResult> => ({
  rows: [],
});

const createRecordingExecutor = (
  log: StatementLog,
  handler: (statement: SaveSqlStatement) => Promise<SaveQueryOutcome>,
): SaveSqlExecutor => ({
  execute: async (statement: SaveSqlStatement): Promise<SaveQueryOutcome> => {
    log.statements.push(statement);
    return handler(statement);
  },
});

const baseArgv = (extra: readonly string[]): string[] => [
  JRA_RACECARD_ID,
  SECONDARY_RACE_ID,
  "--venue-code",
  VENUE_CODE,
  "--race-number",
  RACE_NUMBER,
  ...extra,
];

const defaultEnv: Record<string, string | undefined> = {
  OVERSEA_SECONDARY_CARD_URL_TEMPLATE: SECONDARY_TEMPLATE,
};

const buildPorts = (input: {
  readonly logger: LoggerPort;
  readonly fetchPort: HtmlFetchPort;
  readonly fileReadPort: FileReadPort;
  readonly executor: SaveSqlExecutor;
  readonly transactionExecutor?: SaveSqlExecutor;
  readonly masterLookupRunner?: MasterLookupQueryRunner;
  readonly onTransaction?: () => void;
  readonly injectSecondarySourceMarkupProfile?: boolean;
}): RunSavePorts => ({
  fetchPort: input.fetchPort,
  fileReadPort: input.fileReadPort,
  secondarySourceMarkupProfile:
    input.injectSecondarySourceMarkupProfile === false ? undefined : TEST_SECONDARY_PROFILE,
  masterLookupRunner: input.masterLookupRunner ?? emptyMasterLookupRunner,
  executor: input.executor,
  withTransaction: async <T>(callback: (executor: SaveSqlExecutor) => Promise<T>): Promise<T> => {
    if (input.onTransaction !== undefined) {
      input.onTransaction();
    }
    return callback(input.transactionExecutor ?? input.executor);
  },
  logger: input.logger,
});

const emptyRowsOutcome: SaveQueryOutcome = { rowCount: 0, rows: [] };

const selectEmptyHandler = async (statement: SaveSqlStatement): Promise<SaveQueryOutcome> => {
  if (statement.text.startsWith("SELECT * FROM jvd_se")) {
    return emptyRowsOutcome;
  }
  if (statement.text.startsWith("SELECT 1 FROM jvd_se")) {
    return emptyRowsOutcome;
  }
  if (statement.text.startsWith("INSERT INTO")) {
    return { rowCount: 1, rows: [] };
  }
  if (statement.text.startsWith("UPDATE jvd_se")) {
    return emptyRowsOutcome;
  }
  return emptyRowsOutcome;
};

test("parseSaveCliArgs rejects missing positional arguments with usage text", () => {
  const result = parseSaveCliArgs(["--venue-code", "A6", "--race-number", "05"]);
  expect(result.ok).toBe(false);
  if (result.ok) {
    throw new Error("expected parse failure");
  }
  expect(result.message.startsWith("Expected exactly 2 positional arguments, got 0.")).toBe(true);
});

test("parseSaveCliArgs rejects a single positional argument", () => {
  const result = parseSaveCliArgs([JRA_RACECARD_ID, "--venue-code", "A6", "--race-number", "05"]);
  expect(result.ok).toBe(false);
  if (result.ok) {
    throw new Error("expected parse failure");
  }
  expect(result.message.startsWith("Expected exactly 2 positional arguments, got 1.")).toBe(true);
});

test("parseSaveCliArgs rejects missing venue code", () => {
  const result = parseSaveCliArgs([JRA_RACECARD_ID, SECONDARY_RACE_ID, "--race-number", "05"]);
  expect(result.ok).toBe(false);
  if (result.ok) {
    throw new Error("expected parse failure");
  }
  expect(result.message.startsWith("Missing required option --venue-code.")).toBe(true);
});

test("parseSaveCliArgs rejects missing race number", () => {
  const result = parseSaveCliArgs([JRA_RACECARD_ID, SECONDARY_RACE_ID, "--venue-code", "A6"]);
  expect(result.ok).toBe(false);
  if (result.ok) {
    throw new Error("expected parse failure");
  }
  expect(result.message.startsWith("Missing required option --race-number.")).toBe(true);
});

test("parseSaveCliArgs accepts equals-form flags and defaults to dry-run", () => {
  const result = parseSaveCliArgs([
    JRA_RACECARD_ID,
    SECONDARY_RACE_ID,
    "--venue-code=A6",
    "--race-number=05",
    "--jra-file=/cache/jra.html",
    "--secondary-file=/cache/secondary.html",
  ]);
  expect(result).toStrictEqual({
    ok: true,
    args: {
      jraRacecardId: JRA_RACECARD_ID,
      secondaryRaceId: SECONDARY_RACE_ID,
      apply: false,
      jraCachePath: "/cache/jra.html",
      secondaryCachePath: "/cache/secondary.html",
      venueCode: "A6",
      raceNumber: "05",
    },
  });
});

test("parseSaveCliArgs enables apply when --apply is present", () => {
  const result = parseSaveCliArgs(baseArgv(["--apply"]));
  expect(result).toStrictEqual({
    ok: true,
    args: {
      jraRacecardId: JRA_RACECARD_ID,
      secondaryRaceId: SECONDARY_RACE_ID,
      apply: true,
      jraCachePath: null,
      secondaryCachePath: null,
      venueCode: "A6",
      raceNumber: "05",
    },
  });
});

test("parseSaveCliArgs rejects unknown options", () => {
  const result = parseSaveCliArgs(baseArgv(["--force"]));
  expect(result.ok).toBe(false);
  if (result.ok) {
    throw new Error("expected parse failure");
  }
  expect(result.message.startsWith("Unknown option: --force")).toBe(true);
});

test("parseSaveCliArgs rejects a flag missing its value", () => {
  const result = parseSaveCliArgs([JRA_RACECARD_ID, SECONDARY_RACE_ID, "--venue-code"]);
  expect(result.ok).toBe(false);
  if (result.ok) {
    throw new Error("expected parse failure");
  }
  expect(result.message.startsWith("Missing value for --venue-code.")).toBe(true);
});

test("parseSaveCliArgs rejects empty positional identifiers", () => {
  const result = parseSaveCliArgs(["", "", "--venue-code", "A6", "--race-number", "05"]);
  expect(result.ok).toBe(false);
  if (result.ok) {
    throw new Error("expected parse failure");
  }
  expect(
    result.message.startsWith("JRA racecard id and secondary race id must be non-empty."),
  ).toBe(true);
});

test("parseSaveCliArgs lets --dry-run clear a prior --apply", () => {
  const result = parseSaveCliArgs(baseArgv(["--apply", "--dry-run"]));
  expect(result).toStrictEqual({
    ok: true,
    args: {
      jraRacecardId: JRA_RACECARD_ID,
      secondaryRaceId: SECONDARY_RACE_ID,
      apply: false,
      jraCachePath: null,
      secondaryCachePath: null,
      venueCode: "A6",
      raceNumber: "05",
    },
  });
});

test("parseSaveCliArgs accepts space-separated secondary-file and race-number flags", () => {
  const result = parseSaveCliArgs([
    JRA_RACECARD_ID,
    SECONDARY_RACE_ID,
    "--venue-code",
    "A6",
    "--race-number",
    "05",
    "--secondary-file",
    "cache/secondary.html",
  ]);
  expect(result).toStrictEqual({
    ok: true,
    args: {
      jraRacecardId: JRA_RACECARD_ID,
      secondaryRaceId: SECONDARY_RACE_ID,
      apply: false,
      jraCachePath: null,
      secondaryCachePath: "cache/secondary.html",
      venueCode: "A6",
      raceNumber: "05",
    },
  });
});

test("createMasterLookupRunnerFromExecutor maps query rows through the save executor", async () => {
  const statementLog: StatementLog = { statements: [] };
  const runner = createMasterLookupRunnerFromExecutor(
    createRecordingExecutor(statementLog, async () => ({
      rowCount: 1,
      rows: [
        {
          code: "2021190001",
          canonical_name: "カランダガン",
          tozai_shozoku_code: "4",
          banushi_code: "166803",
          banushimei: "TEST OWNER",
          banushimei_hojinkaku: "",
          banushimei_eur: "TEST OWNER",
        },
      ],
    })),
  );

  const result = await runner({
    text: "SELECT 1",
    values: [["2021190001"]],
  });

  expect(result).toStrictEqual({
    rows: [
      {
        code: "2021190001",
        canonical_name: "カランダガン",
        tozai_shozoku_code: "4",
        banushi_code: "166803",
        banushimei: "TEST OWNER",
        banushimei_hojinkaku: "",
        banushimei_eur: "TEST OWNER",
      },
    ],
  });
  expect(statementLog.statements).toStrictEqual([
    {
      text: "SELECT 1",
      values: [["2021190001"]],
    },
  ]);
});

test("runSave dry-run default never writes and exits 0 when the verdict is safe", async () => {
  const logger = createFakeLogger();
  const fetchLog: FetchCallLog = { urls: [] };
  const fileLog: FileReadCallLog = { paths: [] };
  const statementLog: StatementLog = { statements: [] };
  const transactionState: { count: number } = { count: 0 };

  const result = await runSave({
    argv: baseArgv([]),
    env: defaultEnv,
    ports: buildPorts({
      logger: logger.port,
      fetchPort: createFetchPort(
        fetchLog,
        new Map([
          ["https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0110420260101051", JRA_HTML],
          ["https://example.invalid/card/20260725A605", SECONDARY_HTML],
        ]),
      ),
      fileReadPort: createFileReadPort(fileLog, new Map()),
      executor: createRecordingExecutor(statementLog, selectEmptyHandler),
      onTransaction: (): void => {
        transactionState.count += 1;
      },
    }),
  });

  expect(result).toStrictEqual({
    exitCode: 0,
    wrote: false,
    dryRunVerdict: "safe",
    writeSummary: null,
    networkRequestCount: 2,
  });
  expect(transactionState.count).toBe(0);
  expect(fetchLog.urls).toStrictEqual([
    "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0110420260101051",
    "https://example.invalid/card/20260725A605",
  ]);
  expect(fileLog.paths).toStrictEqual([]);
  expect(
    statementLog.statements.some((statement) => statement.text.startsWith("INSERT INTO")),
  ).toBe(false);
  expect(
    logger.infos.some((line) =>
      line.startsWith("Dry-run complete (no write). Pass --apply to write after a safe verdict."),
    ),
  ).toBe(true);
  expect(
    logger.infos.some((line) =>
      line.startsWith(
        "WARNING: gate disagreement between JRA and secondary source for horse number 1:",
      ),
    ),
  ).toBe(true);
  expect(logger.infos.some((line) => line.startsWith("Unmatched secondary horse numbers: 9"))).toBe(
    true,
  );
  expect(logger.infos.some((line) => line === "HTTP requests made: 2")).toBe(true);
});

test("runSave with local files performs zero network fetches", async () => {
  const logger = createFakeLogger();
  const fetchLog: FetchCallLog = { urls: [] };
  const fileLog: FileReadCallLog = { paths: [] };
  const statementLog: StatementLog = { statements: [] };
  const jraPath = "cache/jra.html";
  const secondaryPath = "cache/secondary.html";

  const result = await runSave({
    argv: baseArgv(["--jra-file", jraPath, "--secondary-file", secondaryPath]),
    env: {},
    ports: buildPorts({
      logger: logger.port,
      fetchPort: createFetchPort(fetchLog, new Map()),
      fileReadPort: createFileReadPort(
        fileLog,
        new Map([
          [jraPath, JRA_HTML],
          [secondaryPath, SECONDARY_HTML],
        ]),
      ),
      executor: createRecordingExecutor(statementLog, selectEmptyHandler),
    }),
  });

  expect(result.exitCode).toBe(0);
  expect(result.networkRequestCount).toBe(0);
  expect(result.wrote).toBe(false);
  expect(fetchLog.urls).toStrictEqual([]);
  expect(fileLog.paths).toStrictEqual(["cache/jra.html", "cache/secondary.html"]);
  expect(logger.infos.some((line) => line === "HTTP requests made: 0")).toBe(true);
});

test("runSave loads an operator-supplied secondary markup profile through the file port", async () => {
  const logger = createFakeLogger();
  const fetchLog: FetchCallLog = { urls: [] };
  const fileLog: FileReadCallLog = { paths: [] };
  const statementLog: StatementLog = { statements: [] };
  const jraPath: string = "cache/jra.html";
  const secondaryPath: string = "cache/secondary.html";
  const profilePath: string = "config/secondary-profile.json";

  const result = await runSave({
    argv: baseArgv(["--jra-file", jraPath, "--secondary-file", secondaryPath]),
    env: { [OVERSEA_SECONDARY_MARKUP_PROFILE_PATH]: profilePath },
    ports: buildPorts({
      logger: logger.port,
      fetchPort: createFetchPort(fetchLog, new Map()),
      fileReadPort: createFileReadPort(
        fileLog,
        new Map([
          [jraPath, JRA_HTML],
          [secondaryPath, SECONDARY_HTML],
          [profilePath, TEST_SECONDARY_PROFILE_JSON],
        ]),
      ),
      executor: createRecordingExecutor(statementLog, selectEmptyHandler),
      injectSecondarySourceMarkupProfile: false,
    }),
  });

  expect(result.exitCode).toBe(0);
  expect(fileLog.paths).toStrictEqual([
    "cache/jra.html",
    "cache/secondary.html",
    "config/secondary-profile.json",
  ]);
});

test("runSave rejects a missing operator-supplied secondary markup profile path", async () => {
  const logger = createFakeLogger();
  const fetchLog: FetchCallLog = { urls: [] };
  const fileLog: FileReadCallLog = { paths: [] };
  const statementLog: StatementLog = { statements: [] };
  const jraPath: string = "cache/jra.html";
  const secondaryPath: string = "cache/secondary.html";

  await expect(
    runSave({
      argv: baseArgv(["--jra-file", jraPath, "--secondary-file", secondaryPath]),
      env: {},
      ports: buildPorts({
        logger: logger.port,
        fetchPort: createFetchPort(fetchLog, new Map()),
        fileReadPort: createFileReadPort(
          fileLog,
          new Map([
            [jraPath, JRA_HTML],
            [secondaryPath, SECONDARY_HTML],
          ]),
        ),
        executor: createRecordingExecutor(statementLog, selectEmptyHandler),
        injectSecondarySourceMarkupProfile: false,
      }),
    }),
  ).rejects.toThrowError(
    "Set OVERSEA_SECONDARY_MARKUP_PROFILE_PATH to the path of the operator-supplied secondary-source markup profile JSON file.",
  );
});

test("runSave with only one local file fetches the other source exactly once", async () => {
  const logger = createFakeLogger();
  const fetchLog: FetchCallLog = { urls: [] };
  const fileLog: FileReadCallLog = { paths: [] };
  const statementLog: StatementLog = { statements: [] };
  const jraPath = "cache/jra-only.html";

  const result = await runSave({
    argv: baseArgv(["--jra-file", jraPath]),
    env: defaultEnv,
    ports: buildPorts({
      logger: logger.port,
      fetchPort: createFetchPort(
        fetchLog,
        new Map([["https://example.invalid/card/20260725A605", SECONDARY_HTML]]),
      ),
      fileReadPort: createFileReadPort(fileLog, new Map([[jraPath, JRA_HTML]])),
      executor: createRecordingExecutor(statementLog, selectEmptyHandler),
    }),
  });

  expect(result.networkRequestCount).toBe(1);
  expect(fetchLog.urls).toStrictEqual(["https://example.invalid/card/20260725A605"]);
  expect(fileLog.paths).toStrictEqual(["cache/jra-only.html"]);
  expect(result.exitCode).toBe(0);
});

test("runSave --apply with a safe verdict writes inside one transaction", async () => {
  const logger = createFakeLogger();
  const fetchLog: FetchCallLog = { urls: [] };
  const fileLog: FileReadCallLog = { paths: [] };
  const outerLog: StatementLog = { statements: [] };
  const txLog: StatementLog = { statements: [] };
  const transactionState: { count: number } = { count: 0 };
  const jraPath = "cache/jra.html";
  const secondaryPath = "cache/secondary.html";

  const result = await runSave({
    argv: baseArgv(["--apply", "--jra-file", jraPath, "--secondary-file", secondaryPath]),
    env: {},
    ports: buildPorts({
      logger: logger.port,
      fetchPort: createFetchPort(fetchLog, new Map()),
      fileReadPort: createFileReadPort(
        fileLog,
        new Map([
          [jraPath, JRA_HTML],
          [secondaryPath, SECONDARY_HTML],
        ]),
      ),
      executor: createRecordingExecutor(outerLog, selectEmptyHandler),
      transactionExecutor: createRecordingExecutor(txLog, selectEmptyHandler),
      onTransaction: (): void => {
        transactionState.count += 1;
      },
    }),
  });

  expect(result.exitCode).toBe(0);
  expect(result.wrote).toBe(true);
  expect(result.dryRunVerdict).toBe("safe");
  expect(result.writeSummary).toStrictEqual({
    migrated: 0,
    inserted: 2,
    updated: 0,
    skipped: 0,
    conflicts: [],
  });
  expect(transactionState.count).toBe(1);
  expect(
    txLog.statements.some((statement) => statement.text.startsWith("INSERT INTO jvd_ra")),
  ).toBe(true);
  expect(
    txLog.statements.some((statement) => statement.text.startsWith("INSERT INTO jvd_se")),
  ).toBe(true);
  expect(outerLog.statements.some((statement) => statement.text.startsWith("INSERT INTO"))).toBe(
    false,
  );
  expect(
    logger.infos.some(
      (line) => line === "Write summary: migrated=0 inserted=2 updated=0 skipped=0",
    ),
  ).toBe(true);
  expect(logger.infos.some((line) => line === "Apply complete.")).toBe(true);
});

test("runSave --apply updates existing runners when identity lookup returns ketto rows", async () => {
  // Guards the adapter that must forward SELECT rows into writeJvdSeRunnersIdempotently.
  // Previously only rowCount was forwarded, so rowCount>0 with rows===undefined fail-closed.
  const logger = createFakeLogger();
  const fetchLog: FetchCallLog = { urls: [] };
  const fileLog: FileReadCallLog = { paths: [] };
  const outerLog: StatementLog = { statements: [] };
  const txLog: StatementLog = { statements: [] };
  const transactionState: { count: number } = { count: 0 };
  const jraPath = "cache/jra.html";
  const secondaryPath = "cache/secondary.html";

  const existingRunnerHandler = async (statement: SaveSqlStatement): Promise<SaveQueryOutcome> => {
    if (statement.text.startsWith("SELECT * FROM jvd_se")) {
      return emptyRowsOutcome;
    }
    if (statement.text.startsWith("SELECT ketto_toroku_bango FROM jvd_se")) {
      const umaban: string = String(statement.values[4] ?? "");
      const kettoByUmaban: Record<string, string> = {
        "01": "2021190001",
        "02": "2020190005",
      };
      const ketto: string | undefined = kettoByUmaban[umaban];
      if (ketto === undefined) {
        return emptyRowsOutcome;
      }
      return { rowCount: 1, rows: [{ ketto_toroku_bango: ketto }] };
    }
    if (statement.text.startsWith("INSERT INTO")) {
      return { rowCount: 1, rows: [] };
    }
    return emptyRowsOutcome;
  };

  // Master must verify secondary horse ids so incoming ketto matches stored keys (update path).
  const verifiedHorseMasterLookup: MasterLookupQueryRunner = async (
    statement: MasterLookupStatement,
  ): Promise<MasterLookupResult> => {
    if (!statement.text.includes("jvd_um")) {
      return { rows: [] };
    }
    const codes: readonly string[] = statement.values[0] ?? [];
    return {
      rows: codes.map((code: string) => ({ code, canonical_name: `horse-${code}` })),
    };
  };

  const result = await runSave({
    argv: baseArgv(["--apply", "--jra-file", jraPath, "--secondary-file", secondaryPath]),
    env: {},
    ports: buildPorts({
      logger: logger.port,
      fetchPort: createFetchPort(fetchLog, new Map()),
      fileReadPort: createFileReadPort(
        fileLog,
        new Map([
          [jraPath, JRA_HTML],
          [secondaryPath, SECONDARY_HTML],
        ]),
      ),
      masterLookupRunner: verifiedHorseMasterLookup,
      executor: createRecordingExecutor(outerLog, existingRunnerHandler),
      transactionExecutor: createRecordingExecutor(txLog, existingRunnerHandler),
      onTransaction: (): void => {
        transactionState.count += 1;
      },
    }),
  });

  expect(result.exitCode).toBe(0);
  expect(result.wrote).toBe(true);
  expect(result.dryRunVerdict).toBe("safe");
  expect(result.writeSummary).toStrictEqual({
    migrated: 0,
    inserted: 0,
    updated: 2,
    skipped: 0,
    conflicts: [],
  });
  expect(transactionState.count).toBe(1);
  expect(
    logger.infos.some(
      (line) => line === "Write summary: migrated=0 inserted=0 updated=2 skipped=0",
    ),
  ).toBe(true);
  expect(logger.errors).toHaveLength(0);
});

test("runSave --apply with a REGRESSION aborts without writing and exits non-zero", async () => {
  const logger = createFakeLogger();
  const fetchLog: FetchCallLog = { urls: [] };
  const fileLog: FileReadCallLog = { paths: [] };
  const statementLog: StatementLog = { statements: [] };
  const transactionState: { count: number } = { count: 0 };
  const jraPath = "cache/jra.html";
  const secondaryPath = "cache/secondary.html";

  const regressionHandler = async (statement: SaveSqlStatement): Promise<SaveQueryOutcome> => {
    if (statement.text.startsWith("SELECT * FROM jvd_se")) {
      return {
        rowCount: 1,
        rows: [
          {
            umaban: "01",
            // Real stored horse code; master lookup returns empty so incoming falls back to placeholder.
            ketto_toroku_bango: "2021190001",
            kishu_code: "05504",
            chokyoshi_code: "05701",
            banushi_code: "166803",
            bamei: "テストホース",
          },
        ],
      };
    }
    return emptyRowsOutcome;
  };

  const result = await runSave({
    argv: baseArgv(["--apply", "--jra-file", jraPath, "--secondary-file", secondaryPath]),
    env: {},
    ports: buildPorts({
      logger: logger.port,
      fetchPort: createFetchPort(fetchLog, new Map()),
      fileReadPort: createFileReadPort(
        fileLog,
        new Map([
          [jraPath, JRA_HTML],
          [secondaryPath, SECONDARY_HTML],
        ]),
      ),
      // Empty master lookup forces placeholder codes on every entity field.
      masterLookupRunner: emptyMasterLookupRunner,
      executor: createRecordingExecutor(statementLog, regressionHandler),
      onTransaction: (): void => {
        transactionState.count += 1;
      },
    }),
  });

  expect(result.exitCode).toBe(1);
  expect(result.wrote).toBe(false);
  expect(result.dryRunVerdict).toBe("blocked");
  expect(result.writeSummary).toBe(null);
  expect(transactionState.count).toBe(0);
  expect(
    statementLog.statements.some((statement) => statement.text.startsWith("INSERT INTO")),
  ).toBe(false);
  expect(
    logger.errors.some((line) =>
      line.startsWith("BLOCKED: dry-run reported one or more REGRESSION columns"),
    ),
  ).toBe(true);
  expect(logger.infos.some((line) => line.startsWith("VERDICT blocked"))).toBe(true);
  expect(logger.infos.some((line) => line.startsWith("REGRESSION umaban=01"))).toBe(true);
});

// Horse 1 has a number and name but no gate cell → incomplete for reconcile adapter.
const SECONDARY_HTML_INCOMPLETE_GATE: string = `
<table><tbody>
<tr class="RunnerList">
  <td class="RunnerNumber1 Txt_C">1</td>
  <td class="RunnerInfo">
    <span class="RunnerName">
      <a href="https://example.com/db/entity-a/2021190001" target="_blank" title="テストホース">
        テストホース
      </a>
    </span>
  </td>
  <td class="Pilot">
    <a href="https://example.com/db/entity-b/05504/" title="Pilot A">
      <span>Pilot A</span>
    </a>
  </td>
  <td class="Yard">
    <span class="YardLabel">ForeignYard</span>
    <a href="https://example.com/db/entity-c/05701/" title="Yard A">
      <span>Yard A</span>
    </a>
  </td>
</tr>
<tr class="RunnerList">
  <td class="StartStall3 Txt_C "><span>3</span></td>
  <td class="RunnerNumber2 Txt_C">2</td>
  <td class="RunnerInfo">
    <span class="RunnerName">
      <a href="https://example.com/db/entity-a/2020190005" target="_blank" title="サンプルホース">
        サンプルホース
      </a>
    </span>
  </td>
  <td class="Pilot">
    <a href="https://example.com/db/entity-b/05271/" title="Pilot B">
      <span>Pilot B</span>
    </a>
  </td>
  <td class="Yard">
    <span class="YardLabel">StableHome</span>
    <a href="https://example.com/db/entity-c/01038/" title="Yard B">
      <span>Yard B</span>
    </a>
  </td>
</tr>
</tbody></table>
`;

test("runSave reports incomplete secondary runners skipped by the reconcile adapter", async () => {
  const logger = createFakeLogger();
  const fetchLog: FetchCallLog = { urls: [] };
  const fileLog: FileReadCallLog = { paths: [] };
  const statementLog: StatementLog = { statements: [] };
  const jraPath = "cache/jra.html";
  const secondaryPath = "cache/secondary-incomplete.html";

  const result = await runSave({
    argv: baseArgv(["--jra-file", jraPath, "--secondary-file", secondaryPath]),
    env: {},
    ports: buildPorts({
      logger: logger.port,
      fetchPort: createFetchPort(fetchLog, new Map()),
      fileReadPort: createFileReadPort(
        fileLog,
        new Map([
          [jraPath, JRA_HTML],
          [secondaryPath, SECONDARY_HTML_INCOMPLETE_GATE],
        ]),
      ),
      executor: createRecordingExecutor(statementLog, selectEmptyHandler),
    }),
  });

  expect(result.exitCode).toBe(0);
  expect(
    logger.infos.some((line) =>
      line.startsWith("Secondary runners skipped (incomplete gate/name): 1"),
    ),
  ).toBe(true);
  expect(logger.infos.some((line) => line.startsWith("Unmatched JRA horse numbers: 1"))).toBe(true);
});

test("runSave surfaces secondary parse issues in the report", async () => {
  const logger = createFakeLogger();
  const fetchLog: FetchCallLog = { urls: [] };
  const fileLog: FileReadCallLog = { paths: [] };
  const statementLog: StatementLog = { statements: [] };
  const jraPath = "cache/jra.html";
  const secondaryPath = "cache/secondary-issue.html";

  const result = await runSave({
    argv: baseArgv(["--jra-file", jraPath, "--secondary-file", secondaryPath]),
    env: {},
    ports: buildPorts({
      logger: logger.port,
      fetchPort: createFetchPort(fetchLog, new Map()),
      fileReadPort: createFileReadPort(
        fileLog,
        new Map([
          [jraPath, JRA_HTML],
          [secondaryPath, SECONDARY_HTML_WITH_ISSUE],
        ]),
      ),
      executor: createRecordingExecutor(statementLog, selectEmptyHandler),
    }),
  });

  expect(result.exitCode).toBe(0);
  expect(
    logger.infos.some((line) =>
      line.startsWith("[missing_trainer_affiliation] row=0 horseNumber=1"),
    ),
  ).toBe(true);
});

test("runSave reports usage errors on the error logger and exits non-zero", async () => {
  const logger = createFakeLogger();
  const fetchLog: FetchCallLog = { urls: [] };
  const fileLog: FileReadCallLog = { paths: [] };
  const statementLog: StatementLog = { statements: [] };

  const result = await runSave({
    argv: [],
    env: defaultEnv,
    ports: buildPorts({
      logger: logger.port,
      fetchPort: createFetchPort(fetchLog, new Map()),
      fileReadPort: createFileReadPort(fileLog, new Map()),
      executor: createRecordingExecutor(statementLog, selectEmptyHandler),
    }),
  });

  expect(result.exitCode).toBe(1);
  expect(result.networkRequestCount).toBe(0);
  expect(fetchLog.urls).toStrictEqual([]);
  expect(logger.errors.length > 0).toBe(true);
  expect(logger.errors[0]?.startsWith("Expected exactly 2 positional arguments, got 0.")).toBe(
    true,
  );
});

test("runSave dry-run path still blocks on regression even without --apply", async () => {
  const logger = createFakeLogger();
  const fetchLog: FetchCallLog = { urls: [] };
  const fileLog: FileReadCallLog = { paths: [] };
  const statementLog: StatementLog = { statements: [] };
  const jraPath = "cache/jra.html";
  const secondaryPath = "cache/secondary.html";

  const regressionHandler = async (statement: SaveSqlStatement): Promise<SaveQueryOutcome> => {
    if (statement.text.startsWith("SELECT * FROM jvd_se")) {
      return {
        rowCount: 1,
        rows: [
          {
            umaban: "01",
            ketto_toroku_bango: "2021190001",
            kishu_code: "05504",
            chokyoshi_code: "05701",
            banushi_code: "166803",
            bamei: "テストホース",
          },
        ],
      };
    }
    return emptyRowsOutcome;
  };

  const result = await runSave({
    argv: baseArgv(["--jra-file", jraPath, "--secondary-file", secondaryPath]),
    env: {},
    ports: buildPorts({
      logger: logger.port,
      fetchPort: createFetchPort(fetchLog, new Map()),
      fileReadPort: createFileReadPort(
        fileLog,
        new Map([
          [jraPath, JRA_HTML],
          [secondaryPath, SECONDARY_HTML],
        ]),
      ),
      masterLookupRunner: emptyMasterLookupRunner,
      executor: createRecordingExecutor(statementLog, regressionHandler),
    }),
  });

  expect(result.exitCode).toBe(1);
  expect(result.wrote).toBe(false);
  expect(result.dryRunVerdict).toBe("blocked");
});

test("runSave loads the secondary markup profile from the operator path when not injected", async () => {
  const logger = createFakeLogger();
  const fetchLog: FetchCallLog = { urls: [] };
  const fileLog: FileReadCallLog = { paths: [] };
  const statementLog: StatementLog = { statements: [] };
  const jraPath: string = "cache/jra.html";
  const secondaryPath: string = "cache/secondary.html";
  const profilePath: string = "operator/secondary-markup-profile.json";

  const result = await runSave({
    argv: baseArgv(["--jra-file", jraPath, "--secondary-file", secondaryPath]),
    env: {
      [OVERSEA_SECONDARY_MARKUP_PROFILE_PATH]: profilePath,
    },
    ports: buildPorts({
      logger: logger.port,
      fetchPort: createFetchPort(fetchLog, new Map()),
      fileReadPort: createFileReadPort(
        fileLog,
        new Map([
          [jraPath, JRA_HTML],
          [secondaryPath, SECONDARY_HTML],
          [profilePath, TEST_SECONDARY_PROFILE_JSON],
        ]),
      ),
      executor: createRecordingExecutor(statementLog, selectEmptyHandler),
      injectSecondarySourceMarkupProfile: false,
    }),
  });

  expect(result.exitCode).toBe(0);
  expect(result.networkRequestCount).toBe(0);
  expect(fetchLog.urls).toStrictEqual([]);
  expect(fileLog.paths).toStrictEqual([
    "cache/jra.html",
    "cache/secondary.html",
    "operator/secondary-markup-profile.json",
  ]);
});

test("runSave rejects a missing operator markup profile path when no profile is injected", async () => {
  const logger = createFakeLogger();
  const fetchLog: FetchCallLog = { urls: [] };
  const fileLog: FileReadCallLog = { paths: [] };
  const statementLog: StatementLog = { statements: [] };
  const jraPath: string = "cache/jra.html";
  const secondaryPath: string = "cache/secondary.html";

  const run = runSave({
    argv: baseArgv(["--jra-file", jraPath, "--secondary-file", secondaryPath]),
    env: {},
    ports: buildPorts({
      logger: logger.port,
      fetchPort: createFetchPort(fetchLog, new Map()),
      fileReadPort: createFileReadPort(
        fileLog,
        new Map([
          [jraPath, JRA_HTML],
          [secondaryPath, SECONDARY_HTML],
        ]),
      ),
      executor: createRecordingExecutor(statementLog, selectEmptyHandler),
      injectSecondarySourceMarkupProfile: false,
    }),
  });

  await expect(run).rejects.toThrow(
    "Set OVERSEA_SECONDARY_MARKUP_PROFILE_PATH to the path of the operator-supplied secondary-source markup profile JSON file.",
  );
  expect(fetchLog.urls).toStrictEqual([]);
  expect(fileLog.paths).toStrictEqual(["cache/jra.html", "cache/secondary.html"]);
});
