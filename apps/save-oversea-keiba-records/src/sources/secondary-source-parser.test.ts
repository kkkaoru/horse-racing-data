// This test runs with Bun and Vitest.
import { expect, test } from "vitest";
import {
  loadSecondarySourceMarkupProfile,
  OVERSEA_SECONDARY_MARKUP_PROFILE_PATH,
  parseSecondarySourceMarkupProfileJson,
  parseSecondarySourceRacecard,
  type SecondarySourceMarkupProfile,
} from "./secondary-source-parser";

// Fully neutral synthetic tokens/routes invented for tests. They must not
// resemble any live secondary-source class names or identity path prefixes.
const TEST_PROFILE: SecondarySourceMarkupProfile = {
  horseNumberClassToken: "RunnerNumber",
  gateClassToken: "StartStall",
  horsePathSegment: "/entity-a/",
  jockeyPathPrefix: "/entity-b/",
  trainerPathPrefix: "/entity-c/",
  affiliationLabels: ["StableHome", "StableAway", "ForeignYard"],
};

const TWO_RUNNER_HTML: string = `
<table><tbody>
<tr class="RunnerList">
  <td class="StartStall1 Txt_C "><span>3</span></td>
  <td class="RunnerNumber2 Txt_C">2</td>
  <td class="RunnerInfo">
    <span class="RunnerName">
      <a href="https://example.com/db/entity-a/000a02caba" target="_blank" title="Alpha Sample">
        Alpha Sample<img src="/icon.png" alt="" />
      </a>
    </span>
  </td>
  <td class="Pilot">
    <a href="https://example.com/db/entity-b/a02c7/" title="Jockey A">
      <span>Jockey A</span>
    </a>
  </td>
  <td class="Coach">
    <span class="YardTag">ForeignYard</span>
    <a href="https://example.com/db/entity-c/05518/" title="Trainer A">
      <span>Trainer A</span>
    </a>
  </td>
</tr>
<tr class="RunnerList">
  <td class="StartStall1 Txt_C "><span>1</span></td>
  <td class="RunnerNumber1 Txt_C">1</td>
  <td class="RunnerInfo">
    <span class="RunnerName">
      <a href="https://example.com/db/entity-a/2021190001" target="_blank" title="Beta Sample">
        Beta Sample
      </a>
    </span>
  </td>
  <td class="Pilot">
    <a href="https://example.com/db/entity-b/05504/" title="Jockey B">
      <span>Jockey B</span>
    </a>
  </td>
  <td class="Coach">
    <span class="YardTag">StableHome</span>
    <a href="https://example.com/db/entity-c/05701/" title="Trainer B">
      <span>Trainer B</span>
    </a>
  </td>
</tr>
</tbody></table>
`;

test("parses runners sorted by horse number and accepts numeric plus alphanumeric ids", () => {
  expect(
    parseSecondarySourceRacecard({ html: TWO_RUNNER_HTML, profile: TEST_PROFILE }),
  ).toStrictEqual({
    runners: [
      {
        horseNumber: 1,
        gate: 1,
        horseName: "Beta Sample",
        horseId: "2021190001",
        jockeyId: "05504",
        trainerId: "05701",
        trainerAffiliation: "StableHome",
      },
      {
        horseNumber: 2,
        gate: 3,
        horseName: "Alpha Sample",
        horseId: "000a02caba",
        jockeyId: "a02c7",
        trainerId: "05518",
        trainerAffiliation: "ForeignYard",
      },
    ],
    issues: [],
  });
});

test("reads horse number from the number cell even when rows are ordered by gate", () => {
  const gateOrderedHtml: string = `
<table><tbody>
<tr>
  <td class="StartStall2 Txt_C"><span>2</span></td>
  <td class="RunnerNumber9 Txt_C">9</td>
  <td><a href="/entity-a/2020103060" title="Gate First">Gate First</a></td>
  <td><a href="/entity-b/05366">J9</a></td>
  <td><span>StableAway</span><a href="/entity-c/01073">T9</a></td>
</tr>
<tr>
  <td class="StartStall1 Txt_C"><span>1</span></td>
  <td class="RunnerNumber4 Txt_C">4</td>
  <td><a href="/entity-a/000a029d48" title="Gate Second">Gate Second</a></td>
  <td><a href="/entity-b/a033f/">J4</a></td>
  <td><span>ForeignYard</span><a href="/entity-c/05519/">T4</a></td>
</tr>
</tbody></table>`;

  expect(
    parseSecondarySourceRacecard({ html: gateOrderedHtml, profile: TEST_PROFILE }).runners,
  ).toStrictEqual([
    {
      horseNumber: 4,
      gate: 1,
      horseName: "Gate Second",
      horseId: "000a029d48",
      jockeyId: "a033f",
      trainerId: "05519",
      trainerAffiliation: "ForeignYard",
    },
    {
      horseNumber: 9,
      gate: 2,
      horseName: "Gate First",
      horseId: "2020103060",
      jockeyId: "05366",
      trainerId: "01073",
      trainerAffiliation: "StableAway",
    },
  ]);
});

test("reports an empty document when no runner rows are present", () => {
  expect(
    parseSecondarySourceRacecard({
      html: "<html><body><p>no table</p></body></html>",
      profile: TEST_PROFILE,
    }),
  ).toStrictEqual({
    runners: [],
    issues: [
      {
        code: "no_runner_rows",
        message: "Secondary source document has no runner rows.",
        rowIndex: -1,
        horseNumber: null,
      },
    ],
  });
});

test("records a missing horse-number cell without inventing a runner", () => {
  const html: string = `
<table><tbody>
<tr>
  <td class="StartStall1 Txt_C"><span>1</span></td>
  <td class="other">x</td>
  <td><a href="/entity-a/2021190001" title="No Number">No Number</a></td>
  <td><a href="/entity-b/05504">J</a></td>
  <td><span>ForeignYard</span><a href="/entity-c/05701">T</a></td>
</tr>
</tbody></table>`;

  expect(parseSecondarySourceRacecard({ html, profile: TEST_PROFILE })).toStrictEqual({
    runners: [],
    issues: [
      {
        code: "missing_horse_number",
        message: "Runner row is missing a horse-number cell.",
        rowIndex: 0,
        horseNumber: null,
      },
    ],
  });
});

test("records an invalid horse number without inventing a runner", () => {
  const html: string = `
<table><tbody>
<tr>
  <td class="StartStall1 Txt_C"><span>1</span></td>
  <td class="RunnerNumberX Txt_C">N/A</td>
  <td><a href="/entity-a/2021190001" title="Bad Number">Bad Number</a></td>
  <td><a href="/entity-b/05504">J</a></td>
  <td><span>ForeignYard</span><a href="/entity-c/05701">T</a></td>
</tr>
</tbody></table>`;

  expect(parseSecondarySourceRacecard({ html, profile: TEST_PROFILE })).toStrictEqual({
    runners: [],
    issues: [
      {
        code: "invalid_horse_number",
        message: "Runner row has a non-numeric horse number.",
        rowIndex: 0,
        horseNumber: null,
      },
    ],
  });
});

test("keeps a runner and reports missing optional identity fields", () => {
  const html: string = `
<table><tbody>
<tr>
  <td class="RunnerNumber5 Txt_C">5</td>
</tr>
</tbody></table>`;

  expect(parseSecondarySourceRacecard({ html, profile: TEST_PROFILE })).toStrictEqual({
    runners: [
      {
        horseNumber: 5,
        gate: null,
        horseName: null,
        horseId: null,
        jockeyId: null,
        trainerId: null,
        trainerAffiliation: null,
      },
    ],
    issues: [
      {
        code: "missing_gate",
        message: "Runner row is missing a gate cell.",
        rowIndex: 0,
        horseNumber: 5,
      },
      {
        code: "missing_horse_id",
        message: "Runner row is missing a horse identity path.",
        rowIndex: 0,
        horseNumber: 5,
      },
      {
        code: "missing_horse_name",
        message: "Runner row is missing a horse name.",
        rowIndex: 0,
        horseNumber: 5,
      },
      {
        code: "missing_jockey_id",
        message: "Runner row is missing a jockey identity path.",
        rowIndex: 0,
        horseNumber: 5,
      },
      {
        code: "missing_trainer_id",
        message: "Runner row is missing a trainer identity path.",
        rowIndex: 0,
        horseNumber: 5,
      },
      {
        code: "missing_trainer_affiliation",
        message: "Runner row is missing a trainer affiliation label.",
        rowIndex: 0,
        horseNumber: 5,
      },
    ],
  });
});

test("reports an invalid gate while still returning the runner", () => {
  const html: string = `
<table><tbody>
<tr>
  <td class="StartStall1 Txt_C"><span>G</span></td>
  <td class="RunnerNumber3 Txt_C">3</td>
  <td><a href="/entity-a/2022105519" title="Odd Gate">Odd Gate</a></td>
  <td><a href="/entity-b/05339">J</a></td>
  <td><span>StableAway</span><a href="/entity-c/01038">T</a></td>
</tr>
</tbody></table>`;

  expect(parseSecondarySourceRacecard({ html, profile: TEST_PROFILE })).toStrictEqual({
    runners: [
      {
        horseNumber: 3,
        gate: null,
        horseName: "Odd Gate",
        horseId: "2022105519",
        jockeyId: "05339",
        trainerId: "01038",
        trainerAffiliation: "StableAway",
      },
    ],
    issues: [
      {
        code: "invalid_gate",
        message: "Runner row has a non-numeric gate.",
        rowIndex: 0,
        horseNumber: 3,
      },
    ],
  });
});

test("falls back to anchor text when the horse link has no title", () => {
  const html: string = `
<table><tbody>
<tr>
  <td class="StartStall1 Txt_C"><span>1</span></td>
  <td class="RunnerNumber1 Txt_C">1</td>
  <td><a href="/entity-a/000a024a0d">Text Only &amp; Horse</a></td>
  <td><a href="/entity-b/a03b2">J</a></td>
  <td><span>StableHome</span><a href="/entity-c/05518">T</a></td>
</tr>
</tbody></table>`;

  expect(parseSecondarySourceRacecard({ html, profile: TEST_PROFILE }).runners).toStrictEqual([
    {
      horseNumber: 1,
      gate: 1,
      horseName: "Text Only & Horse",
      horseId: "000a024a0d",
      jockeyId: "a03b2",
      trainerId: "05518",
      trainerAffiliation: "StableHome",
    },
  ]);
});

test("reports duplicate horse numbers after sorting", () => {
  const html: string = `
<table><tbody>
<tr>
  <td class="StartStall1 Txt_C"><span>1</span></td>
  <td class="RunnerNumber2 Txt_C">2</td>
  <td><a href="/entity-a/2020190005" title="Dup A">Dup A</a></td>
  <td><a href="/entity-b/05271">J</a></td>
  <td><span>ForeignYard</span><a href="/entity-c/05701">T</a></td>
</tr>
<tr>
  <td class="StartStall2 Txt_C"><span>2</span></td>
  <td class="RunnerNumber2 Txt_C">2</td>
  <td><a href="/entity-a/000a029d4f" title="Dup B">Dup B</a></td>
  <td><a href="/entity-b/a02c7">J</a></td>
  <td><span>ForeignYard</span><a href="/entity-c/05518">T</a></td>
</tr>
</tbody></table>`;

  expect(parseSecondarySourceRacecard({ html, profile: TEST_PROFILE })).toStrictEqual({
    runners: [
      {
        horseNumber: 2,
        gate: 1,
        horseName: "Dup A",
        horseId: "2020190005",
        jockeyId: "05271",
        trainerId: "05701",
        trainerAffiliation: "ForeignYard",
      },
      {
        horseNumber: 2,
        gate: 2,
        horseName: "Dup B",
        horseId: "000a029d4f",
        jockeyId: "a02c7",
        trainerId: "05518",
        trainerAffiliation: "ForeignYard",
      },
    ],
    issues: [
      {
        code: "duplicate_horse_number",
        message: "Multiple runner rows share the same horse number.",
        rowIndex: -1,
        horseNumber: 2,
      },
    ],
  });
});

test("ignores non-runner table rows that lack horse and number markers", () => {
  const html: string = `
<table>
<thead>
<tr><th>Header</th><th>Only</th></tr>
</thead>
<tbody>
<tr><td class="memo">note row</td></tr>
<tr>
  <td class="StartStall1 Txt_C"><span>1</span></td>
  <td class="RunnerNumber1 Txt_C">1</td>
  <td><a href="/entity-a/2021190001" title="Only Runner">Only Runner</a></td>
  <td><a href="/entity-b/05504">J</a></td>
  <td><span>ForeignYard</span><a href="/entity-c/05701">T</a></td>
</tr>
</tbody>
</table>`;

  expect(parseSecondarySourceRacecard({ html, profile: TEST_PROFILE }).runners).toStrictEqual([
    {
      horseNumber: 1,
      gate: 1,
      horseName: "Only Runner",
      horseId: "2021190001",
      jockeyId: "05504",
      trainerId: "05701",
      trainerAffiliation: "ForeignYard",
    },
  ]);
  expect(parseSecondarySourceRacecard({ html, profile: TEST_PROFILE }).issues).toStrictEqual([]);
});

test("treats an empty horse-number cell as missing rather than invalid", () => {
  const html: string = `
<table><tbody>
<tr>
  <td class="StartStall1 Txt_C"><span>1</span></td>
  <td class="RunnerNumber1 Txt_C">   </td>
  <td><a href="/entity-a/2021190001" title="Blank Number">Blank Number</a></td>
  <td><a href="/entity-b/05504">J</a></td>
  <td><span>ForeignYard</span><a href="/entity-c/05701">T</a></td>
</tr>
</tbody></table>`;

  expect(parseSecondarySourceRacecard({ html, profile: TEST_PROFILE }).issues).toStrictEqual([
    {
      code: "missing_horse_number",
      message: "Runner row is missing a horse-number cell.",
      rowIndex: 0,
      horseNumber: null,
    },
  ]);
});

test("treats an empty gate cell as missing rather than invalid", () => {
  const html: string = `
<table><tbody>
<tr>
  <td class="StartStall1 Txt_C"><span>  </span></td>
  <td class="RunnerNumber8 Txt_C">8</td>
  <td><a href="/entity-a/000a02d3fe" title="Blank Gate">Blank Gate</a></td>
  <td><a href="/entity-b/a03b2">J</a></td>
  <td><span>ForeignYard</span><a href="/entity-c/05518">T</a></td>
</tr>
</tbody></table>`;

  expect(parseSecondarySourceRacecard({ html, profile: TEST_PROFILE })).toStrictEqual({
    runners: [
      {
        horseNumber: 8,
        gate: null,
        horseName: "Blank Gate",
        horseId: "000a02d3fe",
        jockeyId: "a03b2",
        trainerId: "05518",
        trainerAffiliation: "ForeignYard",
      },
    ],
    issues: [
      {
        code: "missing_gate",
        message: "Runner row is missing a gate cell.",
        rowIndex: 0,
        horseNumber: 8,
      },
    ],
  });
});

test("uses title when present even if anchor text is empty after tag stripping", () => {
  const html: string = `
<table><tbody>
<tr>
  <td class="StartStall1 Txt_C"><span>1</span></td>
  <td class="RunnerNumber1 Txt_C">1</td>
  <td><a href="/entity-a/2021190001" title="Title Horse"><img src="/x.png" alt="" /></a></td>
  <td><a href="/entity-b/05504">J</a></td>
  <td><span>ForeignYard</span><a href="/entity-c/05701">T</a></td>
</tr>
</tbody></table>`;

  expect(parseSecondarySourceRacecard({ html, profile: TEST_PROFILE }).runners[0]?.horseName).toBe(
    "Title Horse",
  );
});

test("returns null horse name when both title and anchor text are blank", () => {
  const html: string = `
<table><tbody>
<tr>
  <td class="StartStall1 Txt_C"><span>1</span></td>
  <td class="RunnerNumber1 Txt_C">1</td>
  <td><a href="/entity-a/2021190001" title="   "><img src="/x.png" alt="" /></a></td>
  <td><a href="/entity-b/05504">J</a></td>
  <td><span>ForeignYard</span><a href="/entity-c/05701">T</a></td>
</tr>
</tbody></table>`;

  expect(parseSecondarySourceRacecard({ html, profile: TEST_PROFILE })).toStrictEqual({
    runners: [
      {
        horseNumber: 1,
        gate: 1,
        horseName: null,
        horseId: "2021190001",
        jockeyId: "05504",
        trainerId: "05701",
        trainerAffiliation: "ForeignYard",
      },
    ],
    issues: [
      {
        code: "missing_horse_name",
        message: "Runner row is missing a horse name.",
        rowIndex: 0,
        horseNumber: 1,
      },
    ],
  });
});

test("keeps horse id when only a bare horse href is present without an anchor element", () => {
  const html: string = `
<table><tbody>
<tr>
  <td class="StartStall1 Txt_C"><span>1</span></td>
  <td class="RunnerNumber1 Txt_C">1</td>
  <td>href="/entity-a/2021190001"</td>
  <td><a href="/entity-b/05504">J</a></td>
  <td><span>ForeignYard</span><a href="/entity-c/05701">T</a></td>
</tr>
</tbody></table>`;

  expect(parseSecondarySourceRacecard({ html, profile: TEST_PROFILE })).toStrictEqual({
    runners: [
      {
        horseNumber: 1,
        gate: 1,
        horseName: null,
        horseId: "2021190001",
        jockeyId: "05504",
        trainerId: "05701",
        trainerAffiliation: "ForeignYard",
      },
    ],
    issues: [
      {
        code: "missing_horse_name",
        message: "Runner row is missing a horse name.",
        rowIndex: 0,
        horseNumber: 1,
      },
    ],
  });
});

test("treats a gate-only marker row as a candidate that still needs a horse number", () => {
  const html: string = `
<table><tbody>
<tr>
  <td class="StartStall9 Txt_C"><span>9</span></td>
  <td>no number cell</td>
</tr>
</tbody></table>`;

  expect(parseSecondarySourceRacecard({ html, profile: TEST_PROFILE })).toStrictEqual({
    runners: [],
    issues: [
      {
        code: "missing_horse_number",
        message: "Runner row is missing a horse-number cell.",
        rowIndex: 0,
        horseNumber: null,
      },
    ],
  });
});

test("parseSecondarySourceMarkupProfileJson accepts a complete profile object", () => {
  expect(
    parseSecondarySourceMarkupProfileJson(
      JSON.stringify({
        horseNumberClassToken: "RunnerNumber",
        gateClassToken: "StartStall",
        horsePathSegment: "/entity-a/",
        jockeyPathPrefix: "/entity-b/",
        trainerPathPrefix: "/entity-c/",
        affiliationLabels: ["StableHome", "ForeignYard"],
      }),
    ),
  ).toStrictEqual({
    horseNumberClassToken: "RunnerNumber",
    gateClassToken: "StartStall",
    horsePathSegment: "/entity-a/",
    jockeyPathPrefix: "/entity-b/",
    trainerPathPrefix: "/entity-c/",
    affiliationLabels: ["StableHome", "ForeignYard"],
  });
});

test("parseSecondarySourceMarkupProfileJson rejects a non-object root", () => {
  expect(() => parseSecondarySourceMarkupProfileJson("[]")).toThrowError(
    "Secondary-source markup profile must be a JSON object.",
  );
});

test("parseSecondarySourceMarkupProfileJson rejects a missing string field", () => {
  expect(() =>
    parseSecondarySourceMarkupProfileJson(
      JSON.stringify({
        horseNumberClassToken: "RunnerNumber",
        gateClassToken: "",
        horsePathSegment: "/entity-a/",
        jockeyPathPrefix: "/entity-b/",
        trainerPathPrefix: "/entity-c/",
        affiliationLabels: ["StableHome"],
      }),
    ),
  ).toThrowError(
    'Secondary-source markup profile field "gateClassToken" must be a non-empty string.',
  );
});

test("parseSecondarySourceMarkupProfileJson rejects empty affiliation labels", () => {
  expect(() =>
    parseSecondarySourceMarkupProfileJson(
      JSON.stringify({
        horseNumberClassToken: "RunnerNumber",
        gateClassToken: "StartStall",
        horsePathSegment: "/entity-a/",
        jockeyPathPrefix: "/entity-b/",
        trainerPathPrefix: "/entity-c/",
        affiliationLabels: [],
      }),
    ),
  ).toThrowError(
    'Secondary-source markup profile field "affiliationLabels" must be a non-empty array of non-empty strings.',
  );
});

test("loadSecondarySourceMarkupProfile reads the env path through the injected reader", () => {
  const files: ReadonlyMap<string, string> = new Map([
    [
      "/operator/local-secondary-markup-profile.json",
      JSON.stringify({
        horseNumberClassToken: "RunnerNumber",
        gateClassToken: "StartStall",
        horsePathSegment: "/entity-a/",
        jockeyPathPrefix: "/entity-b/",
        trainerPathPrefix: "/entity-c/",
        affiliationLabels: ["StableHome"],
      }),
    ],
  ]);

  expect(
    loadSecondarySourceMarkupProfile({
      env: {
        [OVERSEA_SECONDARY_MARKUP_PROFILE_PATH]: "/operator/local-secondary-markup-profile.json",
      },
      readTextFile: (path: string): string => {
        const content: string | undefined = files.get(path);
        if (content === undefined) {
          throw new Error(`missing fixture file: ${path}`);
        }
        return content;
      },
    }),
  ).toStrictEqual({
    horseNumberClassToken: "RunnerNumber",
    gateClassToken: "StartStall",
    horsePathSegment: "/entity-a/",
    jockeyPathPrefix: "/entity-b/",
    trainerPathPrefix: "/entity-c/",
    affiliationLabels: ["StableHome"],
  });
});

test("loadSecondarySourceMarkupProfile fails clearly when the env var is unset", () => {
  expect(() =>
    loadSecondarySourceMarkupProfile({
      env: {},
      readTextFile: (): string => {
        throw new Error("readTextFile must not be called when the env var is unset");
      },
    }),
  ).toThrowError(
    "Set OVERSEA_SECONDARY_MARKUP_PROFILE_PATH to the absolute path of your local secondary-source markup profile JSON file. The profile is operator-supplied and intentionally not version-controlled.",
  );
});

test("loadSecondarySourceMarkupProfile fails clearly when the env var is empty", () => {
  expect(() =>
    loadSecondarySourceMarkupProfile({
      env: { [OVERSEA_SECONDARY_MARKUP_PROFILE_PATH]: "" },
      readTextFile: (): string => {
        throw new Error("readTextFile must not be called when the env var is empty");
      },
    }),
  ).toThrowError(
    "Set OVERSEA_SECONDARY_MARKUP_PROFILE_PATH to the absolute path of your local secondary-source markup profile JSON file. The profile is operator-supplied and intentionally not version-controlled.",
  );
});
