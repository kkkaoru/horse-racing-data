import { expect, it } from "vitest";

import {
  parseSecondaryHorsePedigree,
  type SecondaryPedigreeMarkupProfile,
} from "./secondary-pedigree-parser";

const PROFILE: SecondaryPedigreeMarkupProfile = {
  horsePathPrefix: "/horse/ped/",
  sourceUrlTemplate: "https://secondary.test/horse/ped/{HORSE_ID}/",
  tableMarker: 'class="blood_table"',
};

const VALID_HTML = `<table class="blood_table">
<tr><td><a href="https://secondary.test/horse/ped/sire-1/"><span>Night &amp; Thunder</span></a></td><td><a href="/horse/ped/sire-sire-1/">Dubawi</a></td></tr>
<tr><td><a href="/horse/ped/sire-dam-1/">Forest Storm</a></td></tr>
<tr><td><a href="/horse/ped/dam-1/">Rhea</a></td><td><a href="/horse/ped/dam-sire-1/">Siyouni</a></td></tr>
<tr><td><a href="/horse/ped/dam-dam-1/">Titian&nbsp;Pride</a></td></tr>
</table>`;
const VALID_JSON = JSON.stringify({ data: VALID_HTML, status: "OK" });

it("parses source-native sire, sire-sire, dam, and dam-sire identities", () => {
  expect(parseSecondaryHorsePedigree(VALID_JSON, "horse-1", PROFILE)).toStrictEqual({
    dam: { name: "Rhea", sourceHorseId: "dam-1" },
    damSire: { name: "Siyouni", sourceHorseId: "dam-sire-1" },
    sire: { name: "Night & Thunder", sourceHorseId: "sire-1" },
    sireSire: { name: "Dubawi", sourceHorseId: "sire-sire-1" },
    sourceHorseId: "horse-1",
    sourceUrl: "https://secondary.test/horse/ped/horse-1/",
  });
});

it.each(["null", '"invalid"', "[]"])("rejects a non-object response %s", (json) => {
  expect(() => parseSecondaryHorsePedigree(json, "horse-1", PROFILE)).toThrow(
    "Secondary pedigree response must be an object.",
  );
});

it("rejects a response without status and data", () => {
  expect(() => parseSecondaryHorsePedigree("{}", "horse-1", PROFILE)).toThrow(
    "Secondary pedigree response is missing status or data.",
  );
});

it.each(['{"status":"ERROR","data":null}', '{"status":"OK","data":1}'])(
  "rejects an unsuccessful response %s",
  (json) => {
    expect(() => parseSecondaryHorsePedigree(json, "horse-1", PROFILE)).toThrow(
      "Secondary pedigree response is not successful.",
    );
  },
);

it("rejects a missing table marker", () => {
  expect(() =>
    parseSecondaryHorsePedigree('{"status":"OK","data":"<table></table>"}', "horse-1", PROFILE),
  ).toThrow("Secondary pedigree table marker was not found.");
});

it("rejects an unclosed pedigree table", () => {
  expect(() =>
    parseSecondaryHorsePedigree(
      '{"status":"OK","data":"<table class=\\"blood_table\\">"}',
      "horse-1",
      PROFILE,
    ),
  ).toThrow("Secondary pedigree table is not closed.");
});

it("rejects a pedigree cell without an ancestor link", () => {
  const json = JSON.stringify({
    data: VALID_HTML.replace('<a href="/horse/ped/dam-dam-1/">Titian&nbsp;Pride</a>', "Unknown"),
    status: "OK",
  });
  expect(() => parseSecondaryHorsePedigree(json, "horse-1", PROFILE)).toThrow(
    "Secondary pedigree cell is missing an ancestor link.",
  );
});

it("rejects an ancestor link with no linked text", () => {
  const json = JSON.stringify({
    data: VALID_HTML.replace("<span>Night &amp; Thunder</span>", ""),
    status: "OK",
  });
  expect(() => parseSecondaryHorsePedigree(json, "horse-1", PROFILE)).toThrow(
    "Secondary pedigree cell is missing an ancestor link.",
  );
});

it("rejects an empty ancestor name", () => {
  const json = JSON.stringify({
    data: VALID_HTML.replace("<span>Night &amp; Thunder</span>", "<span> </span>"),
    status: "OK",
  });
  expect(() => parseSecondaryHorsePedigree(json, "horse-1", PROFILE)).toThrow(
    "Secondary pedigree ancestor name is empty.",
  );
});

it("rejects a pedigree with an unexpected ancestor count", () => {
  const json = JSON.stringify({
    data: VALID_HTML.replace('<td><a href="/horse/ped/dam-dam-1/">Titian&nbsp;Pride</a></td>', ""),
    status: "OK",
  });
  expect(() => parseSecondaryHorsePedigree(json, "horse-1", PROFILE)).toThrow(
    "Secondary pedigree has 5 ancestors.",
  );
});
