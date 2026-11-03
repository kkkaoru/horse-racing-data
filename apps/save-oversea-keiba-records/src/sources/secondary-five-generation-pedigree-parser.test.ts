// This file runs with Bun and Vitest. The markup is a synthetic five-generation table.
import { expect, it } from "vitest";
import { parseSecondaryAncestorPedigrees } from "./secondary-five-generation-pedigree-parser";

const PROFILE = { tableMarker: 'class="five"', horsePathPrefix: "/horse/" };
const ROWSPANS: readonly string[] = [
  ' rowspan="16"',
  ' rowspan="8"',
  ' rowspan="4"',
  ' rowspan="2"',
  "",
];
const ID_WIDTH: number = 10;
const MAX_DEPTH: number = 5;

// Ancestor ids are their path from the horse ("s" sire, "d" dam), padded to a 10-character id.
const ancestorId = (path: string): string => path.padEnd(ID_WIDTH, "0");
const subtree = (path: string): string[] =>
  path.length > MAX_DEPTH
    ? []
    : [
        `<td${ROWSPANS[path.length - 1]}><a href="/horse/${ancestorId(path)}/">${path.toUpperCase()}&nbsp;(米)</a><a href="/horse/ped/${ancestorId(path)}/">血統</a></td>`,
        ...subtree(`${path}s`),
        ...subtree(`${path}d`),
      ];
const PAGE: string = `<table class="five"><tr>${[...subtree("s"), ...subtree("d")].join("</tr><tr>")}</tr></table>`;

it("reads the two-generation pedigree of the sire, the dam and the dam-sire", () => {
  expect(
    parseSecondaryAncestorPedigrees(PAGE, PROFILE, "https://source.test/ped/h/"),
  ).toStrictEqual([
    {
      sourceHorseId: "s000000000",
      sire: { sourceHorseId: "ss00000000", name: "SS" },
      sireSire: { sourceHorseId: "sss0000000", name: "SSS" },
      dam: { sourceHorseId: "sd00000000", name: "SD" },
      damSire: { sourceHorseId: "sds0000000", name: "SDS" },
      sourceUrl: "https://source.test/ped/h/",
    },
    {
      sourceHorseId: "d000000000",
      sire: { sourceHorseId: "ds00000000", name: "DS" },
      sireSire: { sourceHorseId: "dss0000000", name: "DSS" },
      dam: { sourceHorseId: "dd00000000", name: "DD" },
      damSire: { sourceHorseId: "dds0000000", name: "DDS" },
      sourceUrl: "https://source.test/ped/h/",
    },
    {
      sourceHorseId: "ds00000000",
      sire: { sourceHorseId: "dss0000000", name: "DSS" },
      sireSire: { sourceHorseId: "dsss000000", name: "DSSS" },
      dam: { sourceHorseId: "dsd0000000", name: "DSD" },
      damSire: { sourceHorseId: "dsds000000", name: "DSDS" },
      sourceUrl: "https://source.test/ped/h/",
    },
  ]);
});

it("skips an ancestor whose own parents are not linked", () => {
  const unlinkedDamSire = PAGE.replace('<a href="/horse/dsd0000000/">DSD&nbsp;(米)</a>', "DSD");
  expect(
    parseSecondaryAncestorPedigrees(unlinkedDamSire, PROFILE, "u").map((row) => row.sourceHorseId),
  ).toStrictEqual(["s000000000", "d000000000"]);
});

it("returns nothing for a page without a complete table", () => {
  expect(parseSecondaryAncestorPedigrees("<html></html>", PROFILE, "u")).toStrictEqual([]);
  expect(parseSecondaryAncestorPedigrees('<table class="five">', PROFILE, "u")).toStrictEqual([]);
  expect(
    parseSecondaryAncestorPedigrees(PAGE.replace(' rowspan="16"', ""), PROFILE, "u"),
  ).toStrictEqual([]);
});
