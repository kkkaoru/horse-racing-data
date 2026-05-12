import { expect, it } from "vitest";

import {
  buildJraRaceEntryUrl,
  buildJraRaceResultUrl,
  buildJraRaceUrl,
  computeJraChecksum,
} from "./jra-url";

it("computes entry checksum for Tokyo R1 on 2026/05/09", () => {
  expect(computeJraChecksum("0105202602050120260509", 150)).toBe("6A");
});

it("computes entry checksum for Tokyo R12 on 2026/05/09", () => {
  expect(computeJraChecksum("0105202602051220260509", 150)).toBe("71");
});

it("computes entry checksum for Hanshin R10 on 2026/03/07", () => {
  expect(computeJraChecksum("0109202601051020260307", 150)).toBe("8E");
});

it("computes entry checksum for Nakayama R2 on 2026/03/08", () => {
  expect(computeJraChecksum("0106202602040220260308", 150)).toBe("43");
});

it("computes result checksum for Kyoto R3 on 2023/11/26", () => {
  expect(computeJraChecksum("1008202303080320231126", 52)).toBe("6B");
});

it("pads single-digit checksum with leading zero", () => {
  expect(computeJraChecksum("0105202602051020260509", 150)).toBe("07");
});

it("builds entry URL for Tokyo R1 on 2026/05/09", () => {
  expect(
    buildJraRaceEntryUrl({
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "05",
      kaisaiTsukihi: "0509",
      keibajoCode: "05",
      raceBango: "01",
      source: "jra",
    }),
  ).toBe("https://www.jra.go.jp/JRADB/accessD.html?CNAME=pw01dde0105202602050120260509/6A");
});

it("builds entry URL for Hanshin R10 on 2026/03/07", () => {
  expect(
    buildJraRaceEntryUrl({
      kaisaiKai: "01",
      kaisaiNen: "2026",
      kaisaiNichime: "05",
      kaisaiTsukihi: "0307",
      keibajoCode: "09",
      raceBango: "10",
      source: "jra",
    }),
  ).toBe("https://www.jra.go.jp/JRADB/accessD.html?CNAME=pw01dde0109202601051020260307/8E");
});

it("builds result URL for Kyoto R3 on 2023/11/26", () => {
  expect(
    buildJraRaceResultUrl({
      kaisaiKai: "03",
      kaisaiNen: "2023",
      kaisaiNichime: "08",
      kaisaiTsukihi: "1126",
      keibajoCode: "08",
      raceBango: "03",
      source: "jra",
    }),
  ).toBe("https://www.jra.go.jp/JRADB/accessS.html?CNAME=pw01sde1008202303080320231126/6B");
});

it("dispatches via buildJraRaceUrl with variant entry", () => {
  expect(
    buildJraRaceUrl(
      {
        kaisaiKai: "02",
        kaisaiNen: "2026",
        kaisaiNichime: "05",
        kaisaiTsukihi: "0509",
        keibajoCode: "05",
        raceBango: "01",
        source: "jra",
      },
      "entry",
    ),
  ).toBe("https://www.jra.go.jp/JRADB/accessD.html?CNAME=pw01dde0105202602050120260509/6A");
});

it("dispatches via buildJraRaceUrl with variant result", () => {
  expect(
    buildJraRaceUrl(
      {
        kaisaiKai: "03",
        kaisaiNen: "2023",
        kaisaiNichime: "08",
        kaisaiTsukihi: "1126",
        keibajoCode: "08",
        raceBango: "03",
        source: "jra",
      },
      "result",
    ),
  ).toBe("https://www.jra.go.jp/JRADB/accessS.html?CNAME=pw01sde1008202303080320231126/6B");
});

it("returns null when source is NAR for entry URL", () => {
  expect(
    buildJraRaceEntryUrl({
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "05",
      kaisaiTsukihi: "0509",
      keibajoCode: "05",
      raceBango: "01",
      source: "nar",
    }),
  ).toBeNull();
});

it("returns null when source is NAR for result URL", () => {
  expect(
    buildJraRaceResultUrl({
      kaisaiKai: "03",
      kaisaiNen: "2023",
      kaisaiNichime: "08",
      kaisaiTsukihi: "1126",
      keibajoCode: "08",
      raceBango: "03",
      source: "nar",
    }),
  ).toBeNull();
});

it("returns null when kaisai_kai is missing", () => {
  expect(
    buildJraRaceEntryUrl({
      kaisaiKai: null,
      kaisaiNen: "2026",
      kaisaiNichime: "05",
      kaisaiTsukihi: "0509",
      keibajoCode: "05",
      raceBango: "01",
      source: "jra",
    }),
  ).toBeNull();
});

it("returns null when kaisai_nichime is missing", () => {
  expect(
    buildJraRaceResultUrl({
      kaisaiKai: "03",
      kaisaiNen: "2023",
      kaisaiNichime: null,
      kaisaiTsukihi: "1126",
      keibajoCode: "08",
      raceBango: "03",
      source: "jra",
    }),
  ).toBeNull();
});

it("returns null when keibajo_code is empty", () => {
  expect(
    buildJraRaceEntryUrl({
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "05",
      kaisaiTsukihi: "0509",
      keibajoCode: "",
      raceBango: "01",
      source: "jra",
    }),
  ).toBeNull();
});

it("returns null when race_bango is empty", () => {
  expect(
    buildJraRaceResultUrl({
      kaisaiKai: "03",
      kaisaiNen: "2023",
      kaisaiNichime: "08",
      kaisaiTsukihi: "1126",
      keibajoCode: "08",
      raceBango: "",
      source: "jra",
    }),
  ).toBeNull();
});
