import type { RaceSource } from "./codes";

export type JraRaceUrlVariant = "entry" | "result";

export interface JraRaceDetailUrlInput {
  kaisaiKai: string | null;
  kaisaiNen: string;
  kaisaiNichime: string | null;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  source: RaceSource;
}

interface JraCnameBodyInput {
  dataKubun: string;
  kaisaiKai: string;
  kaisaiNen: string;
  kaisaiNichime: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

const JRA_BASE_URL_BY_VARIANT: Record<JraRaceUrlVariant, string> = {
  entry: "https://www.jra.go.jp/JRADB/accessD.html",
  result: "https://www.jra.go.jp/JRADB/accessS.html",
};

const JRA_CNAME_PREFIX_BY_VARIANT: Record<JraRaceUrlVariant, string> = {
  entry: "pw01dde",
  result: "pw01sde",
};

const JRA_DATA_KUBUN_BY_VARIANT: Record<JraRaceUrlVariant, string> = {
  entry: "01",
  result: "10",
};

// Reverse-engineered from 2396 verified URLs spanning accessS and accessD,
// years 2024-2026, all months. Each entry maps a position within the CNAME
// body (the substring after the prefix) to its multiplier.
const CHECKSUM_WEIGHTS: Record<number, number> = {
  2: 41,
  3: 74,
  9: 49,
  10: 16,
  11: 115,
  12: 82,
  13: 181,
  19: 123,
  20: 90,
  21: 189,
};

// Calibration anchor: entry URL, data_kubun "01", year 2026, month under 10.
// Result URLs use data_kubun "10" and a different prefix; year and month also
// alter the offset. Deltas below are derived from the same dataset.
const CONSTANT_BASE = 150;
const CONSTANT_RESULT_SHIFT = -101;
const CONSTANT_YEAR_LAST_DIGIT_DELTA = 7;
const CONSTANT_MONTH_TENS_SHIFT = 24;
const CALIBRATION_YEAR = 2026;
const CHECKSUM_MODULUS = 256;
const HEX_RADIX = 16;
const HEX_PAD_LENGTH = 2;

const buildCnameBody = (input: JraCnameBodyInput): string =>
  `${input.dataKubun}${input.keibajoCode}${input.kaisaiNen}${input.kaisaiKai}${input.kaisaiNichime}${input.raceBango}${input.kaisaiNen}${input.kaisaiTsukihi}`;

const getConstantOffset = (
  variant: JraRaceUrlVariant,
  kaisaiNen: string,
  kaisaiTsukihi: string,
): number => {
  const variantShift = variant === "result" ? CONSTANT_RESULT_SHIFT : 0;
  const yearShift = (Number(kaisaiNen) - CALIBRATION_YEAR) * CONSTANT_YEAR_LAST_DIGIT_DELTA;
  const monthShift = Number(kaisaiTsukihi.charAt(0)) * CONSTANT_MONTH_TENS_SHIFT;
  return CONSTANT_BASE + variantShift + yearShift + monthShift;
};

export const computeJraChecksum = (body: string, constant: number): string => {
  const total = Object.entries(CHECKSUM_WEIGHTS).reduce(
    (acc, [position, weight]) => acc + weight * body.charCodeAt(Number(position)),
    constant,
  );
  const checksum = ((total % CHECKSUM_MODULUS) + CHECKSUM_MODULUS) % CHECKSUM_MODULUS;
  return checksum.toString(HEX_RADIX).toUpperCase().padStart(HEX_PAD_LENGTH, "0");
};

export const buildJraRaceUrl = (
  input: JraRaceDetailUrlInput,
  variant: JraRaceUrlVariant,
): string | null => {
  if (input.source !== "jra") {
    return null;
  }
  if (!input.kaisaiKai || !input.kaisaiNichime || !input.keibajoCode || !input.raceBango) {
    return null;
  }
  const body = buildCnameBody({
    dataKubun: JRA_DATA_KUBUN_BY_VARIANT[variant],
    kaisaiKai: input.kaisaiKai,
    kaisaiNen: input.kaisaiNen,
    kaisaiNichime: input.kaisaiNichime,
    kaisaiTsukihi: input.kaisaiTsukihi,
    keibajoCode: input.keibajoCode,
    raceBango: input.raceBango,
  });
  const constant = getConstantOffset(variant, input.kaisaiNen, input.kaisaiTsukihi);
  const checksum = computeJraChecksum(body, constant);
  return `${JRA_BASE_URL_BY_VARIANT[variant]}?CNAME=${JRA_CNAME_PREFIX_BY_VARIANT[variant]}${body}/${checksum}`;
};

export const buildJraRaceEntryUrl = (input: JraRaceDetailUrlInput): string | null =>
  buildJraRaceUrl(input, "entry");

export const buildJraRaceResultUrl = (input: JraRaceDetailUrlInput): string | null =>
  buildJraRaceUrl(input, "result");
