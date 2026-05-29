// Run with bun. R2 key builder for per-race Parquet objects.

export interface RaceParquetKeyInput {
  source: "jra" | "nar";
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

const R2_PREFIX = "features/by-race";

export const buildRaceParquetR2Key = (input: RaceParquetKeyInput): string => {
  const year = input.kaisaiNen;
  const month = input.kaisaiTsukihi.slice(0, 2);
  const day = input.kaisaiTsukihi.slice(2, 4);
  return `${R2_PREFIX}/${year}/${month}/${day}/${input.source}/${input.keibajoCode.padStart(2, "0")}/${input.raceBango.padStart(2, "0")}.parquet`;
};

export const buildRaceParquetPrefix = (input: {
  source: "jra" | "nar";
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
}): string => {
  const year = input.kaisaiNen;
  const month = input.kaisaiTsukihi.slice(0, 2);
  const day = input.kaisaiTsukihi.slice(2, 4);
  return `${R2_PREFIX}/${year}/${month}/${day}/${input.source}/${input.keibajoCode.padStart(2, "0")}/`;
};
