import { readFile } from "node:fs/promises";

import { Pool } from "pg";

import { getConnectionString, loadEnv } from "./compare-corner-predictions";

type Options = {
  input: string;
  modelVersion: string;
  target: "local" | "neon";
};

type PredictionRow = {
  source: string;
  race_date: string;
  race_id: string;
  horse_key: string;
  keibajo_code: string;
  race_bango: string;
  umaban: string;
  predicted_corner1_norm: string;
  predicted_corner2_norm: string;
  predicted_corner3_norm: string;
  predicted_corner4_norm: string;
};

const parseArgs = (args: string[]): Options => {
  const options: Options = {
    input: "tmp/corner-lightgbm-predictions.csv",
    modelVersion: "lightgbm-local-dev",
    target: "local",
  };
  for (let index = 0; index < args.length; index += 1) {
    const name = args[index];
    const value = args[index + 1];
    if (name === "--help" || name === "-h") {
      console.log(`Usage:
  bun run src/scripts/import-corner-lightgbm-predictions.ts [options]

Options:
  --input path/to/predictions.csv
  --model-version lightgbm-v1
  --target local|neon
`);
      process.exit(0);
    }
    if (!value) {
      throw new Error(`${name} requires a value.`);
    }
    if (name === "--input") {
      options.input = value;
    } else if (name === "--model-version") {
      options.modelVersion = value;
    } else if (name === "--target") {
      if (value !== "local" && value !== "neon") {
        throw new Error("--target must be local or neon.");
      }
      options.target = value;
    } else {
      throw new Error(`Unknown argument: ${name}`);
    }
    index += 1;
  }
  return options;
};

const parseCsvLine = (line: string): string[] => {
  const values: string[] = [];
  let current = "";
  let quoted = false;
  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    const next = line[index + 1];
    if (char === '"' && quoted && next === '"') {
      current += '"';
      index += 1;
    } else if (char === '"') {
      quoted = !quoted;
    } else if (char === "," && !quoted) {
      values.push(current);
      current = "";
    } else {
      current += char;
    }
  }
  values.push(current);
  return values;
};

const parseCsv = async (path: string): Promise<PredictionRow[]> => {
  const text = await readFile(path, "utf8");
  const [headerLine, ...lines] = text.trim().split(/\r?\n/u);
  if (!headerLine) {
    return [];
  }
  const headers = parseCsvLine(headerLine);
  return lines.map((line) => {
    const values = parseCsvLine(line);
    const row = Object.fromEntries(headers.map((header, index) => [header, values[index] ?? ""]));
    return {
      horse_key: row.horse_key ?? "",
      keibajo_code: row.keibajo_code ?? "",
      predicted_corner1_norm: row.predicted_corner1_norm ?? "",
      predicted_corner2_norm: row.predicted_corner2_norm ?? "",
      predicted_corner3_norm: row.predicted_corner3_norm ?? "",
      predicted_corner4_norm: row.predicted_corner4_norm ?? "",
      race_bango: row.race_bango ?? "",
      race_date: row.race_date ?? "",
      race_id: row.race_id ?? "",
      source: row.source ?? "",
      umaban: row.umaban ?? "",
    };
  });
};

const createTableSql = `
  create table if not exists race_entry_corner_model_predictions (
    model_version text not null,
    source text not null,
    race_date text not null,
    kaisai_nen text not null,
    kaisai_tsukihi text not null,
    keibajo_code text not null,
    race_bango text not null,
    ketto_toroku_bango text not null,
    umaban integer not null,
    predicted_corner1_norm numeric,
    predicted_corner2_norm numeric,
    predicted_corner3_norm numeric,
    predicted_corner4_norm numeric,
    updated_at timestamptz not null default now(),
    primary key (
      model_version,
      source,
      kaisai_nen,
      kaisai_tsukihi,
      keibajo_code,
      race_bango,
      ketto_toroku_bango
    )
  );

  create index if not exists race_entry_corner_model_predictions_lookup_idx
    on race_entry_corner_model_predictions (
      model_version,
      source,
      kaisai_nen,
      kaisai_tsukihi,
      keibajo_code,
      race_bango
    );
`;

const toNumberOrNull = (value: string): number | null => {
  if (!value) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const main = async () => {
  await loadEnv();
  const options = parseArgs(process.argv.slice(2));
  const rows = await parseCsv(options.input);
  const pool = new Pool({ connectionString: getConnectionString(options.target), max: 4 });
  try {
    await pool.query(createTableSql);
    const chunkStarts = Array.from(
      { length: Math.ceil(rows.length / 500) },
      (_, index) => index * 500,
    );
    await Promise.all(
      chunkStarts.map(async (chunkStart) => {
        const chunk = rows.slice(chunkStart, chunkStart + 500);
        if (chunk.length === 0) {
          return;
        }
        const values: unknown[] = [];
        const placeholders = chunk.map((row, rowIndex) => {
          const offset = rowIndex * 13;
          const raceDate = row.race_date;
          values.push(
            options.modelVersion,
            row.source,
            raceDate,
            raceDate.slice(0, 4),
            raceDate.slice(4, 8),
            row.keibajo_code,
            row.race_bango,
            row.horse_key,
            Number(row.umaban),
            toNumberOrNull(row.predicted_corner1_norm),
            toNumberOrNull(row.predicted_corner2_norm),
            toNumberOrNull(row.predicted_corner3_norm),
            toNumberOrNull(row.predicted_corner4_norm),
          );
          return `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10}, $${offset + 11}, $${offset + 12}, $${offset + 13})`;
        });
        await pool.query(
          `
          insert into race_entry_corner_model_predictions (
            model_version,
            source,
            race_date,
            kaisai_nen,
            kaisai_tsukihi,
            keibajo_code,
            race_bango,
            ketto_toroku_bango,
            umaban,
            predicted_corner1_norm,
            predicted_corner2_norm,
            predicted_corner3_norm,
            predicted_corner4_norm
          )
          values ${placeholders.join(",")}
          on conflict (
            model_version,
            source,
            kaisai_nen,
            kaisai_tsukihi,
            keibajo_code,
            race_bango,
            ketto_toroku_bango
          )
          do update set
            race_date = excluded.race_date,
            umaban = excluded.umaban,
            predicted_corner1_norm = excluded.predicted_corner1_norm,
            predicted_corner2_norm = excluded.predicted_corner2_norm,
            predicted_corner3_norm = excluded.predicted_corner3_norm,
            predicted_corner4_norm = excluded.predicted_corner4_norm,
            updated_at = now()
        `,
          values,
        );
      }),
    );
    console.log(`imported=${rows.length}`);
    console.log(`model_version=${options.modelVersion}`);
  } finally {
    await pool.end();
  }
};

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
