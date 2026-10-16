// Runs with bun. Bounded, provider-qualified source mapping; display identities remain separate.
import type { RaceRunnersReadInput } from "./race-runners-read";

export interface OverseasRunnerMapping {
  umaban: string;
  sourceHorseId: string;
  gateNumber: number | null;
}
const PROVIDER_ID: RegExp = /^(?!0+$)[A-Za-z0-9]+$/u;
const UMABAN: RegExp = /^(0[1-9]|1[0-8])$/u;

export const isOverseasRunnerRace = (input: RaceRunnersReadInput): boolean =>
  input.source === "jra" && /^[A-Z][0-9A-Z]$/u.test(input.keibajoCode);

// The caller validates the complete race input before constructing this SQL.
export const buildOverseasRunnerMappingSql = (input: RaceRunnersReadInput): string =>
  `SELECT umaban, source_horse_id, gate_number
FROM ${input.namespace}.oversea_runner_source_id
WHERE race_source = '${input.source}' AND source = 'netkeiba'
  AND kaisai_nen = '${input.date.slice(0, 4)}'
  AND kaisai_tsukihi = '${input.date.slice(4)}'
  AND keibajo_code = '${input.keibajoCode}'
  AND race_bango = '${input.raceBango}'
ORDER BY umaban ASC
LIMIT 19`;

export const parseOverseasRunnerMappings = (rows: unknown[]): OverseasRunnerMapping[] => {
  if (rows.length > 18) throw new Error("Too many overseas source mappings");
  const seen: Set<string> = new Set();
  return rows.map((row) => {
    if (typeof row !== "object" || row === null || Array.isArray(row))
      throw new Error("Invalid overseas source mapping");
    const umaban: unknown = Reflect.get(row, "umaban");
    const sourceHorseId: unknown = Reflect.get(row, "source_horse_id");
    const gateNumber: unknown = Reflect.get(row, "gate_number");
    if (
      typeof umaban !== "string" ||
      !UMABAN.test(umaban) ||
      seen.has(umaban) ||
      typeof sourceHorseId !== "string" ||
      !PROVIDER_ID.test(sourceHorseId) ||
      (gateNumber !== null &&
        (typeof gateNumber !== "number" ||
          !Number.isSafeInteger(gateNumber) ||
          gateNumber < 1 ||
          gateNumber > 32767))
    )
      throw new Error("Invalid overseas source mapping");
    seen.add(umaban);
    return { umaban, sourceHorseId, gateNumber };
  });
};
