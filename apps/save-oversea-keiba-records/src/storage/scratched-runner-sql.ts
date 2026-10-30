// This file runs with Bun. A horse shown as 取消 after it was saved leaves the active field:
// its gate is cleared (later horses' shifted gates arrive through the normal upsert) and its
// unplaced jvd_se row is marked 取消 (ijo_kubun_code 1). Placed or already-coded rows are untouched.
import type { SqlStatement } from "../types";

export interface ScratchedRunnerInput {
  readonly raceKey: {
    readonly kaisai_nen: string;
    readonly kaisai_tsukihi: string;
    readonly keibajo_code: string;
    readonly race_bango: string;
  };
  readonly horseNames: readonly string[];
}

const SCRATCHED_CODE: string = "1";
// JV pads bamei; both sides drop full-width and ASCII spaces before comparing.
const NAME_SQL: string = "replace(replace(s.bamei, chr(12288), ''), ' ', '')";
const ACTIVE_UNPLACED_SQL: string =
  "btrim(s.ijo_kubun_code) IN ('', '0') AND btrim(s.kakutei_chakujun) IN ('', '0', '00')";
const RACE_SQL: string =
  "s.kaisai_nen = $1 AND s.kaisai_tsukihi = $2 AND s.keibajo_code = $3 AND s.race_bango = $4";
const CLEAR_GATE_SQL: string = `UPDATE oversea_runner_source_id o SET gate_number = NULL, updated_at = now()
FROM jvd_se s
WHERE ${RACE_SQL} AND ${NAME_SQL} = $5 AND ${ACTIVE_UNPLACED_SQL}
  AND o.race_source = 'jra' AND o.kaisai_nen = s.kaisai_nen AND o.kaisai_tsukihi = s.kaisai_tsukihi
  AND o.keibajo_code = s.keibajo_code AND o.race_bango = s.race_bango AND o.umaban = s.umaban
  AND o.gate_number IS NOT NULL`;
const MARK_SCRATCHED_SQL: string = `UPDATE jvd_se s SET ijo_kubun_code = $6
WHERE ${RACE_SQL} AND ${NAME_SQL} = $5 AND ${ACTIVE_UNPLACED_SQL}`;

const compactName = (name: string): string => name.replaceAll("　", "").replaceAll(" ", "");

// The gate is cleared first: marking the row 取消 removes it from the active-row predicate.
export const buildScratchedRunnerStatements = ({
  raceKey,
  horseNames,
}: ScratchedRunnerInput): SqlStatement[] =>
  horseNames.flatMap((name: string): SqlStatement[] => {
    const values: string[] = [
      raceKey.kaisai_nen,
      raceKey.kaisai_tsukihi,
      raceKey.keibajo_code,
      raceKey.race_bango,
      compactName(name),
    ];
    return [
      { text: CLEAR_GATE_SQL, values },
      { text: MARK_SCRATCHED_SQL, values: [...values, SCRATCHED_CODE] },
    ];
  });
