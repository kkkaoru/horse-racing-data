// Runs with bun. Reject legacy score caches even when their row count looks complete.
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const positiveId = (value: unknown): value is string =>
  typeof value === "string" && /^(?!0+$)[A-Za-z0-9]+$/u.test(value);
const count = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0;

export const hasOverseasTimeScoreEvidence = (body: string): boolean => {
  try {
    const payload: unknown = JSON.parse(body);
    if (
      !isRecord(payload) ||
      payload.type !== "time-score" ||
      !Array.isArray(payload.runners) ||
      !Array.isArray(payload.rows)
    )
      return false;
    const rows: Map<string, Record<string, unknown>> = new Map();
    for (const row of payload.rows) {
      if (
        !isRecord(row) ||
        typeof row.horseNumber !== "string" ||
        rows.has(String(Number(row.horseNumber)))
      )
        return false;
      rows.set(String(Number(row.horseNumber)), row);
    }
    const expected: Set<string> = new Set();
    for (const runner of payload.runners) {
      if (
        !isRecord(runner) ||
        typeof runner.umaban !== "string" ||
        !/^(0[1-9]|1[0-8])$/u.test(runner.umaban)
      )
        return false;
      // Missing mappings are incomplete inputs, not a complete no-history result.
      // Never persist a newly computed two-row result while mappings are unavailable.
      if (!positiveId(runner.sourceHorseId)) return false;
      const id: string = runner.sourceHorseId;
      const horseNumber: string = String(Number(runner.umaban));
      if (expected.has(horseNumber)) return false;
      expected.add(horseNumber);
      const evidence: unknown = rows.get(horseNumber)?.historyEvidence;
      if (
        !isRecord(evidence) ||
        evidence.generation !== "overseas-readers-v1" ||
        evidence.historyHorseId !== id ||
        !count(evidence.selectedHistoryCount) ||
        !count(evidence.overseasHistoryCount) ||
        (evidence.selection !== "domestic-preferred" && evidence.selection !== "overseas")
      )
        return false;
      if (
        evidence.selection === "overseas" &&
        evidence.selectedHistoryCount !== evidence.overseasHistoryCount
      )
        return false;
    }
    return expected.size === rows.size;
  } catch {
    return false;
  }
};
