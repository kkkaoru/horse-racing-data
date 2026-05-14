import { describe, expect, it } from "vitest";

import {
  FINISH_POSITION_PREDICTION_EVALUATIONS,
  getFinishPredictionEvaluation,
} from "./finish-position-prediction-evaluation";

describe("finish position prediction evaluation data", () => {
  it("returns JRA evaluation for central races", () => {
    expect(getFinishPredictionEvaluation({ keibajoCode: "05", source: "jra" })).toEqual(
      FINISH_POSITION_PREDICTION_EVALUATIONS.jra,
    );
  });

  it("returns ban-ei evaluation for ban-ei NAR races", () => {
    expect(getFinishPredictionEvaluation({ keibajoCode: "83", source: "nar" })).toEqual(
      FINISH_POSITION_PREDICTION_EVALUATIONS["ban-ei"],
    );
  });

  it("returns NAR evaluation for non-ban-ei local races", () => {
    expect(getFinishPredictionEvaluation({ keibajoCode: "45", source: "nar" })).toEqual(
      FINISH_POSITION_PREDICTION_EVALUATIONS.nar,
    );
  });
});
