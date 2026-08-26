import { describe, expect, it } from "vitest";
import {
  buildPremiumPaddockRunnerSignals,
  type TimestampedPremiumPaddockBulletin,
} from "./premium-paddock-signals";

const bulletin = (
  horseNumber: string,
  fetchedAt: string,
  groupKey: "favorite" | "value" = "value",
): TimestampedPremiumPaddockBulletin => ({
  commentText: "好気配",
  evaluationText: "上向き",
  fetchedAt,
  frameNumber: "1",
  groupKey,
  horseName: `horse-${horseNumber}`,
  horseNumber,
});

describe("buildPremiumPaddockRunnerSignals", () => {
  it("uses the latest strictly pre-start bulletin and preserves unselected horses", () => {
    const result = buildPremiumPaddockRunnerSignals(
      ["01", "02", "03"],
      [
        bulletin("01", "2026-09-05T14:50:00+09:00", "favorite"),
        bulletin("01", "2026-09-05T14:55:00+09:00"),
        bulletin("02", "2026-09-05T15:00:00+09:00"),
        bulletin("99", "2026-09-05T14:58:00+09:00"),
      ],
      "2026-09-05T15:00:00+09:00",
      true,
    );

    expect(result).toStrictEqual({
      "01": {
        commentAvailable: true,
        evaluationAvailable: true,
        groupKey: "value",
        selected: true,
        snapshotFetchedAt: "2026-09-05T14:58:00+09:00",
      },
      "02": {
        commentAvailable: false,
        evaluationAvailable: false,
        groupKey: null,
        selected: false,
        snapshotFetchedAt: "2026-09-05T14:58:00+09:00",
      },
      "03": {
        commentAvailable: false,
        evaluationAvailable: false,
        groupKey: null,
        selected: false,
        snapshotFetchedAt: "2026-09-05T14:58:00+09:00",
      },
    });
  });

  it("fails closed when the premium snapshot is incomplete", () => {
    expect(
      buildPremiumPaddockRunnerSignals(
        ["01", "02"],
        [bulletin("01", "2026-09-05T14:55:00+09:00")],
        "2026-09-05T15:00:00+09:00",
        false,
      ),
    ).toStrictEqual({ "01": null, "02": null });
  });

  it("fails closed when no bulletin is strictly before the start", () => {
    expect(
      buildPremiumPaddockRunnerSignals(
        ["01"],
        [bulletin("01", "2026-09-05T15:00:00+09:00")],
        "2026-09-05T15:00:00+09:00",
        true,
      ),
    ).toStrictEqual({ "01": null });
  });

  it("rejects malformed timestamps and duplicate expected horses", () => {
    expect(() => buildPremiumPaddockRunnerSignals(["01"], [], "invalid", true)).toThrowError(
      "scheduledStart must be an ISO timestamp",
    );
    expect(() =>
      buildPremiumPaddockRunnerSignals(["01", "01"], [], "2026-09-05T15:00:00+09:00", true),
    ).toThrowError("expectedHorseNumbers must be unique");
    expect(() =>
      buildPremiumPaddockRunnerSignals(
        ["01"],
        [bulletin("01", "invalid")],
        "2026-09-05T15:00:00+09:00",
        true,
      ),
    ).toThrowError("bulletin.fetchedAt must be an ISO timestamp");
  });
});
