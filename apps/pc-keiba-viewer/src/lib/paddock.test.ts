import { describe, expect, it } from "vitest";

import {
  PADDOCK_HISTORY_LIMIT,
  PADDOCK_METRIC_COEFFICIENT,
  PADDOCK_METRIC_MAX_COUNT,
  applyPaddockAction,
  createPaddockState,
  getPaddockKvKey,
  getRacePaddockKey,
  isPaddockAction,
  isPaddockHorseNotifiable,
  isPaddockState,
  normalizePaddockHorseScore,
  shouldSkipPaddockDiscordNotification,
  type PaddockState,
} from "./paddock";

describe("paddock helpers", () => {
  it("creates stable keys and initial state", () => {
    const state = createPaddockState("20260513:45:12", "2026-05-13T12:00:00.000Z");

    expect(state).toEqual({
      history: [],
      horses: {},
      raceKey: "20260513:45:12",
      updatedAt: "2026-05-13T12:00:00.000Z",
    });
    expect(getPaddockKvKey(state.raceKey)).toBe("paddock:20260513:45:12");
    expect(
      getRacePaddockKey({
        day: "13",
        keibajoCode: "45",
        month: "05",
        raceNumber: "12",
        year: "2026",
      }),
    ).toBe("20260513:45:12");
  });

  it("normalizes horse scores with fallbacks and totals", () => {
    expect(
      normalizePaddockHorseScore(
        {
          attention: 2,
          horseName: "",
          horseNumber: "",
          kaeshi: 1,
          officialRank: 3,
          paddock: 4,
          preference: 6,
        },
        { horseName: "テストホース", horseNumber: "01" },
      ),
    ).toEqual({
      attention: 2,
      horseName: "テストホース",
      horseNumber: "01",
      kaeshi: 1,
      officialRank: 3,
      paddock: 4,
      preference: 6,
      total: 8.7,
    });
  });

  it("defaults kaeshi to zero for legacy data missing the field", () => {
    expect(
      normalizePaddockHorseScore(
        {
          attention: 1,
          paddock: 2,
          preference: 0,
        },
        { horseName: "旧データ", horseNumber: "5" },
      ),
    ).toEqual({
      attention: 1,
      horseName: "旧データ",
      horseNumber: "5",
      kaeshi: 0,
      officialRank: null,
      paddock: 2,
      preference: 0,
      total: 3.1,
    });
  });

  it("validates score and official rank actions", () => {
    expect(
      isPaddockAction({
        category: "paddock",
        delta: 1,
        horseName: "一番",
        horseNumber: "01",
      }),
    ).toBe(true);
    expect(
      isPaddockAction({
        horseName: "一番",
        horseNumber: "01",
        rank: 10,
        type: "official-rank",
      }),
    ).toBe(true);
    expect(isPaddockAction(null)).toBe(false);
    expect(
      isPaddockAction({ category: "kaeshi", delta: 1, horseName: "一番", horseNumber: "01" }),
    ).toBe(true);
    expect(
      isPaddockAction({ category: "bad", delta: 1, horseName: "一番", horseNumber: "01" }),
    ).toBe(false);
    expect(
      isPaddockAction({ category: "paddock", delta: 2, horseName: "一番", horseNumber: "01" }),
    ).toBe(false);
    expect(
      isPaddockAction({ horseName: "一番", horseNumber: "01", rank: 11, type: "official-rank" }),
    ).toBe(false);
  });

  it("applies score actions and normalizes horse numbers", () => {
    const initial = createPaddockState("race");
    const state = applyPaddockAction(
      initial,
      {
        category: "preference",
        delta: 1,
        horseName: "一番",
        horseNumber: "01",
      },
      "2026-05-13T12:00:00.000Z",
    );

    expect(state.horses["1"]).toMatchObject({
      attention: 0,
      horseName: "一番",
      horseNumber: "1",
      paddock: 0,
      preference: 1,
      total: 0.3,
    });
    expect(state.history[0]).toMatchObject({
      category: "preference",
      delta: 1,
      horseName: "一番",
      horseNumber: "1",
      type: "score",
    });

    const next = applyPaddockAction(
      state,
      {
        category: "paddock",
        delta: -1,
        horseName: "",
        horseNumber: "1",
      },
      "2026-05-13T12:01:00.000Z",
    );
    expect(next.horses["1"]).toMatchObject({
      horseName: "一番",
      paddock: -1,
      preference: 1,
      total: -1,
    });
  });

  it("applies official ranks and clears duplicate ranks", () => {
    const initial: PaddockState = {
      history: [],
      horses: {
        "1": {
          attention: 0,
          horseName: "一番",
          horseNumber: "1",
          kaeshi: 0,
          officialRank: 1,
          paddock: 0,
          preference: 0,
          total: 0,
        },
      },
      raceKey: "race",
      updatedAt: "old",
    };

    const state = applyPaddockAction(
      initial,
      {
        horseName: "二番",
        horseNumber: "02",
        rank: 1,
        type: "official-rank",
      },
      "2026-05-13T12:00:00.000Z",
    );

    expect(state.horses["1"]?.officialRank).toBeNull();
    expect(state.horses["2"]?.officialRank).toBe(1);
    expect(state.history[0]).toMatchObject({
      horseName: "二番",
      horseNumber: "2",
      officialRank: 1,
      type: "official-rank",
    });

    const cleared = applyPaddockAction(
      state,
      {
        horseName: "",
        horseNumber: "02",
        rank: null,
        type: "official-rank",
      },
      "2026-05-13T12:01:00.000Z",
    );
    expect(cleared.horses["2"]?.horseName).toBe("二番");
    expect(cleared.horses["2"]?.officialRank).toBeNull();
  });

  it("applies official ranks up to tenth place", () => {
    const state = applyPaddockAction(
      createPaddockState("race"),
      {
        horseName: "十番",
        horseNumber: "10",
        rank: 10,
        type: "official-rank",
      },
      "2026-05-13T12:00:00.000Z",
    );

    expect(state.horses["10"]?.officialRank).toBe(10);
    expect(state.history[0]).toMatchObject({
      horseName: "十番",
      horseNumber: "10",
      officialRank: 10,
      type: "official-rank",
    });
  });

  it("limits history and validates state shape", () => {
    const state = createPaddockState("race", "old");
    const filled = Array.from({ length: PADDOCK_HISTORY_LIMIT + 5 }).reduce<PaddockState>(
      (current, _, index) =>
        applyPaddockAction(
          current,
          {
            category: index % 2 === 0 ? "attention" : "paddock",
            delta: 1,
            horseName: `馬${index}`,
            horseNumber: String((index % 18) + 1),
          },
          `2026-05-13T12:${String(index).padStart(2, "0")}:00.000Z`,
        ),
      state,
    );

    expect(filled.history).toHaveLength(PADDOCK_HISTORY_LIMIT);
    expect(isPaddockState(filled)).toBe(true);
    expect(isPaddockState({ ...filled, horses: null })).toBe(false);
    expect(isPaddockState({ ...filled, history: {} })).toBe(false);
    expect(isPaddockState({ ...filled, raceKey: 123 })).toBe(false);
    expect(isPaddockState(undefined)).toBe(false);
  });

  it("skips notification when both paddock total and official rank are empty for every horse", () => {
    expect(
      shouldSkipPaddockDiscordNotification([
        { officialRank: null, total: 0 },
        { officialRank: null, total: 0 },
      ]),
    ).toBe(true);
  });

  it("allows notification when at least one horse has a positive paddock total even without official ranks", () => {
    expect(
      shouldSkipPaddockDiscordNotification([
        { officialRank: null, total: 0 },
        { officialRank: null, total: 2.5 },
      ]),
    ).toBe(false);
  });

  it("allows notification when at least one horse has an official rank even without paddock totals", () => {
    expect(
      shouldSkipPaddockDiscordNotification([
        { officialRank: null, total: 0 },
        { officialRank: 3, total: 0 },
      ]),
    ).toBe(false);
  });

  it("allows notification when paddock totals and official ranks are both present", () => {
    expect(
      shouldSkipPaddockDiscordNotification([
        { officialRank: 1, total: 4.1 },
        { officialRank: 2, total: 1.2 },
      ]),
    ).toBe(false);
  });

  it("treats an empty input list as a skip case for the notification gate", () => {
    expect(shouldSkipPaddockDiscordNotification([])).toBe(true);
  });

  it("marks a horse as notifiable when only the paddock total is positive", () => {
    expect(isPaddockHorseNotifiable({ officialRank: null, total: 1.5 })).toBe(true);
  });

  it("marks a horse as notifiable when only the official rank is set", () => {
    expect(isPaddockHorseNotifiable({ officialRank: 5, total: 0 })).toBe(true);
  });

  it("marks a horse as non-notifiable when both paddock total and official rank are empty", () => {
    expect(isPaddockHorseNotifiable({ officialRank: null, total: 0 })).toBe(false);
  });

  it("treats a negative paddock total without an official rank as non-notifiable", () => {
    expect(isPaddockHorseNotifiable({ officialRank: null, total: -2 })).toBe(false);
  });

  it("score-action-records-user-id-in-history", () => {
    const state = applyPaddockAction(
      createPaddockState("race", "2026-05-13T11:59:00.000Z"),
      {
        category: "paddock",
        delta: 1,
        horseName: "一番",
        horseNumber: "01",
        userId: "user-abc",
      },
      "2026-05-13T12:00:00.000Z",
    );

    expect(state.history[0]?.userId).toBe("user-abc");
    expect(state.history[0]?.type).toBe("score");
  });

  it("score-action-without-user-id-leaves-undefined", () => {
    const state = applyPaddockAction(
      createPaddockState("race", "2026-05-13T11:59:00.000Z"),
      {
        category: "attention",
        delta: 1,
        horseName: "二番",
        horseNumber: "02",
      },
      "2026-05-13T12:00:00.000Z",
    );

    expect(state.history[0]?.userId).toBeUndefined();
    expect(state.history[0]?.type).toBe("score");
  });

  it("official-rank-action-does-not-record-user-id", () => {
    const state = applyPaddockAction(
      createPaddockState("race", "2026-05-13T11:59:00.000Z"),
      {
        horseName: "三番",
        horseNumber: "03",
        rank: 1,
        type: "official-rank",
      },
      "2026-05-13T12:00:00.000Z",
    );

    expect(state.history[0]?.userId).toBeUndefined();
    expect(state.history[0]?.type).toBe("official-rank");
  });

  it("exposes the metric coefficient map with the new weights", () => {
    expect(PADDOCK_METRIC_COEFFICIENT).toStrictEqual({
      attention: 0.5,
      kaeshi: 0.7,
      paddock: 1.3,
      preference: 0.3,
    });
  });

  it("exposes the per-metric maximum count map", () => {
    expect(PADDOCK_METRIC_MAX_COUNT).toStrictEqual({
      attention: 5,
      kaeshi: 3,
      paddock: 5,
      preference: 10,
    });
  });

  it("clamps the paddock count at the positive cap of five", () => {
    const state = Array.from({ length: 7 }).reduce<PaddockState>(
      (current, _, index) =>
        applyPaddockAction(
          current,
          {
            category: "paddock",
            delta: 1,
            horseName: "一番",
            horseNumber: "01",
          },
          `2026-05-13T12:0${String(index)}:00.000Z`,
        ),
      createPaddockState("race"),
    );

    expect(state.horses["1"]?.paddock).toBe(5);
    expect(state.horses["1"]?.total).toBe(6.5);
  });

  it("clamps the paddock count at the negative cap of minus five", () => {
    const state = Array.from({ length: 7 }).reduce<PaddockState>(
      (current, _, index) =>
        applyPaddockAction(
          current,
          {
            category: "paddock",
            delta: -1,
            horseName: "一番",
            horseNumber: "01",
          },
          `2026-05-13T12:0${String(index)}:00.000Z`,
        ),
      createPaddockState("race"),
    );

    expect(state.horses["1"]?.paddock).toBe(-5);
    expect(state.horses["1"]?.total).toBe(-6.5);
  });

  it("clamps the attention count at the positive cap of five", () => {
    const state = Array.from({ length: 7 }).reduce<PaddockState>(
      (current, _, index) =>
        applyPaddockAction(
          current,
          {
            category: "attention",
            delta: 1,
            horseName: "二番",
            horseNumber: "02",
          },
          `2026-05-13T12:0${String(index)}:00.000Z`,
        ),
      createPaddockState("race"),
    );

    expect(state.horses["2"]?.attention).toBe(5);
    expect(state.horses["2"]?.total).toBe(2.5);
  });

  it("clamps the attention count at the negative cap of minus five", () => {
    const state = Array.from({ length: 7 }).reduce<PaddockState>(
      (current, _, index) =>
        applyPaddockAction(
          current,
          {
            category: "attention",
            delta: -1,
            horseName: "二番",
            horseNumber: "02",
          },
          `2026-05-13T12:0${String(index)}:00.000Z`,
        ),
      createPaddockState("race"),
    );

    expect(state.horses["2"]?.attention).toBe(-5);
    expect(state.horses["2"]?.total).toBe(-2.5);
  });

  it("clamps the kaeshi count at the positive cap of three", () => {
    const state = Array.from({ length: 5 }).reduce<PaddockState>(
      (current, _, index) =>
        applyPaddockAction(
          current,
          {
            category: "kaeshi",
            delta: 1,
            horseName: "三番",
            horseNumber: "03",
          },
          `2026-05-13T12:0${String(index)}:00.000Z`,
        ),
      createPaddockState("race"),
    );

    expect(state.horses["3"]?.kaeshi).toBe(3);
  });

  it("clamps the kaeshi count at the negative cap of minus three", () => {
    const state = Array.from({ length: 5 }).reduce<PaddockState>(
      (current, _, index) =>
        applyPaddockAction(
          current,
          {
            category: "kaeshi",
            delta: -1,
            horseName: "三番",
            horseNumber: "03",
          },
          `2026-05-13T12:0${String(index)}:00.000Z`,
        ),
      createPaddockState("race"),
    );

    expect(state.horses["3"]?.kaeshi).toBe(-3);
  });

  it("clamps the preference count at the positive cap of ten", () => {
    const state = Array.from({ length: 12 }).reduce<PaddockState>(
      (current, _, index) =>
        applyPaddockAction(
          current,
          {
            category: "preference",
            delta: 1,
            horseName: "四番",
            horseNumber: "04",
          },
          `2026-05-13T12:${String(index).padStart(2, "0")}:00.000Z`,
        ),
      createPaddockState("race"),
    );

    expect(state.horses["4"]?.preference).toBe(10);
    expect(state.horses["4"]?.total).toBe(3);
  });

  it("clamps the preference count at the negative cap of minus ten", () => {
    const state = Array.from({ length: 12 }).reduce<PaddockState>(
      (current, _, index) =>
        applyPaddockAction(
          current,
          {
            category: "preference",
            delta: -1,
            horseName: "四番",
            horseNumber: "04",
          },
          `2026-05-13T12:${String(index).padStart(2, "0")}:00.000Z`,
        ),
      createPaddockState("race"),
    );

    expect(state.horses["4"]?.preference).toBe(-10);
    expect(state.horses["4"]?.total).toBe(-3);
  });

  it("reaches the paddock cap exactly from the boundary then holds steady", () => {
    const atFour = Array.from({ length: 4 }).reduce<PaddockState>(
      (current, _, index) =>
        applyPaddockAction(
          current,
          {
            category: "paddock",
            delta: 1,
            horseName: "五番",
            horseNumber: "05",
          },
          `2026-05-13T12:0${String(index)}:00.000Z`,
        ),
      createPaddockState("race"),
    );
    expect(atFour.horses["5"]?.paddock).toBe(4);

    const atFive = applyPaddockAction(
      atFour,
      {
        category: "paddock",
        delta: 1,
        horseName: "五番",
        horseNumber: "05",
      },
      "2026-05-13T12:05:00.000Z",
    );
    expect(atFive.horses["5"]?.paddock).toBe(5);

    const stillFive = applyPaddockAction(
      atFive,
      {
        category: "paddock",
        delta: 1,
        horseName: "五番",
        horseNumber: "05",
      },
      "2026-05-13T12:06:00.000Z",
    );
    expect(stillFive.horses["5"]?.paddock).toBe(5);
  });

  it("score-history-with-user-id-survives-100-entry-limit", () => {
    const base = createPaddockState("race", "2026-05-13T11:59:00.000Z");
    const seeded = applyPaddockAction(
      base,
      {
        category: "paddock",
        delta: 1,
        horseName: "先頭",
        horseNumber: "01",
        userId: "user-first",
      },
      "2026-05-13T12:00:00.000Z",
    );
    const filled = Array.from({ length: PADDOCK_HISTORY_LIMIT - 1 }).reduce<PaddockState>(
      (current, _, index) =>
        applyPaddockAction(
          current,
          {
            category: "paddock",
            delta: 1,
            horseName: `馬${index}`,
            horseNumber: String((index % 18) + 1),
            userId: `user-${index}`,
          },
          `2026-05-13T13:${String(index).padStart(2, "0")}:00.000Z`,
        ),
      seeded,
    );

    expect(filled.history).toHaveLength(PADDOCK_HISTORY_LIMIT);
    expect(filled.history[0]?.userId).toBe("user-98");
    expect(filled.history[PADDOCK_HISTORY_LIMIT - 1]?.userId).toBe("user-first");
  });
});
