import { describe, expect, it } from "vitest";

import {
  getAgeLabel,
  getConditionLabel,
  getGradeLabel,
  getRaceClassLabel,
  getRaceSymbolLabel,
  getRaceTagText,
  getRaceTags,
  getWeightLabel,
} from "./race-classification";

type RaceTagInput = Parameters<typeof getRaceTags>[0];

const race = (overrides: Partial<RaceTagInput>): RaceTagInput => ({
  gradeCode: null,
  kyosoJokenCode: null,
  kyosoJokenMeisho: null,
  kyosoKigoCode: null,
  kyosoShubetsuCode: null,
  juryoShubetsuCode: null,
  source: "jra",
  ...overrides,
});

describe("race classification", () => {
  it("builds age, class, and female limited tags", () => {
    expect(
      getRaceTags(
        race({
          kyosoShubetsuCode: "02",
          kyosoJokenCode: "703",
          kyosoKigoCode: "023",
        }),
      ),
    ).toEqual(["3歳", "未勝利", "牝馬限定"]);
  });

  it("prefers graded and listed labels over open class", () => {
    expect(
      getRaceTags(race({ kyosoShubetsuCode: "12", gradeCode: "A", kyosoJokenCode: "999" })),
    ).toEqual(["3歳", "G1"]);
    expect(getRaceTagText(race({ kyosoShubetsuCode: "05", gradeCode: "L" }))).toBe(
      "4歳以上 リステッド競走",
    );
  });

  it("adds young rider and handicap tags", () => {
    expect(
      getRaceTags(
        race({
          kyosoShubetsuCode: "05",
          kyosoJokenCode: "005",
          kyosoKigoCode: "002",
          juryoShubetsuCode: "6",
        }),
      ),
    ).toEqual(["4歳以上", "1勝クラス", "若手騎手", "ハンデ戦"]);
  });

  it("formats weight type labels", () => {
    expect(getWeightLabel("3")).toBe("馬齢");
    expect(getWeightLabel("6")).toBe("騎手ハンデ");
    expect(getWeightLabel("9")).toBe("重量種別 9");
    expect(getWeightLabel(null)).toBe("-");
  });

  it("formats class and symbol labels", () => {
    expect(getAgeLabel("02")).toBe("3歳");
    expect(getAgeLabel("99")).toBe("年齢条件 99");
    expect(getAgeLabel(null)).toBe("-");
    expect(getConditionLabel("703")).toBe("未勝利");
    expect(getConditionLabel("123")).toBe("条件 123");
    expect(getConditionLabel(null)).toBe("-");
    expect(getRaceClassLabel("02", "703")).toBe("3歳 未勝利");
    expect(getRaceSymbolLabel("023")).toBe("牝馬限定");
    expect(getRaceSymbolLabel("999")).toBe("競走記号 999");
  });

  it("formats grade labels", () => {
    expect(getGradeLabel("B")).toBe("G2");
    expect(getGradeLabel("B", "nar")).toBe("Jpn2");
    expect(getGradeLabel("Z")).toBe("グレード Z");
    expect(getGradeLabel(null)).toBe("-");
  });

  it("uses Japan grade labels for NAR graded races", () => {
    expect(getRaceTags(race({ gradeCode: "B", kyosoJokenCode: "000", source: "nar" }))).toEqual([
      "Jpn2",
    ]);
  });

  it("falls back to open condition when no other tag exists", () => {
    expect(getRaceTags(race({ kyosoJokenCode: "999" }))).toEqual(["オープン"]);
  });

  it("extracts NAR open and local class labels from condition names", () => {
    expect(
      getRaceTags(
        race({
          kyosoJokenMeisho: "３歳上ＯＰ",
          kyosoShubetsuCode: "04",
        }),
      ),
    ).toEqual(["3歳以上", "３歳上ＯＰ"]);

    expect(
      getRaceTags(
        race({
          kyosoJokenMeisho: "　　　Ｂ２－４　　　　　　　　　　　      　　　 670.0万未満",
          kyosoShubetsuCode: "04",
        }),
      ),
    ).toEqual(["3歳以上", "B2-4"]);

    expect(
      getRaceTags(
        race({
          kyosoJokenMeisho: "　　　Ｃ２　　　　　　　　　　　　　　　　　　　　　　　　　",
          kyosoShubetsuCode: "49",
        }),
      ),
    ).toEqual(["一般", "C2"]);

    expect(
      getRaceTags(
        race({
          kyosoJokenMeisho: "　　　Ｃ１－７　　　　　　　　　　　      　　　 340.0万未満",
          kyosoShubetsuCode: "49",
        }),
      ),
    ).toEqual(["一般", "C1-7"]);

    expect(
      getRaceTags(
        race({
          kyosoJokenMeisho: "3歳上OP サラブレッド系",
        }),
      ),
    ).toEqual(["3歳上OP"]);
  });

  it("does not add unsupported condition names as tags", () => {
    expect(
      getRaceTags(
        race({
          gradeCode: "E",
          kyosoJokenMeisho: "一般普通競走",
        }),
      ),
    ).toEqual([]);
  });
});
