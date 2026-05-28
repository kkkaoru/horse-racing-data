import { describe, expect, it } from "vitest";

import {
  getAgeLabel,
  getConditionLabel,
  getGradeLabel,
  getRaceClassLabel,
  getRaceSymbolDetailLabel,
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
    expect(getWeightLabel("0")).toBe("指定なし");
    expect(getWeightLabel("3")).toBe("馬齢");
    expect(getWeightLabel("6")).toBe("騎手ハンデ");
    expect(getWeightLabel("9")).toBe("その他");
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
    expect(getRaceSymbolDetailLabel("000")).toBe("制限なし");
    expect(getRaceSymbolLabel("023")).toBe("牝馬限定");
    expect(getRaceSymbolDetailLabel("023")).toBe("牝馬限定 [指定]");
    expect(getRaceSymbolDetailLabel("N23")).toBe("国際競走 牝馬限定 [指定]");
    expect(getRaceSymbolLabel("999")).toBe("競走記号 999");
    expect(getRaceSymbolDetailLabel("999")).toBe("競走記号 999");
  });

  it("formats grade labels", () => {
    expect(getGradeLabel("B")).toBe("G2");
    expect(getGradeLabel("B", "nar")).toBe("Jpn2");
    expect(getGradeLabel("P", "nar")).toBe("地区限定重賞 1");
    expect(getGradeLabel("T", "nar")).toBe("準重賞");
    expect(getGradeLabel("Z", "nar")).toBe("グレード Z");
    expect(getGradeLabel("Z")).toBe("グレード Z");
    expect(getGradeLabel(null)).toBe("-");
    expect(getGradeLabel(" ", "nar")).toBe("普通");
  });

  it("keeps non-handicap weight labels out of tags", () => {
    expect(
      getRaceTags(
        race({
          kyosoJokenCode: "005",
          juryoShubetsuCode: "0",
        }),
      ),
    ).toEqual(["1勝クラス"]);
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

  it("returns OP condition label when normalized name matches the OP keyword", () => {
    expect(
      getRaceTags(
        race({
          kyosoJokenMeisho: "OP 特別",
        }),
      ),
    ).toEqual(["OP 特別"]);
  });

  it("returns the fallback weight label for an unknown weight code", () => {
    expect(getWeightLabel("99")).toBe("重量種別 99");
  });

  it("returns hyphen for weight when the code is empty", () => {
    expect(getWeightLabel(null)).toBe("-");
  });

  it("returns the fallback symbol label for an unknown symbol code", () => {
    expect(getRaceSymbolLabel("999")).toBe("競走記号 999");
  });

  it("returns hyphen for symbol when the code is empty", () => {
    expect(getRaceSymbolLabel(null)).toBe("-");
  });

  it("returns the fallback detail symbol label for an unknown code", () => {
    expect(getRaceSymbolDetailLabel("ZZZ")).toBe("競走記号 ZZZ");
  });

  it("returns hyphen for the detail symbol label when the code is empty", () => {
    expect(getRaceSymbolDetailLabel(null)).toBe("-");
  });

  it("appends ハンデ戦 tag for handicap races", () => {
    expect(
      getRaceTags(
        race({
          kyosoJokenMeisho: "3歳以上1勝クラス",
          juryoShubetsuCode: "6",
        }),
      ),
    ).toStrictEqual(["ハンデ戦"]);
  });
});
