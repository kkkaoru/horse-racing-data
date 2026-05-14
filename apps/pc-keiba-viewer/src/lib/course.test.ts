import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  estimateCornerCount,
  formatCourseParagraphs,
  getCourseFacts,
  getCourseImagePath,
} from "./course";

const courseText =
  "スタート地点はスタンド前直線の４コーナー寄り｡最初の１コーナーまでの距離は約３８９ｍで平坦｡新潟ダートコースは高低差が０.５ｍと少ない｡最後の直線距離は３５４ｍ｡フルゲートは１５頭｡　●クラス別水準ラップ";

const courseDir = path.join(process.cwd(), "public", "courses");

afterEach(() => {
  rmSync(courseDir, { force: true, recursive: true });
});

describe("course helpers", () => {
  it("extracts key facts from course explanation text", () => {
    expect(getCourseFacts(courseText, "1800", "23")).toEqual([
      { label: "距離", value: "1800m" },
      { label: "コーナー回数", value: "4回" },
      { label: "高低差", value: "0.5m" },
      { label: "最後の直線", value: "354m" },
      { label: "1コーナーまで", value: "389m" },
      { label: "フルゲート", value: "15頭" },
    ]);
  });

  it("omits missing fact values", () => {
    expect(getCourseFacts("説明のみ", null)).toEqual([]);
  });

  it("splits course text into readable paragraphs", () => {
    expect(formatCourseParagraphs(courseText)).toEqual([
      "スタート地点はスタンド前直線の４コーナー寄り。",
      "最初の１コーナーまでの距離は約３８９ｍで平坦。",
      "新潟ダートコースは高低差が０.５ｍと少ない。",
      "最後の直線距離は３５４ｍ。",
      "フルゲートは１５頭。",
      "●クラス別水準ラップ",
    ]);
  });

  it("estimates corner count from track and course text", () => {
    expect(estimateCornerCount(courseText, "23")).toBe(4);
    expect(estimateCornerCount("向正面から1～4コーナーを通るコース", "23")).toBe(4);
    expect(estimateCornerCount("説明", "10")).toBe(0);
    expect(estimateCornerCount("説明", "21")).toBe(8);
    expect(estimateCornerCount("説明", "23")).toBeNull();
  });

  it("extracts numeric course traits beyond basic distances", () => {
    const text =
      "ＪＲＡ全１０場で比較｡良馬場･稍重での逃げ馬の連対率は３５％前後｡重馬場以上になると約４５％までアップ｡枠順は多頭数の１４番､１５番ゲート以外はほぼフラット｡　●クラス別水準ラップ(３Ｆ-３Ｆ-３Ｆ)と勝ち時計　３歳以上５００万(３６.６-３８.１-３８.５=１.５３.２)";

    expect(getCourseFacts(text, "1800")).toEqual([
      { label: "距離", value: "1800m" },
      { label: "比較対象", value: "10場" },
      { label: "良・稍重 逃げ連対率", value: "35%前後" },
      { label: "重以上 逃げ連対率", value: "45%まで" },
      { label: "外枠注意", value: "14番､15番ゲート" },
      { label: "3歳以上500万 水準", value: "36.6-38.1-38.5=1.53.2" },
    ]);
  });

  it("returns an existing public course image path", () => {
    mkdirSync(courseDir, { recursive: true });
    writeFileSync(path.join(courseDir, "04-23-1800.png"), "");

    expect(getCourseImagePath("04", "23", "1800")).toBe("/courses/04-23-1800.png");
  });

  it("returns null when course image lookup inputs are missing", () => {
    expect(getCourseImagePath("04", "", "1800")).toBeNull();
    expect(getCourseImagePath("04", "23", "")).toBeNull();
    expect(getCourseImagePath("04", "23", "9999")).toBeNull();
  });
});
