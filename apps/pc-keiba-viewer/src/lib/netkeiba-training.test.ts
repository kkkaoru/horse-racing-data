import { describe, expect, it } from "vitest";

import { buildNetkeibaRaceId, parseNetkeibaTrainingReviews } from "./netkeiba-training";

describe("netkeiba training helpers", () => {
  it("builds JRA netkeiba race ids from meeting fields", () => {
    expect(
      buildNetkeibaRaceId({
        kaisaiKai: "02",
        kaisaiNen: "2026",
        kaisaiNichime: "08",
        keibajoCode: "05",
        raceBango: "11",
        source: "jra",
      }),
    ).toBe("202605020811");
  });

  it("parses evaluation rows from netkeiba oikiri html", () => {
    const html = `
      <tr class="OikiriDataHead1 HorseList">
        <td class="Umaban">1</td>
        <td class="Horse_Info fc"><div class="Horse_Name"><a>カピリナ</a></div></td>
        <td class="Training_Critic">元気一杯</td>
        <td class="Rank_元気一杯">B</td>
      </tr>
      <tr class="OikiriDataHead2 HorseList">
        <td class="Umaban">2</td>
        <td class="Horse_Info fc"><div class="Horse_Name"><a>ワイドラトゥール</a></div></td>
        <td class="Training_Critic">気合乗る</td>
        <td class="Rank_気合乗る">A</td>
      </tr>
    `;

    expect(parseNetkeibaTrainingReviews(html)).toEqual([
      {
        commentText: null,
        evaluationGrade: "B",
        evaluationText: "元気一杯",
        horseName: "カピリナ",
        horseNumber: "1",
        riderName: null,
        trainingDate: "",
      },
      {
        commentText: null,
        evaluationGrade: "A",
        evaluationText: "気合乗る",
        horseName: "ワイドラトゥール",
        horseNumber: "2",
        riderName: null,
        trainingDate: "",
      },
    ]);
  });

  it("returns null race id when source is not jra", () => {
    expect(
      buildNetkeibaRaceId({
        kaisaiKai: "02",
        kaisaiNen: "2026",
        kaisaiNichime: "08",
        keibajoCode: "05",
        raceBango: "11",
        source: "nar",
      }),
    ).toBeNull();
  });

  it("returns null race id when kaisai kai is missing", () => {
    expect(
      buildNetkeibaRaceId({
        kaisaiKai: null,
        kaisaiNen: "2026",
        kaisaiNichime: "08",
        keibajoCode: "05",
        raceBango: "11",
        source: "jra",
      }),
    ).toBeNull();
  });

  it("returns null race id when kaisai nichime is missing", () => {
    expect(
      buildNetkeibaRaceId({
        kaisaiKai: "02",
        kaisaiNen: "2026",
        kaisaiNichime: null,
        keibajoCode: "05",
        raceBango: "11",
        source: "jra",
      }),
    ).toBeNull();
  });

  it("pads single-digit race bango when building race id", () => {
    expect(
      buildNetkeibaRaceId({
        kaisaiKai: "02",
        kaisaiNen: "2026",
        kaisaiNichime: "08",
        keibajoCode: "05",
        raceBango: "3",
        source: "jra",
      }),
    ).toBe("202605020803");
  });

  it("skips rows with missing horse number", () => {
    const html = `
      <tr class="OikiriDataHead1 HorseList">
        <td class="Umaban"></td>
        <td class="Horse_Info fc"><div class="Horse_Name"><a>カピリナ</a></div></td>
        <td class="Training_Critic">元気一杯</td>
        <td class="Rank_元気一杯">B</td>
      </tr>
    `;
    expect(parseNetkeibaTrainingReviews(html)).toEqual([]);
  });

  it("skips rows without evaluation text and grade", () => {
    const html = `
      <tr class="OikiriDataHead1 HorseList">
        <td class="Umaban">1</td>
        <td class="Horse_Info fc"><div class="Horse_Name"><a>カピリナ</a></div></td>
        <td class="Training_Critic"></td>
      </tr>
    `;
    expect(parseNetkeibaTrainingReviews(html)).toEqual([]);
  });

  it("returns null horse name when name cell is empty", () => {
    const html = `
      <tr class="OikiriDataHead1 HorseList">
        <td class="Umaban">1</td>
        <td class="Training_Critic">元気一杯</td>
        <td class="Rank_元気一杯">B</td>
      </tr>
    `;
    expect(parseNetkeibaTrainingReviews(html)).toEqual([
      {
        commentText: null,
        evaluationGrade: "B",
        evaluationText: "元気一杯",
        horseName: null,
        horseNumber: "1",
        riderName: null,
        trainingDate: "",
      },
    ]);
  });

  it("decodes named and numeric html entities", () => {
    const html = `
      <tr class="OikiriDataHead1 HorseList">
        <td class="Umaban">1</td>
        <td class="Horse_Info fc"><div class="Horse_Name"><a>&#x4e00;&amp;&unknownentity;</a></div></td>
        <td class="Training_Critic">気合&nbsp;乗る</td>
        <td class="Rank_気合乗る">A</td>
      </tr>
    `;
    expect(parseNetkeibaTrainingReviews(html)).toEqual([
      {
        commentText: null,
        evaluationGrade: "A",
        evaluationText: "気合 乗る",
        horseName: "&#x4e00;&",
        horseNumber: "1",
        riderName: null,
        trainingDate: "",
      },
    ]);
  });

  it("rejects negative or non-integer horse numbers", () => {
    const html = `
      <tr class="OikiriDataHead1 HorseList">
        <td class="Umaban">0</td>
        <td class="Horse_Info fc"><div class="Horse_Name"><a>foo</a></div></td>
        <td class="Training_Critic">気合乗る</td>
        <td class="Rank_気合乗る">A</td>
      </tr>
    `;
    expect(parseNetkeibaTrainingReviews(html)).toEqual([]);
  });
});
