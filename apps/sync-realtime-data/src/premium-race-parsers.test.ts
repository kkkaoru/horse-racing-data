// run with: bun run test
import { expect, it } from "vitest";
import { parsePremiumStableComments, parsePremiumTrainingReviews } from "./premium-race";

it("parsePremiumTrainingReviews returns rows when class selectors match", () => {
  const env = {
    PREMIUM_RACE_WORK_COMMENT_CLASS: "Comment_Cell",
    PREMIUM_RACE_WORK_DATE_CLASS: "Date",
    PREMIUM_RACE_WORK_GRADE_CLASS: "Grade",
    PREMIUM_RACE_WORK_HORSE_NAME_CLASS: "Horse_Name",
    PREMIUM_RACE_WORK_HORSE_NUMBER_CLASS: "Horse_Number",
    PREMIUM_RACE_WORK_RIDER_CLASS: "Rider",
    PREMIUM_RACE_WORK_ROW_CLASS: "Work_Row",
    PREMIUM_RACE_WORK_TEXT_CLASS: "Evaluation",
  };
  const html = `
    <tr class="Work_Row">
      <td class="Horse_Number">1</td>
      <td class="Horse_Name">サンプル</td>
      <td class="Date">2026/05/10</td>
      <td class="Evaluation">良好</td>
      <td class="Grade">A</td>
      <td class="Rider">調教師</td>
      <td class="Comment_Cell">良い動き</td>
    </tr>
  `;
  const result = parsePremiumTrainingReviews(html, env);
  expect(result.length).toBe(1);
  expect(result[0]!.horseNumber).toBe("1");
  expect(result[0]!.horseName).toBe("サンプル");
  expect(result[0]!.trainingDate).toBe("2026/05/10");
  expect(result[0]!.evaluationText).toBe("良好");
});

it("parsePremiumTrainingReviews returns no rows when no Work_Row matches", () => {
  const result = parsePremiumTrainingReviews("<div>plain text</div>", {
    PREMIUM_RACE_WORK_ROW_CLASS: "Work_Row",
  });
  expect(result).toStrictEqual([]);
});

it("parsePremiumTrainingReviews inherits horse identity from prior row", () => {
  const env = {
    PREMIUM_RACE_WORK_COMMENT_CLASS: "Comment_Cell",
    PREMIUM_RACE_WORK_DATE_CLASS: "Date",
    PREMIUM_RACE_WORK_HORSE_NAME_CLASS: "Horse_Name",
    PREMIUM_RACE_WORK_HORSE_NUMBER_CLASS: "Horse_Number",
    PREMIUM_RACE_WORK_ROW_CLASS: "Work_Row",
  };
  const html = `
    <tr class="Work_Row">
      <td class="Horse_Number">1</td>
      <td class="Horse_Name">サンプル</td>
      <td class="Comment_Cell">前回コメント</td>
    </tr>
    <tr class="Work_Row">
      <td class="Date">2026/05/10</td>
    </tr>
  `;
  const result = parsePremiumTrainingReviews(html, env);
  expect(result.length).toBe(1);
  expect(result[0]!.horseName).toBe("サンプル");
  expect(result[0]!.horseNumber).toBe("1");
  expect(result[0]!.trainingDate).toBe("2026/05/10");
});

it("parsePremiumStableComments returns class-based rows when row class matches", () => {
  const env = {
    PREMIUM_RACE_COMMENT_LABEL_FRAME: "Waku",
    PREMIUM_RACE_COMMENT_LABEL_HORSE_NAME: "Horse_Name",
    PREMIUM_RACE_COMMENT_LABEL_HORSE_NUMBER: "Horse_Number",
    PREMIUM_RACE_COMMENT_LABEL_TEXT: "Comment_Text",
    PREMIUM_RACE_COMMENT_ROW_CLASS: "Comment_Row",
  };
  const html = `
    <tr class="Comment_Row">
      <td class="Waku">1</td>
      <td class="Horse_Number">3</td>
      <td class="Horse_Name">サンプル</td>
      <td class="Comment_Text">期待値高い</td>
    </tr>
  `;
  const result = parsePremiumStableComments(html, env);
  expect(result.length).toBe(1);
  expect(result[0]!.horseNumber).toBe("3");
  expect(result[0]!.commentText).toBe("期待値高い");
});

it("parsePremiumStableComments falls back to raw table cells when no row class matches", () => {
  const html = `
    <table>
      <tr>
        <td>1</td>
        <td>3</td>
        <td>サンプル</td>
        <td>期待値高い</td>
      </tr>
    </table>
  `;
  const result = parsePremiumStableComments(html, {});
  expect(result.length).toBe(1);
  expect(result[0]!.commentText).toBe("期待値高い");
});

it("parsePremiumStableComments returns empty array when no rows have a comment", () => {
  const html = `
    <table>
      <tr><td>1</td><td>3</td><td>サンプル</td><td>コメント</td></tr>
    </table>
  `;
  expect(parsePremiumStableComments(html, {})).toStrictEqual([]);
});
