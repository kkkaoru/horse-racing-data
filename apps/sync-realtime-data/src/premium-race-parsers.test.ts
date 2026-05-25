// run with: bun run test
import { expect, it } from "vitest";
import {
  buildJraPremiumSourceRaceId,
  buildNarPremiumSourceRaceId,
  buildPremiumRaceLinkFromRace,
  getPremiumRaceConfig,
  matchPremiumLinkToRace,
  parsePremiumDataTopHorses,
  parsePremiumPaddockBulletins,
  parsePremiumStableComments,
  parsePremiumTrainingReviews,
  sourceRaceIdCandidates,
} from "./premium-race";

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

it("buildJraPremiumSourceRaceId returns null when source is not jra", () => {
  expect(
    buildJraPremiumSourceRaceId({
      kaisaiKai: "01",
      kaisaiNen: "2026",
      kaisaiNichime: "02",
      keibajoCode: "08",
      raceBango: "01",
      source: "nar",
    }),
  ).toBeNull();
});

it("buildJraPremiumSourceRaceId returns null when kaisaiKai is missing", () => {
  expect(
    buildJraPremiumSourceRaceId({
      kaisaiKai: null,
      kaisaiNen: "2026",
      kaisaiNichime: "02",
      keibajoCode: "08",
      raceBango: "01",
      source: "jra",
    }),
  ).toBeNull();
});

it("buildJraPremiumSourceRaceId concatenates the JRA race-id format", () => {
  expect(
    buildJraPremiumSourceRaceId({
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      keibajoCode: "08",
      raceBango: "1",
      source: "jra",
    }),
  ).toBe("2026080206" + "01");
});

it("buildNarPremiumSourceRaceId returns null when source is not nar", () => {
  expect(
    buildNarPremiumSourceRaceId({
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "55",
      raceBango: "01",
      source: "jra",
    }),
  ).toBeNull();
});

it("buildNarPremiumSourceRaceId returns null when keibajoCode is Ban-ei (83)", () => {
  expect(
    buildNarPremiumSourceRaceId({
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "83",
      raceBango: "01",
      source: "nar",
    }),
  ).toBeNull();
});

it("buildNarPremiumSourceRaceId returns null when kaisaiTsukihi is malformed", () => {
  expect(
    buildNarPremiumSourceRaceId({
      kaisaiNen: "2026",
      kaisaiTsukihi: "abc",
      keibajoCode: "55",
      raceBango: "01",
      source: "nar",
    }),
  ).toBeNull();
});

it("buildNarPremiumSourceRaceId composes the NAR race-id format", () => {
  expect(
    buildNarPremiumSourceRaceId({
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "55",
      raceBango: "1",
      source: "nar",
    }),
  ).toBe("20265505120" + "1");
});

it("buildPremiumRaceLinkFromRace falls back to sourceIdQueryKey=... when no path templates exist", () => {
  const config = getPremiumRaceConfig({ PREMIUM_RACE_ORIGIN: "https://x.test" });
  const result = buildPremiumRaceLinkFromRace(
    {
      babaCode: "08",
      debaUrl: "u",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      lastOddsFetchAt: null,
      lastWeightFetchAt: null,
      oddsLinks: {},
      raceBango: "01",
      raceKey: "jra:2026:0512:08:01",
      raceName: null,
      raceStartAtJst: "2026-05-12T13:00:00+09:00",
      source: "jra",
    },
    config,
  );
  expect(result?.entryUrl).toBe("race_id=2026080206" + "01");
});

it("buildPremiumRaceLinkFromRace returns null when sourceRaceId cannot be built", () => {
  const config = getPremiumRaceConfig({ PREMIUM_RACE_ORIGIN: "https://x.test" });
  const result = buildPremiumRaceLinkFromRace(
    {
      babaCode: "83",
      debaUrl: "u",
      kaisaiKai: null,
      kaisaiNen: "2026",
      kaisaiNichime: null,
      kaisaiTsukihi: "0512",
      keibajoCode: "83",
      lastOddsFetchAt: null,
      lastWeightFetchAt: null,
      oddsLinks: {},
      raceBango: "01",
      raceKey: "nar:2026:0512:83:01",
      raceName: null,
      raceStartAtJst: "2026-05-12T13:00:00+09:00",
      source: "nar",
    },
    config,
  );
  expect(result).toBeNull();
});

it("sourceRaceIdCandidates produces both padded and trimmed raceBango variants", () => {
  const result = sourceRaceIdCandidates({
    babaCode: "55",
    debaUrl: "u",
    kaisaiKai: null,
    kaisaiNen: "2026",
    kaisaiNichime: null,
    kaisaiTsukihi: "0512",
    keibajoCode: "55",
    lastOddsFetchAt: null,
    lastWeightFetchAt: null,
    oddsLinks: {},
    raceBango: "01",
    raceKey: "nar:2026:0512:55:01",
    raceName: null,
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
    source: "nar",
  });
  expect(result.length).toBeGreaterThanOrEqual(3);
  expect(result.some((id) => id.endsWith("01"))).toBe(true);
  expect(result.some((id) => id.endsWith("1") && !id.endsWith("01"))).toBe(true);
});

it("matchPremiumLinkToRace falls back to suffix match on padded raceBango", () => {
  const links = [
    { entryUrl: "https://x.test/race?race_id=999999999901", sourceRaceId: "999999999901" },
  ];
  const link = matchPremiumLinkToRace(links, {
    babaCode: "55",
    debaUrl: "u",
    kaisaiKai: null,
    kaisaiNen: "2026",
    kaisaiNichime: null,
    kaisaiTsukihi: "0512",
    keibajoCode: "55",
    lastOddsFetchAt: null,
    lastWeightFetchAt: null,
    oddsLinks: {},
    raceBango: "01",
    raceKey: "nar:2026:0512:55:01",
    raceName: null,
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
    source: "nar",
  });
  expect(link?.sourceRaceId).toBe("999999999901");
});

it("matchPremiumLinkToRace returns null when no link suffix matches the race", () => {
  expect(
    matchPremiumLinkToRace(
      [{ entryUrl: "u", sourceRaceId: "1234567890" }],
      {
        babaCode: "55",
        debaUrl: "u",
        kaisaiKai: null,
        kaisaiNen: "2026",
        kaisaiNichime: null,
        kaisaiTsukihi: "0512",
        keibajoCode: "55",
        lastOddsFetchAt: null,
        lastWeightFetchAt: null,
        oddsLinks: {},
        raceBango: "07",
        raceKey: "nar:2026:0512:55:07",
        raceName: null,
        raceStartAtJst: "2026-05-12T13:00:00+09:00",
        source: "nar",
      },
    ),
  ).toBeNull();
});

it("parsePremiumDataTopHorses extracts rank-ordered horses from PickupHorseArea", () => {
  const html = `
    <div class="DataPickupHorseArea">
      <dl>
        <dt><span class="Umaban_Num">3</span></dt>
        <dd>
          <a class="data_top_horse_link">ウマ1</a>
          <dd class="PickupDataBox">
            <ul><li>理由A</li><li>理由B</li></ul>
          </dd>
        </dd>
      </dl>
      <dl>
        <dt><span class="Umaban_Num">5</span></dt>
        <dd>
          <a class="data_top_horse_link">ウマ2</a>
          <dd class="PickupDataBox">
            <ul><li>理由C</li></ul>
          </dd>
        </dd>
      </dl>
    </div>
  `;
  const result = parsePremiumDataTopHorses(html, {});
  expect(result).toHaveLength(2);
  expect(result[0]?.horseNumber).toBe("3");
  expect(result[0]?.rank).toBe(1);
  expect(result[0]?.reasons).toStrictEqual(["理由A", "理由B"]);
  expect(result[1]?.horseNumber).toBe("5");
  expect(result[1]?.rank).toBe(2);
});

it("parsePremiumDataTopHorses returns empty when the pickup area is missing", () => {
  expect(parsePremiumDataTopHorses("<div>nothing</div>", {})).toStrictEqual([]);
});

it("parsePremiumDataTopHorses drops entries with no reasons", () => {
  const html = `
    <div class="DataPickupHorseArea">
      <dl>
        <dt><span class="Umaban_Num">3</span></dt>
        <dd>
          <a class="data_top_horse_link">ウマ1</a>
          <dd class="PickupDataBox"></dd>
        </dd>
      </dl>
    </div>
  `;
  expect(parsePremiumDataTopHorses(html, {})).toStrictEqual([]);
});

it("parsePremiumPaddockBulletins detects authRequired marker", () => {
  const result = parsePremiumPaddockBulletins('<div class="Premium_Regist_Box"></div>', {});
  expect(result.authRequired).toBe(true);
  expect(result.pending).toBe(true);
});

it("parsePremiumPaddockBulletins detects unavailable marker text", () => {
  const result = parsePremiumPaddockBulletins(
    "<div>サービス停止中</div>",
    { PREMIUM_RACE_PADDOCK_UNAVAILABLE_TEXT: "サービス停止中" },
  );
  expect(result.unavailable).toBe(true);
});

it("parsePremiumPaddockBulletins detects pendingText marker", () => {
  const result = parsePremiumPaddockBulletins(
    "<div>準備中です</div>",
    { PREMIUM_RACE_PADDOCK_PENDING_TEXT: "準備中" },
  );
  expect(result.pending).toBe(true);
});

it("parsePremiumPaddockBulletins splits row-based grouping into favorite/value halves", () => {
  const html = `
    <table>
      <tr class="Paddock_Row"><td class="Horse_Num">1</td><td class="Eval">A</td></tr>
      <tr class="Paddock_Row"><td class="Horse_Num">2</td><td class="Eval">B</td></tr>
      <tr class="Paddock_Row"><td class="Horse_Num">3</td><td class="Eval">C</td></tr>
      <tr class="Paddock_Row"><td class="Horse_Num">4</td><td class="Eval">D</td></tr>
    </table>
  `;
  const result = parsePremiumPaddockBulletins(html, {
    PREMIUM_RACE_PADDOCK_LABEL_EVALUATION: "Eval",
    PREMIUM_RACE_PADDOCK_LABEL_HORSE_NUMBER: "Horse_Num",
    PREMIUM_RACE_PADDOCK_ROW_CLASS: "Paddock_Row",
  });
  expect(result.bulletins).toHaveLength(4);
  expect(result.bulletins.filter((b) => b.groupKey === "favorite")).toHaveLength(2);
  expect(result.bulletins.filter((b) => b.groupKey === "value")).toHaveLength(2);
});

it("parsePremiumTrainingReviews inherits actionComment + horseName when subsequent row has only date and rider", () => {
  const env = {
    PREMIUM_RACE_WORK_COMMENT_CLASS: "Comment_Cell",
    PREMIUM_RACE_WORK_DATE_CLASS: "Date",
    PREMIUM_RACE_WORK_HORSE_NAME_CLASS: "Horse_Name",
    PREMIUM_RACE_WORK_HORSE_NUMBER_CLASS: "Horse_Number",
    PREMIUM_RACE_WORK_RIDER_CLASS: "Rider",
    PREMIUM_RACE_WORK_ROW_CLASS: "Work_Row",
  };
  const html = `
    <tr class="Work_Row">
      <td class="Horse_Number">2</td>
      <td class="Horse_Name">タロウ</td>
      <td class="Comment_Cell">仕掛けに反応</td>
    </tr>
    <tr class="Work_Row">
      <td class="Date">2026/05/12</td>
      <td class="Rider">乗り役</td>
    </tr>
  `;
  const result = parsePremiumTrainingReviews(html, env);
  expect(result).toHaveLength(1);
  expect(result[0]?.horseNumber).toBe("2");
  expect(result[0]?.commentText).toBe("仕掛けに反応");
  expect(result[0]?.horseName).toBe("タロウ");
  expect(result[0]?.riderName).toBe("乗り役");
});

it("parsePremiumTrainingReviews skips rows that have no horseNumber and no inheritable currentHorse", () => {
  const env = {
    PREMIUM_RACE_WORK_DATE_CLASS: "Date",
    PREMIUM_RACE_WORK_ROW_CLASS: "Work_Row",
  };
  const html = `<tr class="Work_Row"><td class="Date">2026/05/12</td></tr>`;
  expect(parsePremiumTrainingReviews(html, env)).toStrictEqual([]);
});

it("parsePremiumStableComments uses raw-cell fallback variants for frameNumber and horseName", () => {
  const html = `
    <table>
      <tr>
        <th>枠</th>
        <th>馬番</th>
        <th>馬名</th>
        <th>コメント</th>
      </tr>
      <tr>
        <td>3</td>
        <td>5</td>
        <td>馬太郎</td>
        <td>動き軽快</td>
      </tr>
    </table>
  `;
  const result = parsePremiumStableComments(html, {});
  expect(result).toHaveLength(1);
  expect(result[0]?.frameNumber).toBe("3");
  expect(result[0]?.horseNumber).toBe("5");
  expect(result[0]?.horseName).toBe("馬太郎");
  expect(result[0]?.commentText).toBe("動き軽快");
});

it("parsePremiumStableComments detects evaluationGrade from Icon_Mark image classnames", () => {
  const html = `
    <tr class="Comment_Row">
      <td class="Horse_Number">2</td>
      <td class="Comment_Text">コメント本文</td>
      <td class="Evaluation"><img class="Icon_Mark_02"/></td>
    </tr>
  `;
  const result = parsePremiumStableComments(html, {
    PREMIUM_RACE_COMMENT_LABEL_EVALUATION: "Evaluation",
    PREMIUM_RACE_COMMENT_LABEL_HORSE_NUMBER: "Horse_Number",
    PREMIUM_RACE_COMMENT_LABEL_TEXT: "Comment_Text",
    PREMIUM_RACE_COMMENT_ROW_CLASS: "Comment_Row",
  });
  expect(result[0]?.evaluationGrade).toBe(2);
});

it("isPremiumStableCommentHtmlAuthorized returns true only when full-table class present", async () => {
  const { isPremiumStableCommentHtmlAuthorized } = await import("./premium-race");
  expect(
    isPremiumStableCommentHtmlAuthorized('<div class="Comment_Table_Show_All">x</div>'),
  ).toBe(true);
  expect(isPremiumStableCommentHtmlAuthorized("<div></div>")).toBe(false);
});

it("parsePremiumPaddockBulletins assigns favorite/value group based on table heading", () => {
  const html = `
    <h2>本命馬</h2>
    <table class="Paddock_Table">
      <tr><td class="Horse_Num">1</td></tr>
    </table>
    <h2>穴馬</h2>
    <table class="Paddock_Table">
      <tr><td class="Horse_Num">2</td></tr>
    </table>
  `;
  const result = parsePremiumPaddockBulletins(html, {
    PREMIUM_RACE_PADDOCK_GROUP_VALUE_LABEL: "穴馬",
    PREMIUM_RACE_PADDOCK_LABEL_HORSE_NUMBER: "Horse_Num",
    PREMIUM_RACE_PADDOCK_TABLE_CLASS: "Paddock_Table",
  });
  expect(result.bulletins.find((b) => b.horseNumber === "1")?.groupKey).toBe("favorite");
  expect(result.bulletins.find((b) => b.horseNumber === "2")?.groupKey).toBe("value");
});

it("parsePremiumPaddockBulletins skips PaddockDummy and SampleDummy tables", () => {
  const html = `
    <h2>本命</h2>
    <table class="Paddock_Table PaddockDummy">
      <tr><td class="Horse_Num">9</td></tr>
    </table>
    <h2>本命馬</h2>
    <table class="Paddock_Table">
      <tr><td class="Horse_Num">3</td></tr>
    </table>
  `;
  const result = parsePremiumPaddockBulletins(html, {
    PREMIUM_RACE_PADDOCK_LABEL_HORSE_NUMBER: "Horse_Num",
    PREMIUM_RACE_PADDOCK_TABLE_CLASS: "Paddock_Table",
  });
  expect(result.bulletins.map((b) => b.horseNumber)).toStrictEqual(["3"]);
});

it("parsePremiumPaddockBulletins fills commentText/evaluationText/horseName/frameNumber when env labels match", () => {
  const html = `
    <table>
      <tr class="Row">
        <td class="Frame">2</td>
        <td class="Num">7</td>
        <td class="Name">ウマA</td>
        <td class="Eval">A</td>
        <td class="Comment">良いコメント</td>
      </tr>
    </table>
  `;
  const result = parsePremiumPaddockBulletins(html, {
    PREMIUM_RACE_PADDOCK_LABEL_COMMENT: "Comment",
    PREMIUM_RACE_PADDOCK_LABEL_EVALUATION: "Eval",
    PREMIUM_RACE_PADDOCK_LABEL_FRAME: "Frame",
    PREMIUM_RACE_PADDOCK_LABEL_HORSE_NAME: "Name",
    PREMIUM_RACE_PADDOCK_LABEL_HORSE_NUMBER: "Num",
    PREMIUM_RACE_PADDOCK_ROW_CLASS: "Row",
  });
  expect(result.bulletins[0]).toStrictEqual({
    commentText: "良いコメント",
    evaluationText: "A",
    frameNumber: "2",
    groupKey: "favorite",
    horseName: "ウマA",
    horseNumber: "7",
  });
});

it("parsePremiumPaddockBulletins skips rows without a valid horseNumber", () => {
  const html = `
    <table>
      <tr class="Row"><td class="Num">abc</td></tr>
      <tr class="Row"><td class="Num">5</td></tr>
    </table>
  `;
  const result = parsePremiumPaddockBulletins(html, {
    PREMIUM_RACE_PADDOCK_LABEL_HORSE_NUMBER: "Num",
    PREMIUM_RACE_PADDOCK_ROW_CLASS: "Row",
  });
  expect(result.bulletins).toHaveLength(1);
  expect(result.bulletins[0]?.horseNumber).toBe("5");
});

it("parsePremiumDataTopHorses uses env-provided class names when supplied", () => {
  const html = `
    <div class="MyArea">
      <dl>
        <dt><span class="MyNum">7</span></dt>
        <dd>
          <a class="MyLink">テスト馬</a>
          <dd class="MyReasons">
            <ul><li>好調</li></ul>
          </dd>
        </dd>
      </dl>
    </div>
  `;
  const result = parsePremiumDataTopHorses(html, {
    PREMIUM_RACE_DATA_TOP_AREA_CLASS: "MyArea",
    PREMIUM_RACE_DATA_TOP_HORSE_LINK_CLASS: "MyLink",
    PREMIUM_RACE_DATA_TOP_HORSE_NUMBER_CLASS: "MyNum",
    PREMIUM_RACE_DATA_TOP_REASON_LIST_CLASS: "MyReasons",
  });
  expect(result).toHaveLength(1);
  expect(result[0]?.horseNumber).toBe("7");
  expect(result[0]?.horseName).toBe("テスト馬");
});

it("parsePremiumTrainingReviews uses extractRelativeCellText fallback for riderName when class missing", () => {
  const env = {
    PREMIUM_RACE_WORK_DATE_CLASS: "Date",
    PREMIUM_RACE_WORK_HORSE_NUMBER_CLASS: "Horse_Number",
    PREMIUM_RACE_WORK_ROW_CLASS: "Work_Row",
  };
  const html = `
    <tr class="Work_Row">
      <td class="Horse_Number">8</td>
      <td class="Date">2026/05/12</td>
      <td>cell1</td>
      <td>cell2</td>
      <td>調教師Y</td>
    </tr>
  `;
  const result = parsePremiumTrainingReviews(html, env);
  expect(result[0]?.riderName).toBe("調教師Y");
});
