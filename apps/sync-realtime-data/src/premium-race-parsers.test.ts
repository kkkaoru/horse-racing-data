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
  parseNetkeibaTrainingWorkouts,
  mergeNetkeibaTrainingWorkouts,
  parsePremiumTrainingReviews,
  sourceRaceIdCandidates,
} from "./premium-race";

it("parseNetkeibaTrainingWorkouts parses a same-row workout with its evaluation mark", () => {
  const result = parseNetkeibaTrainingWorkouts(
    `
      <tr class="OikiriDataHead1 HorseList">
        <td class="Umaban">3</td>
        <td class="Horse_Name">テストホース</td>
        <td class="Date">8/20</td>
        <td class="TrainingType">ウッド</td>
        <td class="Course">札幌ダート 良</td>
        <td class="Time6F">82.4</td><td class="Lap6F">15.2</td>
        <td class="Time5F">67.2</td><td class="Lap5F">14.1</td>
        <td class="Time4F">53.1</td><td class="Lap4F">13.8</td>
        <td class="Time3F">39.3</td><td class="Lap3F">13.2</td>
        <td class="Time2F">26.1</td><td class="Lap2F">12.9</td>
        <td class="Lap1F">13.2</td>
        <td class="Training_Critic">動き上々</td><td class="Rank_動き上々">B</td>
      </tr>
    `,
    "20260822",
  );

  expect(result).toHaveLength(1);
  expect(result[0]).toMatchObject({
    course: "札幌ダート 良",
    evaluationGrade: "B",
    evaluationText: "動き上々",
    horseName: "テストホース",
    horseNumber: "3",
    lapTime1f: "132",
    timeGokei6f: "0824",
    trainingDate: "20260820",
    trainingType: "ウッド",
    workoutIndex: 1,
  });
});

it("parseNetkeibaTrainingWorkouts inherits horse and mark across multiple detail rows", () => {
  const result = parseNetkeibaTrainingWorkouts(
    `
      <tr class="OikiriDataHead1 HorseList">
        <td class="Umaban">7</td><td class="Horse_Name">継承馬</td>
        <td class="Training_Critic">絶好調</td><td class="Rank_絶好調">A</td>
      </tr>
      <tr class="OikiriData1">
        <td class="Training_Day">12/30</td><td class="Training_Place">美浦W</td>
        <td class="TrainingTimeData"><ul class="TrainingTimeDataList">
          <li>55.0<span class="RapTime">14.0</span></li>
          <li>41.0<span class="RapTime">13.5</span></li>
          <li>27.5<span class="RapTime">13.0</span></li>
          <li>14.5<span class="RapTime">14.5</span></li>
        </ul></td>
      </tr>
      <tr class="OikiriData2">
        <td class="Training_Day">1/2</td><td class="Training_Place">美浦坂路</td>
        <td class="Time4F">54.0</td><td class="Lap1F">12.0</td>
      </tr>
    `,
    "20260104",
  );

  expect(result).toHaveLength(2);
  expect(result[0]).toMatchObject({
    evaluationGrade: "A",
    horseNumber: "7",
    lapTime1f: "145",
    timeGokei4f: "0550",
    trainingDate: "20251230",
    workoutIndex: 1,
  });
  expect(result[1]).toMatchObject({
    evaluationGrade: "A",
    lapTime1f: "120",
    timeGokei4f: "0540",
    trainingDate: "20260102",
    workoutIndex: 2,
  });
});

it("mergeNetkeibaTrainingWorkouts deduplicates overlapping final and intermediate pages", () => {
  const finalPage: ReturnType<typeof parseNetkeibaTrainingWorkouts> = [
    {
      commentText: null,
      course: "札幌ダート",
      courseDirection: null,
      evaluationGrade: "A",
      evaluationText: "最終",
      horseName: "テストホース",
      horseNumber: "3",
      lapTime10f: null,
      lapTime1f: "132",
      lapTime2f: null,
      lapTime3f: null,
      lapTime4f: null,
      lapTime5f: null,
      lapTime6f: null,
      lapTime7f: null,
      lapTime8f: null,
      lapTime9f: null,
      riderName: null,
      timeGokei10f: null,
      timeGokei2f: null,
      timeGokei3f: null,
      timeGokei4f: "0531",
      timeGokei5f: null,
      timeGokei6f: null,
      timeGokei7f: null,
      timeGokei8f: null,
      timeGokei9f: null,
      tracenKubun: null,
      trainingDate: "20260822",
      trainingTime: "",
      trainingType: "ウッド",
      workoutIndex: 1,
    },
  ];
  const intermediatePage = [
    finalPage[0]!,
    {
      ...finalPage[0]!,
      evaluationText: "中間",
      timeGokei4f: "0540",
      trainingDate: "20260820",
      workoutIndex: 1,
    },
  ];
  const merged = mergeNetkeibaTrainingWorkouts([finalPage, intermediatePage]);
  expect(merged).toHaveLength(2);
  expect(merged[0]).toMatchObject({
    horseNumber: "3",
    trainingDate: "20260820",
    workoutIndex: 1,
  });
  expect(merged[1]).toMatchObject({
    horseNumber: "3",
    trainingDate: "20260822",
    workoutIndex: 2,
  });
});

it("parsePremiumTrainingReviews reads public netkeiba oikiri classes without selector configuration", () => {
  expect(
    parsePremiumTrainingReviews(
      `
        <tr class="OikiriDataHead1 HorseList">
          <td class="Umaban">3</td>
          <td class="Horse_Name">テストホース</td>
          <td class="Training_Critic">動き上々</td>
          <td class="Rank_動き上々">B</td>
        </tr>
      `,
      {},
    ),
  ).toStrictEqual([
    {
      commentText: null,
      evaluationGrade: "B",
      evaluationText: "動き上々",
      horseName: "テストホース",
      horseNumber: "3",
      riderName: null,
      trainingDate: "",
    },
  ]);
});

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
    matchPremiumLinkToRace([{ entryUrl: "u", sourceRaceId: "1234567890" }], {
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
    }),
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

it("parsePremiumDataTopHorses sets horseName=null when anchor is missing", () => {
  const html = `
    <div class="DataPickupHorseArea">
      <dl>
        <dt><span class="Umaban_Num">4</span></dt>
        <dd>
          <dd class="PickupDataBox">
            <ul><li>反応良し</li></ul>
          </dd>
        </dd>
      </dl>
    </div>
  `;
  const result = parsePremiumDataTopHorses(html, {});
  expect(result[0]?.horseName).toBeNull();
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
  const result = parsePremiumPaddockBulletins("<div>サービス停止中</div>", {
    PREMIUM_RACE_PADDOCK_UNAVAILABLE_TEXT: "サービス停止中",
  });
  expect(result.unavailable).toBe(true);
});

it("parsePremiumPaddockBulletins detects pendingText marker", () => {
  const result = parsePremiumPaddockBulletins("<div>準備中です</div>", {
    PREMIUM_RACE_PADDOCK_PENDING_TEXT: "準備中",
  });
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

it("parsePremiumPaddockBulletins extracts current table rows without env selectors", () => {
  const html = `
    <h3>人気馬</h3>
    <table class="Paddock_Table race_table_01">
      <thead>
        <tr><th>枠</th><th>馬番</th><th>馬名</th><th>評価</th><th>コメント</th></tr>
      </thead>
      <tbody>
        <tr>
          <td nowrap="" class="Waku4">4</td>
          <td nowrap="" class="Waku">4</td>
          <td class="Horse_Name Txt_L"><a href="/horse/1">カラペルソナ</a></td>
          <td class="Hyoka text-center"><div class="PaddockRank"><span class="Rank_A">A</span></div></td>
          <td class="Comment Txt_L"><p class="Comment_Cell">いい体つきで仕上がっている。</p></td>
        </tr>
        <tr>
          <td nowrap="" class="Waku5">5</td>
          <td nowrap="" class="Waku">5</td>
          <td class="Horse_Name Txt_L"><a href="/horse/2">エイシンビーコン</a></td>
          <td class="Hyoka text-center"><div class="PaddockRank"><span class="Rank_B">B</span></div></td>
          <td class="Comment Txt_L"><p class="Comment_Cell">毛ヅヤ良く歩様もいい。</p></td>
        </tr>
      </tbody>
    </table>
  `;
  const result = parsePremiumPaddockBulletins(html, {});
  expect(result).toMatchObject({ authRequired: false, pending: false, unavailable: false });
  expect(result.bulletins).toStrictEqual([
    {
      commentText: "いい体つきで仕上がっている。",
      evaluationText: "A",
      frameNumber: "4",
      groupKey: "favorite",
      horseName: "カラペルソナ",
      horseNumber: "4",
    },
    {
      commentText: "毛ヅヤ良く歩様もいい。",
      evaluationText: "B",
      frameNumber: "5",
      groupKey: "favorite",
      horseName: "エイシンビーコン",
      horseNumber: "5",
    },
  ]);
});

it("parsePremiumPaddockBulletins falls back to Paddock_Table when configured table class is stale", () => {
  const html = `
    <h3>人気馬</h3>
    <table class="Paddock_Table race_table_01">
      <tbody>
        <tr>
          <td class="Waku4">4</td>
          <td class="Waku">4</td>
          <td class="Horse_Name Txt_L">カラペルソナ</td>
          <td class="Hyoka"><span class="Rank_A">A</span></td>
          <td class="Comment"><p class="Comment_Cell">いい体つき</p></td>
        </tr>
      </tbody>
    </table>
  `;
  const result = parsePremiumPaddockBulletins(html, {
    PREMIUM_RACE_PADDOCK_TABLE_CLASS: "Old_Paddock_Table",
  });
  expect(result.bulletins).toHaveLength(1);
  expect(result.bulletins[0]).toMatchObject({
    evaluationText: "A",
    horseName: "カラペルソナ",
    horseNumber: "4",
  });
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
  expect(isPremiumStableCommentHtmlAuthorized('<div class="Comment_Table_Show_All">x</div>')).toBe(
    true,
  );
  expect(isPremiumStableCommentHtmlAuthorized("<div></div>")).toBe(false);
});

it("isPremiumDataTopHtmlAuthorized returns true when Icon_Account is present without teaser markers", async () => {
  const { isPremiumDataTopHtmlAuthorized } = await import("./premium-race");
  expect(isPremiumDataTopHtmlAuthorized('<div class="Icon_Account">user</div>')).toBe(true);
});

it("isPremiumDataTopHtmlAuthorized returns false when Icon_Account is absent", async () => {
  const { isPremiumDataTopHtmlAuthorized } = await import("./premium-race");
  expect(isPremiumDataTopHtmlAuthorized("<div>no account marker</div>")).toBe(false);
});

it("isPremiumDataTopHtmlAuthorized returns false on the unauthenticated teaser page", async () => {
  const { isPremiumDataTopHtmlAuthorized } = await import("./premium-race");
  const html = '<div class="DummyBox"></div><div class="Premium_Regist_Box"></div>';
  expect(isPremiumDataTopHtmlAuthorized(html)).toBe(false);
});

it("isPremiumDataTopHtmlAuthorized returns false when Icon_Account co-occurs with DummyBox", async () => {
  const { isPremiumDataTopHtmlAuthorized } = await import("./premium-race");
  const html = '<div class="Icon_Account">user</div><div class="DummyBox"></div>';
  expect(isPremiumDataTopHtmlAuthorized(html)).toBe(false);
});

it("isPremiumDataTopHtmlAuthorized returns false when Icon_Account co-occurs with Premium_Regist_Box", async () => {
  const { isPremiumDataTopHtmlAuthorized } = await import("./premium-race");
  const html = '<div class="Icon_Account">user</div><div class="Premium_Regist_Box"></div>';
  expect(isPremiumDataTopHtmlAuthorized(html)).toBe(false);
});

it("isPremiumDataTopHtmlAuthorized returns false on an empty body", async () => {
  const { isPremiumDataTopHtmlAuthorized } = await import("./premium-race");
  expect(isPremiumDataTopHtmlAuthorized("")).toBe(false);
});

it("detectPremiumLoginPrompt fires when both subscription-gate markers appear", async () => {
  const { detectPremiumLoginPrompt } = await import("./premium-race");
  const html = "<div>プレミアムサービス 登録でご覧になれます</div>";
  expect(detectPremiumLoginPrompt(html)).toBe(true);
});

it("detectPremiumLoginPrompt returns false on a fully authenticated detail page", async () => {
  const { detectPremiumLoginPrompt } = await import("./premium-race");
  const html =
    '<div class="Icon_Account">user</div><div>プレミアムサービス 登録でご覧になれます</div>';
  expect(detectPremiumLoginPrompt(html)).toBe(false);
});

it("detectPremiumLoginPrompt returns false when only the primary marker appears", async () => {
  const { detectPremiumLoginPrompt } = await import("./premium-race");
  expect(detectPremiumLoginPrompt("<div>プレミアムサービス案内ページ</div>")).toBe(false);
});

it("detectPremiumLoginPrompt returns false on an empty body", async () => {
  const { detectPremiumLoginPrompt } = await import("./premium-race");
  expect(detectPremiumLoginPrompt("")).toBe(false);
});

it("parsePremiumStateMessage returns zero count when message is null", async () => {
  const { parsePremiumStateMessage } = await import("./premium-race");
  expect(parsePremiumStateMessage(null)).toStrictEqual({ authRetryCount: 0 });
});

it("parsePremiumStateMessage returns zero count when JSON is malformed", async () => {
  const { parsePremiumStateMessage } = await import("./premium-race");
  expect(parsePremiumStateMessage("not-json")).toStrictEqual({ authRetryCount: 0 });
});

it("parsePremiumStateMessage returns zero count when JSON shape lacks authRetryCount", async () => {
  const { parsePremiumStateMessage } = await import("./premium-race");
  expect(parsePremiumStateMessage('{"other":"value"}')).toStrictEqual({ authRetryCount: 0 });
});

it("parsePremiumStateMessage returns zero count when authRetryCount is non-numeric", async () => {
  const { parsePremiumStateMessage } = await import("./premium-race");
  expect(parsePremiumStateMessage('{"authRetryCount":"3"}')).toStrictEqual({ authRetryCount: 0 });
});

it("parsePremiumStateMessage returns parsed authRetryCount when shape is valid", async () => {
  const { parsePremiumStateMessage } = await import("./premium-race");
  expect(parsePremiumStateMessage('{"authRetryCount":4}')).toStrictEqual({ authRetryCount: 4 });
});

it("parsePremiumStateMessage returns zero when JSON parses to a non-object primitive", async () => {
  const { parsePremiumStateMessage } = await import("./premium-race");
  expect(parsePremiumStateMessage("42")).toStrictEqual({ authRetryCount: 0 });
});

it("parsePremiumStateMessage returns zero when authRetryCount is NaN", async () => {
  const { parsePremiumStateMessage } = await import("./premium-race");
  expect(parsePremiumStateMessage('{"authRetryCount":null}')).toStrictEqual({ authRetryCount: 0 });
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

it("parsePremiumPaddockBulletins drops rows whose horseNumber parses to zero or negative", () => {
  const html = `
    <table>
      <tr class="Row"><td class="Num">0</td></tr>
      <tr class="Row"><td class="Num">7</td></tr>
    </table>
  `;
  const result = parsePremiumPaddockBulletins(html, {
    PREMIUM_RACE_PADDOCK_LABEL_HORSE_NUMBER: "Num",
    PREMIUM_RACE_PADDOCK_ROW_CLASS: "Row",
  });
  expect(result.bulletins.map((b) => b.horseNumber)).toStrictEqual(["7"]);
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

it("parsePremiumStableComments resolves rows when class label ends with wildcard prefix marker", () => {
  const html = `
    <tr class="Comment_Row">
      <td class="Horse_Number_main">3</td>
      <td class="Comment_Text">テキスト</td>
    </tr>
  `;
  const result = parsePremiumStableComments(html, {
    PREMIUM_RACE_COMMENT_LABEL_HORSE_NUMBER: "Horse_Number_*",
    PREMIUM_RACE_COMMENT_LABEL_TEXT: "Comment_Text",
    PREMIUM_RACE_COMMENT_ROW_CLASS: "Comment_Row",
  });
  expect(result).toHaveLength(1);
  expect(result[0]?.horseNumber).toBe("3");
});

it("parsePremiumTrainingReviews findCellIndexByClass returns -1 when class ends with wildcard prefix", () => {
  const env = {
    PREMIUM_RACE_WORK_DATE_CLASS: "Work_Date_*",
    PREMIUM_RACE_WORK_HORSE_NUMBER_CLASS: "Horse_Number",
    PREMIUM_RACE_WORK_ROW_CLASS: "Work_Row",
  };
  const html = `
    <tr class="Work_Row">
      <td class="Horse_Number">11</td>
      <td class="Work_Date_jra">2026/05/12</td>
      <td>a</td>
      <td>b</td>
      <td>調教師R</td>
    </tr>
  `;
  const result = parsePremiumTrainingReviews(html, env);
  expect(result[0]?.riderName).toBe("調教師R");
});

it("parsePremiumTrainingReviews extractRelativeCellText returns empty when anchor class is undefined", () => {
  const env = {
    PREMIUM_RACE_WORK_HORSE_NUMBER_CLASS: "Horse_Number",
    PREMIUM_RACE_WORK_ROW_CLASS: "Work_Row",
    PREMIUM_RACE_WORK_TEXT_CLASS: "Text",
  };
  const html = `
    <tr class="Work_Row">
      <td class="Horse_Number">9</td>
      <td class="Text">評価本文</td>
      <td>x</td>
    </tr>
  `;
  const result = parsePremiumTrainingReviews(html, env);
  expect(result).toHaveLength(1);
  expect(result[0]?.riderName).toBeNull();
});

it("parsePremiumStableComments class-based path skips rows missing horseNumber or commentText", () => {
  const html = `
    <tr class="Comment_Row">
      <td class="Horse_Number"></td>
      <td class="Comment_Text">本文無視されない</td>
    </tr>
    <tr class="Comment_Row">
      <td class="Horse_Number">7</td>
      <td class="Comment_Text"></td>
    </tr>
    <tr class="Comment_Row">
      <td class="Horse_Number">8</td>
      <td class="Comment_Text">良い動き</td>
    </tr>
  `;
  const result = parsePremiumStableComments(html, {
    PREMIUM_RACE_COMMENT_LABEL_HORSE_NUMBER: "Horse_Number",
    PREMIUM_RACE_COMMENT_LABEL_TEXT: "Comment_Text",
    PREMIUM_RACE_COMMENT_ROW_CLASS: "Comment_Row",
  });
  expect(result).toHaveLength(1);
  expect(result[0]?.horseNumber).toBe("8");
});

it("parsePremiumStableComments raw-cell uses nonEmptyTextCells[3] when textCells[3] and [4] are both empty", () => {
  const html = `
    <table>
      <tr><td>1</td><td>2</td><td>馬名X</td><td></td><td></td><td>素晴らしい仕上がり</td></tr>
    </table>
  `;
  const result = parsePremiumStableComments(html, {});
  expect(result).toHaveLength(1);
  expect(result[0]?.horseNumber).toBe("2");
  expect(result[0]?.commentText).toBe("素晴らしい仕上がり");
});

it("parsePremiumStableComments raw-cell horseName falls through to null when name slots are empty", () => {
  const html = `
    <table>
      <tr><td></td><td></td><td></td><td>面白いコメント</td><td>4</td></tr>
    </table>
  `;
  const result = parsePremiumStableComments(html, {});
  expect(result).toHaveLength(1);
  expect(result[0]?.horseName).toBeNull();
});

it("parsePremiumStableComments raw-cell fallback uses textCells[4] when textCells[3] is empty", () => {
  const html = `
    <table>
      <tr>
        <th>枠</th><th>馬番</th><th>馬名</th><th>備考</th><th>コメント</th>
      </tr>
      <tr>
        <td>2</td><td>4</td><td>馬丙</td><td></td><td>差し脚鋭く</td>
      </tr>
    </table>
  `;
  const result = parsePremiumStableComments(html, {});
  expect(result).toHaveLength(1);
  expect(result[0]?.commentText).toBe("差し脚鋭く");
});

it("parsePremiumStableComments raw-cell evaluationGrade picks Icon_Mark in raw cells", () => {
  const html = `
    <table>
      <tr><th>枠</th><th>馬番</th><th>馬名</th><th>コメント</th></tr>
      <tr><td>1</td><td>3</td><td>馬乙</td><td><img class="Icon_Mark_03"/>動き上々</td></tr>
    </table>
  `;
  const result = parsePremiumStableComments(html, {});
  expect(result[0]?.evaluationGrade).toBe(3);
});

it("parsePremiumStableComments raw-cell frameNumber and horseName fall back to nonEmptyTextCells", () => {
  const html = `
    <table>
      <tr><th>枠</th><th>馬番</th><th>馬名</th><th>コメント</th></tr>
      <tr><td></td><td></td><td></td><td>抜群の仕上がり</td><td>2</td><td>9</td><td>馬己</td></tr>
    </table>
  `;
  const result = parsePremiumStableComments(html, {});
  expect(result).toHaveLength(1);
  expect(result[0]?.horseNumber).toBe("2");
  expect(result[0]?.horseName).toBe("9");
});

it("parsePremiumDataTopHorses survives entries where PickupDataBox dd is absent", () => {
  const html = `
    <div class="DataPickupHorseArea">
      <dl>
        <dt><span class="Umaban_Num">6</span></dt>
        <dd><a class="data_top_horse_link">馬庚</a></dd>
      </dl>
    </div>
  `;
  expect(parsePremiumDataTopHorses(html, {})).toStrictEqual([]);
});

it("parsePremiumPaddockBulletins detects pendingText pure pending without authRequired", () => {
  const result = parsePremiumPaddockBulletins("<div>準備中の本日</div>", {
    PREMIUM_RACE_PADDOCK_PENDING_TEXT: "準備中",
  });
  expect(result.pending).toBe(true);
  expect(result.authRequired).toBe(false);
});

it("buildPremiumRaceLinkFromRace uses dataTopPathTemplate when supplied", () => {
  const config = getPremiumRaceConfig({
    PREMIUM_RACE_DATA_TOP_PATH_TEMPLATE: "/data_top.html?race_id={sourceRaceId}",
    PREMIUM_RACE_ORIGIN: "https://race.example",
  });
  const link = buildPremiumRaceLinkFromRace(
    {
      babaCode: "tokyo",
      debaUrl: "https://race.example/",
      kaisaiKai: "03",
      kaisaiNen: "2025",
      kaisaiNichime: "07",
      kaisaiTsukihi: "0510",
      keibajoCode: "05",
      lastOddsFetchAt: null,
      lastWeightFetchAt: null,
      oddsLinks: {},
      raceBango: "11",
      raceKey: "rk1",
      raceName: null,
      raceStartAtJst: "2025-05-10T10:00:00+09:00",
      source: "jra",
    },
    config,
  );
  expect(link?.entryUrl).toBe("https://race.example/data_top.html?race_id=202505030711");
});

it("buildPremiumRaceLinkFromRace falls back to workPathTemplate when only that one is set", () => {
  const config = getPremiumRaceConfig({
    PREMIUM_RACE_ORIGIN: "https://race.example",
    PREMIUM_RACE_WORK_PATH_TEMPLATE: "/work.html?race_id={sourceRaceId}",
  });
  const link = buildPremiumRaceLinkFromRace(
    {
      babaCode: "tokyo",
      debaUrl: "https://race.example/",
      kaisaiKai: "01",
      kaisaiNen: "2025",
      kaisaiNichime: "02",
      kaisaiTsukihi: "0102",
      keibajoCode: "06",
      lastOddsFetchAt: null,
      lastWeightFetchAt: null,
      oddsLinks: {},
      raceBango: "03",
      raceKey: "rk2",
      raceName: null,
      raceStartAtJst: "2025-01-02T10:00:00+09:00",
      source: "jra",
    },
    config,
  );
  expect(link?.entryUrl).toBe("https://race.example/work.html?race_id=202506010203");
});

it("matchPremiumLinkToRace matches via candidate suffix when first list lookup hits", () => {
  const result = matchPremiumLinkToRace(
    [{ entryUrl: "https://x/?race_id=202506010203", sourceRaceId: "202506010203" }],
    {
      babaCode: "ban",
      debaUrl: "https://x/",
      kaisaiKai: "01",
      kaisaiNen: "2025",
      kaisaiNichime: "02",
      kaisaiTsukihi: "0102",
      keibajoCode: "06",
      lastOddsFetchAt: null,
      lastWeightFetchAt: null,
      oddsLinks: {},
      raceBango: "03",
      raceKey: "rk",
      raceName: null,
      raceStartAtJst: "2025-01-02T10:00:00+09:00",
      source: "jra",
    },
  );
  expect(result?.sourceRaceId).toBe("202506010203");
});
