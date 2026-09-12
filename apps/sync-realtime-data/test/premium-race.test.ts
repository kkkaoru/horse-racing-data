// Run with bun via the package Vitest scripts.
import { describe, expect, it } from "vitest";

import {
  buildJraPremiumSourceRaceId,
  buildNarPremiumSourceRaceId,
  buildPremiumRaceLinkFromRace,
  isPremiumStableCommentHtmlAuthorized,
  matchPremiumLinkToRace,
  parsePremiumDataTopHorses,
  parsePremiumPaddockBulletins,
  sourceRaceIdCandidates,
} from "../src/premium-race";

const dataTopEnv = {
  PREMIUM_RACE_DATA_TOP_AREA_CLASS: "DataPickupHorseArea",
  PREMIUM_RACE_DATA_TOP_HORSE_LINK_CLASS: "data_top_horse_link",
  PREMIUM_RACE_DATA_TOP_HORSE_NUMBER_CLASS: "Umaban_Num",
  PREMIUM_RACE_DATA_TOP_REASON_LIST_CLASS: "PickupDataBox",
} as const;

const paddockEnv = {
  PREMIUM_RACE_PADDOCK_GROUP_VALUE_LABEL: "穴馬",
  PREMIUM_RACE_PADDOCK_LABEL_COMMENT: "Comment_Cell",
  PREMIUM_RACE_PADDOCK_LABEL_EVALUATION: "Hyoka",
  PREMIUM_RACE_PADDOCK_LABEL_FRAME: "Waku*",
  PREMIUM_RACE_PADDOCK_LABEL_HORSE_NAME: "Horse_Name",
  PREMIUM_RACE_PADDOCK_LABEL_HORSE_NUMBER: "Waku",
  PREMIUM_RACE_PADDOCK_PENDING_TEXT: "公開予定",
  PREMIUM_RACE_PADDOCK_TABLE_CLASS: "Paddock_Table",
  PREMIUM_RACE_PADDOCK_UNAVAILABLE_TEXT: "公開対象外",
} as const;

describe("premium race parsing", () => {
  it("builds JRA premium source race ids from meeting metadata", () => {
    expect(
      buildJraPremiumSourceRaceId({
        kaisaiKai: "02",
        kaisaiNichime: "09",
        kaisaiNen: "2026",
        keibajoCode: "05",
        raceBango: "02",
        source: "jra",
      }),
    ).toBe("202605020902");
  });

  it("builds NAR premium source race ids from kaisai date metadata", () => {
    expect(
      buildNarPremiumSourceRaceId({
        kaisaiNen: "2025",
        kaisaiTsukihi: "1109",
        keibajoCode: "54",
        raceBango: "06",
        source: "nar",
      }),
    ).toBe("202554110906");
  });

  it("zero-pads NAR raceBango when present without leading zero", () => {
    expect(
      buildNarPremiumSourceRaceId({
        kaisaiNen: "2026",
        kaisaiTsukihi: "0525",
        keibajoCode: "42",
        raceBango: "1",
        source: "nar",
      }),
    ).toBe("202642052501");
  });

  it("rejects Ban-ei keibajo when building NAR premium source race ids", () => {
    expect(
      buildNarPremiumSourceRaceId({
        kaisaiNen: "2026",
        kaisaiTsukihi: "0525",
        keibajoCode: "83",
        raceBango: "07",
        source: "nar",
      }),
    ).toBeNull();
  });

  it("rejects JRA races when building NAR premium source race ids", () => {
    expect(
      buildNarPremiumSourceRaceId({
        kaisaiNen: "2026",
        kaisaiTsukihi: "0525",
        keibajoCode: "05",
        raceBango: "11",
        source: "jra",
      }),
    ).toBeNull();
  });

  it("uses NAR source race id for the fallback premium link", () => {
    const link = buildPremiumRaceLinkFromRace(
      {
        babaCode: "42",
        debaUrl: "",
        kaisaiKai: null,
        kaisaiNen: "2026",
        kaisaiNichime: null,
        kaisaiTsukihi: "0525",
        keibajoCode: "42",
        lastOddsFetchAt: null,
        lastWeightFetchAt: null,
        oddsLinks: {},
        raceBango: "01",
        raceKey: "nar:2026:0525:42:01",
        raceName: null,
        raceStartAtJst: "2026-05-25T14:50:00+09:00",
        source: "nar",
      },
      {
        commentPathTemplate: null,
        cookie: null,
        dataTopPathTemplate: "/race/data_top.html?race_id={sourceRaceId}",
        entryLinkPattern: null,
        narOrigin: "https://nar.netkeiba.com",
        narTopPathTemplate: null,
        origin: "https://race.netkeiba.com",
        paddockFallbackPathTemplate: null,
        paddockPathTemplate: null,
        proxyBearer: null,
        proxyUrl: null,
        proxyUserId: null,
        responseCharset: null,
        sourceIdQueryKey: "race_id",
        topPathTemplate: null,
        workPathTemplate: null,
      },
    );
    expect(link).toStrictEqual({
      entryUrl: "https://nar.netkeiba.com/race/data_top.html?race_id=202642052501",
      sourceRaceId: "202642052501",
    });
  });

  it("includes the NAR source race id in candidate matches", () => {
    expect(
      sourceRaceIdCandidates({
        babaCode: "42",
        debaUrl: "",
        kaisaiKai: null,
        kaisaiNen: "2026",
        kaisaiNichime: null,
        kaisaiTsukihi: "0525",
        keibajoCode: "42",
        lastOddsFetchAt: null,
        lastWeightFetchAt: null,
        oddsLinks: {},
        raceBango: "01",
        raceKey: "nar:2026:0525:42:01",
        raceName: null,
        raceStartAtJst: "2026-05-25T14:50:00+09:00",
        source: "nar",
      }),
    ).toStrictEqual(["202642052501", "20264201", "2026421"]);
  });

  it("parses ranked horses from a self-contained data-top page", () => {
    const html: string =
      '<div class="DataPickupHorseArea">\n  <dl>\n    <dt><span class="Umaban_Num">1</span></dt>\n    <dd>\n      <a class="data_top_horse_link">ヌクレオチド</a>\n      <dd class="PickupDataBox">\n        <ul>\n          <li>このコースが得意な馬</li>\n          <li>このコースに実績がある種牡馬</li>\n          <li>今回の馬場状態が得意な馬</li>\n        </ul>\n      </dd>\n    </dd>\n  </dl>\n  <dl>\n    <dt><span class="Umaban_Num">14</span></dt>\n    <dd>\n      <a class="data_top_horse_link">ウインビギニング</a>\n      <dd class="PickupDataBox">\n        <ul>\n          <li>このコースが得意な騎手</li>\n          <li>今回のレース間隔で実績がある馬</li>\n          <li>今回の馬場状態が得意な馬</li>\n        </ul>\n      </dd>\n    </dd>\n  </dl>\n  <dl>\n    <dt><span class="Umaban_Num">8</span></dt>\n    <dd>\n      <a class="data_top_horse_link">アイデアユー</a>\n      <dd class="PickupDataBox">\n        <ul>\n          <li>このコースが得意な調教師</li>\n          <li>今回の馬場状態が得意な馬</li>\n          <li>このコースで有利な枠順</li>\n        </ul>\n      </dd>\n    </dd>\n  </dl>\n</div>\n';
    const parsed = parsePremiumDataTopHorses(html, dataTopEnv);

    expect(parsed).toStrictEqual([
      {
        horseName: "ヌクレオチド",
        horseNumber: "1",
        rank: 1,
        reasons: [
          "このコースが得意な馬",
          "このコースに実績がある種牡馬",
          "今回の馬場状態が得意な馬",
        ],
      },
      {
        horseName: "ウインビギニング",
        horseNumber: "14",
        rank: 2,
        reasons: [
          "このコースが得意な騎手",
          "今回のレース間隔で実績がある馬",
          "今回の馬場状態が得意な馬",
        ],
      },
      {
        horseName: "アイデアユー",
        horseNumber: "8",
        rank: 3,
        reasons: ["このコースが得意な調教師", "今回の馬場状態が得意な馬", "このコースで有利な枠順"],
      },
    ]);
  });

  it("does not let hidden unavailable text suppress active paddock rows", () => {
    const parsed = parsePremiumPaddockBulletins(
      `
        <div style="display:none">このレースはパドック速報公開対象外です。</div>
        <div class="SubTitle"><h3>人気馬</h3></div>
        <table class="Paddock_Table race_table_01">
          <tbody>
            <tr>
              <td class="Waku3">3</td>
              <td class="Waku">5</td>
              <td class="Horse_Name Txt_L"><a>ジョワイユノエル</a></td>
              <td class="Hyoka text-center"><div><span>A</span></div></td>
              <td class="Comment Txt_L"><p class="Comment_Cell">気配上向き</p></td>
            </tr>
          </tbody>
        </table>
      `,
      paddockEnv,
    );

    expect(parsed).toEqual({
      authRequired: false,
      bulletins: [
        {
          commentText: "気配上向き",
          evaluationText: "A",
          frameNumber: "3",
          groupKey: "favorite",
          horseName: "ジョワイユノエル",
          horseNumber: "5",
        },
      ],
      pending: false,
      unavailable: false,
    });
  });

  it("ignores commented dummy tables and treats registration pages as pending", () => {
    const parsed = parsePremiumPaddockBulletins(
      `
        <!--
        <table class="Paddock_Table SampleDummy">
          <tr>
            <td class="Waku"><span class="PaddockDummy01"></span></td>
            <td class="Horse_Name"><span class="PaddockDummy02"></span></td>
          </tr>
        </table>
        -->
        <div class="Premium_Regist_Box">登録して続きを見る</div>
      `,
      paddockEnv,
    );

    expect(parsed).toEqual({
      authRequired: true,
      bulletins: [],
      pending: true,
      unavailable: false,
    });
  });

  it("does not parse visible dummy tables as published paddock bulletins", () => {
    const parsed = parsePremiumPaddockBulletins(
      `
        <table class="Paddock_Table SampleDummy">
          <tbody>
            <tr>
              <td class="Waku2">2</td>
              <td class="Waku">2</td>
              <td class="Horse_Name">ドンパッショーネ</td>
              <td class="Hyoka">A</td>
              <td class="Comment"><p class="Comment_Cell">dummy comment</p></td>
            </tr>
          </tbody>
        </table>
      `,
      paddockEnv,
    );

    expect(parsed).toEqual({
      authRequired: false,
      bulletins: [],
      pending: true,
      unavailable: false,
    });
  });

  it("prioritizes pending registration pages over unavailable text", () => {
    const parsed = parsePremiumPaddockBulletins(
      `
        <div>このレースはパドック速報公開対象外です。</div>
        <div class="Premium_Regist_Box">登録して続きを見る</div>
      `,
      paddockEnv,
    );

    expect(parsed).toEqual({
      authRequired: true,
      bulletins: [],
      pending: true,
      unavailable: false,
    });
  });

  it("detects login guidance as an authentication problem", () => {
    const parsed = parsePremiumPaddockBulletins(
      `
        <div class="Premium_Regist_Box02">
          <a class="Premium_Regist_Btn">パドック速報を見る</a>
          <p>登録済みの方はこちらからログイン</p>
        </div>
      `,
      paddockEnv,
    );

    expect(parsed).toEqual({
      authRequired: true,
      bulletins: [],
      pending: true,
      unavailable: false,
    });
  });

  it("matches JRA premium links by meeting number and day", () => {
    const link = matchPremiumLinkToRace(
      [
        {
          entryUrl: "https://race.netkeiba.com/race/shutuba.html?race_id=202604010601",
          sourceRaceId: "202604010601",
        },
        {
          entryUrl: "https://race.netkeiba.com/race/shutuba.html?race_id=202605020811",
          sourceRaceId: "202605020811",
        },
      ],
      {
        babaCode: "05",
        debaUrl: "https://www.jra.go.jp/JRADB/accessD.html",
        kaisaiKai: "02",
        kaisaiNen: "2026",
        kaisaiNichime: "08",
        kaisaiTsukihi: "0517",
        keibajoCode: "05",
        lastOddsFetchAt: null,
        lastWeightFetchAt: null,
        oddsLinks: {},
        raceBango: "11",
        raceKey: "jra:2026:0517:05:11",
        raceName: "ヴィクトリアマイル",
        raceStartAtJst: "2026-05-17T15:40:00+09:00",
        source: "jra",
      },
    );

    expect(link?.sourceRaceId).toBe("202605020811");
  });

  it("treats html marked with Comment_Table_Show_All as authorized", () => {
    const html =
      '<table id="All_Comment_Table" class="Stable_Comment Comment_Table_Show_All"><tbody></tbody></table>';
    expect(isPremiumStableCommentHtmlAuthorized(html)).toBe(true);
  });

  it("treats html without the Comment_Table_Show_All marker as unauthorized preview", () => {
    const html =
      '<table id="All_Comment_Table" class="Stable_Comment"><tbody></tbody></table><div class="Premium_Regist_Box"></div>';
    expect(isPremiumStableCommentHtmlAuthorized(html)).toBe(false);
  });

  it("recognises an authorized table even when a registration prompt is also present", () => {
    const html: string =
      '<div class="Premium_Regist_Box"></div><table id="All_Comment_Table" class="Stable_Comment Comment_Table_Show_All"><tbody></tbody></table>';
    expect(isPremiumStableCommentHtmlAuthorized(html)).toBe(true);
  });
});
