// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import { CHATGPT_APP_DESCRIPTION, chatgptAppDescriptionFitsLimit } from "./mcp-chatgpt-app";

it("describes the MCP as a connection to the horse-racing prediction system", () => {
  expect(CHATGPT_APP_DESCRIPTION).toBe(
    "競馬の予測システムに接続するMCP。出馬表・オッズ・傾向・勝率ヒートマップ・着順予測をサイトと同じデータで返す。数値は推測せず必ずツールで取得。失敗時は不明。馬・騎手・馬主・調教師はsearch→fetch。開催はlist_top_races。",
  );
});

it("keeps the ChatGPT description within the 200 character field limit", () => {
  expect(
    chatgptAppDescriptionFitsLimit(
      "競馬の予測システムに接続するMCP。出馬表・オッズ・傾向・勝率ヒートマップ・着順予測をサイトと同じデータで返す。数値は推測せず必ずツールで取得。失敗時は不明。馬・騎手・馬主・調教師はsearch→fetch。開催はlist_top_races。",
    ),
  ).toBe(true);
});

it("accepts a ChatGPT description of exactly 200 characters", () => {
  expect(
    chatgptAppDescriptionFitsLimit(
      "01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",
    ),
  ).toBe(true);
});

it("rejects a ChatGPT description longer than 200 characters", () => {
  expect(
    chatgptAppDescriptionFitsLimit(
      "012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890",
    ),
  ).toBe(false);
});
