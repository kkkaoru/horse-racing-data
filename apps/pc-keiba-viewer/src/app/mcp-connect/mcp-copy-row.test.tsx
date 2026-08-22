// bun で実行する (bunx vitest)
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";

import { McpCopyRow } from "./mcp-copy-row";

const writeDescription = async (value: string): Promise<void> => {
  expect(value).toBe(
    "競馬の予測システムに接続するMCP。出馬表・オッズ・傾向・勝率ヒートマップ・着順予測をサイトと同じデータで返す。数値は推測せず必ずツールで取得。失敗時は不明。馬・騎手・馬主・調教師はsearch→fetch。開催はlist_top_races。",
  );
};

afterEach(() => {
  cleanup();
});

it("copies the ChatGPT description field in one click", async () => {
  Object.defineProperty(navigator, "clipboard", {
    configurable: true,
    value: { writeText: writeDescription },
  });
  render(
    <McpCopyRow value="競馬の予測システムに接続するMCP。出馬表・オッズ・傾向・勝率ヒートマップ・着順予測をサイトと同じデータで返す。数値は推測せず必ずツールで取得。失敗時は不明。馬・騎手・馬主・調教師はsearch→fetch。開催はlist_top_races。" />,
  );
  fireEvent.click(screen.getByRole("button", { name: "コピー" }));
  await waitFor(() => {
    expect(screen.getByRole("button", { name: "コピー済み" })).toBeTruthy();
  });
});

it("shows the ChatGPT description text", () => {
  render(
    <McpCopyRow value="競馬の予測システムに接続するMCP。出馬表・オッズ・傾向・勝率ヒートマップ・着順予測をサイトと同じデータで返す。数値は推測せず必ずツールで取得。失敗時は不明。馬・騎手・馬主・調教師はsearch→fetch。開催はlist_top_races。" />,
  );
  expect(
    screen.getByText(
      "競馬の予測システムに接続するMCP。出馬表・オッズ・傾向・勝率ヒートマップ・着順予測をサイトと同じデータで返す。数値は推測せず必ずツールで取得。失敗時は不明。馬・騎手・馬主・調教師はsearch→fetch。開催はlist_top_races。",
    ),
  ).toBeTruthy();
});
