// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

export const CHATGPT_APP_DESCRIPTION_MAX_LENGTH: number = 200;

export const CHATGPT_APP_DESCRIPTION: string =
  "競馬の予測システムに接続するMCP。出馬表・オッズ・傾向・勝率ヒートマップ・着順予測をサイトと同じデータで返す。数値は推測せず必ずツールで取得。失敗時は不明。馬・騎手・馬主・調教師はsearch→fetch。開催はlist_top_races。";

export const chatgptAppDescriptionFitsLimit = (value: string): boolean =>
  value.length <= CHATGPT_APP_DESCRIPTION_MAX_LENGTH;
