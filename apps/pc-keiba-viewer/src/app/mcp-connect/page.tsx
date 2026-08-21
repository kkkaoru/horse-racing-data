import type { Metadata } from "next";
import { headers } from "next/headers";
import Link from "next/link";

import { mcpResourceUrl, originFromForwardedHeaders } from "../../lib/mcp-oauth-origin";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "MCP 接続",
};

export default async function McpConnectPage() {
  const origin = originFromForwardedHeaders(await headers());
  const mcpUrl = origin === null ? "/mcp" : mcpResourceUrl(origin);
  return (
    <section className="page-shell">
      <div className="page-title-row">
        <div>
          <p className="eyebrow">MCP</p>
          <h1>AI エージェント接続</h1>
        </div>
      </div>
      <p>
        このサイトに Cloudflare Access でログインできる人が、AI エージェントへ MCP
        利用を許可できます。許可はエージェントが開く同意画面で行います。
      </p>
      <p>
        MCP エンドポイント: <code>{mcpUrl}</code>
      </p>
      <ol>
        <li>
          エージェントの MCP 設定に上記の <code>/mcp</code> URL を追加します。静的トークンや Access
          サービストークンは不要です。
        </li>
        <li>初回起動時、エージェントがブラウザでこのサイトの許可画面を開きます。</li>
        <li>「許可する」を押すと、エージェントがトークンを保存し MCP が使えます。</li>
      </ol>
      <p>
        人間向けの Access ログインはそのままです。エージェントは OAuth
        アクセストークンだけを保存します。
      </p>
      <p>
        <Link href="/mypage">マイページへ戻る</Link>
      </p>
    </section>
  );
}
