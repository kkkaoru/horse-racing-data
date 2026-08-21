import type { Metadata } from "next";
import { headers } from "next/headers";
import Image from "next/image";
import Link from "next/link";

import chatgptAllow from "../../../public/mcp-connect/chatgpt-allow.jpg";
import chatgptCreateApp from "../../../public/mcp-connect/chatgpt-create-app.jpg";
import chatgptDeveloperMode from "../../../public/mcp-connect/chatgpt-developer-mode.jpg";
import { mcpResourceUrl, originFromForwardedHeaders } from "../../lib/mcp-oauth-origin";
import { McpUrlCopy } from "./mcp-url-copy";

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

      <div className="mcp-connect-intro">
        <p>
          このサイトに Cloudflare Access でログインできる人が、AI エージェントへ MCP
          利用を許可できます。許可はエージェントが開く同意画面で行います。
        </p>
        <p>MCP エンドポイント（ChatGPT のカスタム MCP URL にもこれを使います）</p>
        <McpUrlCopy mcpUrl={mcpUrl} />
      </div>

      <article className="mcp-connect-card">
        <h2>ChatGPT（ブラウザ）</h2>
        <p>
          ChatGPT のストアからこのリポジトリの Agent Plugin を入れることはできません。ブラウザの
          ChatGPT で <strong>カスタム MCP アプリ</strong> として上記 URL を追加します。公式には MCP
          アプリの追加は web only です。iPhone / Android の ChatGPT アプリから新規追加はできません。
        </p>
        <ol>
          <li>
            有料プランで <a href="https://chatgpt.com">chatgpt.com</a> を開きます。
          </li>
          <li>
            <strong>Settings → Security and login</strong>（または Apps → Advanced）で
            <strong>Developer mode</strong> をオンにします。
          </li>
        </ol>
        <figure className="mcp-connect-figure">
          <Image
            alt="ChatGPT の Settings で Developer mode をオンにする手順の見本"
            src={chatgptDeveloperMode}
          />
          <figcaption>
            Developer mode をオンにする（見本。実際のメニュー名は ChatGPT 側の表記に従ってください）
          </figcaption>
        </figure>
        <ol start={3}>
          <li>
            <a href="https://chatgpt.com/plugins">chatgpt.com/plugins</a> の <strong>+</strong>{" "}
            からアプリを作成します。
          </li>
          <li>
            MCP server URL に次の URL をそのまま貼り、認証は <strong>OAuth</strong> にします。
            <McpUrlCopy mcpUrl={mcpUrl} />
          </li>
        </ol>
        <figure className="mcp-connect-figure">
          <Image alt="ChatGPT の Create app に MCP URL を貼る手順の見本" src={chatgptCreateApp} />
          <figcaption>Create app に MCP URL を貼り、OAuth を選びます</figcaption>
        </figure>
        <ol start={5}>
          <li>
            ChatGPT がこのサイトの許可画面を開きます。Cloudflare Access でログインし、
            <strong>許可する</strong> を押します。
          </li>
          <li>
            新しいチャットで <strong>+ → More</strong> からこのアプリを選びます。
          </li>
        </ol>
        <figure className="mcp-connect-figure">
          <Image alt="PC-KEIBA Viewer の MCP 許可画面で許可するを押す見本" src={chatgptAllow} />
          <figcaption>許可すると ChatGPT がトークンを保存し、MCP が使えます</figcaption>
        </figure>
      </article>

      <article className="mcp-connect-card">
        <h2>その他のエージェント</h2>
        <ol>
          <li>
            MCP 設定に次の URL を追加します。静的トークンや Access サービストークンは不要です。
            <McpUrlCopy mcpUrl={mcpUrl} />
          </li>
          <li>初回起動時、エージェントがブラウザでこのサイトの許可画面を開きます。</li>
          <li>「許可する」を押すと、エージェントがトークンを保存し MCP が使えます。</li>
        </ol>
        <p>
          人間向けの Access ログインはそのままです。エージェントは OAuth
          アクセストークンだけを保存します。
        </p>
      </article>

      <p>
        <Link className="mcp-connect-back" href="/mypage">
          マイページへ戻る
        </Link>
      </p>
    </section>
  );
}
