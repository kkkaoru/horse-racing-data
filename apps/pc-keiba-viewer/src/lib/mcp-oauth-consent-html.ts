// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

interface ConsentPageInput {
  clientName: string;
  requestId: string;
  subject: string;
}

const escapeHtml = (value: string): string =>
  value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");

export const renderMcpConsentPage = (input: ConsentPageInput): string => {
  const clientName = escapeHtml(input.clientName);
  const subject = escapeHtml(input.subject);
  const requestId = escapeHtml(input.requestId);
  return `<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>MCP 接続の許可 | PC-KEIBA Viewer</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 0; background: #111; color: #f4f1ea; }
    main { max-width: 40rem; margin: 3rem auto; padding: 0 1.25rem; }
    h1 { font-size: 1.5rem; }
    .card { background: #1c1c1c; border: 1px solid #333; border-radius: 12px; padding: 1.25rem 1.5rem; }
    p { line-height: 1.6; }
    .subject { color: #c8bfa8; }
    form { display: flex; gap: 0.75rem; margin-top: 1.25rem; }
    button { border: 0; border-radius: 8px; padding: 0.7rem 1.1rem; font-size: 1rem; cursor: pointer; }
    .allow { background: #d4a017; color: #111; font-weight: 700; }
    .deny { background: #333; color: #f4f1ea; }
  </style>
</head>
<body>
  <main>
    <p>PC-KEIBA Viewer</p>
    <h1>AI エージェントに MCP を許可しますか？</h1>
    <div class="card">
      <p>アクセス中のユーザー: <span class="subject">${subject}</span></p>
      <p>クライアント <strong>${clientName}</strong> が、このサイトと同じレースデータ（オッズ・傾向・ヒートマップ等）を MCP 経由で読むことを要求しています。</p>
      <p>許可すると、エージェントはアクセストークンを保存し、以降 MCP を利用できます。人間向け Cloudflare Access ログインはそのままです。</p>
      <form method="post" action="/oauth/authorize">
        <input type="hidden" name="request_id" value="${requestId}" />
        <button class="allow" type="submit" name="decision" value="allow">許可する</button>
        <button class="deny" type="submit" name="decision" value="deny">拒否する</button>
      </form>
    </div>
  </main>
</body>
</html>`;
};

export const renderMcpLoginRequiredPage = (): string => `<!DOCTYPE html>
<html lang="ja"><head><meta charset="utf-8" /><title>ログインが必要です</title></head>
<body><p>Cloudflare Access でサイトにログインしてから、もう一度エージェントの接続を開始してください。</p></body></html>`;
