// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import { renderMcpConsentPage, renderMcpLoginRequiredPage } from "./mcp-oauth-consent-html";

it("escapes client names on the consent page", () => {
  const html = renderMcpConsentPage({
    clientName: "<script>x</script>",
    requestId: "rid",
    subject: "user@example.test",
  });
  expect(html.indexOf("<script>x</script>")).toBe(-1);
  expect(html.indexOf("&lt;script&gt;x&lt;/script&gt;") === -1).toBe(false);
});

it("escapes ampersands, quotes, and greater-than in consent fields", () => {
  const html = renderMcpConsentPage({
    clientName: 'A&B "C"',
    requestId: 'id>"',
    subject: "a<b>",
  });
  expect(html.indexOf("A&B")).toBe(-1);
  expect(html.indexOf("A&amp;B &quot;C&quot;") === -1).toBe(false);
  expect(html.indexOf('value="id&gt;&quot;"') === -1).toBe(false);
  expect(html.indexOf("a&lt;b&gt;") === -1).toBe(false);
});

it("renders a login-required page", () => {
  expect(renderMcpLoginRequiredPage().indexOf("Cloudflare Access") === -1).toBe(false);
});
