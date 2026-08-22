// bun で実行する (bunx vitest)
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";

import { McpExternalLink } from "./mcp-external-link";

afterEach(() => {
  cleanup();
});

it("opens ChatGPT in a new tab", () => {
  render(<McpExternalLink href="https://chatgpt.com" label="ChatGPT を開く（別タブ）" />);
  const link = screen.getByRole("link", { name: "ChatGPT を開く（別タブ）" });
  expect(link.getAttribute("href")).toBe("https://chatgpt.com");
  expect(link.getAttribute("target")).toBe("_blank");
  expect(link.getAttribute("rel")).toBe("noopener noreferrer");
});

it("opens the ChatGPT app creation page in a new tab", () => {
  render(
    <McpExternalLink href="https://chatgpt.com/plugins" label="アプリ作成ページを開く（別タブ）" />,
  );
  const link = screen.getByRole("link", { name: "アプリ作成ページを開く（別タブ）" });
  expect(link.getAttribute("href")).toBe("https://chatgpt.com/plugins");
  expect(link.getAttribute("target")).toBe("_blank");
  expect(link.getAttribute("rel")).toBe("noopener noreferrer");
});
