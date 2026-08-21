// bun で実行する (bunx vitest)
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";

import { McpUrlCopy } from "./mcp-url-copy";

const writeText = async (value: string): Promise<void> => {
  expect(value).toBe("https://viewer.example.test/mcp");
};

afterEach(() => {
  cleanup();
});

it("copies the MCP URL and shows copied state", async () => {
  Object.defineProperty(navigator, "clipboard", {
    configurable: true,
    value: { writeText },
  });
  render(<McpUrlCopy mcpUrl="https://viewer.example.test/mcp" />);
  fireEvent.click(screen.getByRole("button", { name: "コピー" }));
  await waitFor(() => {
    expect(screen.getByRole("button", { name: "コピー済み" })).toBeTruthy();
  });
});

it("keeps an absolute MCP URL in the code element", () => {
  render(<McpUrlCopy mcpUrl="https://viewer.example.test/mcp" />);
  expect(screen.getByText("https://viewer.example.test/mcp")).toBeTruthy();
});
