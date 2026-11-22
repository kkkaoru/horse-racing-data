// Run with: bunx vitest run src/app/header-share-button.test.tsx

import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

let currentPath = "/races";

vi.mock("next/navigation", () => ({
  usePathname: () => currentPath,
}));

import { SHARE_ORIGIN } from "../lib/share-link";
import { HeaderShareButton } from "./header-share-button";

const STORAGE_KEY = "pc-keiba-shared-page:/races";
const HOUR_MS = 3_600_000;

const writeText = vi.fn<(text: string) => Promise<void>>();

const clickShare = async () => {
  await act(async () => {
    fireEvent.click(screen.getByRole("button", { name: "共有" }));
  });
};

describe("HeaderShareButton", () => {
  beforeEach(() => {
    currentPath = "/races";
    writeText.mockReset();
    writeText.mockResolvedValue();
    const items = new Map<string, string>();
    vi.stubGlobal("localStorage", {
      clear: () => {
        items.clear();
      },
      getItem: (key: string): string | null => items.get(key) ?? null,
      removeItem: (key: string) => {
        items.delete(key);
      },
      setItem: (key: string, value: string) => {
        items.set(key, value);
      },
    });
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });
  });

  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  test("defaults the share deadline to 48 hours", () => {
    render(<HeaderShareButton />);

    expect(screen.getByLabelText("共有期限")).toHaveProperty("value", "48");
    expect(screen.getByRole("button", { name: "共有" })).toBeTruthy();
  });

  test("copies the production URL and reports that sharing finished", async () => {
    render(<HeaderShareButton />);

    await clickShare();

    expect(writeText).toHaveBeenCalledWith(`${SHARE_ORIGIN}/races`);
    expect(screen.getByRole("status").textContent).toBe("共有しました");
    expect(screen.getByRole("button", { name: "共有済み" })).toHaveProperty("disabled", true);
  });

  test("does not offer sharing from a page that is already shared", () => {
    localStorage.setItem(STORAGE_KEY, String(Date.now() + HOUR_MS));

    render(<HeaderShareButton />);

    expect(screen.getByRole("button", { name: "共有済み" })).toHaveProperty("disabled", true);
    expect(screen.queryByRole("button", { name: "共有" })).toBeNull();
    expect(screen.queryByLabelText("共有期限")).toBeNull();
  });

  test("offers sharing again once the stored deadline has passed", () => {
    localStorage.setItem(STORAGE_KEY, String(Date.now() - 1));

    render(<HeaderShareButton />);

    expect(screen.getByRole("button", { name: "共有" })).toBeTruthy();
  });

  test("reports a failed share without marking the page as shared", async () => {
    writeText.mockRejectedValue(new Error("denied"));

    render(<HeaderShareButton />);
    await clickShare();

    expect(screen.getByRole("status").textContent).toBe("共有できませんでした");
    expect(screen.getByRole("button", { name: "共有" })).toBeTruthy();
    expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
  });

  test("stores the deadline picked in the menu", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00Z"));

    render(<HeaderShareButton />);
    fireEvent.change(screen.getByLabelText("共有期限"), { target: { value: "24" } });
    await clickShare();

    expect(localStorage.getItem(STORAGE_KEY)).toBe(String(Date.now() + 24 * HOUR_MS));
  });

  test("clears the completion notice after the reset delay", async () => {
    vi.useFakeTimers();

    render(<HeaderShareButton />);
    await clickShare();
    expect(screen.getByRole("status").textContent).toBe("共有しました");

    act(() => {
      vi.advanceTimersByTime(2400);
    });

    expect(screen.queryByRole("status")).toBeNull();
  });

  test("forgets the notice when the page changes", async () => {
    writeText.mockRejectedValue(new Error("denied"));

    const { rerender } = render(<HeaderShareButton />);
    await clickShare();
    expect(screen.getByRole("status").textContent).toBe("共有できませんでした");

    currentPath = "/horses";
    rerender(<HeaderShareButton />);

    expect(screen.queryByRole("status")).toBeNull();
  });
});
