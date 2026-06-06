// Run with: bunx vitest run src/app/races/detail/mobile-collapsible-section.test.tsx

import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import React from "react";
import { afterEach, expect, test, vi } from "vitest";

import { MobileCollapsibleSection } from "./mobile-collapsible-section";

interface MockMediaQueryEvent {
  matches: boolean;
}

interface MockMediaQueryListController {
  matches: boolean;
  fire: (matches: boolean) => void;
}

const installMatchMediaMock = (initialMatches: boolean): MockMediaQueryListController => {
  const listeners = new Set<(event: MockMediaQueryEvent) => void>();
  const controller: MockMediaQueryListController = {
    fire: (matches: boolean) => {
      controller.matches = matches;
      listeners.forEach((listener) => {
        listener({ matches });
      });
    },
    matches: initialMatches,
  };
  const mediaQueryList = {
    addEventListener: (_: string, listener: (event: MockMediaQueryEvent) => void) => {
      listeners.add(listener);
    },
    addListener: (listener: (event: MockMediaQueryEvent) => void) => {
      listeners.add(listener);
    },
    get matches() {
      return controller.matches;
    },
    removeEventListener: (_: string, listener: (event: MockMediaQueryEvent) => void) => {
      listeners.delete(listener);
    },
    removeListener: (listener: (event: MockMediaQueryEvent) => void) => {
      listeners.delete(listener);
    },
  };
  vi.stubGlobal(
    "matchMedia",
    vi.fn(() => mediaQueryList),
  );
  return controller;
};

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

test("renders desktop heading, toggle button, and visible body when viewport is desktop", () => {
  installMatchMediaMock(false);
  render(
    <MobileCollapsibleSection heading={<h2>レース情報</h2>} title="レース情報">
      <div data-testid="body-content">body text</div>
    </MobileCollapsibleSection>,
  );
  const toggle = screen.getByRole("button", { name: "レース情報 セクションを閉じる" });
  expect(toggle.getAttribute("aria-expanded")).toStrictEqual("true");
  expect(screen.getByTestId("body-content").parentElement?.hasAttribute("hidden")).toStrictEqual(
    false,
  );
});

test("hides body when viewport is mobile and the user has not toggled yet", () => {
  installMatchMediaMock(true);
  render(
    <MobileCollapsibleSection heading={<h2>レース情報</h2>} title="レース情報">
      <div data-testid="body-content">body text</div>
    </MobileCollapsibleSection>,
  );
  const toggle = screen.getByRole("button", { name: "レース情報 セクションを開く" });
  expect(toggle.getAttribute("aria-expanded")).toStrictEqual("false");
  expect(screen.getByTestId("body-content").parentElement?.hasAttribute("hidden")).toStrictEqual(
    true,
  );
});

test("toggles body visibility when the toggle is clicked on mobile", () => {
  installMatchMediaMock(true);
  render(
    <MobileCollapsibleSection heading={<h2>コース情報</h2>} title="コース情報">
      <div data-testid="body-content">course info</div>
    </MobileCollapsibleSection>,
  );
  const initialToggle = screen.getByRole("button", { name: "コース情報 セクションを開く" });
  fireEvent.click(initialToggle);
  const expandedToggle = screen.getByRole("button", { name: "コース情報 セクションを閉じる" });
  expect(expandedToggle.getAttribute("aria-expanded")).toStrictEqual("true");
  expect(screen.getByTestId("body-content").parentElement?.hasAttribute("hidden")).toStrictEqual(
    false,
  );
  fireEvent.click(expandedToggle);
  const collapsedToggle = screen.getByRole("button", { name: "コース情報 セクションを開く" });
  expect(collapsedToggle.getAttribute("aria-expanded")).toStrictEqual("false");
});

test("ignores click on desktop because the toggle is suppressed by media query", () => {
  installMatchMediaMock(false);
  render(
    <MobileCollapsibleSection heading={<h2>着順予測</h2>} title="着順予測">
      <div data-testid="body-content">prediction</div>
    </MobileCollapsibleSection>,
  );
  const toggle = screen.getByRole("button", { name: "着順予測 セクションを閉じる" });
  fireEvent.click(toggle);
  expect(toggle.getAttribute("aria-expanded")).toStrictEqual("true");
  expect(screen.getByTestId("body-content").parentElement?.hasAttribute("hidden")).toStrictEqual(
    false,
  );
});

test("reacts to viewport changes by closing on mobile and opening on desktop", () => {
  const controller = installMatchMediaMock(false);
  render(
    <MobileCollapsibleSection heading={<h2>レース情報</h2>} title="レース情報">
      <div data-testid="body-content">race info</div>
    </MobileCollapsibleSection>,
  );
  act(() => {
    controller.fire(true);
  });
  expect(
    screen
      .getByRole("button", { name: "レース情報 セクションを開く" })
      .getAttribute("aria-expanded"),
  ).toStrictEqual("false");
  act(() => {
    controller.fire(false);
  });
  expect(
    screen
      .getByRole("button", { name: "レース情報 セクションを閉じる" })
      .getAttribute("aria-expanded"),
  ).toStrictEqual("true");
});

test("falls back to expanded state when matchMedia is unavailable", () => {
  vi.stubGlobal("matchMedia", undefined);
  render(
    <MobileCollapsibleSection heading={<h2>レース情報</h2>} title="レース情報">
      <div data-testid="body-content">body</div>
    </MobileCollapsibleSection>,
  );
  const toggle = screen.getByRole("button", { name: "レース情報 セクションを閉じる" });
  expect(toggle.getAttribute("aria-expanded")).toStrictEqual("true");
});

test("renders without desktop heading when heading prop is omitted", () => {
  installMatchMediaMock(true);
  render(
    <MobileCollapsibleSection title="着順予測精度">
      <div data-testid="body-content">accuracy</div>
    </MobileCollapsibleSection>,
  );
  const toggle = screen.getByRole("button", { name: "着順予測精度 セクションを開く" });
  expect(toggle.getAttribute("aria-expanded")).toStrictEqual("false");
  expect(document.querySelector(".mobile-collapsible-section-desktop-heading")).toStrictEqual(null);
});

test("supports browsers that only expose addListener / removeListener", () => {
  const listeners = new Set<(event: MockMediaQueryEvent) => void>();
  const mediaQueryList = {
    addListener: (listener: (event: MockMediaQueryEvent) => void) => {
      listeners.add(listener);
    },
    matches: true,
    removeListener: (listener: (event: MockMediaQueryEvent) => void) => {
      listeners.delete(listener);
    },
  };
  vi.stubGlobal(
    "matchMedia",
    vi.fn(() => mediaQueryList),
  );
  const { unmount } = render(
    <MobileCollapsibleSection heading={<h2>レース情報</h2>} title="レース情報">
      <div data-testid="body-content">body</div>
    </MobileCollapsibleSection>,
  );
  expect(listeners.size).toStrictEqual(1);
  unmount();
  expect(listeners.size).toStrictEqual(0);
});
