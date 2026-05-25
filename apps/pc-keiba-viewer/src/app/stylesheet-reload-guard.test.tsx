// Execute with bun: bun run test src/app/stylesheet-reload-guard.test.tsx

import { cleanup, render } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { StylesheetReloadGuard } from "./stylesheet-reload-guard";

const RELOAD_GUARD_KEY = "pc-keiba-stylesheet-reload-attempted";

const installReloadSpy = (): ReturnType<typeof vi.fn> => {
  const reload = vi.fn<() => void>();
  Object.defineProperty(window, "location", {
    configurable: true,
    value: { ...window.location, reload },
  });
  return reload;
};

const appendStylesheetLink = (loaded: boolean): HTMLLinkElement => {
  const link = document.createElement("link");
  link.rel = "stylesheet";
  Object.defineProperty(link, "sheet", {
    configurable: true,
    value: loaded ? ({} as CSSStyleSheet) : null,
  });
  document.head.appendChild(link);
  return link;
};

const dispatchPageShow = (persisted: boolean): void => {
  const event = new Event("pageshow") as PageTransitionEvent;
  Object.defineProperty(event, "persisted", { value: persisted });
  window.dispatchEvent(event);
};

afterEach(() => {
  cleanup();
  document.head.querySelectorAll('link[rel="stylesheet"]').forEach((node) => node.remove());
  sessionStorage.removeItem(RELOAD_GUARD_KEY);
  vi.restoreAllMocks();
});

it("reloads when bfcache restores a page with an unloaded stylesheet", () => {
  appendStylesheetLink(false);
  const reload = installReloadSpy();
  render(<StylesheetReloadGuard />);
  dispatchPageShow(true);
  expect(reload).toHaveBeenCalledTimes(1);
  expect(sessionStorage.getItem(RELOAD_GUARD_KEY)).toBe("1");
});

it("does not reload when bfcache restores a page with healthy stylesheets", () => {
  appendStylesheetLink(true);
  const reload = installReloadSpy();
  render(<StylesheetReloadGuard />);
  dispatchPageShow(true);
  expect(reload).not.toHaveBeenCalled();
});

it("does not reload on the initial pageshow (persisted=false)", () => {
  appendStylesheetLink(false);
  const reload = installReloadSpy();
  render(<StylesheetReloadGuard />);
  dispatchPageShow(false);
  expect(reload).not.toHaveBeenCalled();
});

it("reloads once when a stylesheet link errors out", () => {
  const link = appendStylesheetLink(false);
  const reload = installReloadSpy();
  render(<StylesheetReloadGuard />);
  link.dispatchEvent(new Event("error", { bubbles: true }));
  expect(reload).toHaveBeenCalledTimes(1);
});

it("skips reload when the session already attempted recovery", () => {
  sessionStorage.setItem(RELOAD_GUARD_KEY, "1");
  appendStylesheetLink(false);
  const reload = installReloadSpy();
  render(<StylesheetReloadGuard />);
  dispatchPageShow(true);
  expect(reload).not.toHaveBeenCalled();
});

it("ignores error events from non-stylesheet elements", () => {
  const img = document.createElement("img");
  document.body.appendChild(img);
  const reload = installReloadSpy();
  render(<StylesheetReloadGuard />);
  img.dispatchEvent(new Event("error", { bubbles: true }));
  expect(reload).not.toHaveBeenCalled();
  img.remove();
});
