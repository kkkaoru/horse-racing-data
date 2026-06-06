// Execute with bun: bun run --filter pc-keiba-viewer test src/app/brand-runner.test.tsx

import { act, cleanup, render } from "@testing-library/react";
import { renderToString } from "react-dom/server";
import { afterEach, beforeEach, expect, it, vi } from "vitest";

import { BrandRunner } from "./brand-runner";

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-05T12:00:00Z"));
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.restoreAllMocks();
});

it("does not emit brand-runner CSS variables in SSR HTML", () => {
  const html = renderToString(<BrandRunner />);
  expect(html).toStrictEqual(
    '<span class="brand-mark" aria-hidden="true"><span class="brand-runner">🏇</span><span class="brand-track"></span></span>',
  );
});

it("applies deterministic --brand-runner-filter after mount", () => {
  render(<BrandRunner />);
  const runner = document.querySelector(".brand-runner");
  expect(runner?.getAttribute("style")).toStrictEqual(
    "--brand-runner-filter: sepia(0.18) saturate(1.25) brightness(0.98);",
  );
});

it("never emits --brand-runner-size in inline style after mount", () => {
  render(<BrandRunner />);
  const runner = document.querySelector(".brand-runner");
  const styleAttr = runner?.getAttribute("style") ?? "";
  expect(styleAttr.indexOf("--brand-runner-size")).toStrictEqual(-1);
});

it("never emits --brand-runner-size in SSR HTML", () => {
  const html = renderToString(<BrandRunner />);
  expect(html.indexOf("--brand-runner-size")).toStrictEqual(-1);
});

it("renders the horse emoji and track sibling", () => {
  render(<BrandRunner />);
  const brandMark = document.querySelector(".brand-mark");
  expect(brandMark?.textContent).toStrictEqual("🏇");
});

it("marks the wrapper as aria-hidden for decorative use", () => {
  render(<BrandRunner />);
  const brandMark = document.querySelector(".brand-mark");
  expect(brandMark?.getAttribute("aria-hidden")).toStrictEqual("true");
});

it("schedules a setTimeout aligned to the next 10-minute boundary after mount", () => {
  const setTimeoutSpy = vi.spyOn(window, "setTimeout");
  render(<BrandRunner />);
  const firstCallDelay = setTimeoutSpy.mock.calls[0]?.[1];
  expect(firstCallDelay).toStrictEqual(10 * 60 * 1000);
});

it("clears the scheduled timer on unmount", () => {
  const clearTimeoutSpy = vi.spyOn(window, "clearTimeout");
  const view = render(<BrandRunner />);
  view.unmount();
  expect(clearTimeoutSpy).toHaveBeenCalled();
});

it("refreshes the filter when the next 10-minute bucket yields a different color", () => {
  // 12:10:00Z → bucket 2967769 (sepia). +10 minutes → bucket 2967770 (hue-rotate 205deg).
  vi.setSystemTime(new Date("2026-06-05T12:10:00Z"));
  render(<BrandRunner />);
  const before = document.querySelector(".brand-runner")?.getAttribute("style");
  expect(before).toStrictEqual(
    "--brand-runner-filter: sepia(0.18) saturate(1.25) brightness(0.98);",
  );
  act(() => {
    vi.advanceTimersByTime(10 * 60 * 1000);
  });
  const after = document.querySelector(".brand-runner")?.getAttribute("style");
  expect(after).toStrictEqual(
    "--brand-runner-filter: hue-rotate(205deg) saturate(1.16) brightness(1.02);",
  );
});
