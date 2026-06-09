// Run with: bunx vitest run src/app/races/detail/lazy-detail-sections.test.tsx

import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import React from "react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

interface CapturedOverallScoreProps {
  expandAll: boolean;
}

const capturedProps: CapturedOverallScoreProps[] = [];

vi.mock("next/navigation", () => ({
  useSearchParams: () => new URLSearchParams(),
}));

vi.mock("./overall-score-table", () => ({
  OverallScoreTable: (props: CapturedOverallScoreProps) => {
    capturedProps.push({ expandAll: props.expandAll });
    return (
      <div data-testid="overall-score-table-stub">{`expandAll=${String(props.expandAll)}`}</div>
    );
  },
}));

import { LazyOverallScoreSection } from "./lazy-detail-sections";

interface MockMediaQueryListController {
  matches: boolean;
}

interface MockMediaQueryEvent {
  matches: boolean;
}

const installMatchMediaMock = (initialMatches: boolean): MockMediaQueryListController => {
  const listeners = new Set<(event: MockMediaQueryEvent) => void>();
  const controller: MockMediaQueryListController = {
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

const installFetchMock = (): void => {
  vi.stubGlobal(
    "fetch",
    vi.fn(
      () =>
        new Promise<Response>((resolve) => {
          resolve(
            new Response(
              JSON.stringify({
                rows: [],
                type: "overall-score",
              }),
              { headers: { "content-type": "application/json" }, status: 200 },
            ),
          );
        }),
    ),
  );
};

interface SectionProps {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  realtimeApiBaseUrl: string;
  source: "jra" | "nar";
  year: string;
}

const sectionProps: SectionProps = {
  day: "01",
  keibajoCode: "05",
  month: "06",
  raceNumber: "01",
  realtimeApiBaseUrl: "",
  source: "jra",
  year: "2026",
};

beforeEach(() => {
  capturedProps.length = 0;
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

test("LazyOverallScoreSection wraps OverallScoreTable in mobile-collapsible-section on mobile", async () => {
  installMatchMediaMock(true);
  installFetchMock();
  await act(async () => {
    render(<LazyOverallScoreSection {...sectionProps} />);
  });
  await waitFor(() => {
    expect(screen.getByTestId("overall-score-table-stub")).toBeDefined();
  });
  const titleSpans = screen.getAllByText("総合スコア");
  expect(titleSpans.length >= 1).toStrictEqual(true);
  const sectionToggles = screen.getAllByRole("button", { name: /^総合スコア セクションを/ });
  expect(sectionToggles.length).toStrictEqual(1);
});

test("LazyOverallScoreSection passes expandAll based on section open state", async () => {
  installMatchMediaMock(true);
  installFetchMock();
  await act(async () => {
    render(<LazyOverallScoreSection {...sectionProps} />);
  });
  await waitFor(() => {
    expect(screen.getByTestId("overall-score-table-stub")).toBeDefined();
  });
  await waitFor(() => {
    const latest = capturedProps.at(-1);
    expect(latest?.expandAll).toStrictEqual(false);
  });
  const closedToggle = screen.getByRole("button", { name: /^総合スコア セクションを/ });
  await act(async () => {
    fireEvent.click(closedToggle);
  });
  await waitFor(() => {
    const latest = capturedProps.at(-1);
    expect(latest?.expandAll).toStrictEqual(true);
  });
});
