// Run with bun. `bun run --filter pc-keiba-viewer test`
import { act, cleanup, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";

import { useHorseWeightStream, type HorseWeightSnapshot } from "./horse-weight-stream-client";

interface MockEventSource {
  addEventListener: (type: string, listener: (event: MessageEvent) => void) => void;
  close: ReturnType<typeof vi.fn>;
  dispatchWeightsEvent: (data: string) => void;
  removeEventListener: (type: string, listener: (event: MessageEvent) => void) => void;
  url: string;
}

class FakeEventSource implements MockEventSource {
  static readonly instances: MockEventSource[] = [];
  readonly addEventListener: MockEventSource["addEventListener"];
  readonly close: ReturnType<typeof vi.fn>;
  readonly dispatchWeightsEvent: MockEventSource["dispatchWeightsEvent"];
  readonly removeEventListener: MockEventSource["removeEventListener"];
  readonly url: string;
  constructor(url: string) {
    const listeners = new Map<string, Set<(event: MessageEvent) => void>>();
    this.url = url;
    this.close = vi.fn<() => void>();
    this.addEventListener = (type, listener) => {
      const existing = listeners.get(type) ?? new Set();
      existing.add(listener);
      listeners.set(type, existing);
    };
    this.removeEventListener = (type, listener) => {
      const set = listeners.get(type);
      if (set) set.delete(listener);
    };
    this.dispatchWeightsEvent = (data) => {
      const set = listeners.get("weights");
      if (set) set.forEach((listener) => listener(new MessageEvent("weights", { data })));
    };
    FakeEventSource.instances.push(this);
  }
}

const resetFakeInstances = (): void => {
  FakeEventSource.instances.splice(0, FakeEventSource.instances.length);
};

beforeEach(() => {
  resetFakeInstances();
  vi.stubGlobal("EventSource", FakeEventSource);
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

it("returns initial snapshot before any event arrives", () => {
  const initial: HorseWeightSnapshot = {
    fetchedAt: "2026-05-30T08:00:00.000Z",
    horses: [
      { changeAmount: 0, changeSign: "+", horseName: "Alpha", horseNumber: "1", weight: 480 },
    ],
  };
  const { result } = renderHook(() =>
    useHorseWeightStream({
      day: "30",
      initial,
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      source: "jra",
      year: "2026",
    }),
  );
  expect(result.current).toStrictEqual({
    fetchedAt: "2026-05-30T08:00:00.000Z",
    horses: [
      { changeAmount: 0, changeSign: "+", horseName: "Alpha", horseNumber: "1", weight: 480 },
    ],
  });
});

it("updates on incoming weights event", () => {
  const { result } = renderHook(() =>
    useHorseWeightStream({
      day: "30",
      initial: null,
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      source: "jra",
      year: "2026",
    }),
  );
  const instance = FakeEventSource.instances[0];
  if (!instance) throw new Error("missing fake EventSource instance");
  act(() => {
    instance.dispatchWeightsEvent(
      JSON.stringify({
        fetchedAt: "2026-05-30T08:30:00.000Z",
        horses: [
          { changeAmount: 4, changeSign: "+", horseName: "Beta", horseNumber: "2", weight: 502 },
        ],
      }),
    );
  });
  expect(result.current).toStrictEqual({
    fetchedAt: "2026-05-30T08:30:00.000Z",
    horses: [
      { changeAmount: 4, changeSign: "+", horseName: "Beta", horseNumber: "2", weight: 502 },
    ],
  });
});

it("ignores malformed event payload", () => {
  const initial: HorseWeightSnapshot = {
    fetchedAt: "2026-05-30T08:00:00.000Z",
    horses: [],
  };
  const { result } = renderHook(() =>
    useHorseWeightStream({
      day: "30",
      initial,
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      source: "jra",
      year: "2026",
    }),
  );
  const instance = FakeEventSource.instances[0];
  if (!instance) throw new Error("missing fake EventSource instance");
  act(() => {
    instance.dispatchWeightsEvent("not json");
  });
  expect(result.current).toStrictEqual({
    fetchedAt: "2026-05-30T08:00:00.000Z",
    horses: [],
  });
});

it("ignores payload missing required fields", () => {
  const initial: HorseWeightSnapshot = {
    fetchedAt: "2026-05-30T08:00:00.000Z",
    horses: [
      { changeAmount: null, changeSign: null, horseName: null, horseNumber: "9", weight: null },
    ],
  };
  const { result } = renderHook(() =>
    useHorseWeightStream({
      day: "30",
      initial,
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      source: "jra",
      year: "2026",
    }),
  );
  const instance = FakeEventSource.instances[0];
  if (!instance) throw new Error("missing fake EventSource instance");
  act(() => {
    instance.dispatchWeightsEvent(JSON.stringify({ foo: "bar" }));
  });
  expect(result.current).toStrictEqual({
    fetchedAt: "2026-05-30T08:00:00.000Z",
    horses: [
      { changeAmount: null, changeSign: null, horseName: null, horseNumber: "9", weight: null },
    ],
  });
});

it("ignores payload whose horses field is not an array", () => {
  const { result } = renderHook(() =>
    useHorseWeightStream({
      day: "30",
      initial: null,
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      source: "jra",
      year: "2026",
    }),
  );
  const instance = FakeEventSource.instances[0];
  if (!instance) throw new Error("missing fake EventSource instance");
  act(() => {
    instance.dispatchWeightsEvent(
      JSON.stringify({ fetchedAt: "2026-05-30T08:30:00.000Z", horses: "nope" }),
    );
  });
  expect(result.current).toBeNull();
});

it("ignores payload whose fetchedAt is not a string", () => {
  const { result } = renderHook(() =>
    useHorseWeightStream({
      day: "30",
      initial: null,
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      source: "jra",
      year: "2026",
    }),
  );
  const instance = FakeEventSource.instances[0];
  if (!instance) throw new Error("missing fake EventSource instance");
  act(() => {
    instance.dispatchWeightsEvent(JSON.stringify({ fetchedAt: 1234, horses: [] }));
  });
  expect(result.current).toBeNull();
});

it("ignores non-object JSON payload (null)", () => {
  const { result } = renderHook(() =>
    useHorseWeightStream({
      day: "30",
      initial: null,
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      source: "jra",
      year: "2026",
    }),
  );
  const instance = FakeEventSource.instances[0];
  if (!instance) throw new Error("missing fake EventSource instance");
  act(() => {
    instance.dispatchWeightsEvent("null");
  });
  expect(result.current).toBeNull();
});

it("constructs the SSE URL with the encoded source query param", () => {
  renderHook(() =>
    useHorseWeightStream({
      day: "30",
      initial: null,
      keibajoCode: "47",
      month: "05",
      raceNumber: "12",
      source: "nar",
      year: "2026",
    }),
  );
  const instance = FakeEventSource.instances[0];
  if (!instance) throw new Error("missing fake EventSource instance");
  expect(instance.url).toBe("/api/races/2026/05/30/47/12/horse-weights-stream?source=nar");
});

it("does not open EventSource when source is not streamable", () => {
  const initial: HorseWeightSnapshot = {
    fetchedAt: "2026-05-30T08:00:00.000Z",
    horses: [],
  };
  const { result } = renderHook(() =>
    useHorseWeightStream({
      day: "30",
      initial,
      keibajoCode: "83",
      month: "05",
      raceNumber: "01",
      source: "banei",
      year: "2026",
    }),
  );
  expect(FakeEventSource.instances).toStrictEqual([]);
  expect(result.current).toStrictEqual({
    fetchedAt: "2026-05-30T08:00:00.000Z",
    horses: [],
  });
});

it("closes EventSource on unmount", () => {
  const { unmount } = renderHook(() =>
    useHorseWeightStream({
      day: "30",
      initial: null,
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      source: "jra",
      year: "2026",
    }),
  );
  const instance = FakeEventSource.instances[0];
  if (!instance) throw new Error("missing fake EventSource instance");
  expect(instance.close).not.toHaveBeenCalled();
  unmount();
  expect(instance.close).toHaveBeenCalledTimes(1);
});
