// Run with: bunx vitest run src/lib/share-link.test.ts

import { beforeEach, describe, expect, test, vi } from "vitest";

import {
  buildShareUrl,
  DEFAULT_SHARE_HOURS,
  readShareDeadline,
  SHARE_HOUR_OPTIONS,
  SHARE_ORIGIN,
  writeShareDeadline,
} from "./share-link";

describe("buildShareUrl", () => {
  test("builds the production URL for a bare path", () => {
    expect(buildShareUrl("/races")).toBe(`${SHARE_ORIGIN}/races`);
  });

  test("keeps the query string as given", () => {
    expect(buildShareUrl("/mypage", "?fav=1")).toBe(`${SHARE_ORIGIN}/mypage?fav=1`);
  });
});

describe("share deadline defaults", () => {
  test("defaults to 48 hours and offers it as an option", () => {
    expect(DEFAULT_SHARE_HOURS).toBe(48);
    expect(SHARE_HOUR_OPTIONS).toContain(DEFAULT_SHARE_HOURS);
  });
});

describe("share deadline storage", () => {
  beforeEach(() => {
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
  });

  test("returns 0 when the page was never shared", () => {
    expect(readShareDeadline("/races", 1_000)).toBe(0);
  });

  test("returns the stored deadline while it is still in the future", () => {
    writeShareDeadline("/races", 5_000);
    expect(readShareDeadline("/races", 1_000)).toBe(5_000);
  });

  test("returns 0 once the deadline has passed", () => {
    writeShareDeadline("/races", 5_000);
    expect(readShareDeadline("/races", 5_000)).toBe(0);
  });

  test("returns 0 for an unreadable stored value", () => {
    localStorage.setItem("pc-keiba-shared-page:/races", "not-a-number");
    expect(readShareDeadline("/races", 1_000)).toBe(0);
  });

  test("ignores a missing storage without throwing", () => {
    vi.unstubAllGlobals();

    expect(readShareDeadline("/races", 1_000)).toBe(0);
    expect(() => {
      writeShareDeadline("/races", 5_000);
    }).not.toThrow();
  });

  test("keeps pages independent", () => {
    writeShareDeadline("/races", 5_000);
    expect(readShareDeadline("/mypage", 1_000)).toBe(0);
  });
});
