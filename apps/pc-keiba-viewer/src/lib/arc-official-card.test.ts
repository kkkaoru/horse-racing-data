// Runs with Bun through Vitest. Native workerd parity is recorded in official-card-integration.
import { afterEach, expect, it, vi } from "vitest";

import evidence from "./__fixtures__/arc-official-card.json";
import {
  enrichOfficialHistory,
  enrichOfficialRunners,
  parseOfficialCard,
  validateOfficialRoster,
} from "./arc-official-card";
const scope = {
  source: "jra",
  year: "2026",
  month: "10",
  day: "04",
  keibajoCode: "A8",
  raceNumber: "05",
};
afterEach(() => vi.unstubAllGlobals());
it("validates all sixteen declared horses, names and last-cell gates", () => {
  expect(validateOfficialRoster(evidence.card, evidence.runners)).toBe(true);
  expect(validateOfficialRoster(evidence.card, evidence.runners.slice(1))).toBe(false);
  expect(
    validateOfficialRoster(
      evidence.card,
      evidence.runners.map((r) => ({ ...r, wakuban: "1" })),
    ),
  ).toBe(false);
});
it("fills owners and pedigree without replacing known values", () => {
  const missing = evidence.runners.map((r) => ({
    ...r,
    ownerNameFull: null,
    sireName: null,
    damSireName: null,
  }));
  expect(enrichOfficialRunners(missing, evidence.card).map((r) => r.ownerNameFull)).toStrictEqual([
    "AGA KHAN STUDS SC",
    "SHEIKH AHMED AL MAKTOUM",
    "GP.GRZEGOROWSKI",
    "近藤 旬子",
    "松本 好隆",
    "MME W.SPANNER",
    "VICTORIOUS FOREVER",
    "DERRICK SMITH,ET AL.",
    "JUDDMONTE",
    "G.STRAWBRIDGE",
    "AGA KHAN STUDS SCEA",
    "BRANT/MAGNIER/TABOR/SMITH",
    "ACE STUD",
    "GEORGE WAUD",
    "SHAPOOR MISTRY",
    "TABOR/SMITH/MAGNIER/WESTE",
  ]);
  expect(enrichOfficialRunners(evidence.runners, null)).toBe(evidence.runners);
  expect(enrichOfficialRunners([], evidence.card)).toStrictEqual([]);
  const known = evidence.runners.map((r) => ({
    ...r,
    ownerNameFull: "Known owner",
    sireName: "Known sire",
    damSireName: "Known dam sire",
  }));
  expect(enrichOfficialRunners(known, evidence.card)[0]).toMatchObject({
    ownerNameFull: "Known owner",
    sireName: "Known sire",
    damSireName: "Known dam sire",
  });
});
it("keeps existing history and domestic races unchanged, rejects mismatched dates", () => {
  const input = {
    scope,
    runners: evidence.runners,
    results: evidence.results,
    card: evidence.card,
  };
  expect(enrichOfficialHistory({ ...input, scope: { ...scope, keibajoCode: "05" } })).toBe(
    evidence.results,
  );
  expect(enrichOfficialHistory({ ...input, card: null })).toBe(evidence.results);
  expect(enrichOfficialHistory({ ...input, runners: [] })).toBe(evidence.results);
  const changed = evidence.results.map((r) => ({ ...r, kaisaiNen: "1900" }));
  expect(enrichOfficialHistory({ ...input, results: changed })).toStrictEqual(changed);
  const filled = enrichOfficialHistory(input);
  expect(
    new Set(
      filled
        .filter((r) => r.corner1 && r.corner2 && r.corner3 && r.corner4)
        .map((r) => r.currentUmaban),
    ).size,
  ).toBe(16);
});
it("rejects wrong URL, invalid retrieval time and mojibake before parsing", async () => {
  await expect(
    parseOfficialCard({
      html: "",
      url: "https://example.com",
      retrievedAt: evidence.card.retrievedAt,
    }),
  ).rejects.toThrow("Invalid official provenance or encoding");
  await expect(
    parseOfficialCard({ html: "", url: evidence.card.url, retrievedAt: "invalid" }),
  ).rejects.toThrow("Invalid official provenance or encoding");
  await expect(
    parseOfficialCard({
      html: "\ufffd",
      url: evidence.card.url,
      retrievedAt: evidence.card.retrievedAt,
    }),
  ).rejects.toThrow("Invalid official provenance or encoding");
});

interface TestHandlers {
  element?: (element: Element) => void;
  text?: (text: { text: string }) => void;
}
class FixtureRewriter {
  handlers: { selector: string; handlers: TestHandlers }[] = [];
  on(selector: string, handlers: TestHandlers) {
    this.handlers.push({ selector, handlers });
    return this;
  }
  transform(response: Response) {
    return {
      text: async () => {
        const document = new DOMParser().parseFromString(await response.text(), "text/html");
        const selected = new Map<Element, TestHandlers[]>();
        this.handlers.forEach((entry) =>
          document
            .querySelectorAll(entry.selector)
            .forEach((element) =>
              selected.set(element, [...(selected.get(element) ?? []), entry.handlers]),
            ),
        );
        document.querySelectorAll("*").forEach((element) =>
          selected.get(element)?.forEach((handlers) => {
            handlers.element?.(element);
            handlers.text?.({ text: element.textContent ?? "" });
          }),
        );
        return "";
      },
    };
  }
}
const html: string = evidence.html;
it.each([null, "00", " "])(
  "fills missing field size %s from the same official past race",
  (shussoTosu) => {
    const first = evidence.results[0];
    if (!first) throw new Error("Missing fixture history");
    const rows = enrichOfficialHistory({
      scope,
      runners: evidence.runners,
      card: evidence.card,
      results: [{ ...first, shussoTosu }],
    });
    expect(rows[0]).toMatchObject({
      shussoTosu: "4",
      corner1: "2",
      corner2: "2",
      corner3: "2",
      corner4: "2",
      sohaTime: "2308",
    });
  },
);
it("preserves a known field size and rejects duplicate matching histories", () => {
  const first = evidence.results[0];
  if (!first) throw new Error("Missing fixture history");
  const results = [{ ...first, shussoTosu: "18" }];
  expect(
    enrichOfficialHistory({ scope, runners: evidence.runners, card: evidence.card, results })[0]
      ?.shussoTosu,
  ).toBe("18");
  const card = {
    ...evidence.card,
    horses: evidence.card.horses.map((h) => ({ ...h, past: [...h.past, ...h.past] })),
  };
  expect(enrichOfficialHistory({ scope, runners: evidence.runners, card, results })[0]).toBe(
    results[0],
  );
});
it("never equates an unknown official finish with a zero result", () => {
  const first = evidence.results[0];
  if (!first) throw new Error("Missing fixture history");
  const results = [{ ...first, kakuteiChakujun: "00" }];
  const card = {
    ...evidence.card,
    horses: evidence.card.horses.map((h) => ({
      ...h,
      past: h.past.map((p) => ({ ...p, finish: null })),
    })),
  };
  expect(enrichOfficialHistory({ scope, runners: evidence.runners, card, results })[0]).toBe(
    results[0],
  );
});
it.each([
  ["2026年9月6日", "2026年2月30日"],
  ["2026年9月6日", "2026年10月4日"],
  ["2026年9月6日", "2026年10月5日"],
  ['class="max">4', 'class="max">1'],
  ["2:30.8", "2:60.8"],
])("rejects invalid or future past observations %s → %s", async (before, after) => {
  vi.stubGlobal("HTMLRewriter", FixtureRewriter);
  await expect(
    parseOfficialCard({
      html: html.replace(before, after),
      url: evidence.card.url,
      retrievedAt: evidence.card.retrievedAt,
    }),
  ).rejects.toThrow("Invalid official past identity");
});
it("parses the entire official fixture", async () => {
  vi.stubGlobal("HTMLRewriter", FixtureRewriter);
  const input = { html, url: evidence.card.url, retrievedAt: evidence.card.retrievedAt };
  expect(await parseOfficialCard(input)).toStrictEqual(evidence.card);
});
it.each([
  ["2026年10月4日", "2025年10月4日", "Official race scope mismatch"],
  ['class="waku">5', 'class="waku">9', "Official roster incomplete"],
  ['class="red">4.3', 'class="red">取消', "Invalid official declaration"],
  ["2026年9月6日", "unknown", "Invalid official past identity"],
])("rejects altered official fixture %s", async (before, after, message) => {
  vi.stubGlobal("HTMLRewriter", FixtureRewriter);
  await expect(
    parseOfficialCard({
      html: html.replaceAll(before, after),
      url: evidence.card.url,
      retrievedAt: evidence.card.retrievedAt,
    }),
  ).rejects.toThrow(message);
});
