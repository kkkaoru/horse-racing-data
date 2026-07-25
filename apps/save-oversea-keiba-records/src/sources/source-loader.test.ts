// This test runs with Bun and Vitest.
import { expect, test } from "vitest";
import {
  buildJraCardUrl,
  buildSecondaryCardUrl,
  loadRaceSources,
  type FileReadPort,
  type HtmlFetchPort,
  type LoadedSources,
} from "./source-loader";

const JRA_URL: string = "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0110420260101051";
const SECONDARY_URL: string = "https://secondary-source.example/card/2026010105";
const JRA_HTML: string = "<html>jra card</html>";
const SECONDARY_HTML: string = "<html>secondary card</html>";
const MALFORMED_ID_ERROR: string =
  "JRA racecard id is malformed; expected a shape like pk01dde0110420260101051.";
const MISSING_TEMPLATE_ERROR: string =
  "Set OVERSEA_SECONDARY_CARD_URL_TEMPLATE to the secondary-source card URL template before fetching.";
const MISSING_PLACEHOLDER_ERROR: string =
  "OVERSEA_SECONDARY_CARD_URL_TEMPLATE must contain the {RACE_ID} placeholder.";

interface RecordingFetchPort {
  readonly port: HtmlFetchPort;
  readonly requestedUrls: string[];
}

const createRecordingFetchPort = (
  htmlByUrl: Readonly<Record<string, string>>,
): RecordingFetchPort => {
  const requestedUrls: string[] = [];
  const port: HtmlFetchPort = {
    fetchHtml: (url: string): Promise<string> => {
      requestedUrls.push(url);
      const html: string | undefined = htmlByUrl[url];
      if (html === undefined) {
        return Promise.reject(new Error("Unexpected fetch in test."));
      }
      return Promise.resolve(html);
    },
  };
  return { port, requestedUrls };
};

interface RecordingFileReadPort {
  readonly port: FileReadPort;
  readonly requestedPaths: string[];
}

const createRecordingFileReadPort = (
  htmlByPath: Readonly<Record<string, string>>,
): RecordingFileReadPort => {
  const requestedPaths: string[] = [];
  const port: FileReadPort = {
    readFile: (path: string): Promise<string> => {
      requestedPaths.push(path);
      const html: string | undefined = htmlByPath[path];
      if (html === undefined) {
        return Promise.reject(new Error("Unexpected file read in test."));
      }
      return Promise.resolve(html);
    },
  };
  return { port, requestedPaths };
};

test("buildJraCardUrl builds the JRADB access URL for a well-formed racecard id", () => {
  expect(buildJraCardUrl("pk01dde0110420260101051")).toBe(
    "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0110420260101051",
  );
});

test("buildJraCardUrl keeps an optional suffix segment after the racecard id", () => {
  expect(buildJraCardUrl("pk01dde0110420260101051/abc123")).toBe(
    "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0110420260101051/abc123",
  );
});

test("buildJraCardUrl rejects an empty racecard id", () => {
  expect(() => buildJraCardUrl("")).toThrowError(MALFORMED_ID_ERROR);
});

test("buildJraCardUrl rejects an id containing forbidden characters", () => {
  expect(() => buildJraCardUrl("pk01dde011??")).toThrowError(MALFORMED_ID_ERROR);
});

test("buildJraCardUrl rejects a path-traversal id", () => {
  expect(() => buildJraCardUrl("../etc/passwd")).toThrowError(MALFORMED_ID_ERROR);
});

test("buildSecondaryCardUrl substitutes the race id into the template placeholder", () => {
  expect(
    buildSecondaryCardUrl("2026010105", "https://secondary-source.example/card/{RACE_ID}"),
  ).toBe("https://secondary-source.example/card/2026010105");
});

test("buildSecondaryCardUrl rejects when the template env var is unset", () => {
  expect(() => buildSecondaryCardUrl("2026010105", undefined)).toThrowError(MISSING_TEMPLATE_ERROR);
});

test("buildSecondaryCardUrl rejects an empty template", () => {
  expect(() => buildSecondaryCardUrl("2026010105", "")).toThrowError(MISSING_TEMPLATE_ERROR);
});

test("buildSecondaryCardUrl rejects a template without the race id placeholder", () => {
  expect(() =>
    buildSecondaryCardUrl("2026010105", "https://secondary-source.example/card"),
  ).toThrowError(MISSING_PLACEHOLDER_ERROR);
});

test("loadRaceSources reads both sources from cache and makes zero network requests", async () => {
  const fetchPort: RecordingFetchPort = createRecordingFetchPort({});
  const fileReadPort: RecordingFileReadPort = createRecordingFileReadPort({
    "jra-cache.html": JRA_HTML,
    "secondary-cache.html": SECONDARY_HTML,
  });

  const loaded: LoadedSources = await loadRaceSources({
    jraCardUrl: JRA_URL,
    secondaryCardUrl: SECONDARY_URL,
    jraCachePath: "jra-cache.html",
    secondaryCachePath: "secondary-cache.html",
    fetchPort: fetchPort.port,
    fileReadPort: fileReadPort.port,
  });

  expect(loaded).toStrictEqual({
    jraHtml: JRA_HTML,
    secondaryHtml: SECONDARY_HTML,
    jraOrigin: "cache",
    secondaryOrigin: "cache",
    networkRequestCount: 0,
  });
  expect(fetchPort.requestedUrls).toStrictEqual([]);
  expect(fileReadPort.requestedPaths).toStrictEqual(["jra-cache.html", "secondary-cache.html"]);
});

test("loadRaceSources fetches both sources when no cache paths are supplied", async () => {
  const fetchPort: RecordingFetchPort = createRecordingFetchPort({
    [JRA_URL]: JRA_HTML,
    [SECONDARY_URL]: SECONDARY_HTML,
  });
  const fileReadPort: RecordingFileReadPort = createRecordingFileReadPort({});

  const loaded: LoadedSources = await loadRaceSources({
    jraCardUrl: JRA_URL,
    secondaryCardUrl: SECONDARY_URL,
    jraCachePath: null,
    secondaryCachePath: null,
    fetchPort: fetchPort.port,
    fileReadPort: fileReadPort.port,
  });

  expect(loaded).toStrictEqual({
    jraHtml: JRA_HTML,
    secondaryHtml: SECONDARY_HTML,
    jraOrigin: "network",
    secondaryOrigin: "network",
    networkRequestCount: 2,
  });
  expect(fetchPort.requestedUrls).toStrictEqual([JRA_URL, SECONDARY_URL]);
  expect(fileReadPort.requestedPaths).toStrictEqual([]);
});

test("loadRaceSources fetches only the secondary source when the JRA card is cached", async () => {
  const fetchPort: RecordingFetchPort = createRecordingFetchPort({
    [SECONDARY_URL]: SECONDARY_HTML,
  });
  const fileReadPort: RecordingFileReadPort = createRecordingFileReadPort({
    "jra-cache.html": JRA_HTML,
  });

  const loaded: LoadedSources = await loadRaceSources({
    jraCardUrl: JRA_URL,
    secondaryCardUrl: SECONDARY_URL,
    jraCachePath: "jra-cache.html",
    secondaryCachePath: null,
    fetchPort: fetchPort.port,
    fileReadPort: fileReadPort.port,
  });

  expect(loaded).toStrictEqual({
    jraHtml: JRA_HTML,
    secondaryHtml: SECONDARY_HTML,
    jraOrigin: "cache",
    secondaryOrigin: "network",
    networkRequestCount: 1,
  });
  expect(fetchPort.requestedUrls).toStrictEqual([SECONDARY_URL]);
  expect(fileReadPort.requestedPaths).toStrictEqual(["jra-cache.html"]);
});

test("loadRaceSources fetches only the JRA card when the secondary source is cached", async () => {
  const fetchPort: RecordingFetchPort = createRecordingFetchPort({
    [JRA_URL]: JRA_HTML,
  });
  const fileReadPort: RecordingFileReadPort = createRecordingFileReadPort({
    "secondary-cache.html": SECONDARY_HTML,
  });

  const loaded: LoadedSources = await loadRaceSources({
    jraCardUrl: JRA_URL,
    secondaryCardUrl: SECONDARY_URL,
    jraCachePath: null,
    secondaryCachePath: "secondary-cache.html",
    fetchPort: fetchPort.port,
    fileReadPort: fileReadPort.port,
  });

  expect(loaded).toStrictEqual({
    jraHtml: JRA_HTML,
    secondaryHtml: SECONDARY_HTML,
    jraOrigin: "network",
    secondaryOrigin: "cache",
    networkRequestCount: 1,
  });
  expect(fetchPort.requestedUrls).toStrictEqual([JRA_URL]);
  expect(fileReadPort.requestedPaths).toStrictEqual(["secondary-cache.html"]);
});
