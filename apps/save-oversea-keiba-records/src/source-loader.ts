// This file runs with Bun.

// The ports own every character-encoding decision. The JRA card can arrive in a legacy
// Japanese encoding on some paths while the secondary card is UTF-8, so each port must
// return an already-decoded string and this module stays encoding-agnostic.
export interface HtmlFetchPort {
  readonly fetchHtml: (url: string) => Promise<string>;
}

export interface FileReadPort {
  readonly readFile: (path: string) => Promise<string>;
}

export type SourceOrigin = "cache" | "network";

export interface LoadSourcesInput {
  readonly jraCardUrl: string;
  readonly secondaryCardUrl: string;
  readonly jraCachePath: string | null;
  readonly secondaryCachePath: string | null;
  readonly fetchPort: HtmlFetchPort;
  readonly fileReadPort: FileReadPort;
}

export interface LoadedSources {
  readonly jraHtml: string;
  readonly secondaryHtml: string;
  readonly jraOrigin: SourceOrigin;
  readonly secondaryOrigin: SourceOrigin;
  readonly networkRequestCount: number;
}

interface LoadOneInput {
  readonly cachePath: string | null;
  readonly url: string;
  readonly fetchPort: HtmlFetchPort;
  readonly fileReadPort: FileReadPort;
}

interface LoadOneResult {
  readonly html: string;
  readonly origin: SourceOrigin;
}

const JRA_CARD_BASE_URL: string = "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=";
const JRA_RACECARD_ID_PATTERN: RegExp = /^[a-z]{2}\d{2}[a-z]{3}\d{6,}(\/[a-z0-9]+)?$/i;
export const OVERSEA_SECONDARY_CARD_URL_TEMPLATE: string = "OVERSEA_SECONDARY_CARD_URL_TEMPLATE";
const SECONDARY_RACE_ID_TOKEN: string = "{RACE_ID}";

export const buildJraCardUrl = (racecardId: string): string => {
  if (!JRA_RACECARD_ID_PATTERN.test(racecardId)) {
    throw new Error("JRA racecard id is malformed; expected a shape like pk01dde0110420260101051.");
  }
  return `${JRA_CARD_BASE_URL}${racecardId}`;
};

export const buildSecondaryCardUrl = (raceId: string, template: string | undefined): string => {
  if (template === undefined || template.length === 0) {
    throw new Error(
      `Set ${OVERSEA_SECONDARY_CARD_URL_TEMPLATE} to the secondary-source card URL template before fetching.`,
    );
  }
  if (!template.includes(SECONDARY_RACE_ID_TOKEN)) {
    throw new Error(
      `${OVERSEA_SECONDARY_CARD_URL_TEMPLATE} must contain the ${SECONDARY_RACE_ID_TOKEN} placeholder.`,
    );
  }
  return template.replaceAll(SECONDARY_RACE_ID_TOKEN, raceId);
};

const loadOneSource = async ({
  cachePath,
  url,
  fetchPort,
  fileReadPort,
}: LoadOneInput): Promise<LoadOneResult> => {
  if (cachePath !== null) {
    const html: string = await fileReadPort.readFile(cachePath);
    return { html, origin: "cache" };
  }
  const html: string = await fetchPort.fetchHtml(url);
  return { html, origin: "network" };
};

export const loadRaceSources = async (input: LoadSourcesInput): Promise<LoadedSources> => {
  const requests: [Promise<LoadOneResult>, Promise<LoadOneResult>] = [
    loadOneSource({
      cachePath: input.jraCachePath,
      url: input.jraCardUrl,
      fetchPort: input.fetchPort,
      fileReadPort: input.fileReadPort,
    }),
    loadOneSource({
      cachePath: input.secondaryCachePath,
      url: input.secondaryCardUrl,
      fetchPort: input.fetchPort,
      fileReadPort: input.fileReadPort,
    }),
  ];
  const [jra, secondary]: [LoadOneResult, LoadOneResult] = await Promise.all(requests);
  const origins: readonly SourceOrigin[] = [jra.origin, secondary.origin];
  return {
    jraHtml: jra.html,
    secondaryHtml: secondary.html,
    jraOrigin: jra.origin,
    secondaryOrigin: secondary.origin,
    networkRequestCount: origins.filter((origin: SourceOrigin): boolean => origin === "network")
      .length,
  };
};
