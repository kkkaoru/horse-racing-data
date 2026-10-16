// Runs with Bun; production parsing uses native Workers APIs. No stale odds fallback.
import "server-only";
import { ARC_OFFICIAL_URL, parseOfficialCard, type OfficialCard } from "./arc-official-card";
import { isArcPredictionRace, type ArcRaceScope } from "./arc-prediction";
import { safeGetCloudflareRuntime } from "./cloudflare-context.server";

interface CardEnvelope {
  html: string;
  retrievedAt: string;
  url: string;
}
const CACHE_KEY: string = "official-card:jra:2026:1004:A8:05:v1";
const TTL: number = 60;
const MAX_BYTES: number = 512 * 1024;
export const loadOfficialCard = async (scope: ArcRaceScope): Promise<OfficialCard | null> => {
  if (!isArcPredictionRace(scope) || typeof Reflect.get(globalThis, "HTMLRewriter") !== "function")
    return null;
  try {
    const { env } = await safeGetCloudflareRuntime();
    const cached: unknown = await env?.DETAIL_SECTION_CACHE_KV?.get(CACHE_KEY, { type: "json" });
    if (
      typeof cached === "object" &&
      cached !== null &&
      "html" in cached &&
      typeof cached.html === "string" &&
      "retrievedAt" in cached &&
      typeof cached.retrievedAt === "string" &&
      "url" in cached &&
      cached.url === ARC_OFFICIAL_URL &&
      Date.now() - Date.parse(cached.retrievedAt) >= 0 &&
      Date.now() - Date.parse(cached.retrievedAt) < TTL * 1000
    ) {
      return await parseOfficialCard({
        html: cached.html,
        retrievedAt: cached.retrievedAt,
        url: cached.url,
      });
    }
    const signal = AbortSignal.timeout(2500);
    const response = await fetch(ARC_OFFICIAL_URL, {
      signal,
      redirect: "manual",
      cache: "no-store",
    });
    if (!response.ok || response.body === null || response.url !== ARC_OFFICIAL_URL)
      throw new Error(`Official card response rejected: HTTP ${response.status}`);
    const decoder = new TextDecoder("shift_jis", { fatal: true });
    const chunks: string[] = [];
    const state = { bytes: 0 };
    await response.body.pipeTo(
      new WritableStream<Uint8Array>({
        write(chunk) {
          state.bytes += chunk.byteLength;
          if (state.bytes > MAX_BYTES) throw new Error("Official card exceeds response bound");
          chunks.push(decoder.decode(chunk, { stream: true }));
        },
      }),
      { signal },
    );
    chunks.push(decoder.decode());
    const envelope: CardEnvelope = {
      html: chunks.join(""),
      url: ARC_OFFICIAL_URL,
      retrievedAt: new Date().toISOString(),
    };
    const card = await parseOfficialCard(envelope);
    await env?.DETAIL_SECTION_CACHE_KV?.put(CACHE_KEY, JSON.stringify(envelope), {
      expirationTtl: TTL,
    });
    return card;
  } catch (error: unknown) {
    console.warn("arc_official_card_unavailable", {
      error: error instanceof Error ? error.message : "Unknown official card failure",
    });
    return null;
  }
};
