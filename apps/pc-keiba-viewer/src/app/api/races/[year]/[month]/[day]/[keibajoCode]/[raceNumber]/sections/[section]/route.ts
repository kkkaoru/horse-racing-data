// Runs with Bun for tests and the Workers runtime in production.
import { NextResponse } from "next/server";

import {
  getRaceDetail,
  getRaceRunners,
  getRaceSourceByRoute,
} from "../../../../../../../../../../db/queries";
import { enrichOfficialRunners } from "../../../../../../../../../../lib/arc-official-card";
import { loadOfficialCard } from "../../../../../../../../../../lib/arc-official-card.server";
import { isArcPredictionRace } from "../../../../../../../../../../lib/arc-prediction";
import { safeGetCloudflareExecutionContext } from "../../../../../../../../../../lib/cloudflare-context.server";
import {
  buildFinishPredictionInputsCacheKey,
  deleteFinishPredictionInputsCache,
  type FinishPredictionStaticPayload,
  getCachedFinishPredictionInputs,
  putFinishPredictionInputsCache,
} from "../../../../../../../../../../lib/finish-prediction-inputs-cache.server";
import { canonicalHeatmapQuery } from "../../../../../../../../../../lib/heatmap-execution";
import {
  getHeatmapExecutionOwner,
  runOwnedHeatmap,
  heatmapPendingResponse,
} from "../../../../../../../../../../lib/heatmap-execution.server";
import { secretsEqual } from "../../../../../../../../../../lib/mcp-auth";
import { normalizeExpectedPredictionGeneratedAt } from "../../../../../../../../../../lib/prediction-generation-freshness";
import {
  buildDetailSectionCacheKey,
  DETAIL_SECTION_CACHE_WARM_PARAM,
  isDefaultDetailSectionCacheRequest,
  PREDICTION_REFRESH_PARAM,
  stripDetailSectionCacheWarmParams,
} from "../../../../../../../../../../lib/race-detail-section-cache";
import {
  buildStaleDetailSectionResponse,
  getCachedDetailSectionResponse,
  getStaleDetailSectionBody,
  putDetailSectionCache,
} from "../../../../../../../../../../lib/race-detail-section-cache.server";
import {
  buildWinRateHeatmapCacheKey,
  buildWinRateHeatmapRunnerSignature,
  isWinRateHeatmapSectionPayload,
  serializeWinRateHeatmapCacheQuery,
  type WinRateHeatmapSectionPayload,
} from "../../../../../../../../../../lib/win-rate-heatmap-cache";
import {
  getCachedWinRateHeatmapPayload,
  putWinRateHeatmapCache,
  refreshWinRateHeatmapLiveWeights,
} from "../../../../../../../../../../lib/win-rate-heatmap-cache.server";
import { fetchHeatmapLiveWeights } from "../../../../../../../../../../lib/win-rate-heatmap-live-weights.server";
import {
  type DetailSection,
  getDetailSectionPayload,
  getFinishPositionBucketSectionData,
} from "../../../../../../../../../races/detail/detail-section-data";

export const dynamic = "force-dynamic";

interface DetailSectionRouteProps {
  params: Promise<{
    day: string;
    keibajoCode: string;
    month: string;
    raceNumber: string;
    section: string;
    year: string;
  }>;
}

const SECTIONS = [
  "ability",
  "bloodline",
  "condition",
  "finish-prediction",
  "premium-data-top",
  "overall-score",
  "pace-prediction",
  "results",
  "similar",
  "time-score",
  "training",
  "win-rate-heatmap",
] as const satisfies readonly DetailSection[];

const NON_EMPTY_MODEL_PREDICTION_FEATURES_MARKER = '"modelPredictionFeatures":[{';
const EXPECTED_PREDICTION_GENERATED_AT_PARAM = "expectedPredictionGeneratedAt";
const INTERNAL_AUTH_HEADER = "x-pc-keiba-internal-token";

const FINISH_PREDICTION_BROWSER_CACHE_CONTROL = "private, no-cache, max-age=0, must-revalidate";

const HTTP_STATUS_NO_CONTENT = 204;
const HTTP_STATUS_METHOD_NOT_ALLOWED = 405;
const HEATMAP_SECTION_UNAVAILABLE = "unavailable";
// Workflow cache probe: answer only status + X-Win-Rate-Heatmap-Cache with no
// body, so the caller never has to cancel a multi-MB payload (which Workers
// logs as a "canceled" outcome).
const HEATMAP_CACHE_PROBE_PARAM = "__cacheProbe";

const isValidSection = (section: string): section is DetailSection =>
  SECTIONS.some((candidate) => candidate === section);

const isValidParams = (
  year: string,
  month: string,
  day: string,
  keibajoCode: string,
  raceNumber: string,
): boolean =>
  /^\d{4}$/.test(year) &&
  /^\d{2}$/.test(month) &&
  /^\d{2}$/.test(day) &&
  /^[0-9A-Z]{2}$/.test(keibajoCode) &&
  /^\d{2}$/.test(raceNumber);

const searchParamsToRecord = (searchParams: URLSearchParams): Record<string, string> =>
  Object.fromEntries(searchParams.entries());

const isEmptyPremiumDataTopSectionBody = (body: string): boolean => {
  try {
    const parsed: unknown = JSON.parse(body);
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      !("type" in parsed) ||
      parsed.type !== "premium-data-top" ||
      !("dataTopHorses" in parsed) ||
      !Array.isArray(parsed.dataTopHorses)
    ) {
      return false;
    }
    return parsed.dataTopHorses.length === 0;
  } catch {
    return false;
  }
};

const hasFinishPredictionModelFeatures = (body: string): boolean =>
  body.includes(NON_EMPTY_MODEL_PREDICTION_FEATURES_MARKER);

const shouldCacheFinishPrediction = (section: string, body: string): boolean =>
  section !== "finish-prediction" || hasFinishPredictionModelFeatures(body);

const withFinishPredictionCacheControl = (response: Response, section: string): Response => {
  if (section !== "finish-prediction") return response;
  const headers = new Headers(response.headers);
  headers.set("Cache-Control", FINISH_PREDICTION_BROWSER_CACHE_CONTROL);
  return new Response(response.body, { headers, status: response.status });
};

interface ComputeSectionParams {
  cacheKey: string | null;
  day: string;
  expectedPredictionGeneratedAt: string | undefined;
  finishPredictionInputsCacheKey: string | null;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  section: DetailSection;
  sectionSearchParams: URLSearchParams;
  year: string;
}

interface ComputedSectionResult {
  body: string;
  payloadType: string;
}

interface FinishPredictionCacheHitParams {
  cachedStatic: FinishPredictionStaticPayload;
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  sectionSearchParams: URLSearchParams;
  year: string;
}

const buildFinishPredictionCacheHitResponse = async (
  params: FinishPredictionCacheHitParams,
): Promise<Response | null> => {
  const raceSource = await getRaceSourceByRoute(
    params.year,
    params.month,
    params.day,
    params.keibajoCode,
    params.raceNumber,
  );
  if (!raceSource) return null;
  const bucket = await getFinishPositionBucketSectionData({
    day: params.day,
    keibajoCode: params.keibajoCode,
    month: params.month,
    query: searchParamsToRecord(params.sectionSearchParams),
    raceNumber: params.raceNumber,
    raceSource,
    year: params.year,
  });
  return NextResponse.json({
    bucket,
    evaluation: params.cachedStatic.evaluation,
    inputs: params.cachedStatic.inputs,
    type: "finish-prediction",
  });
};

const computeAndStoreSection = async (
  params: ComputeSectionParams,
): Promise<ComputedSectionResult | null> => {
  const {
    cacheKey,
    day,
    expectedPredictionGeneratedAt,
    finishPredictionInputsCacheKey,
    keibajoCode,
    month,
    raceNumber,
    section,
    sectionSearchParams,
    year,
  } = params;
  const raceSource = await getRaceSourceByRoute(year, month, day, keibajoCode, raceNumber);
  if (!raceSource) {
    return null;
  }
  const race =
    cacheKey || finishPredictionInputsCacheKey
      ? await getRaceDetail(raceSource, year, month, day, keibajoCode, raceNumber)
      : null;
  const payload = await getDetailSectionPayload(section, {
    day,
    expectedPredictionGeneratedAt,
    keibajoCode,
    month,
    query: searchParamsToRecord(sectionSearchParams),
    raceNumber,
    raceSource,
    year,
  });
  if (!payload) {
    return null;
  }
  const body = JSON.stringify(payload);
  if (
    finishPredictionInputsCacheKey &&
    race &&
    payload.type === "finish-prediction" &&
    "inputs" in payload &&
    "evaluation" in payload &&
    shouldCacheFinishPrediction(section, body)
  ) {
    await putFinishPredictionInputsCache({
      awaitWrite: expectedPredictionGeneratedAt !== undefined,
      body: JSON.stringify({
        evaluation: payload.evaluation,
        inputs: payload.inputs,
      }),
      cacheKey: finishPredictionInputsCacheKey,
      race,
    });
  }
  if (
    cacheKey &&
    race &&
    !(section === "premium-data-top" && isEmptyPremiumDataTopSectionBody(body)) &&
    shouldCacheFinishPrediction(section, body)
  ) {
    await putDetailSectionCache({ body, cacheKey, race });
  }
  return { body, payloadType: payload.type };
};

const getExecutionContext = async (): Promise<PcKeibaExecutionContext | null> =>
  safeGetCloudflareExecutionContext();

const isInternalPredictionRefresh = (request: Request): boolean => {
  const expectedToken = process.env.PC_KEIBA_INTERNAL_TOKEN;
  const providedToken = request.headers.get(INTERNAL_AUTH_HEADER);
  return (
    typeof expectedToken === "string" &&
    expectedToken.length > 0 &&
    providedToken !== null &&
    secretsEqual(providedToken, expectedToken)
  );
};

const hasCurrentHeatmapRunners = (
  cachedRunners: Parameters<typeof buildWinRateHeatmapRunnerSignature>[0],
  currentRunners: Parameters<typeof buildWinRateHeatmapRunnerSignature>[0],
): boolean => {
  const cachedSignature = buildWinRateHeatmapRunnerSignature(cachedRunners);
  const currentSignature = buildWinRateHeatmapRunnerSignature(currentRunners);
  return cachedSignature !== null && cachedSignature === currentSignature;
};

// An old presentation with no bloodline names is not ready when runners have
// known pedigrees. Fresh names-only cells are valid without fabricated rates.
const isHeatmapCacheReady = (
  payload: Pick<WinRateHeatmapSectionPayload, "bloodlineRows" | "presentation" | "runners">,
  currentRunners: Parameters<typeof buildWinRateHeatmapRunnerSignature>[0],
): boolean =>
  hasCurrentHeatmapRunners(payload.runners, currentRunners) &&
  (payload.bloodlineRows.length > 0 ||
    !currentRunners.some(
      (runner) =>
        (typeof runner.sireName === "string" && runner.sireName.trim() !== "") ||
        (typeof runner.damSireName === "string" && runner.damSireName.trim() !== ""),
    ) ||
    payload.presentation === undefined ||
    payload.presentation.rows.some((row) => row.cells.sire.name || row.cells.damSire.name));

const loadHeatmapSectionPayload = async (
  params: Parameters<typeof getDetailSectionPayload>[1],
): Promise<unknown> => {
  try {
    return await getDetailSectionPayload("win-rate-heatmap", params);
  } catch (error: unknown) {
    console.error(
      "Heatmap cache warm failed",
      error instanceof Error ? error.message : "Unknown error",
    );
    return HEATMAP_SECTION_UNAVAILABLE;
  }
};

export async function GET(request: Request, { params }: DetailSectionRouteProps) {
  const { day, keibajoCode, month, raceNumber, section, year } = await params;
  if (!isValidSection(section) || !isValidParams(year, month, day, keibajoCode, raceNumber)) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }

  const requestUrl = new URL(request.url);
  const isQueueWarm = requestUrl.searchParams.has(DETAIL_SECTION_CACHE_WARM_PARAM);
  const sectionSearchParams = stripDetailSectionCacheWarmParams(requestUrl.searchParams);
  sectionSearchParams.delete(EXPECTED_PREDICTION_GENERATED_AT_PARAM);
  const isHeatmapCacheProbe = requestUrl.searchParams.has(HEATMAP_CACHE_PROBE_PARAM);
  sectionSearchParams.delete(HEATMAP_CACHE_PROBE_PARAM);
  const ownerToken = requestUrl.searchParams.get("__heatmapOwner");
  sectionSearchParams.delete("__heatmapOwner");
  const rawExpectedPredictionGeneratedAt = requestUrl.searchParams.get(
    EXPECTED_PREDICTION_GENERATED_AT_PARAM,
  );
  const expectedPredictionGeneratedAt =
    rawExpectedPredictionGeneratedAt === null
      ? undefined
      : (normalizeExpectedPredictionGeneratedAt(rawExpectedPredictionGeneratedAt) ?? undefined);
  if (
    rawExpectedPredictionGeneratedAt !== null &&
    (expectedPredictionGeneratedAt === undefined ||
      section !== "finish-prediction" ||
      !requestUrl.searchParams.has(PREDICTION_REFRESH_PARAM))
  ) {
    return NextResponse.json({ error: "invalid_expected_prediction_generation" }, { status: 400 });
  }
  if (typeof expectedPredictionGeneratedAt === "string" && !isInternalPredictionRefresh(request)) {
    return NextResponse.json({ error: "forbidden" }, { status: 403 });
  }
  if (section === "win-rate-heatmap") {
    if (isQueueWarm && !isInternalPredictionRefresh(request))
      return NextResponse.json({ error: "forbidden" }, { status: 403 });
    try {
      canonicalHeatmapQuery(sectionSearchParams);
    } catch {
      return NextResponse.json({ error: "invalid_heatmap_query" }, { status: 400 });
    }
    const heatmapCacheKey = buildWinRateHeatmapCacheKey({
      day,
      keibajoCode,
      month,
      query: serializeWinRateHeatmapCacheQuery(sectionSearchParams),
      raceNumber,
      year,
    });
    const raceSource = await getRaceSourceByRoute(year, month, day, keibajoCode, raceNumber);
    if (!raceSource) {
      return NextResponse.json({ error: "not_found" }, { status: 404 });
    }
    const storedRunners = await getRaceRunners(
      raceSource,
      year,
      month,
      day,
      keibajoCode,
      raceNumber,
    );
    const currentRunners = enrichOfficialRunners(
      storedRunners,
      await loadOfficialCard({ source: raceSource, year, month, day, keibajoCode, raceNumber }),
    );
    const cachedHeatmap = await getCachedWinRateHeatmapPayload(heatmapCacheKey, currentRunners);
    if (isHeatmapCacheProbe) {
      // Legacy entries without weight-change rows still serve readers but are
      // reported as misses so the warm sweep rebuilds them.
      const hit =
        cachedHeatmap?.presentation !== undefined &&
        cachedHeatmap.weightChangeClassStats !== undefined &&
        isHeatmapCacheReady(cachedHeatmap, currentRunners);
      // The warm sweep probes every 15 minutes; it re-renders a hit whose
      // presentation predates the posted weights so 馬体重/馬体重増減 appear.
      if (hit) {
        await fetchHeatmapLiveWeights({
          day,
          keibajoCode,
          month,
          raceNumber,
          source: raceSource,
          year,
        })
          .then((liveHorseWeights) =>
            refreshWinRateHeatmapLiveWeights({
              cacheKey: heatmapCacheKey,
              cached: cachedHeatmap,
              keibajoCode,
              liveHorseWeights,
            }),
          )
          .catch(() => false);
      }
      return new Response(null, {
        headers: {
          "Cache-Control": "private, max-age=0, no-store",
          "X-Win-Rate-Heatmap-Cache": hit ? "HIT" : "MISS",
        },
        status: hit ? 204 : 503,
      });
    }
    if (
      cachedHeatmap?.presentation !== undefined &&
      !isQueueWarm &&
      isHeatmapCacheReady(cachedHeatmap, currentRunners)
    ) {
      return NextResponse.json(cachedHeatmap, {
        headers: {
          "Cache-Control": "private, max-age=0, no-store",
          "X-Win-Rate-Heatmap-Cache": "HIT",
        },
      });
    }
    try {
      const owner = await getHeatmapExecutionOwner({
        day,
        keibajoCode,
        month,
        raceNumber,
        year,
        source: raceSource,
        query: canonicalHeatmapQuery(sectionSearchParams),
      });
      if (!isQueueWarm || ownerToken === null) {
        return heatmapPendingResponse(
          await owner.ensure({
            day,
            keibajoCode,
            month,
            raceNumber,
            year,
            source: raceSource,
            query: canonicalHeatmapQuery(sectionSearchParams),
          }),
        );
      }
      return await runOwnedHeatmap({
        owner,
        token: ownerToken,
        compute: async () => {
          const heatmapPayload = await loadHeatmapSectionPayload({
            day,
            keibajoCode,
            month,
            query: searchParamsToRecord(sectionSearchParams),
            raceNumber,
            raceSource,
            year,
          });
          if (heatmapPayload === HEATMAP_SECTION_UNAVAILABLE) {
            return NextResponse.json(
              { error: "section_unavailable", section: "win-rate-heatmap" },
              {
                headers: {
                  "Cache-Control": "private, max-age=0, no-store",
                  "Retry-After": "30",
                },
                status: 503,
              },
            );
          }
          if (!isWinRateHeatmapSectionPayload(heatmapPayload)) {
            return NextResponse.json({ error: "not_found" }, { status: 404 });
          }
          if (!isHeatmapCacheReady(heatmapPayload, currentRunners)) {
            return NextResponse.json(
              { error: "heatmap_catalog_unavailable" },
              {
                status: 503,
                headers: { "Cache-Control": "private, max-age=0, no-store", "Retry-After": "30" },
              },
            );
          }
          try {
            await putWinRateHeatmapCache({
              cacheKey: heatmapCacheKey,
              keibajoCode,
              payload: heatmapPayload,
            });
          } catch {
            return NextResponse.json({ error: "heatmap_cache_store_failed" }, { status: 503 });
          }
          // Warm callers need only the status and cache header; skip the payload.
          return new Response(null, {
            headers: {
              "Cache-Control": "private, max-age=0, no-store",
              "X-Win-Rate-Heatmap-Cache": "MISS-STORED",
            },
            status: 200,
          });
        },
      });
    } catch {
      return heatmapPendingResponse("unknown");
    }
  }
  const defaultSectionRequest = sectionSearchParams.toString() === "";
  // This card's official history is refreshed independently of the legacy section cache.
  const liveOfficialPace =
    section === "pace-prediction" &&
    isArcPredictionRace({
      source: "jra",
      year,
      month,
      day,
      keibajoCode,
      raceNumber,
    });
  const cacheableDefaultRequest =
    isDefaultDetailSectionCacheRequest(section, sectionSearchParams) &&
    defaultSectionRequest &&
    !liveOfficialPace;
  const finishPredictionInputsCacheKey =
    section === "finish-prediction" && defaultSectionRequest
      ? buildFinishPredictionInputsCacheKey({ day, keibajoCode, month, raceNumber, year })
      : null;
  const skipFinishPredictionInputsRead = requestUrl.searchParams.has(PREDICTION_REFRESH_PARAM);
  if (finishPredictionInputsCacheKey && !skipFinishPredictionInputsRead) {
    const cachedStatic = await getCachedFinishPredictionInputs(finishPredictionInputsCacheKey);
    const cacheHitResponse = cachedStatic
      ? await buildFinishPredictionCacheHitResponse({
          cachedStatic,
          day,
          keibajoCode,
          month,
          raceNumber,
          sectionSearchParams,
          year,
        })
      : null;
    if (cacheHitResponse) return withFinishPredictionCacheControl(cacheHitResponse, section);
  }

  const cacheKey = cacheableDefaultRequest
    ? buildDetailSectionCacheKey({ day, keibajoCode, month, raceNumber, section, year })
    : null;
  const skipPredictionRefresh =
    section === "finish-prediction" && requestUrl.searchParams.has(PREDICTION_REFRESH_PARAM);
  const cachedResponse =
    cacheKey && !skipPredictionRefresh && !isQueueWarm
      ? await getCachedDetailSectionResponse(cacheKey)
      : null;
  if (cachedResponse) {
    if (section === "premium-data-top") {
      const cachedBody = await cachedResponse.clone().text();
      if (!isEmptyPremiumDataTopSectionBody(cachedBody)) {
        return cachedResponse;
      }
    } else if (section === "finish-prediction") {
      const cachedBody = await cachedResponse.clone().text();
      if (hasFinishPredictionModelFeatures(cachedBody)) {
        return withFinishPredictionCacheControl(cachedResponse, section);
      }
    } else {
      return cachedResponse;
    }
  }

  // SWR branch: fresh tier missed, but a long-lived stale snapshot exists.
  // Serve it instantly and let the heavy DB recompute happen off-request
  // via `ctx.waitUntil`. The next visitor sees the refreshed payload.
  if (cacheKey && !isQueueWarm) {
    const staleBody = await getStaleDetailSectionBody(cacheKey);
    const staleEmpty =
      staleBody !== null &&
      section === "premium-data-top" &&
      isEmptyPremiumDataTopSectionBody(staleBody);
    const staleFinishPredictionEmpty =
      staleBody !== null &&
      section === "finish-prediction" &&
      !hasFinishPredictionModelFeatures(staleBody);
    if (staleBody && !staleEmpty && !skipPredictionRefresh && !staleFinishPredictionEmpty) {
      const ctx = await getExecutionContext();
      ctx?.waitUntil(
        computeAndStoreSection({
          cacheKey,
          day,
          expectedPredictionGeneratedAt,
          finishPredictionInputsCacheKey,
          keibajoCode,
          month,
          raceNumber,
          section,
          sectionSearchParams,
          year,
        }).catch((error: unknown) => {
          console.error(`background refresh of section ${section} failed`, error);
        }),
      );
      return withFinishPredictionCacheControl(buildStaleDetailSectionResponse(staleBody), section);
    }
  }

  let result: ComputedSectionResult | null;
  try {
    result = await computeAndStoreSection({
      cacheKey,
      day,
      expectedPredictionGeneratedAt,
      finishPredictionInputsCacheKey,
      keibajoCode,
      month,
      raceNumber,
      section,
      sectionSearchParams,
      year,
    });
  } catch (error) {
    console.error(`section ${section} compute failed`, error);
    if (cacheKey && expectedPredictionGeneratedAt === undefined) {
      const staleBody = await getStaleDetailSectionBody(cacheKey).catch(() => null);
      if (staleBody) {
        return withFinishPredictionCacheControl(
          buildStaleDetailSectionResponse(staleBody),
          section,
        );
      }
    }
    return NextResponse.json(
      { error: "section_unavailable", section },
      {
        headers: {
          "Cache-Control": "private, max-age=0, no-store",
          "Retry-After": "30",
        },
        status: 503,
      },
    );
  }
  if (!result) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }
  // The Queue needs only the warm status; large section bodies exceed its 1 MiB drain limit.
  if (isQueueWarm) return new Response(null, { status: 204 });

  return withFinishPredictionCacheControl(
    new NextResponse(result.body, {
      headers: {
        "Cache-Control": "private, max-age=0, no-store",
        "Content-Type": "application/json; charset=utf-8",
        "X-Detail-Section-Cache": finishPredictionInputsCacheKey
          ? "FINISH-INPUTS-MISS"
          : cacheKey
            ? "MISS-STORED"
            : "BYPASS",
      },
    }),
    section,
  );
}

export async function DELETE(
  _request: Request,
  { params }: DetailSectionRouteProps,
): Promise<Response> {
  const { day, keibajoCode, month, raceNumber, section, year } = await params;
  if (section !== "finish-prediction") {
    return new Response(null, { status: HTTP_STATUS_METHOD_NOT_ALLOWED });
  }
  const inputsCacheKey = buildFinishPredictionInputsCacheKey({
    day,
    keibajoCode,
    month,
    raceNumber,
    year,
  });
  await deleteFinishPredictionInputsCache(inputsCacheKey);
  return new Response(null, { status: HTTP_STATUS_NO_CONTENT });
}
