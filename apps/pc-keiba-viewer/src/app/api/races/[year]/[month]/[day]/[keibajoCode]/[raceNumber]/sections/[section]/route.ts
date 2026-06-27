import { NextResponse } from "next/server";

import { getRaceDetail, getRaceSourceByRoute } from "../../../../../../../../../../db/queries";
import { safeGetCloudflareExecutionContext } from "../../../../../../../../../../lib/cloudflare-context.server";
import {
  buildFinishPredictionInputsCacheKey,
  deleteFinishPredictionInputsCache,
  type FinishPredictionStaticPayload,
  getCachedFinishPredictionInputs,
  putFinishPredictionInputsCache,
} from "../../../../../../../../../../lib/finish-prediction-inputs-cache.server";
import {
  buildDetailSectionCacheKey,
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
] as const satisfies readonly DetailSection[];

const NON_EMPTY_MODEL_PREDICTION_FEATURES_MARKER = '"modelPredictionFeatures":[{';

const FINISH_PREDICTION_BROWSER_CACHE_CONTROL = "private, no-cache, max-age=0, must-revalidate";

const HTTP_STATUS_NO_CONTENT = 204;
const HTTP_STATUS_METHOD_NOT_ALLOWED = 405;

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

export async function GET(request: Request, { params }: DetailSectionRouteProps) {
  const { day, keibajoCode, month, raceNumber, section, year } = await params;
  if (!isValidSection(section) || !isValidParams(year, month, day, keibajoCode, raceNumber)) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }

  const requestUrl = new URL(request.url);
  const sectionSearchParams = stripDetailSectionCacheWarmParams(requestUrl.searchParams);
  const defaultSectionRequest =
    stripDetailSectionCacheWarmParams(requestUrl.searchParams).toString() === "";
  const cacheableDefaultRequest =
    isDefaultDetailSectionCacheRequest(section, requestUrl.searchParams) && defaultSectionRequest;
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
    cacheKey && !skipPredictionRefresh ? await getCachedDetailSectionResponse(cacheKey) : null;
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
  if (cacheKey) {
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
    if (cacheKey) {
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
