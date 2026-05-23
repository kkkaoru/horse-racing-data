import { NextResponse } from "next/server";

import { getRaceDetail, getRaceSourceByRoute } from "../../../../../../../../../../db/queries";
import {
  buildDetailSectionCacheKey,
  isDefaultDetailSectionCacheRequest,
  stripDetailSectionCacheWarmParams,
} from "../../../../../../../../../../lib/race-detail-section-cache";
import {
  buildFinishPredictionInputsCacheKey,
  getCachedFinishPredictionInputs,
  putFinishPredictionInputsCache,
} from "../../../../../../../../../../lib/finish-prediction-inputs-cache.server";
import {
  getCachedDetailSectionResponse,
  putDetailSectionCache,
} from "../../../../../../../../../../lib/race-detail-section-cache.server";
import {
  getDetailSectionPayload,
  type DetailSection,
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
  if (finishPredictionInputsCacheKey) {
    const cachedStatic = await getCachedFinishPredictionInputs(finishPredictionInputsCacheKey);
    if (cachedStatic) {
      return NextResponse.json({
        evaluation: cachedStatic.evaluation,
        inputs: cachedStatic.inputs,
        type: "finish-prediction",
      });
    }
  }

  const cacheKey = cacheableDefaultRequest
    ? buildDetailSectionCacheKey({ day, keibajoCode, month, raceNumber, section, year })
    : null;
  const cachedResponse = cacheKey ? await getCachedDetailSectionResponse(cacheKey) : null;
  if (cachedResponse) {
    if (section === "premium-data-top") {
      const cachedBody = await cachedResponse.clone().text();
      if (!isEmptyPremiumDataTopSectionBody(cachedBody)) {
        return cachedResponse;
      }
    } else {
      return cachedResponse;
    }
  }

  const raceSource = await getRaceSourceByRoute(year, month, day, keibajoCode, raceNumber);
  if (!raceSource) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
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
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }

  const body = JSON.stringify(payload);
  if (
    finishPredictionInputsCacheKey &&
    race &&
    payload.type === "finish-prediction" &&
    "inputs" in payload &&
    "evaluation" in payload
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
  if (cacheKey && race && !(section === "premium-data-top" && isEmptyPremiumDataTopSectionBody(body))) {
    await putDetailSectionCache({ body, cacheKey, race });
  }

  return new NextResponse(body, {
    headers: {
      "Cache-Control": "private, max-age=0, no-store",
      "Content-Type": "application/json; charset=utf-8",
      "X-Detail-Section-Cache": finishPredictionInputsCacheKey
        ? "FINISH-INPUTS-MISS"
        : cacheKey
          ? "MISS-STORED"
          : "BYPASS",
    },
  });
}
