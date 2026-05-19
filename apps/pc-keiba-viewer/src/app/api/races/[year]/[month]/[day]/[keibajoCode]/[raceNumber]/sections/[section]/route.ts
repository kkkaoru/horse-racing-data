import { NextResponse } from "next/server";

import { getRaceDetail, getRaceSourceByRoute } from "../../../../../../../../../../db/queries";
import {
  buildDetailSectionCacheKey,
  isDefaultDetailSectionCacheRequest,
  stripDetailSectionCacheWarmParams,
} from "../../../../../../../../../../lib/race-detail-section-cache";
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

export async function GET(request: Request, { params }: DetailSectionRouteProps) {
  const { day, keibajoCode, month, raceNumber, section, year } = await params;
  if (!isValidSection(section) || !isValidParams(year, month, day, keibajoCode, raceNumber)) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }

  const requestUrl = new URL(request.url);
  const sectionSearchParams = stripDetailSectionCacheWarmParams(requestUrl.searchParams);
  const cacheableDefaultRequest = isDefaultDetailSectionCacheRequest(
    section,
    requestUrl.searchParams,
  );
  const cacheKey = cacheableDefaultRequest
    ? buildDetailSectionCacheKey({ day, keibajoCode, month, raceNumber, section, year })
    : null;
  const cachedResponse = cacheKey ? await getCachedDetailSectionResponse(cacheKey) : null;
  if (cachedResponse) {
    return cachedResponse;
  }

  const raceSource = await getRaceSourceByRoute(year, month, day, keibajoCode, raceNumber);
  if (!raceSource) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }

  const race = cacheKey
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
  if (cacheKey && race) {
    await putDetailSectionCache({ body, cacheKey, race });
  }

  return new NextResponse(body, {
    headers: {
      "Cache-Control": "private, max-age=0, no-store",
      "Content-Type": "application/json; charset=utf-8",
      "X-Detail-Section-Cache": cacheKey ? "MISS-STORED" : "BYPASS",
    },
  });
}
