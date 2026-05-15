"use client";

import type { RealtimeTrackCondition } from "horse-racing-realtime/types";

import { useRealtimeRaceSelector } from "./realtime-client";

type TrackConditionSurface = "both" | "dirt" | "turf";

const TURF_TRACK_CODES = new Set([
  "10",
  "11",
  "12",
  "13",
  "14",
  "15",
  "16",
  "17",
  "18",
  "19",
  "20",
  "21",
  "22",
  "51",
  "54",
  "55",
  "58",
  "59",
]);

const DIRT_TRACK_CODES = new Set(["23", "24", "25", "26", "27", "28", "29", "53"]);
const MIXED_TRACK_CODES = new Set(["52", "56", "57"]);

export const getTrackConditionSurface = (
  trackCode: string | null | undefined,
): TrackConditionSurface => {
  const code = trackCode?.trim() ?? "";
  if (TURF_TRACK_CODES.has(code)) {
    return "turf";
  }
  if (DIRT_TRACK_CODES.has(code)) {
    return "dirt";
  }
  if (MIXED_TRACK_CODES.has(code)) {
    return "both";
  }
  return "both";
};

const formatDateTime = (value: string | null | undefined): string => {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("ja-JP", {
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    month: "numeric",
    timeZone: "Asia/Tokyo",
  }).format(date);
};

const value = (text: string | null | undefined, suffix = ""): string =>
  text && text.trim().length > 0 ? `${text}${suffix}` : "-";

const SummaryMetric = ({ label, value: displayValue }: { label: string; value: string }) => (
  <div>
    <span>{label}</span>
    <strong>{displayValue}</strong>
  </div>
);

const TrackPanel = ({
  label,
  moisture,
}: {
  label: string;
  moisture: {
    finalBend: string | null;
    finalFurlong: string | null;
    measuredAt: string | null;
  };
}) => (
  <div className="track-condition-panel">
    <h3>{label}の測定値</h3>
    <dl>
      <div>
        <dt>含水率 ゴール前</dt>
        <dd>{value(moisture.finalFurlong, "%")}</dd>
      </div>
      <div>
        <dt>含水率 4角</dt>
        <dd>{value(moisture.finalBend, "%")}</dd>
      </div>
      <div>
        <dt>測定</dt>
        <dd>{formatDateTime(moisture.measuredAt)}</dd>
      </div>
    </dl>
  </div>
);

export function TrackConditionSection({ trackCode }: { trackCode: string | null }) {
  const trackCondition = useRealtimeRaceSelector((state) => state.payload?.trackCondition ?? null);

  if (!trackCondition) {
    return null;
  }

  const condition: RealtimeTrackCondition = trackCondition;
  const surface = getTrackConditionSurface(trackCode);
  const showTurf = surface === "turf" || surface === "both";
  const showDirt = surface === "dirt" || surface === "both";

  return (
    <section className="track-condition-section" aria-label="track condition">
      <div className="section-heading compact">
        <h2>馬場状態</h2>
        <span>取得 {formatDateTime(condition.fetchedAt)}</span>
      </div>
      <div className="track-condition-card">
        <div className="track-condition-summary">
          <SummaryMetric label="天候" value={value(condition.weather)} />
          {showTurf ? (
            <>
              <SummaryMetric label="芝" value={value(condition.turf.condition)} />
              <SummaryMetric label="クッション値" value={value(condition.turf.cushionValue)} />
              <SummaryMetric label="コース" value={value(condition.turf.courseLayout)} />
            </>
          ) : null}
          {showDirt ? (
            <SummaryMetric label="ダート" value={value(condition.dirt.condition)} />
          ) : null}
        </div>
        <details className="track-condition-disclosure">
          <summary>詳細を表示</summary>
          <div className="track-condition-panels">
            {showTurf ? <TrackPanel label="芝" moisture={condition.turf.moisture} /> : null}
            {showDirt ? <TrackPanel label="ダート" moisture={condition.dirt.moisture} /> : null}
          </div>
          {showTurf ? (
            <dl className="track-condition-details">
              <div>
                <dt>クッション測定</dt>
                <dd>{formatDateTime(condition.turf.cushionMeasuredAt)}</dd>
              </div>
              <div>
                <dt>野芝</dt>
                <dd>{value(condition.turf.height.japaneseZoysiaGrass)}</dd>
              </div>
              <div>
                <dt>洋芝</dt>
                <dd>{value(condition.turf.height.perennialRyegrass)}</dd>
              </div>
              <div>
                <dt>芝の状態</dt>
                <dd>{value(condition.turf.going)}</dd>
              </div>
            </dl>
          ) : null}
        </details>
      </div>
    </section>
  );
}
