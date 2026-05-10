"use client";

import {
  cleanText,
  formatDate,
  formatDistance,
  formatKeibajo,
  formatRaceNumber,
  formatTrack,
  formatWeather,
} from "../../../lib/format";
import type { AbilityTest } from "../../../lib/race-types";
import {
  formatCarriedWeight,
  formatHorseWeight,
  formatRunnerNumber,
  formatRunnerValue,
  formatSexAge,
} from "../../../lib/runner-format";

interface AbilityTestTableProps {
  abilityTests: AbilityTest[];
}

const ABILITY_TEST_TYPE_LABELS: Record<string, string> = {
  "1": "能力調教試験",
  "2": "馬検査",
  "3": "その他",
};

const GOHI_LABELS: Record<string, string> = {
  "1": "合格",
  "2": "不合格",
  "3": "不成立",
};

const REASON_LABELS: Record<string, string> = {
  "1": "発走調教不良",
  "2": "競走調教不良",
  "3": "タイムオーバー",
  "4": "不明",
  "5": "不明",
};

const STYLE_LABELS: Record<string, string> = {
  "+": "前進",
  "-": "後退",
  "0": "標準",
};

const parseNumber = (value: string | null | undefined): number | null => {
  const cleaned = cleanText(value, "");
  if (!cleaned || /^0+$/.test(cleaned) || /^9+$/.test(cleaned)) {
    return null;
  }
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
};

const formatTenthsTime = (value: string | null | undefined): string => {
  const tenths = parseNumber(value);
  if (tenths === null) {
    return "-";
  }
  const minutes = Math.floor(tenths / 600);
  const seconds = Math.floor((tenths % 600) / 10);
  const remainder = tenths % 10;
  return minutes > 0
    ? `${minutes}:${String(seconds).padStart(2, "0")}.${remainder}`
    : `${seconds}.${remainder}`;
};

const formatCodeLabel = (
  value: string | null | undefined,
  labels: Record<string, string>,
  fallback: string,
): string => {
  const cleaned = cleanText(value, "");
  if (!cleaned || /^0+$/.test(cleaned)) {
    return "-";
  }
  return labels[cleaned] ?? `${fallback} ${cleaned}`;
};

const formatPassDate = (value: string | null | undefined): string => {
  const cleaned = cleanText(value, "");
  return cleaned.length === 8 ? formatDate(cleaned.slice(0, 4), cleaned.slice(4)) : "-";
};

const formatCorner = (...values: (string | null | undefined)[]): string => {
  const corners = values
    .map((value) => formatRunnerValue(value, "00"))
    .filter((value) => value !== "-");
  return corners.length > 0 ? corners.join(" - ") : "-";
};

const formatOpponent = (...values: (string | null | undefined)[]): string => {
  const opponents = values.map((value) => cleanText(value, "")).filter(Boolean);
  return opponents.length > 0 ? opponents.join(" / ") : "-";
};

export function AbilityTestTable({ abilityTests }: AbilityTestTableProps) {
  if (abilityTests.length === 0) {
    return <p className="empty-state">出走予定馬に紐づく能力検査データは見つかりませんでした。</p>;
  }

  return (
    <div className="ability-test-table-wrap">
      <table className="ability-test-table">
        <thead>
          <tr>
            <th>馬番号</th>
            <th>馬名</th>
            <th>検査日</th>
            <th>競馬場</th>
            <th>R</th>
            <th>種別</th>
            <th>合否</th>
            <th>理由</th>
            <th>合否日</th>
            <th>検査馬番</th>
            <th>検査馬名</th>
            <th>性齢</th>
            <th>負担</th>
            <th>騎手</th>
            <th>調教師</th>
            <th>馬体重</th>
            <th>順位</th>
            <th>走破</th>
            <th>後4F</th>
            <th>後3F</th>
            <th>距離</th>
            <th>コース</th>
            <th>天候</th>
            <th>馬場</th>
            <th>コーナー</th>
            <th>脚色</th>
            <th>脚質</th>
            <th>相手馬</th>
          </tr>
        </thead>
        <tbody>
          {abilityTests.map((test) => (
            <tr
              key={[
                test.currentUmaban,
                test.kaisaiNen,
                test.kaisaiTsukihi,
                test.keibajoCode,
                test.raceBango,
                test.umaban,
              ].join("-")}
            >
              <td>{formatRunnerNumber(test.currentUmaban)}</td>
              <td className="ability-test-name-cell">{cleanText(test.currentBamei)}</td>
              <td>{formatDate(test.kaisaiNen, test.kaisaiTsukihi)}</td>
              <td>{formatKeibajo(test.keibajoCode)}</td>
              <td>{formatRaceNumber(test.raceBango)}</td>
              <td>{formatCodeLabel(test.noryokuShikenCode, ABILITY_TEST_TYPE_LABELS, "種別")}</td>
              <td>{formatCodeLabel(test.gohiCode, GOHI_LABELS, "合否")}</td>
              <td>{formatCodeLabel(test.riyuCode, REASON_LABELS, "理由")}</td>
              <td>{formatPassDate(test.gohiNengappi)}</td>
              <td>{formatRunnerNumber(test.umaban)}</td>
              <td className="ability-test-name-cell">{cleanText(test.bamei)}</td>
              <td>{formatSexAge(test.seibetsuCode, test.barei)}</td>
              <td>{formatCarriedWeight(test.futanJuryo, test.keibajoCode === "83")}</td>
              <td>{cleanText(test.kishumeiRyakusho, "-")}</td>
              <td>{cleanText(test.chokyoshimeiRyakusho, "-")}</td>
              <td>
                {formatHorseWeight(
                  test.bataiju,
                  test.zogenFugo,
                  test.zogenSa,
                  test.keibajoCode === "83",
                )}
              </td>
              <td>{formatRunnerValue(test.juni, "00")}</td>
              <td>{formatTenthsTime(test.sohaTime)}</td>
              <td>{formatTenthsTime(test.kohan4f)}</td>
              <td>{formatTenthsTime(test.kohan3f)}</td>
              <td>{formatDistance(test.kyori)}</td>
              <td>{formatTrack(test.trackCode)}</td>
              <td>{formatWeather(test.tenkoCode)}</td>
              <td>{formatRunnerValue(test.babajotaiCodeDirt, "0")}</td>
              <td>{formatCorner(test.corner1, test.corner2, test.corner3, test.corner4)}</td>
              <td>{formatRunnerValue(test.ashiiroCode, "0")}</td>
              <td>{formatCodeLabel(test.kyakushitsuHantei, STYLE_LABELS, "脚質")}</td>
              <td className="ability-test-opponent-cell">
                {formatOpponent(test.aiteumaJoho1, test.aiteumaJoho2, test.aiteumaJoho3)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
