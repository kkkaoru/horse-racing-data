// Runs with Bun for tests and in the browser behind the existing Access origin.
"use client";

import { useEffect, useState } from "react";

import {
  readArcPrediction,
  type ArcPredictionCard,
  type ArcRunner,
} from "../../../lib/arc-prediction";
import { useRealtimeRacePayload } from "./realtime-client";

interface ArcPredictionTableProps {
  runners: readonly ArcRunner[];
}
interface ArcResult {
  runners: readonly ArcRunner[];
  card: ArcPredictionCard | null;
}

const ARC_ENDPOINT: string = "/api/arc-prediction/2026-10-04";

export function ArcPredictionTable({ runners }: ArcPredictionTableProps) {
  const { payload: realtimePayload } = useRealtimeRacePayload(
    { apiBaseUrl: "", source: "", year: "", month: "", day: "", keibajoCode: "", raceNumber: "" },
    null,
  );
  const odds = new Map(
    (realtimePayload?.odds?.latest.tansho ?? []).map((row) => [Number(row.combination), row]),
  );
  const [result, setResult] = useState<ArcResult | null>(null);
  useEffect(() => {
    const controller: AbortController = new AbortController();
    const load = async (): Promise<void> => {
      try {
        const response: Response = await fetch(ARC_ENDPOINT, {
          credentials: "same-origin",
          signal: controller.signal,
          cache: "no-store",
        });
        if (!response.ok) throw new Error("Arc prediction request failed");
        const payload: unknown = await response.json();
        const card: ArcPredictionCard = readArcPrediction(payload, runners);
        if (!controller.signal.aborted) setResult({ runners, card });
      } catch {
        if (!controller.signal.aborted) setResult({ runners, card: null });
      }
    };
    void load();
    return () => controller.abort();
  }, [runners]);

  if (result === null || result.runners !== runners)
    return <output>凱旋門賞専用予測を読み込み中…</output>;
  if (result.card === null)
    return (
      <p role="alert">
        凱旋門賞専用予測を確認できません。出走表の変更または通信状況をご確認ください。
      </p>
    );
  const card: ArcPredictionCard = result.card;
  return (
    <section aria-label="凱旋門賞専用予測">
      <h2>凱旋門賞専用 着順予測</h2>
      <p>
        専用モデルの予測順位です。国内モデルの補正や並べ替えは適用していません。勝率・複勝率は未提供です。
      </p>
      <p>
        入力は固定スナップショット（{card.featureSnapshotAt}
        ）です。最新のオッズ・馬場・履歴を自動反映した予測ではありません。
      </p>
      <p>
        2023～2025年の評価結果はモデル選択にも使用されています。独立した検証精度や将来の的中率を示すものではありません。
      </p>
      <p>
        モデル: {card.modelVersion} ／ 学習データ最終日: {card.trainingMaxDate}
      </p>
      <table className="runner-table">
        <caption>専用モデルの予測順位（確率ではありません）</caption>
        <thead>
          <tr>
            <th scope="col">予測順位</th>
            <th scope="col">馬番</th>
            <th scope="col">馬名</th>
            <th scope="col">単勝</th>
            <th scope="col">人気</th>
          </tr>
        </thead>
        <tbody>
          {card.predictions.map((row) => (
            <tr key={row.horseId}>
              <td>{row.predictedRank}</td>
              <td>{row.umaban}</td>
              <td>{row.name}</td>
              <td>{odds.get(row.umaban)?.odds?.toFixed(1) ?? "-"}</td>
              <td>{odds.get(row.umaban)?.rank ?? "-"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}
