"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import type { RaceSource } from "../../../lib/codes";
import { fetchWithRetry } from "../../../lib/fetch-with-retry";
import { isCornerPacePredictionSupported } from "../../../lib/race-pace-prediction";
import type { CourseInfo, RaceDetail, RaceListItem, Runner } from "../../../lib/race-types";

interface AiJsonExportSectionProps {
  basePostgresqlData: {
    courseInfo: CourseInfo | null;
    race: RaceDetail;
    raceDayRaces: RaceListItem[];
    runners: Runner[];
  };
  baseProcessedData: Record<string, unknown>;
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: RaceSource;
  year: string;
}

type SectionPayloads = Record<string, unknown>;

type ExportData = {
  meta: {
    generatedAt: string;
    purpose: string;
    route: {
      day: string;
      keibajoCode: string;
      month: string;
      raceNumber: string;
      source: RaceSource;
      year: string;
    };
  };
  postgresql: {
    base: AiJsonExportSectionProps["basePostgresqlData"];
    sections: SectionPayloads | null;
  };
  processedForDisplay: {
    base: AiJsonExportSectionProps["baseProcessedData"];
    sections: SectionPayloads | null;
  };
};

const SECTIONS = [
  "results",
  "time-score",
  "training",
  "ability",
  "condition",
  "bloodline",
  "similar",
  "pace-prediction",
  "overall-score",
];

const getSectionUrl = ({
  day,
  keibajoCode,
  month,
  raceNumber,
  section,
  year,
}: {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  section: string;
  year: string;
}): string => {
  const query = typeof window === "undefined" ? "" : window.location.search;
  return `/api/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/sections/${section}${query}`;
};

const fetchSectionPayloads = async (
  props: Pick<AiJsonExportSectionProps, "day" | "keibajoCode" | "month" | "raceNumber" | "year">,
  sections: string[],
): Promise<SectionPayloads> => {
  const entries = await Promise.all(
    sections.map(async (section) => {
      const response = await fetchWithRetry(getSectionUrl({ ...props, section }));
      if (!response.ok) {
        return [
          section,
          {
            error: `${response.status} ${response.statusText}`.trim(),
          },
        ];
      }
      return [section, await response.json()];
    }),
  );
  return Object.fromEntries(entries);
};

export function AiJsonExportSection({
  basePostgresqlData,
  baseProcessedData,
  day,
  keibajoCode,
  month,
  raceNumber,
  source,
  year,
}: AiJsonExportSectionProps) {
  const [copyStatus, setCopyStatus] = useState<"copied" | "error" | "idle">("idle");
  const [jsonText, setJsonText] = useState<string | null>(null);
  const [isGenerating, setIsGenerating] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [showJson, setShowJson] = useState(false);
  const [sectionPayloads, setSectionPayloads] = useState<SectionPayloads | null>(null);
  const copyStatusTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const sections = useMemo(
    () =>
      isCornerPacePredictionSupported({
        distance: basePostgresqlData.race.kyori,
        keibajoCode,
        source,
      })
        ? SECTIONS
        : SECTIONS.filter((section) => section !== "pace-prediction"),
    [basePostgresqlData.race.kyori, keibajoCode, source],
  );

  useEffect(
    () => () => {
      if (copyStatusTimer.current) {
        clearTimeout(copyStatusTimer.current);
      }
    },
    [],
  );

  const exportData = useMemo<ExportData>(
    () => ({
      meta: {
        generatedAt: new Date().toISOString(),
        purpose: "AIに渡すためのレース詳細ページ表示データ",
        route: {
          day,
          keibajoCode,
          month,
          raceNumber,
          source,
          year,
        },
      },
      postgresql: {
        base: basePostgresqlData,
        sections: sectionPayloads,
      },
      processedForDisplay: {
        base: baseProcessedData,
        sections: sectionPayloads,
      },
    }),
    [
      basePostgresqlData,
      baseProcessedData,
      day,
      keibajoCode,
      month,
      raceNumber,
      sectionPayloads,
      source,
      year,
    ],
  );

  const scheduleCopyStatusReset = () => {
    if (copyStatusTimer.current) {
      clearTimeout(copyStatusTimer.current);
    }
    copyStatusTimer.current = setTimeout(() => {
      setCopyStatus("idle");
    }, 1800);
  };

  const loadSectionPayloads = useCallback(async (): Promise<SectionPayloads> => {
    if (sectionPayloads) {
      return sectionPayloads;
    }
    setIsLoading(true);
    setCopyStatus("idle");
    try {
      const payloads = await fetchSectionPayloads(
        {
          day,
          keibajoCode,
          month,
          raceNumber,
          year,
        },
        sections,
      );
      setSectionPayloads(payloads);
      return payloads;
    } finally {
      setIsLoading(false);
    }
  }, [day, keibajoCode, month, raceNumber, sectionPayloads, sections, year]);

  const generateJson = useCallback(
    async (payloads: SectionPayloads | null): Promise<string> => {
      setIsGenerating(true);
      try {
        await new Promise((resolve) => {
          window.setTimeout(resolve, 0);
        });
        return JSON.stringify(
          {
            ...exportData,
            meta: {
              ...exportData.meta,
              generatedAt: new Date().toISOString(),
            },
            postgresql: {
              ...exportData.postgresql,
              sections: payloads,
            },
            processedForDisplay: {
              ...exportData.processedForDisplay,
              sections: payloads,
            },
          },
          null,
          2,
        );
      } finally {
        setIsGenerating(false);
      }
    },
    [exportData],
  );

  const ensureJson = async (): Promise<string> => {
    const payloads = sectionPayloads ?? (await loadSectionPayloads());
    const nextJson = await generateJson(payloads);
    setJsonText(nextJson);
    return nextJson;
  };

  const copyJson = async () => {
    try {
      setCopyStatus("idle");
      await navigator.clipboard.writeText(await ensureJson());
      setCopyStatus("copied");
      scheduleCopyStatusReset();
    } catch {
      setCopyStatus("error");
      scheduleCopyStatusReset();
    }
  };

  const toggleJson = async () => {
    if (showJson) {
      setShowJson(false);
      return;
    }
    setShowJson(true);
    if (!jsonText) {
      await ensureJson();
    }
  };

  return (
    <section className="ai-json-export-section">
      <details
        onToggle={(event) => {
          if (event.currentTarget.open) {
            void loadSectionPayloads();
          }
        }}
      >
        <summary>AI向けJSON出力</summary>
        <div className="ai-json-export-actions">
          <button type="button" onClick={copyJson}>
            JSONをコピー
          </button>
          <button type="button" onClick={() => void toggleJson()}>
            {showJson ? "JSONを隠す" : "JSONを表示"}
          </button>
          <span>
            {isLoading
              ? "取得中"
              : isGenerating
                ? "JSON生成中"
                : copyStatus === "copied"
                  ? "コピーしました"
                  : copyStatus === "error"
                    ? "コピーできませんでした"
                    : sectionPayloads
                      ? "取得済み"
                      : "未取得"}
          </span>
        </div>
        {showJson && jsonText ? <pre className="ai-json-export-code">{jsonText}</pre> : null}
      </details>
    </section>
  );
}
