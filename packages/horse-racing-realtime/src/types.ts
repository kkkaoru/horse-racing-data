export type RealtimeOddsType =
  | "3renpuku"
  | "3rentan"
  | "fukusho"
  | "tansho"
  | "umaren"
  | "umatan"
  | "wakuren"
  | "wide";

export interface RealtimeOddsData {
  averageOdds?: number;
  combination: string;
  maxOdds?: number;
  minOdds?: number;
  odds?: number;
  rank?: number;
}

export interface RealtimeHorseWeight {
  changeAmount: number | null;
  changeSign: string | null;
  horseName: string | null;
  horseNumber: string;
  weight: number | null;
}

export interface RealtimeRaceEntry {
  fetchedAt: string;
  horseName: string | null;
  horseNumber: string;
  jockeyName: string | null;
  status: string | null;
}

export interface RealtimeRaceResult {
  fetchedAt: string;
  finishPosition: string;
  horseName: string | null;
  horseNumber: string;
  time: string | null;
}

export interface RealtimeOddsHistoryPoint {
  fetchedAt: string;
  horseNumber: string;
  odds: number | null;
  popularity: number | null;
}

export interface RealtimeHorseOddsTrend {
  horseNumber: string;
  points: RealtimeOddsHistoryPoint[];
}

export interface RealtimeOddsTrendPoint {
  combination: string;
  fetchedAt: string;
  odds: number | null;
  rank: number | null;
}

export interface RealtimeOddsTrend {
  combination: string;
  points: RealtimeOddsTrendPoint[];
}

export interface RealtimeRaceSource {
  babaCode: string;
  debaUrl: string;
  kaisaiKai?: string | null;
  kaisaiNichime?: string | null;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  lastOddsFetchAt: string | null;
  lastWeightFetchAt: string | null;
  oddsLinks: Partial<Record<RealtimeOddsType, string>>;
  raceBango: string;
  raceKey: string;
  raceName: string | null;
  raceStartAtJst: string;
  source: "jra" | "nar";
}

export interface RealtimeTrackCondition {
  dirt: {
    condition: string | null;
    measurementDate: string | null;
    moisture: {
      finalBend: string | null;
      finalFurlong: string | null;
      measuredAt: string | null;
    };
  };
  fetchedAt: string;
  sourceUpdatedAt: string | null;
  turf: {
    condition: string | null;
    courseLayout: string | null;
    cushionValue: string | null;
    cushionMeasuredAt: string | null;
    going: string | null;
    height: {
      japaneseZoysiaGrass: string | null;
      perennialRyegrass: string | null;
    };
    measurementDate: string | null;
    moisture: {
      finalBend: string | null;
      finalFurlong: string | null;
      measuredAt: string | null;
    };
  };
  weather: string | null;
}

export interface RealtimeRacePayload {
  raceEntries?: {
    fetchedAt: string;
    horses: RealtimeRaceEntry[];
  } | null;
  horseWeights: {
    fetchedAt: string;
    horses: RealtimeHorseWeight[];
  } | null;
  odds: {
    fetchedAt: string;
    horseTrends: RealtimeHorseOddsTrend[];
    history: RealtimeOddsHistoryPoint[];
    historyByType?: Partial<Record<RealtimeOddsType, RealtimeOddsTrendPoint[]>>;
    latest: Partial<Record<RealtimeOddsType, RealtimeOddsData[]>>;
    trendsByType?: Partial<Record<RealtimeOddsType, RealtimeOddsTrend[]>>;
  } | null;
  raceResults: {
    fetchedAt: string;
    horses: RealtimeRaceResult[];
  } | null;
  trackCondition?: RealtimeTrackCondition | null;
  raceKey: string;
  source: RealtimeRaceSource | null;
}
