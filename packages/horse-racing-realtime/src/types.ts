export type RealtimeOddsType =
  | "3renpuku"
  | "3rentan"
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

export interface RealtimeRaceSource {
  babaCode: string;
  debaUrl: string;
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
}

export interface RealtimeRacePayload {
  horseWeights: {
    fetchedAt: string;
    horses: RealtimeHorseWeight[];
  } | null;
  odds: {
    fetchedAt: string;
    horseTrends: RealtimeHorseOddsTrend[];
    history: RealtimeOddsHistoryPoint[];
    latest: Partial<Record<RealtimeOddsType, RealtimeOddsData[]>>;
  } | null;
  raceKey: string;
  source: RealtimeRaceSource | null;
}
