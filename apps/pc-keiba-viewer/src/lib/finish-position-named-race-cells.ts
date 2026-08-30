// Run with bun (bunx vitest)
//
// Compact catalog of JRA venue+race-name cells. Adding a named race is one
// row here (and the matching named_race_cells.json in the predict container);
// do not expand it into per-race variants/rules inside
// finish-position-cell-routing.ts. Lookup is O(1) by category+venue+token so
// hundreds of named races do not linearly scan in front of class rules.

export interface NamedRacePreraceWhen {
  kyori?: string[];
  month?: string[];
}

export interface NamedRacePreraceRoute {
  model_version?: string;
  variant?: string;
  when: NamedRacePreraceWhen;
}

export interface NamedRacePreraceLockRoute {
  model_version: string;
  variant: string;
  when: NamedRacePreraceWhen;
}

export interface NamedRacePreraceRouter {
  routes: NamedRacePreraceRoute[];
}

export interface NamedRaceCell {
  architecture?: string;
  base_variant: string;
  effective_after?: string;
  feature_count?: number;
  model_version?: string;
  prerace_router?: NamedRacePreraceRouter;
  race_name_token: string;
  rerank_feature_count?: number;
  rerank_model_version?: string;
  routing_mode?: string;
  variant: string;
  venue: string;
}

export interface NamedRaceLookupKeyParams {
  category: string;
  raceNameToken: string;
  venue: string;
}

export interface NamedRaceVariantBase {
  architecture: string;
  feature_count: number;
  model_version: string;
}

export interface NamedRaceInheritedVariant extends NamedRaceVariantBase {
  base_variant: string;
  rerank_model_version?: string;
  routing_mode?: string;
}

export interface NamedRaceVariantsForCategoryParams {
  cells: readonly NamedRaceCell[];
  variants: Record<string, NamedRaceVariantBase>;
}

export const NAMED_RACE_CELLS_BY_CATEGORY: Record<string, NamedRaceCell[]> = {
  jra: [
    {
      architecture: "catboost",
      base_variant: "joken_999",
      feature_count: 113,
      model_version: "jra-named-niigata-bsn-v1",
      race_name_token: "BSN賞",
      variant: "niigata_bsn",
      venue: "04",
    },
    {
      architecture: "catboost",
      base_variant: "joken_999",
      feature_count: 113,
      model_version: "jra-named-sapporo-suzuran-v2",
      race_name_token: "すずらん賞",
      variant: "sapporo_suzuran",
      venue: "01",
    },
    {
      architecture: "catboost",
      base_variant: "joken_999",
      feature_count: 113,
      model_version: "jra-named-chukyo-nisai-stakes-focal-v3",
      race_name_token: "中京2歳ステークス",
      rerank_feature_count: 113,
      prerace_router: {
        routes: [
          {
            model_version: "jra-named-chukyo-nisai-stakes-jul1600-ninki2-v4",
            variant: "chukyo_nisai_stakes_jul1600_ninki2",
            when: {
              kyori: ["1600"],
              month: ["07"],
            },
          },
          {
            model_version: "jra-named-chukyo-nisai-stakes-largefield-v3",
            variant: "chukyo_nisai_stakes_largefield",
            when: {
              kyori: ["1200"],
              month: ["12"],
            },
          },
          {
            when: {
              kyori: ["1400"],
              month: ["07", "08"],
            },
          },
        ],
      },
      rerank_model_version: "jra-named-chukyo-nisai-stakes-v2",
      routing_mode: "jra_lock1_rerank_rest",
      variant: "chukyo_nisai_stakes",
      venue: "07",
    },
    {
      architecture: "catboost",
      base_variant: "joken_999",
      feature_count: 113,
      model_version: "jra-named-niigata-kinen-draw-v2",
      race_name_token: "新潟記念",
      rerank_feature_count: 114,
      rerank_model_version: "jra-named-niigata-kinen-going-v2",
      routing_mode: "jra_lock1_rerank_rest",
      variant: "niigata_kinen",
      venue: "04",
    },
  ],
};

const LOOKUP_KEY_SEPARATOR: string = "\0";

export const namedRaceLookupKey = (params: NamedRaceLookupKeyParams): string =>
  `${params.category}${LOOKUP_KEY_SEPARATOR}${params.venue}${LOOKUP_KEY_SEPARATOR}${params.raceNameToken}`;

const variantCollisionKey = (category: string, variant: string): string =>
  `${category}${LOOKUP_KEY_SEPARATOR}${variant}`;

interface NamedRaceCellPair {
  category: string;
  cell: NamedRaceCell;
}

const flattenNamedRaceCells = (
  cellsByCategory: Record<string, readonly NamedRaceCell[]>,
): NamedRaceCellPair[] =>
  Object.entries(cellsByCategory).flatMap(([category, cells]) =>
    cells.map((cell) => ({ category, cell })),
  );

const duplicateNamedRacePair = (
  pairs: readonly NamedRaceCellPair[],
  keyOf: (pair: NamedRaceCellPair) => string,
): NamedRaceCellPair | undefined =>
  pairs.find(
    (pair, index) => pairs.findIndex((candidate) => keyOf(candidate) === keyOf(pair)) !== index,
  );

export const buildNamedRaceCellIndex = (
  cellsByCategory: Record<string, readonly NamedRaceCell[]>,
): Map<string, NamedRaceCell> => {
  const pairs = flattenNamedRaceCells(cellsByCategory);
  const duplicateVenue = duplicateNamedRacePair(pairs, (pair) =>
    namedRaceLookupKey({
      category: pair.category,
      raceNameToken: pair.cell.race_name_token,
      venue: pair.cell.venue,
    }),
  );
  if (duplicateVenue !== undefined) {
    throw new Error(
      `Duplicate named-race cell for category=${duplicateVenue.category} venue=${duplicateVenue.cell.venue} token=${duplicateVenue.cell.race_name_token}`,
    );
  }
  const duplicateVariant = duplicateNamedRacePair(pairs, (pair) =>
    variantCollisionKey(pair.category, pair.cell.variant),
  );
  if (duplicateVariant !== undefined) {
    throw new Error(
      `Duplicate named-race variant '${duplicateVariant.cell.variant}' in category=${duplicateVariant.category}`,
    );
  }
  return new Map(
    pairs.map((pair) => [
      namedRaceLookupKey({
        category: pair.category,
        raceNameToken: pair.cell.race_name_token,
        venue: pair.cell.venue,
      }),
      pair.cell,
    ]),
  );
};

export const NAMED_RACE_CELL_INDEX: Map<string, NamedRaceCell> = buildNamedRaceCellIndex(
  NAMED_RACE_CELLS_BY_CATEGORY,
);

export const inheritNamedRaceVariant = (
  cell: NamedRaceCell,
  base: NamedRaceVariantBase,
): NamedRaceInheritedVariant =>
  cell.rerank_model_version === undefined && cell.routing_mode === undefined
    ? {
        architecture: cell.architecture ?? base.architecture,
        base_variant: cell.base_variant,
        feature_count: cell.feature_count ?? base.feature_count,
        model_version: cell.model_version ?? base.model_version,
      }
    : {
        architecture: cell.architecture ?? base.architecture,
        base_variant: cell.base_variant,
        feature_count: cell.feature_count ?? base.feature_count,
        model_version: cell.model_version ?? base.model_version,
        rerank_model_version: cell.rerank_model_version,
        routing_mode: cell.routing_mode,
      };

export const isNamedRacePreraceLockRoute = (
  route: NamedRacePreraceRoute,
): route is NamedRacePreraceLockRoute =>
  route.variant !== undefined &&
  route.model_version !== undefined &&
  route.variant.length > 0 &&
  route.model_version.length > 0;

const withNamedRaceModelVersion = (
  inherited: NamedRaceInheritedVariant,
  modelVersion: string,
): NamedRaceInheritedVariant =>
  inherited.rerank_model_version === undefined && inherited.routing_mode === undefined
    ? {
        architecture: inherited.architecture,
        base_variant: inherited.base_variant,
        feature_count: inherited.feature_count,
        model_version: modelVersion,
      }
    : {
        architecture: inherited.architecture,
        base_variant: inherited.base_variant,
        feature_count: inherited.feature_count,
        model_version: modelVersion,
        rerank_model_version: inherited.rerank_model_version,
        routing_mode: inherited.routing_mode,
      };

const preraceLockVariantPairs = (
  cell: NamedRaceCell,
  inherited: NamedRaceInheritedVariant,
): [string, NamedRaceInheritedVariant][] =>
  (cell.prerace_router?.routes ?? [])
    .filter(isNamedRacePreraceLockRoute)
    .filter((route) => route.variant !== cell.variant)
    .map((route) => [route.variant, withNamedRaceModelVersion(inherited, route.model_version)]);

export const namedRaceVariantsForCategory = (
  params: NamedRaceVariantsForCategoryParams,
): Record<string, NamedRaceInheritedVariant> => {
  const pairs = params.cells.flatMap<[string, NamedRaceInheritedVariant]>((cell) => {
    if (params.variants[cell.variant] !== undefined) {
      throw new Error(`Named-race variant '${cell.variant}' already exists in the category`);
    }
    const base = params.variants[cell.base_variant];
    if (base === undefined) {
      throw new Error(
        `Named-race cell '${cell.variant}' references missing base_variant '${cell.base_variant}'`,
      );
    }
    const inherited = inheritNamedRaceVariant(cell, base);
    const extras = preraceLockVariantPairs(cell, inherited);
    const extraCollision = extras.find((pair) => params.variants[pair[0]] !== undefined);
    if (extraCollision !== undefined) {
      throw new Error(`Named-race variant '${extraCollision[0]}' already exists in the category`);
    }
    return [[cell.variant, inherited], ...extras];
  });
  const duplicate = pairs.find(
    (pair, index) => pairs.findIndex((candidate) => candidate[0] === pair[0]) !== index,
  );
  if (duplicate !== undefined) {
    throw new Error(`Named-race variant '${duplicate[0]}' already exists in the category`);
  }
  return Object.fromEntries(pairs);
};

export const lookupNamedRaceCell = (params: NamedRaceLookupKeyParams): NamedRaceCell | undefined =>
  NAMED_RACE_CELL_INDEX.get(namedRaceLookupKey(params));
