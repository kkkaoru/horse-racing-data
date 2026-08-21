// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)
import { cleanText } from "./format";

export interface CarriedWeightClass {
  key: string;
  label: string;
  maxKg: number | null;
  minKg: number | null;
}

const SENTINEL_EMPTY_WEIGHT = "000";
const SENTINEL_UNKNOWN_WEIGHT = "FFF";
const CARRIED_WEIGHT_SCALE = 10;
const CARRIED_WEIGHT_LE_49_MAX_KG = 49;
const CARRIED_WEIGHT_49_5_51_MAX_KG = 51;
const CARRIED_WEIGHT_51_5_53_MAX_KG = 53;
const CARRIED_WEIGHT_53_5_55_MAX_KG = 55;
const CARRIED_WEIGHT_55_5_57_MAX_KG = 57;

const CARRIED_WEIGHT_CLASS_LE_49: CarriedWeightClass = {
  key: "le49",
  label: "49kg以下",
  maxKg: CARRIED_WEIGHT_LE_49_MAX_KG,
  minKg: null,
};
const CARRIED_WEIGHT_CLASS_49_5_51: CarriedWeightClass = {
  key: "49.5-51",
  label: "49.5以上51kg以下",
  maxKg: CARRIED_WEIGHT_49_5_51_MAX_KG,
  minKg: 49.5,
};
const CARRIED_WEIGHT_CLASS_51_5_53: CarriedWeightClass = {
  key: "51.5-53",
  label: "51.5kg以上53kg以下",
  maxKg: CARRIED_WEIGHT_51_5_53_MAX_KG,
  minKg: 51.5,
};
const CARRIED_WEIGHT_CLASS_53_5_55: CarriedWeightClass = {
  key: "53.5-55",
  label: "53.5kg以上55kg以下",
  maxKg: CARRIED_WEIGHT_53_5_55_MAX_KG,
  minKg: 53.5,
};
const CARRIED_WEIGHT_CLASS_55_5_57: CarriedWeightClass = {
  key: "55.5-57",
  label: "55.5kg以上57kg以下",
  maxKg: CARRIED_WEIGHT_55_5_57_MAX_KG,
  minKg: 55.5,
};
const CARRIED_WEIGHT_CLASS_57_5_59: CarriedWeightClass = {
  key: "57.5-59",
  label: "57.5kg以上59kg以下",
  maxKg: 59,
  minKg: 57.5,
};

export const CARRIED_WEIGHT_CLASSES: readonly CarriedWeightClass[] = [
  CARRIED_WEIGHT_CLASS_LE_49,
  CARRIED_WEIGHT_CLASS_49_5_51,
  CARRIED_WEIGHT_CLASS_51_5_53,
  CARRIED_WEIGHT_CLASS_53_5_55,
  CARRIED_WEIGHT_CLASS_55_5_57,
  CARRIED_WEIGHT_CLASS_57_5_59,
];

export const parseCarriedWeightKg = (futanJuryo: string | null | undefined): number | null => {
  const cleanWeight = cleanText(futanJuryo, "");
  if (
    cleanWeight === "" ||
    cleanWeight === SENTINEL_EMPTY_WEIGHT ||
    cleanWeight.toUpperCase() === SENTINEL_UNKNOWN_WEIGHT
  ) {
    return null;
  }
  const parsed = Number(cleanWeight) / CARRIED_WEIGHT_SCALE;
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
};

export const getCarriedWeightClass = (kg: number): CarriedWeightClass => {
  if (!Number.isFinite(kg)) {
    return CARRIED_WEIGHT_CLASS_57_5_59;
  }
  if (kg <= CARRIED_WEIGHT_LE_49_MAX_KG) {
    return CARRIED_WEIGHT_CLASS_LE_49;
  }
  if (kg <= CARRIED_WEIGHT_49_5_51_MAX_KG) {
    return CARRIED_WEIGHT_CLASS_49_5_51;
  }
  if (kg <= CARRIED_WEIGHT_51_5_53_MAX_KG) {
    return CARRIED_WEIGHT_CLASS_51_5_53;
  }
  if (kg <= CARRIED_WEIGHT_53_5_55_MAX_KG) {
    return CARRIED_WEIGHT_CLASS_53_5_55;
  }
  if (kg <= CARRIED_WEIGHT_55_5_57_MAX_KG) {
    return CARRIED_WEIGHT_CLASS_55_5_57;
  }
  return CARRIED_WEIGHT_CLASS_57_5_59;
};
