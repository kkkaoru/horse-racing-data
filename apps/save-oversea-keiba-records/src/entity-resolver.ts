// This file runs with Bun.
import type { ResolvedEntityCodes } from "./types";

export interface MasterEntityRecord {
  readonly exists: boolean;
  readonly canonicalName: string | null;
}

export interface TrainerMasterRecord extends MasterEntityRecord {
  readonly tozaiShozokuCode: string | null;
}

export interface OwnerMasterRecord {
  readonly code: string | null;
  readonly canonicalName: string | null;
}

export interface MasterLookupPort {
  readonly findHorse: (kettoTorokuBango: string) => Promise<MasterEntityRecord>;
  readonly findJockey: (kishuCode: string) => Promise<MasterEntityRecord>;
  readonly findTrainer: (chokyoshiCode: string) => Promise<TrainerMasterRecord>;
  readonly findOwnerByName: (ownerName: string) => Promise<OwnerMasterRecord>;
}

export interface SourceRunnerIdentity {
  readonly horseId: string | null;
  readonly jockeyId: string | null;
  readonly trainerId: string | null;
  readonly ownerName: string;
}

export interface EntityFallbacks {
  readonly horse: boolean;
  readonly jockey: boolean;
  readonly trainer: boolean;
  readonly owner: boolean;
}

export interface MasterVerifiedEntityResolution {
  readonly codes: ResolvedEntityCodes;
  readonly fallbacks: EntityFallbacks;
}

export interface ResolveMasterVerifiedEntityCodesRequest {
  readonly identity: SourceRunnerIdentity;
  readonly lookup: MasterLookupPort;
}

interface ExactCodeShapeInput {
  readonly value: string | null;
  readonly length: number;
}

interface MasterCodeVerificationInput {
  readonly sourceCode: string | null;
  readonly placeholder: string;
  readonly isValid: (value: string | null) => value is string;
  readonly find: (code: string) => Promise<MasterEntityRecord>;
}

interface VerifiedEntity {
  readonly code: string;
  readonly canonicalName: string | null;
  readonly fellBack: boolean;
}

export const HORSE_CODE_LENGTH: number = 10;
export const JOCKEY_CODE_LENGTH: number = 5;
export const TRAINER_CODE_LENGTH: number = 5;
export const OWNER_CODE_LENGTH: number = 6;

export const HORSE_CODE_PLACEHOLDER: string = "0000000000";
export const JOCKEY_CODE_PLACEHOLDER: string = "00000";
export const TRAINER_CODE_PLACEHOLDER: string = "00000";
export const OWNER_CODE_PLACEHOLDER: string = "000000";
export const TOZAI_SHOZOKU_CODE_PLACEHOLDER: string = "0";

const ASCII_DIGITS_PATTERN: RegExp = /^[0-9]+$/;
const VALID_TOZAI_SHOZOKU_CODES: ReadonlySet<string> = new Set(["1", "2", "3", "4"]);

const hasExactCodeShape = ({ value, length }: ExactCodeShapeInput): boolean =>
  value !== null && value.length === length && ASCII_DIGITS_PATTERN.test(value);

export const isValidHorseCode = (value: string | null): value is string =>
  hasExactCodeShape({ value, length: HORSE_CODE_LENGTH });

export const isValidJockeyCode = (value: string | null): value is string =>
  hasExactCodeShape({ value, length: JOCKEY_CODE_LENGTH });

export const isValidTrainerCode = (value: string | null): value is string =>
  hasExactCodeShape({ value, length: TRAINER_CODE_LENGTH });

export const isValidOwnerCode = (value: string | null): value is string =>
  hasExactCodeShape({ value, length: OWNER_CODE_LENGTH });

export const isValidTozaiShozokuCode = (value: string | null): value is string =>
  value !== null && VALID_TOZAI_SHOZOKU_CODES.has(value);

export const resolveHorseCode = (sourceId: string | null): string =>
  isValidHorseCode(sourceId) ? sourceId : HORSE_CODE_PLACEHOLDER;

export const resolveJockeyCode = (sourceId: string | null): string =>
  isValidJockeyCode(sourceId) ? sourceId : JOCKEY_CODE_PLACEHOLDER;

export const resolveTrainerCode = (sourceId: string | null): string =>
  isValidTrainerCode(sourceId) ? sourceId : TRAINER_CODE_PLACEHOLDER;

const verifyMasterCode = async ({
  sourceCode,
  placeholder,
  isValid,
  find,
}: MasterCodeVerificationInput): Promise<VerifiedEntity> => {
  if (!isValid(sourceCode)) {
    return { code: placeholder, canonicalName: null, fellBack: true };
  }

  const record: MasterEntityRecord = await find(sourceCode);
  return record.exists
    ? { code: sourceCode, canonicalName: record.canonicalName, fellBack: false }
    : { code: placeholder, canonicalName: null, fellBack: true };
};

const resolveOwner = async ({
  ownerName,
  findOwnerByName,
}: {
  readonly ownerName: string;
  readonly findOwnerByName: MasterLookupPort["findOwnerByName"];
}): Promise<VerifiedEntity> => {
  const record: OwnerMasterRecord = await findOwnerByName(ownerName);
  return isValidOwnerCode(record.code)
    ? { code: record.code, canonicalName: record.canonicalName, fellBack: false }
    : { code: OWNER_CODE_PLACEHOLDER, canonicalName: null, fellBack: true };
};

const resolveTozaiShozokuCode = (trainer: TrainerMasterRecord | null): string => {
  if (trainer === null || !trainer.exists) {
    return TOZAI_SHOZOKU_CODE_PLACEHOLDER;
  }
  return isValidTozaiShozokuCode(trainer.tozaiShozokuCode)
    ? trainer.tozaiShozokuCode
    : TOZAI_SHOZOKU_CODE_PLACEHOLDER;
};

export const resolveMasterVerifiedEntityCodes = async ({
  identity,
  lookup,
}: ResolveMasterVerifiedEntityCodesRequest): Promise<MasterVerifiedEntityResolution> => {
  const trainerRecordPromise: Promise<TrainerMasterRecord | null> = isValidTrainerCode(
    identity.trainerId,
  )
    ? lookup.findTrainer(identity.trainerId)
    : Promise.resolve(null);
  const [horse, jockey, trainerRecord, owner]: readonly [
    VerifiedEntity,
    VerifiedEntity,
    TrainerMasterRecord | null,
    VerifiedEntity,
  ] = await Promise.all([
    verifyMasterCode({
      sourceCode: identity.horseId,
      placeholder: HORSE_CODE_PLACEHOLDER,
      isValid: isValidHorseCode,
      find: lookup.findHorse,
    }),
    verifyMasterCode({
      sourceCode: identity.jockeyId,
      placeholder: JOCKEY_CODE_PLACEHOLDER,
      isValid: isValidJockeyCode,
      find: lookup.findJockey,
    }),
    trainerRecordPromise,
    resolveOwner({
      ownerName: identity.ownerName,
      findOwnerByName: lookup.findOwnerByName,
    }),
  ]);
  const trainer: VerifiedEntity =
    trainerRecord !== null && trainerRecord.exists && identity.trainerId !== null
      ? {
          code: identity.trainerId,
          canonicalName: trainerRecord.canonicalName,
          fellBack: false,
        }
      : { code: TRAINER_CODE_PLACEHOLDER, canonicalName: null, fellBack: true };

  return {
    codes: {
      horseRegistrationNumber: horse.code,
      horseName: horse.canonicalName,
      jockeyCode: jockey.code,
      jockeyName: jockey.canonicalName,
      trainerCode: trainer.code,
      trainerName: trainer.canonicalName,
      ownerCode: owner.code,
      ownerName: owner.canonicalName,
      tozaiShozokuCode: resolveTozaiShozokuCode(trainerRecord),
    },
    fallbacks: {
      horse: horse.fellBack,
      jockey: jockey.fellBack,
      trainer: trainer.fellBack,
      owner: owner.fellBack,
    },
  };
};
