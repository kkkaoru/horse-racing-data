// This test runs with Bun and Vitest.
import { expect, test, vi } from "vitest";
import {
  isValidHorseCode,
  isValidJockeyCode,
  isValidOwnerCode,
  isValidTozaiShozokuCode,
  isValidTrainerCode,
  resolveHorseCode,
  resolveJockeyCode,
  resolveMasterVerifiedEntityCodes,
  resolveTrainerCode,
} from "./entity-resolver";
import type { MasterLookupPort, MasterVerifiedEntityResolution } from "./entity-resolver";

const createLookup = (overrides: Partial<MasterLookupPort> = {}): MasterLookupPort => ({
  findHorse: vi.fn(async () => ({ exists: true, canonicalName: "マスターホース" })),
  findJockey: vi.fn(async () => ({ exists: true, canonicalName: "騎手短名" })),
  findTrainer: vi.fn(async () => ({
    exists: true,
    canonicalName: "調教師短名",
    tozaiShozokuCode: "4",
  })),
  findOwnerByName: vi.fn(async () => ({ code: "166803", canonicalName: "マスター馬主" })),
  ...overrides,
});

test("validates exact-length ASCII-digit entity codes", () => {
  expect(isValidHorseCode("2021190001")).toBe(true);
  expect(isValidHorseCode("202119000")).toBe(false);
  expect(isValidHorseCode("000a029d48")).toBe(false);
  expect(isValidHorseCode(null)).toBe(false);
  expect(isValidJockeyCode("05504")).toBe(true);
  expect(isValidJockeyCode("0550")).toBe(false);
  expect(isValidJockeyCode("a033f")).toBe(false);
  expect(isValidJockeyCode(null)).toBe(false);
  expect(isValidTrainerCode("05701")).toBe(true);
  expect(isValidTrainerCode("057010")).toBe(false);
  expect(isValidTrainerCode("05A01")).toBe(false);
  expect(isValidTrainerCode(null)).toBe(false);
  expect(isValidOwnerCode("166803")).toBe(true);
  expect(isValidOwnerCode("16680")).toBe(false);
  expect(isValidOwnerCode("16A803")).toBe(false);
  expect(isValidOwnerCode(null)).toBe(false);
  expect(isValidTozaiShozokuCode("1")).toBe(true);
  expect(isValidTozaiShozokuCode("2")).toBe(true);
  expect(isValidTozaiShozokuCode("3")).toBe(true);
  expect(isValidTozaiShozokuCode("4")).toBe(true);
  expect(isValidTozaiShozokuCode("0")).toBe(false);
  expect(isValidTozaiShozokuCode("5")).toBe(false);
  expect(isValidTozaiShozokuCode(null)).toBe(false);
});

test("resolves shape-valid source ids and replaces invalid ids with fixed-width placeholders", () => {
  expect(resolveHorseCode("2021190001")).toBe("2021190001");
  expect(resolveHorseCode("000a029d48")).toBe("0000000000");
  expect(resolveHorseCode(null)).toBe("0000000000");
  expect(resolveJockeyCode("05504")).toBe("05504");
  expect(resolveJockeyCode("a033f")).toBe("00000");
  expect(resolveJockeyCode(null)).toBe("00000");
  expect(resolveTrainerCode("05701")).toBe("05701");
  expect(resolveTrainerCode("05A01")).toBe("00000");
  expect(resolveTrainerCode(null)).toBe("00000");
});

test("returns canonical master names for all four resolved entity classes", async () => {
  const lookup: MasterLookupPort = createLookup();
  const result: MasterVerifiedEntityResolution = await resolveMasterVerifiedEntityCodes({
    identity: {
      horseId: "2021190001",
      jockeyId: "05504",
      trainerId: "05701",
      ownerName: "PUBLISHED OWNER",
    },
    lookup,
  });

  expect(result).toStrictEqual({
    codes: {
      horseRegistrationNumber: "2021190001",
      horseName: "マスターホース",
      jockeyCode: "05504",
      jockeyName: "騎手短名",
      trainerCode: "05701",
      trainerName: "調教師短名",
      ownerCode: "166803",
      ownerName: "マスター馬主",
      tozaiShozokuCode: "4",
    },
    fallbacks: { horse: false, jockey: false, trainer: false, owner: false },
  });
  expect(lookup.findHorse).toHaveBeenCalledWith("2021190001");
  expect(lookup.findJockey).toHaveBeenCalledWith("05504");
  expect(lookup.findTrainer).toHaveBeenCalledWith("05701");
  expect(lookup.findOwnerByName).toHaveBeenCalledWith("PUBLISHED OWNER");
});

test("returns placeholder codes and null canonical names for unresolved master records", async () => {
  const lookup: MasterLookupPort = createLookup({
    findHorse: vi.fn(async () => ({ exists: false, canonicalName: null })),
    findJockey: vi.fn(async () => ({ exists: false, canonicalName: null })),
    findTrainer: vi.fn(async () => ({
      exists: false,
      canonicalName: null,
      tozaiShozokuCode: null,
    })),
    findOwnerByName: vi.fn(async () => ({ code: null, canonicalName: null })),
  });
  const result: MasterVerifiedEntityResolution = await resolveMasterVerifiedEntityCodes({
    identity: {
      horseId: "2021199999",
      jockeyId: "05999",
      trainerId: "05998",
      ownerName: "UNKNOWN OWNER",
    },
    lookup,
  });

  expect(result).toStrictEqual({
    codes: {
      horseRegistrationNumber: "0000000000",
      horseName: null,
      jockeyCode: "00000",
      jockeyName: null,
      trainerCode: "00000",
      trainerName: null,
      ownerCode: "000000",
      ownerName: null,
      tozaiShozokuCode: "0",
    },
    fallbacks: { horse: true, jockey: true, trainer: true, owner: true },
  });
});

test("does not query code masters for invalid source id shapes", async () => {
  const lookup: MasterLookupPort = createLookup({
    findOwnerByName: vi.fn(async () => ({ code: null, canonicalName: null })),
  });
  const result: MasterVerifiedEntityResolution = await resolveMasterVerifiedEntityCodes({
    identity: {
      horseId: "000a029d48",
      jockeyId: "a033f",
      trainerId: null,
      ownerName: "UNRESOLVED OWNER",
    },
    lookup,
  });

  expect(result.codes).toStrictEqual({
    horseRegistrationNumber: "0000000000",
    horseName: null,
    jockeyCode: "00000",
    jockeyName: null,
    trainerCode: "00000",
    trainerName: null,
    ownerCode: "000000",
    ownerName: null,
    tozaiShozokuCode: "0",
  });
  expect(lookup.findHorse).not.toHaveBeenCalled();
  expect(lookup.findJockey).not.toHaveBeenCalled();
  expect(lookup.findTrainer).not.toHaveBeenCalled();
  expect(lookup.findOwnerByName).toHaveBeenCalledWith("UNRESOLVED OWNER");
});

test("keeps a resolved code when its master canonical name is blank", async () => {
  const lookup: MasterLookupPort = createLookup({
    findHorse: vi.fn(async () => ({ exists: true, canonicalName: null })),
    findJockey: vi.fn(async () => ({ exists: true, canonicalName: null })),
    findTrainer: vi.fn(async () => ({
      exists: true,
      canonicalName: null,
      tozaiShozokuCode: "2",
    })),
    findOwnerByName: vi.fn(async () => ({ code: "166803", canonicalName: null })),
  });
  const result: MasterVerifiedEntityResolution = await resolveMasterVerifiedEntityCodes({
    identity: {
      horseId: "2021190001",
      jockeyId: "05504",
      trainerId: "05701",
      ownerName: "PUBLISHED OWNER",
    },
    lookup,
  });

  expect(result.codes).toStrictEqual({
    horseRegistrationNumber: "2021190001",
    horseName: null,
    jockeyCode: "05504",
    jockeyName: null,
    trainerCode: "05701",
    trainerName: null,
    ownerCode: "166803",
    ownerName: null,
    tozaiShozokuCode: "2",
  });
  expect(result.fallbacks).toStrictEqual({
    horse: false,
    jockey: false,
    trainer: false,
    owner: false,
  });
});

test("rejects malformed owner and affiliation codes while retaining other resolved entities", async () => {
  const lookup: MasterLookupPort = createLookup({
    findTrainer: vi.fn(async () => ({
      exists: true,
      canonicalName: "調教師短名",
      tozaiShozokuCode: "9",
    })),
    findOwnerByName: vi.fn(async () => ({ code: "16A803", canonicalName: "不正馬主" })),
  });
  const result: MasterVerifiedEntityResolution = await resolveMasterVerifiedEntityCodes({
    identity: {
      horseId: "2021190001",
      jockeyId: "05504",
      trainerId: "05701",
      ownerName: "MALFORMED OWNER",
    },
    lookup,
  });

  expect(result.codes.ownerCode).toBe("000000");
  expect(result.codes.ownerName).toBe(null);
  expect(result.codes.trainerCode).toBe("05701");
  expect(result.codes.trainerName).toBe("調教師短名");
  expect(result.codes.tozaiShozokuCode).toBe("0");
  expect(result.fallbacks.owner).toBe(true);
  expect(result.fallbacks.trainer).toBe(false);
});
