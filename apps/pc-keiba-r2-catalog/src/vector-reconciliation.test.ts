// Runs with bun via Vitest; sample verification separates index visibility from ANN quality.
import { expect, test } from "vitest";
import { assessCornerVectorSample, verifyCornerVectorSample } from "./vector-reconciliation";

const expected: VectorizeVector = {
  id: "v1",
  namespace: "corner-test",
  values: [0.1, 0, 1, 0, 0, 0, 0, 0],
  metadata: { source: "jra", hasFinish: true, distance: 2000 },
};
const actual: VectorizeVector = {
  ...expected,
  values: [Math.fround(0.1), 0, 1, ...Array.from({ length: 29 }, () => 0)],
};

test.each([
  [0.15384615384615385, 0.15384615384615383],
  [0.46153846153846156, 0.4615384615384616],
])(
  "reports one-ULP normalized differences without weakening the strict verifier",
  (source, stored) => {
    const original: VectorizeVector = {
      ...expected,
      metadata: { ...expected.metadata, finishNorm: source },
    };
    const restored: VectorizeVector = {
      ...actual,
      metadata: { ...actual.metadata, finishNorm: stored },
    };
    expect(assessCornerVectorSample([original], [restored])).toStrictEqual({
      verifiedVectors: 1,
      sourceDimensions: 8,
      storageDimensions: 32,
      exactMetadata: false,
      normalizedRoundingFields: 1,
    });
    expect(() => verifyCornerVectorSample([original], [restored])).toThrow("metadata mismatch");
  },
);

test.each([
  [0.5, 0.5000000000000002],
  [0, Number.MIN_VALUE],
  [0.9999999999999999, 1],
  [0.5, "0.5"],
])("rejects larger drift, exact-anchor changes, and type changes", (source, stored) => {
  expect(() =>
    assessCornerVectorSample(
      [{ ...expected, metadata: { finishNorm: source } }],
      [{ ...actual, metadata: { finishNorm: stored } }],
    ),
  ).toThrow("metadata mismatch");
});

test("filter fields still require exact numeric equality", () => {
  expect(() =>
    assessCornerVectorSample(
      [expected],
      [{ ...actual, metadata: { ...actual.metadata, distance: 2000.0000000000002 } }],
    ),
  ).toThrow("metadata mismatch");
  expect(() =>
    assessCornerVectorSample([{ ...expected, metadata: { finishNorm: Infinity } }], [actual]),
  ).toThrow("reconciliation source");
});

test("verifies float32 values, zero padding and exact metadata", () => {
  expect(verifyCornerVectorSample([expected], [actual])).toStrictEqual({
    verifiedVectors: 1,
    sourceDimensions: 8,
    storageDimensions: 32,
  });
  expect(
    verifyCornerVectorSample(
      [expected],
      [{ ...actual, values: [0.1, 0, 1, ...Array.from({ length: 29 }, () => 0)] }],
    ).verifiedVectors,
  ).toBe(1);
  expect(
    verifyCornerVectorSample(
      [expected, { ...expected, id: "v2" }],
      [{ ...actual, id: "v2" }, actual],
    ).verifiedVectors,
  ).toBe(2);
});

test.each([null, [], [actual, actual]])("rejects missing or extra results", (vectors) => {
  expect(() => verifyCornerVectorSample([expected], vectors)).toThrow("count");
});

test("rejects empty, oversized, or duplicate expected samples", () => {
  expect(() => verifyCornerVectorSample([], [])).toThrow("count");
  expect(() =>
    verifyCornerVectorSample(
      Array.from({ length: 501 }, () => expected),
      [],
    ),
  ).toThrow("count");
  expect(() => verifyCornerVectorSample([expected, expected], [actual, actual])).toThrow("count");
});

test.each([null, { id: 1 }])("rejects malformed returned identity", (vector) => {
  expect(() => verifyCornerVectorSample([expected], [vector])).toThrow("identity");
});

test("rejects duplicate result identities", () => {
  expect(() =>
    verifyCornerVectorSample([expected, { ...expected, id: "v2" }], [actual, actual]),
  ).toThrow("identity");
});

test.each([{ id: "other" }, { namespace: "other" }, { values: null }, { values: [1] }])(
  "rejects namespace, identity, and dimensional mismatch",
  (overrides) => {
    expect(() => verifyCornerVectorSample([expected], [{ ...actual, ...overrides }])).toThrow(
      "isolation or dimensions",
    );
  },
);

test.each([
  { id: "" },
  { namespace: undefined },
  { namespace: "" },
  { values: [1] },
  { values: [Infinity, 0, 0, 0, 0, 0, 0, 0] },
  { metadata: undefined },
  { metadata: { source: ["jra"] } },
])("rejects invalid expected source vectors", (overrides) => {
  expect(() => verifyCornerVectorSample([{ ...expected, ...overrides }], [actual])).toThrow(
    "reconciliation source",
  );
});

test("rejects value drift, nonnumeric values and nonzero padding", () => {
  expect(() =>
    verifyCornerVectorSample(
      [expected],
      [{ ...actual, values: Array.from({ length: 32 }, () => 0) }],
    ),
  ).toThrow("values mismatch");
  expect(() =>
    verifyCornerVectorSample(
      [expected],
      [{ ...actual, values: Array.from({ length: 32 }, () => "0") }],
    ),
  ).toThrow("values mismatch");
  expect(() =>
    verifyCornerVectorSample(
      [expected],
      [
        {
          ...actual,
          values: Array.from({ length: 32 }, (_, index) =>
            index === 31 ? 1 : actual.values[index],
          ),
        },
      ],
    ),
  ).toThrow("values mismatch");
  expect(() =>
    verifyCornerVectorSample(
      [expected],
      [
        {
          ...actual,
          values: Array.from({ length: 32 }, (_, index) =>
            index === 31 ? 1e-50 : actual.values[index],
          ),
        },
      ],
    ),
  ).toThrow("values mismatch");
});

test.each([null, { source: "jra" }, { source: "nar", hasFinish: true, distance: 2000 }])(
  "rejects missing or changed metadata",
  (metadata) => {
    expect(() => verifyCornerVectorSample([expected], [{ ...actual, metadata }])).toThrow(
      "metadata mismatch",
    );
  },
);
