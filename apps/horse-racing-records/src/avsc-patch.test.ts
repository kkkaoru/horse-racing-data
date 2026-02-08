// Run with: bun
// Tests for avsc-patch.ts

import { describe, it, expect } from "vitest";
import "./avsc-patch.ts";
import avro from "avsc";

describe("avsc patch", () => {
  it("should allow Type.forSchema to work without new Function", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "test_record",
      fields: [
        { name: "name", type: "string" },
        { name: "value", type: "int" },
      ],
    });
    expect(type.isValid({ name: "test", value: 42 })).toStrictEqual(true);
  });

  it("should encode and decode correctly with patched constructors", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "patched_record",
      fields: [
        { name: "id", type: "string" },
        { name: "count", type: "long" },
      ],
    });
    const original = { id: "abc-123", count: 999 };
    const buf = type.toBuffer(original);
    const decoded = type.fromBuffer(buf) as { id: string; count: number };
    expect(decoded.id).toStrictEqual("abc-123");
    expect(decoded.count).toStrictEqual(999);
  });
});
