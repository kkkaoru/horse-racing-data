// Run with: bun
// Tests for avsc-patch.ts

import { describe, it, expect } from "vitest";
import "./avsc-patch.ts";
import avro from "avsc";

describe("avsc patch - record operations", () => {
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

  it("should validate invalid records as false", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "validation_record",
      fields: [
        { name: "name", type: "string" },
        { name: "age", type: "int" },
      ],
    });
    expect(type.isValid({ name: "test", age: "not-a-number" })).toStrictEqual(false);
    expect(type.isValid(null)).toStrictEqual(false);
    expect(type.isValid(42)).toStrictEqual(false);
  });

  it("should handle records with default values", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "default_record",
      fields: [
        { name: "name", type: "string" },
        { name: "count", type: "int", default: 0 },
        { name: "active", type: "boolean", default: true },
      ],
    });
    const buf = type.toBuffer({ name: "test" });
    const decoded = type.fromBuffer(buf) as { name: string; count: number; active: boolean };
    expect(decoded.name).toStrictEqual("test");
    expect(decoded.count).toStrictEqual(0);
    expect(decoded.active).toStrictEqual(true);
  });

  it("should serialize and deserialize records with default fields undefined", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "default_write_record",
      fields: [
        { name: "id", type: "string" },
        { name: "score", type: "int", default: 100 },
      ],
    });
    const buf = type.toBuffer({ id: "x" });
    const decoded = type.fromBuffer(buf) as { id: string; score: number };
    expect(decoded.id).toStrictEqual("x");
    expect(decoded.score).toStrictEqual(100);
  });

  it("should skip fields correctly during deserialization", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "skip_record",
      fields: [
        { name: "a", type: "string" },
        { name: "b", type: "int" },
        { name: "c", type: "string" },
      ],
    });
    const buf = type.toBuffer({ a: "hello", b: 42, c: "world" });
    const decoded = type.fromBuffer(buf) as { a: string; b: number; c: string };
    expect(decoded.a).toStrictEqual("hello");
    expect(decoded.b).toStrictEqual(42);
    expect(decoded.c).toStrictEqual("world");
  });

  it("should validate with hook callback for detailed error reporting", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "hook_record",
      fields: [
        { name: "name", type: "string" },
        { name: "value", type: "int" },
      ],
    });
    const errors: Array<{ value: unknown; type: unknown }> = [];
    const hook = (val: unknown, t: unknown) => {
      errors.push({ value: val, type: t });
    };
    type.isValid({ name: "test", value: "not-int" }, { errorHook: hook });
    expect(errors.length > 0).toStrictEqual(true);
  });

  it("should validate null as invalid record with hook", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "null_hook_record",
      fields: [{ name: "id", type: "string" }],
    });
    const errors: Array<unknown> = [];
    const hook = (val: unknown) => {
      errors.push(val);
    };
    type.isValid(null, { errorHook: hook });
    expect(errors.length > 0).toStrictEqual(true);
  });

  it("should handle record with optional field via default and hook", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "optional_hook_record",
      fields: [
        { name: "id", type: "string" },
        { name: "count", type: "int", default: 0 },
      ],
    });
    const errors: Array<unknown> = [];
    const hook = (val: unknown) => {
      errors.push(val);
    };
    const valid = type.isValid({ id: "ok" }, { errorHook: hook });
    expect(valid).toStrictEqual(true);
    expect(errors.length).toStrictEqual(0);
  });

  it("should clone records correctly", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "clone_record",
      fields: [
        { name: "id", type: "string" },
        { name: "value", type: "int" },
      ],
    });
    const original = { id: "test", value: 10 };
    const cloned = type.clone(original) as Record<string, unknown>;
    expect(cloned.id).toStrictEqual("test");
    expect(cloned.value).toStrictEqual(10);
  });

  it("should compare records", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "compare_record",
      fields: [{ name: "id", type: "string" }],
    });
    const sameResult = type.compare({ id: "a" }, { id: "a" });
    expect(sameResult).toStrictEqual(0);
  });
});

describe("avsc patch - union/branch operations", () => {
  it("should handle union types with null branch", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "union_null_record",
      fields: [{ name: "value", type: ["null", "string"] }],
    });
    const buf = type.toBuffer({ value: null });
    const decoded = type.fromBuffer(buf) as { value: null };
    expect(decoded.value).toStrictEqual(null);
  });

  it("should handle union types with string branch", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "union_string_record",
      fields: [{ name: "value", type: ["null", "string"] }],
    });
    const buf = type.toBuffer({ value: "hello" });
    const decoded = type.fromBuffer(buf) as { value: string };
    expect(decoded.value).toStrictEqual("hello");
  });
});

describe("avsc patch - LongType", () => {
  it("should read and write long values without precision error", () => {
    const type = avro.Type.forSchema("long");
    const buf = type.toBuffer(9007199254740991);
    const decoded = type.fromBuffer(buf);
    expect(decoded).toStrictEqual(9007199254740991);
  });

  it("should throw for non-number long values", () => {
    const type = avro.Type.forSchema("long");
    expect(() => type.toBuffer("not-a-number" as unknown as number)).toThrow("invalid long value");
  });

  it("should throw for float long values", () => {
    const type = avro.Type.forSchema("long");
    expect(() => type.toBuffer(1.5)).toThrow("invalid long value");
  });

  it("should handle zero long value", () => {
    const type = avro.Type.forSchema("long");
    const buf = type.toBuffer(0);
    const decoded = type.fromBuffer(buf);
    expect(decoded).toStrictEqual(0);
  });

  it("should handle negative long values", () => {
    const type = avro.Type.forSchema("long");
    const buf = type.toBuffer(-1000);
    const decoded = type.fromBuffer(buf);
    expect(decoded).toStrictEqual(-1000);
  });
});

describe("avsc patch - writer with defaults", () => {
  it("should write default value when field is undefined", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "writer_default_record",
      fields: [
        { name: "id", type: "string" },
        { name: "status", type: "int", default: 42 },
      ],
    });
    const buf = type.toBuffer({ id: "test", status: undefined });
    const decoded = type.fromBuffer(buf) as { id: string; status: number };
    expect(decoded.id).toStrictEqual("test");
    expect(decoded.status).toStrictEqual(42);
  });

  it("should write provided value over default", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "writer_override_record",
      fields: [
        { name: "id", type: "string" },
        { name: "status", type: "int", default: 42 },
      ],
    });
    const buf = type.toBuffer({ id: "test", status: 99 });
    const decoded = type.fromBuffer(buf) as { id: string; status: number };
    expect(decoded.id).toStrictEqual("test");
    expect(decoded.status).toStrictEqual(99);
  });
});

describe("avsc patch - checker without hook", () => {
  it("should validate record without hook using every path", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "every_check_record",
      fields: [
        { name: "a", type: "string" },
        { name: "b", type: "int" },
      ],
    });
    expect(type.isValid({ a: "ok", b: 1 })).toStrictEqual(true);
    expect(type.isValid({ a: "ok", b: "bad" })).toStrictEqual(false);
  });

  it("should validate record with default field without hook", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "default_every_record",
      fields: [
        { name: "id", type: "string" },
        { name: "count", type: "int", default: 0 },
      ],
    });
    expect(type.isValid({ id: "ok" })).toStrictEqual(true);
    expect(type.isValid({ id: "ok", count: 5 })).toStrictEqual(true);
    expect(type.isValid({ id: "ok", count: "bad" })).toStrictEqual(false);
  });
});

describe("avsc patch - skipper", () => {
  it("should skip over record bytes correctly in stream", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "wrapper_skip",
      fields: [
        {
          name: "first",
          type: {
            type: "record",
            name: "inner_skip",
            fields: [
              { name: "x", type: "int" },
              { name: "y", type: "string" },
            ],
          },
        },
        { name: "second", type: "int" },
      ],
    });
    const buf = type.toBuffer({ first: { x: 10, y: "test" }, second: 99 });
    const decoded = type.fromBuffer(buf) as {
      first: { x: number; y: string };
      second: number;
    };
    expect(decoded.second).toStrictEqual(99);
    expect(decoded.first.x).toStrictEqual(10);
  });
});

describe("avsc patch - prototype methods", () => {
  it("should call toString on decoded record", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "tostring_record",
      fields: [{ name: "id", type: "string" }],
    });
    const buf = type.toBuffer({ id: "test" });
    const record = type.fromBuffer(buf) as Record<string, unknown>;
    const str = type.toString(record);
    expect(typeof str).toStrictEqual("string");
  });

  it("should call wrap on decoded record", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "wrap_record",
      fields: [{ name: "id", type: "string" }],
    });
    const buf = type.toBuffer({ id: "test" });
    const record = type.fromBuffer(buf) as Record<string, unknown>;
    const wrapped = type.wrap(record);
    expect(wrapped).toBeTruthy();
  });

  it("should call isValid on decoded record via prototype", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "isvalid_proto_record",
      fields: [
        { name: "id", type: "string" },
        { name: "value", type: "int" },
      ],
    });
    const buf = type.toBuffer({ id: "test", value: 5 });
    const record = type.fromBuffer(buf) as Record<string, unknown> & {
      isValid: () => boolean;
    };
    expect(record.isValid()).toStrictEqual(true);
  });
});

describe("avsc patch - error type", () => {
  it("should handle error record type", () => {
    const type = avro.Type.forSchema({
      type: "error",
      name: "test_avsc_error",
      fields: [{ name: "message", type: "string" }],
    });
    const valid = type.isValid({ message: "something went wrong" });
    expect(valid).toStrictEqual(true);
  });
});

describe("avsc patch - empty record", () => {
  it("should handle record with no fields", () => {
    const type = avro.Type.forSchema({
      type: "record",
      name: "empty_record",
      fields: [],
    });
    const valid = type.isValid({});
    expect(valid).toStrictEqual(true);
  });
});
