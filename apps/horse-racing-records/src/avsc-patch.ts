// Run with: bun
// Patches avsc to replace all new Function() calls with closure-based alternatives
// Required for Cloudflare Workers where eval/new Function() is blocked at runtime

import avsc from "avsc";

interface AvroField {
  readonly name: string;
  readonly type: AvroTypeInternal;
  readonly defaultValue: () => unknown;
}

interface AvroTypeInternal {
  readonly branchName: string;
  readonly fields: ReadonlyArray<AvroField>;
  readonly _isError: boolean;
  recordConstructor: RecordCtor;
  _getConstructorName: () => string;
  _checkFields: (val: unknown) => boolean;
  _read: (tap: unknown) => unknown;
  _write: (tap: unknown, val: unknown) => void;
  _skip: (tap: unknown) => void;
  _check: (val: unknown, flags: unknown, hook?: unknown, path?: unknown) => boolean;
  clone: (val: unknown, opts?: unknown) => unknown;
  compare: (val1: unknown, val2: unknown) => number;
  isValid: (val: unknown, opts?: unknown) => boolean;
  toBuffer: (val: unknown) => Buffer;
  toString: (val: unknown) => string;
  wrap: (val: unknown) => unknown;
}

interface Tap {
  readLong: () => number;
  writeLong: (val: number) => void;
  writeBinary: (s: string, l: number) => void;
}

interface BranchCtor {
  (this: Record<string, unknown>, val: unknown): void;
  type: unknown;
  prototype: Record<string, (...args: ReadonlyArray<unknown>) => unknown>;
}

interface RecordCtor {
  new (...args: ReadonlyArray<unknown>): Record<string, unknown>;
  type?: unknown;
  getType?: () => unknown;
  prototype: Record<string, unknown>;
}

const TypeProto = (avsc as unknown as { Type: { prototype: Record<string, unknown> } }).Type
  .prototype;

// Patch 1: _createBranchConstructor (used by all types for union dispatch)
TypeProto._createBranchConstructor = function (this: AvroTypeInternal): BranchCtor | null {
  const name = this.branchName;
  if (name === "null") return null;
  const ctor = function (this: Record<string, unknown>, val: unknown) {
    this[name] = val;
  } as unknown as BranchCtor;
  ctor.type = this;
  ctor.prototype = {} as BranchCtor["prototype"];
  ctor.prototype.unwrap = function (this: Record<string, unknown>) {
    return this[name];
  };
  ctor.prototype.unwrapped = ctor.prototype.unwrap;
  return ctor;
};

// Get RecordType prototype by creating a dummy record type (runs during startup)
const dummyRecord = avsc.Type.forSchema({
  type: "record",
  name: "AvscPatchDummy",
  fields: [{ name: "x", type: "int" }],
});
const RecordProto = Object.getPrototypeOf(dummyRecord) as Record<string, unknown>;

// Helper: attach standard prototype methods to a record constructor
const attachPrototypeMethods = (ctor: RecordCtor, type: AvroTypeInternal): void => {
  ctor.getType = () => type;
  ctor.type = type;
  if (type._isError) {
    Object.setPrototypeOf(ctor.prototype, Error.prototype);
    ctor.prototype.name = type._getConstructorName();
  }
  ctor.prototype.clone = function (o: unknown) {
    return type.clone(this, o);
  };
  ctor.prototype.compare = function (v: unknown) {
    return type.compare(this, v);
  };
  ctor.prototype.isValid = function (o: unknown) {
    return type.isValid(this, o);
  };
  ctor.prototype.toBuffer = function () {
    return type.toBuffer(this);
  };
  ctor.prototype.toString = function () {
    return type.toString(this);
  };
  ctor.prototype.wrap = function () {
    return type.wrap(this);
  };
  ctor.prototype.wrapped = ctor.prototype.wrap;
};

// Patch 2: _createConstructor (used by RecordType to create record instances)
RecordProto._createConstructor = function (
  this: AvroTypeInternal,
  _errorStack?: boolean,
  plainRecords?: boolean,
): RecordCtor {
  const fields = this.fields;
  const Ctor = function (this: Record<string, unknown>, ...args: ReadonlyArray<unknown>) {
    fields.forEach((field, i) => {
      const val = args[i];
      const defaultVal = field.defaultValue();
      this[field.name] = val === undefined && defaultVal !== undefined ? defaultVal : val;
    });
  } as unknown as RecordCtor;
  if (plainRecords) return Ctor;
  attachPrototypeMethods(Ctor, this);
  return Ctor;
};

// Patch 3: _createReader (used by RecordType to deserialize records)
RecordProto._createReader = function (this: AvroTypeInternal) {
  const fields = this.fields;
  const Ctor = this.recordConstructor;
  return function (tap: unknown) {
    const args = fields.map((field) => field.type._read(tap));
    return new Ctor(...args);
  };
};

// Patch 4: _createSkipper (used by RecordType to skip over records in binary)
RecordProto._createSkipper = function (this: AvroTypeInternal) {
  const fields = this.fields;
  return function (tap: unknown) {
    fields.forEach((field) => {
      field.type._skip(tap);
    });
  };
};

// Patch 5: _createWriter (used by RecordType to serialize records)
RecordProto._createWriter = function (this: AvroTypeInternal) {
  const fieldWriters = this.fields.map((field) => {
    const defaultVal = field.defaultValue();
    if (defaultVal === undefined) {
      return (tap: Tap, val: Record<string, unknown>) => {
        field.type._write(tap, val[field.name]);
      };
    }
    const defaultBinary = field.type.toBuffer(defaultVal).toString("binary");
    const defaultLength = defaultBinary.length;
    return (tap: Tap, val: Record<string, unknown>) => {
      const v = val[field.name];
      if (v === undefined) {
        tap.writeBinary(defaultBinary, defaultLength);
      } else {
        field.type._write(tap as unknown, v);
      }
    };
  });
  return function (tap: unknown, val: Record<string, unknown>) {
    fieldWriters.forEach((writer) => writer(tap as Tap, val));
  };
};

// Helper: build checker function for record validation
const buildChecker = (fields: ReadonlyArray<AvroField>, type: AvroTypeInternal) =>
  function (
    val: unknown,
    flags: unknown,
    hook?: (val: unknown, type: unknown) => void,
    path?: Array<string>,
  ): boolean {
    if (val === null || typeof val !== "object" || (flags && !type._checkFields(val))) {
      if (hook) hook(val, type);
      return false;
    }
    const obj = val as Record<string, unknown>;
    if (!fields.length) return true;
    if (hook) {
      const state = { valid: true };
      const j = path ? path.length : 0;
      if (path) path.push("");
      fields.forEach((field) => {
        if (path) path[j] = field.name;
        const v = obj[field.name];
        const hasDefault = field.defaultValue() !== undefined;
        if (hasDefault) {
          if (v !== undefined && !field.type._check(v, flags, hook, path)) state.valid = false;
        } else {
          if (!field.type._check(v, flags, hook, path)) state.valid = false;
        }
      });
      if (path) path.pop();
      return state.valid;
    }
    return fields.every((field) => {
      const v = obj[field.name];
      return field.defaultValue() === undefined
        ? field.type._check(v, flags)
        : v === undefined || field.type._check(v, flags);
    });
  };

// Patch 6: _createChecker (used by RecordType to validate records)
RecordProto._createChecker = function (this: AvroTypeInternal) {
  return buildChecker(this.fields, this);
};

// Patch 7: LongType._read/_write (remove precision check for large snapshot IDs)
// Iceberg snapshot IDs can exceed Number.MAX_SAFE_INTEGER; avsc throws by default.
// Our own IDs stay within safe range (generateSnapshotId), but existing catalog IDs may not.
const longType = avsc.Type.forSchema("long");
const LongProto = Object.getPrototypeOf(longType) as Record<string, unknown>;

LongProto._read = function (tap: Tap): number {
  return tap.readLong();
};

LongProto._write = function (tap: Tap, val: unknown): void {
  if (typeof val !== "number" || val % 1) {
    throw new Error(`invalid long value: ${String(val)}`);
  }
  tap.writeLong(val);
};
