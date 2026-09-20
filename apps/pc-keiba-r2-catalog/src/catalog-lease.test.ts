// Runs with bun via Vitest; typed storage mocks exercise persisted fencing and clock boundaries.
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { mockDeep } from "vitest-mock-extended";
import { CatalogLeaseCoordinator, type CatalogLeaseToken } from "./catalog-lease";

const coordinator = (initial?: unknown) => {
  const ctx = mockDeep<DurableObjectState>();
  ctx.storage.kv.get.mockReturnValue(initial);
  ctx.storage.kv.put.mockImplementation((_key, value) => {
    ctx.storage.kv.get.mockReturnValue(structuredClone(value));
  });
  ctx.storage.transactionSync.mockImplementation((action) => action());
  return { ctx, service: new CatalogLeaseCoordinator(ctx, {}) };
};
const requireLease = (value: CatalogLeaseToken | null): CatalogLeaseToken => {
  if (value === null) throw new Error("Expected a granted lease in test");
  return value;
};

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date(10000));
});
afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

test("starts empty and persists the first fencing token before acknowledging it", () => {
  const { ctx, service } = coordinator();
  expect(service.status()).toBeNull();
  expect(ctx.storage.kv.put).not.toHaveBeenCalled();
  expect(service.acquire("attempt-a", 1000)).toStrictEqual({
    owner: "attempt-a",
    fence: 1,
    expiresAt: 11000,
  });
  expect(ctx.storage.kv.put).toHaveBeenCalledOnce();
  expect(ctx.storage.transactionSync).toHaveBeenCalledTimes(2);
});

test("rejects competitors and makes acquisition retries non-extending", () => {
  const { ctx, service } = coordinator();
  const first = service.acquire("a", 2000);
  vi.setSystemTime(new Date(10500));
  expect(service.acquire("a", 900000)).toStrictEqual(first);
  expect(service.acquire("b", 1000)).toBeNull();
  expect(ctx.storage.kv.put).toHaveBeenCalledOnce();
});

test("restores the same owner and fencing epoch after instance recreation", () => {
  const { ctx, service } = coordinator();
  const first = service.acquire("a", 2000);
  const restored = new CatalogLeaseCoordinator(ctx, {});
  expect(restored.status()).toStrictEqual(first);
  expect(restored.acquire("b", 1000)).toBeNull();
});

test("expiry revokes validation and hands out a strictly newer epoch", () => {
  const { service } = coordinator();
  const first = requireLease(service.acquire("a", 1000));
  expect(service.validate(first)).toBe(true);
  vi.setSystemTime(new Date(11000));
  expect(service.status()).toBeNull();
  expect(service.validate(first)).toBe(false);
  expect(service.renew(first, 1000)).toBeNull();
  const second = requireLease(service.acquire("b", 1000));
  expect(second.fence).toBe(2);
  expect(service.release(first)).toBe(false);
  expect(service.renew(first, 1000)).toBeNull();
  expect(service.status()).toStrictEqual(second);
});

test("fencing prevents ABA release and renewal even when owner names repeat", () => {
  const { service } = coordinator();
  const first = requireLease(service.acquire("a", 1000));
  expect(service.release(first)).toBe(true);
  expect(service.status()).toBeNull();
  const next = requireLease(service.acquire("a", 2000));
  expect(service.release(first)).toBe(false);
  expect(service.renew(first, 1000)).toBeNull();
  expect(service.validate(first)).toBe(false);
  expect(service.validate(next)).toBe(true);
});

test("renewals extend but never shorten a current lease", () => {
  const { service } = coordinator();
  const first = requireLease(service.acquire("a", 3000));
  expect(service.renew(first, 1000)?.expiresAt).toBe(13000);
  vi.setSystemTime(new Date(10500));
  expect(service.renew(first, 3000)?.expiresAt).toBe(13500);
  expect(service.release(first)).toBe(true);
  expect(service.release(first)).toBe(false);
});

test("storage failures do not return a lease or mutate an in-memory owner", () => {
  const { ctx, service } = coordinator();
  ctx.storage.kv.put.mockImplementationOnce(() => {
    throw new Error("storage unavailable");
  });
  expect(() => service.acquire("a", 1000)).toThrow("storage unavailable");
  expect(service.status()).toBeNull();
});

test.each([0, 999, 900001, 1.5, Number.NaN])(
  "rejects invalid lifetime %s before storage",
  (ttl) => {
    const { ctx, service } = coordinator();
    expect(() => service.acquire("a", ttl)).toThrow("lifetime");
    expect(ctx.storage.transactionSync).not.toHaveBeenCalled();
  },
);

test.each(["", "a".repeat(257), 42])("validates owner input at the RPC boundary", (owner) => {
  const { service } = coordinator();
  expect(() => Reflect.apply(service.acquire, service, [owner, 1000])).toThrow("owner");
});

test.each([
  { owner: "", fence: 1, expiresAt: 1000 },
  { owner: "a", fence: 0, expiresAt: 1000 },
  { owner: "a", fence: 1.5, expiresAt: 1000 },
  { owner: "a", fence: 1, expiresAt: -1 },
  { owner: "a", fence: 1, expiresAt: 1.5 },
])("rejects malformed tokens", (token) => {
  const { service } = coordinator();
  expect(() => service.validate(token)).toThrow();
});

test.each([null, 42])("rejects non-object RPC tokens", (token) => {
  const { service } = coordinator();
  expect(() => Reflect.apply(service.validate, service, [token])).toThrow(
    "Invalid Catalog lease token",
  );
});

test("does not wrap the fencing counter", () => {
  const { service } = coordinator({ owner: null, fence: Number.MAX_SAFE_INTEGER, expiresAt: 0 });
  expect(() => service.acquire("a", 1000)).toThrow("counter exhausted");
});
