// Run with bun (vitest) / Cloudflare Workers runtime.
// Preserve Promise.all's first error, but never leave owned branches publishing.
export const settleOwnedWork = async <T extends readonly unknown[]>(
  values: T,
): Promise<{ -readonly [P in keyof T]: Awaited<T[P]> }> => {
  try {
    return await Promise.all(values);
  } catch (error) {
    await Promise.allSettled(values);
    throw error;
  }
};
