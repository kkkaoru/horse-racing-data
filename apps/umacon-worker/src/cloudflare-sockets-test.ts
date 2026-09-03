// Run with bun. Vitest replacement for the Workers-only cloudflare:sockets module.
export const connect = (): never => {
  throw new Error("Unexpected default socket connector in test");
};
