// Run with bun via Vitest. The worker tests replace this module with vi.mock.
// Resolving the generated dependency must not require a production build.
export default {
  fetch: (): never => {
    throw new Error("The OpenNext worker must be mocked in unit tests");
  },
};
