// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

export interface McpOauthStore {
  delete(key: string): Promise<void>;
  get(key: string): Promise<string | null>;
  put(key: string, value: string, expirationTtl: number): Promise<void>;
}

interface OauthKvBinding {
  delete(key: string): Promise<void>;
  get(key: string): Promise<string | null>;
  put(key: string, value: string, options?: { expirationTtl?: number }): Promise<void>;
}

export const createMemoryOauthStore = (): McpOauthStore => {
  const values = new Map<string, string>();
  return {
    delete: async (key: string): Promise<void> => {
      values.delete(key);
    },
    get: async (key: string): Promise<string | null> => values.get(key) ?? null,
    put: async (key: string, value: string): Promise<void> => {
      values.set(key, value);
    },
  };
};

export const createKvOauthStore = (kv: OauthKvBinding): McpOauthStore => ({
  delete: (key: string) => kv.delete(key),
  get: (key: string) => kv.get(key),
  put: (key: string, value: string, expirationTtl: number) => kv.put(key, value, { expirationTtl }),
});
