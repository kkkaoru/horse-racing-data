export type PcKeibaHyperdriveBinding = {
  connectionString: string;
};

declare global {
  interface CloudflareEnv {
    HYPERDRIVE?: PcKeibaHyperdriveBinding;
  }
}
