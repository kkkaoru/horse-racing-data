export const assertLegacyReplicaWriteEnabled = (value: string | undefined): void => {
  if (value !== "break-glass")
    throw new Error(
      "Legacy local R2 Catalog and Neon writes are disabled; daily-keiba-sync Worker is the production authority",
    );
};
