import { mkdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const bucket = process.env.R2_UPLOAD_BUCKET ?? "pc-keiba-finish-position-models";
const filePath = process.env.R2_UPLOAD_FILE;
const key = process.env.R2_UPLOAD_KEY?.replace(/^\/+/u, "");
const wranglerConfig = process.env.R2_UPLOAD_WRANGLER_CONFIG ?? "wrangler.jsonc";
const chunkSize = Number(process.env.R2_UPLOAD_CHUNK_SIZE ?? 128 * 1024 * 1024);
const maxWranglerUploadSize = 300 * 1024 * 1024;

if (!filePath || !key) {
  throw new Error("R2_UPLOAD_FILE and R2_UPLOAD_KEY are required.");
}
if (!Number.isSafeInteger(chunkSize) || chunkSize <= 0 || chunkSize > maxWranglerUploadSize) {
  throw new Error("R2_UPLOAD_CHUNK_SIZE must be between 1 byte and 300 MiB.");
}

const file = Bun.file(filePath);
const size = file.size;
const workDir = join(tmpdir(), `pc-keiba-r2-model-chunks-${Date.now()}`);
const chunkPrefix = `${key}.chunks`;
const manifestKey = `${key}.manifest.json`;
const hasher = new Bun.CryptoHasher("sha256");

const runWranglerPut = (objectKey, sourcePath, contentType) => {
  const objectPath = `${bucket}/${objectKey}`;
  const process = Bun.spawnSync(
    [
      "bunx",
      "wrangler",
      "r2",
      "object",
      "put",
      objectPath,
      "--remote",
      "--force",
      "--config",
      wranglerConfig,
      "--file",
      sourcePath,
      "--content-type",
      contentType,
      "--cache-control",
      "public, max-age=31536000, immutable",
    ],
    {
      stderr: "inherit",
      stdout: "inherit",
    },
  );

  if (process.exitCode !== 0) {
    throw new Error(`wrangler r2 object put failed for ${objectKey}`);
  }
};

await mkdir(workDir, { recursive: true });

try {
  const chunks = [];
  let chunkIndex = 0;

  for (let offset = 0; offset < size; offset += chunkSize) {
    const end = Math.min(offset + chunkSize, size);
    const chunkFile = join(workDir, `chunk-${String(chunkIndex).padStart(6, "0")}`);
    // eslint-disable-next-line no-await-in-loop -- chunks are hashed and uploaded in order to keep memory bounded.
    const chunk = new Uint8Array(await file.slice(offset, end).arrayBuffer());
    hasher.update(chunk);
    // eslint-disable-next-line no-await-in-loop -- each temporary chunk file is uploaded before the next is created.
    await Bun.write(chunkFile, chunk);

    const chunkKey = `${chunkPrefix}/${String(chunkIndex).padStart(6, "0")}`;
    runWranglerPut(chunkKey, chunkFile, "application/octet-stream");

    chunks.push({
      index: chunkIndex,
      key: chunkKey,
      size: chunk.byteLength,
    });
    const progress = Math.round((end / size) * 1000) / 10;
    console.log(`uploaded chunk ${chunkIndex + 1} (${progress}%)`);
    chunkIndex += 1;
  }

  const version = key.match(/models\/gemma-4-e2b\/(v\d{8})\//u)?.[1] ?? "unknown";
  const fileName = key.split("/").at(-1) ?? "model.task";
  const manifest = {
    schemaVersion: 1,
    kind: "pc-keiba-model-chunks",
    model: "gemma-4-e2b",
    version,
    fileName,
    contentType: "application/octet-stream",
    cacheControl: "public, max-age=31536000, immutable",
    size,
    sha256: hasher.digest("hex"),
    chunkSize,
    chunks,
  };
  const manifestFile = join(workDir, "manifest.json");
  await Bun.write(manifestFile, `${JSON.stringify(manifest, null, 2)}\n`);
  runWranglerPut(manifestKey, manifestFile, "application/json");

  console.log(JSON.stringify({ key, manifestKey, size, sha256: manifest.sha256 }, null, 2));
} finally {
  await rm(workDir, { force: true, recursive: true });
}
