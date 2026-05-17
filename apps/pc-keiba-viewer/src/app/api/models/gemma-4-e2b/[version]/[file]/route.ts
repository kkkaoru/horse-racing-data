import { getCloudflareContext } from "@opennextjs/cloudflare";
import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

const MODEL_FILE_NAME = "gemma-4-E2B-it-web.task";
const HUGGING_FACE_MODEL_URL =
  "https://huggingface.co/litert-community/gemma-4-E2B-it-litert-lm/resolve/main/gemma-4-E2B-it-web.task?download=true";
const MODEL_CONTENT_TYPE = "application/octet-stream";
const MODEL_CACHE_CONTROL = "public, max-age=31536000, immutable";

interface RouteContext {
  params: Promise<{
    file: string;
    version: string;
  }>;
}

const getCloudflareEnv = (): CloudflareEnv | null => {
  try {
    return getCloudflareContext().env;
  } catch {
    return null;
  }
};

const isValidVersion = (version: string): boolean => /^v\d{8}$/u.test(version);

interface ModelChunkManifest {
  chunks: Array<{
    key: string;
    size: number;
  }>;
  contentType?: string;
  fileName: string;
  schemaVersion: 1;
  sha256?: string;
  size: number;
  version: string;
}

const isModelChunk = (value: unknown): value is ModelChunkManifest["chunks"][number] => {
  if (!value || typeof value !== "object") {
    return false;
  }
  return (
    "key" in value &&
    "size" in value &&
    typeof value.key === "string" &&
    typeof value.size === "number"
  );
};

const getModelRoute = async ({ params }: RouteContext) => {
  const { file, version } = await params;
  const env = getCloudflareEnv();
  const bucket = env?.FINISH_POSITION_MODELS;
  const key = `models/gemma-4-e2b/${version}/${MODEL_FILE_NAME}`;

  return { bucket, file, key, version };
};

const getModelHeaders = (size: number, contentType = MODEL_CONTENT_TYPE) => ({
  "Accept-Ranges": "bytes",
  "Cache-Control": MODEL_CACHE_CONTROL,
  "Content-Length": String(size),
  "Content-Type": contentType,
});

const readChunkManifest = async (
  bucket: PcKeibaR2Bucket,
  key: string,
  version: string,
): Promise<ModelChunkManifest | null> => {
  const object = await bucket.get(`${key}.manifest.json`);
  if (!object) {
    return null;
  }
  const manifest = await object.json<Partial<ModelChunkManifest>>();
  if (
    manifest.schemaVersion !== 1 ||
    manifest.fileName !== MODEL_FILE_NAME ||
    manifest.version !== version ||
    typeof manifest.size !== "number" ||
    !Array.isArray(manifest.chunks) ||
    manifest.chunks.length === 0
  ) {
    return null;
  }

  const chunks = manifest.chunks.filter(isModelChunk);
  if (chunks.length !== manifest.chunks.length) {
    return null;
  }

  return {
    chunks,
    contentType: manifest.contentType,
    fileName: MODEL_FILE_NAME,
    schemaVersion: 1,
    sha256: manifest.sha256,
    size: manifest.size,
    version,
  };
};

const createChunkStream = (
  bucket: PcKeibaR2Bucket,
  manifest: ModelChunkManifest,
): ReadableStream<Uint8Array> =>
  new ReadableStream<Uint8Array>({
    async start(controller) {
      try {
        for (const chunk of manifest.chunks) {
          // eslint-disable-next-line no-await-in-loop -- model chunks must be streamed in manifest order.
          const object = await bucket.get(chunk.key);
          if (!object) {
            throw new Error(`missing model chunk: ${chunk.key}`);
          }
          const reader = object.body.getReader();
          for (;;) {
            // eslint-disable-next-line no-await-in-loop -- each chunk stream must be read sequentially.
            const { done, value } = await reader.read();
            if (done) {
              break;
            }
            controller.enqueue(value);
          }
        }
        controller.close();
      } catch (error) {
        controller.error(error);
      }
    },
  });

export async function HEAD(_request: Request, context: RouteContext) {
  const { bucket, file, key, version } = await getModelRoute(context);

  if (!isValidVersion(version) || file !== MODEL_FILE_NAME) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }

  const object = bucket ? await bucket.head(key) : null;
  if (object) {
    return new Response(null, {
      headers: getModelHeaders(object.size, object.httpMetadata?.contentType),
    });
  }

  const manifest = bucket ? await readChunkManifest(bucket, key, version) : null;
  if (manifest) {
    return new Response(null, {
      headers: getModelHeaders(manifest.size, manifest.contentType),
    });
  }

  if (process.env.NODE_ENV === "development") {
    return NextResponse.redirect(HUGGING_FACE_MODEL_URL, { status: 302 });
  }

  return NextResponse.json({ error: "model_not_found" }, { status: 404 });
}

export async function GET(_request: Request, context: RouteContext) {
  const { bucket, file, key, version } = await getModelRoute(context);

  if (!isValidVersion(version) || file !== MODEL_FILE_NAME) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }

  const object = bucket ? await bucket.get(key) : null;

  if (!object) {
    const manifest = bucket ? await readChunkManifest(bucket, key, version) : null;
    if (bucket && manifest) {
      return new Response(createChunkStream(bucket, manifest), {
        headers: getModelHeaders(manifest.size, manifest.contentType),
      });
    }

    if (process.env.NODE_ENV === "development") {
      return NextResponse.redirect(HUGGING_FACE_MODEL_URL, { status: 302 });
    }
    return NextResponse.json({ error: "model_not_found" }, { status: 404 });
  }

  return new Response(object.body, {
    headers: getModelHeaders(object.size, object.httpMetadata?.contentType),
  });
}
