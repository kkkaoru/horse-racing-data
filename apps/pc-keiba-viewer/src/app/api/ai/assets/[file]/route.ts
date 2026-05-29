import { NextResponse } from "next/server";

import { safeGetCloudflareEnv } from "../../../../../lib/cloudflare-context.server";

export const dynamic = "force-dynamic";

const AI_ASSET_PREFIX = "assets/ai";
const AI_ASSET_CACHE_CONTROL = "public, max-age=31536000, immutable";
const CONTENT_TYPES = new Map([
  ["png", "image/png"],
  ["jpg", "image/jpeg"],
  ["jpeg", "image/jpeg"],
  ["webp", "image/webp"],
]);

interface RouteContext {
  params: Promise<{
    file: string;
  }>;
}

const isValidAssetFile = (file: string): boolean => /^[a-z0-9][a-z0-9._-]*$/iu.test(file);

const getContentType = (file: string): string => {
  const extension = file.split(".").at(-1)?.toLowerCase() ?? "";
  return CONTENT_TYPES.get(extension) ?? "application/octet-stream";
};

const getAssetRoute = async ({ params }: RouteContext) => {
  const { file } = await params;
  const env = await safeGetCloudflareEnv();
  const bucket = env?.FINISH_POSITION_MODELS;
  return {
    bucket,
    file,
    key: `${AI_ASSET_PREFIX}/${file}`,
  };
};

const getAssetHeaders = (
  file: string,
  object: Pick<PcKeibaR2Object, "httpMetadata" | "size" | "uploaded">,
) => ({
  "Cache-Control": AI_ASSET_CACHE_CONTROL,
  "Content-Length": String(object.size),
  "Content-Type": object.httpMetadata?.contentType ?? getContentType(file),
  ETag: getAssetTag(file, object),
  "Last-Modified": object.uploaded.toUTCString(),
});

const getAssetTag = (file: string, object: Pick<PcKeibaR2Object, "size" | "uploaded">): string =>
  `"${file}-${object.size}-${object.uploaded.getTime()}"`;

const isFreshRequest = (
  request: Request,
  file: string,
  object: Pick<PcKeibaR2Object, "size" | "uploaded">,
): boolean => {
  const ifNoneMatch = request.headers.get("if-none-match");
  if (ifNoneMatch) {
    const etags = new Set(ifNoneMatch.split(",").map((etag) => etag.trim()));
    return etags.has(getAssetTag(file, object));
  }
  const ifModifiedSince = request.headers.get("if-modified-since");
  return ifModifiedSince ? new Date(ifModifiedSince).getTime() >= object.uploaded.getTime() : false;
};

const publicAssetRedirect = (file: string): Response =>
  new Response(null, {
    headers: {
      "Cache-Control": AI_ASSET_CACHE_CONTROL,
      Location: `/ai/${file}`,
    },
    status: 307,
  });

export async function HEAD(request: Request, context: RouteContext) {
  const { bucket, file, key } = await getAssetRoute(context);
  if (!isValidAssetFile(file)) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }
  if (!bucket) {
    return publicAssetRedirect(file);
  }

  const object = await bucket.head(key);
  if (!object) {
    return publicAssetRedirect(file);
  }

  if (isFreshRequest(request, file, object)) {
    return new Response(null, {
      headers: getAssetHeaders(file, object),
      status: 304,
    });
  }
  return new Response(null, {
    headers: getAssetHeaders(file, object),
  });
}

export async function GET(request: Request, context: RouteContext) {
  const { bucket, file, key } = await getAssetRoute(context);
  if (!isValidAssetFile(file)) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }
  if (!bucket) {
    return publicAssetRedirect(file);
  }

  const object = await bucket.get(key);
  if (!object) {
    return publicAssetRedirect(file);
  }

  if (isFreshRequest(request, file, object)) {
    return new Response(null, {
      headers: getAssetHeaders(file, object),
      status: 304,
    });
  }
  return new Response(object.body, {
    headers: getAssetHeaders(file, object),
  });
}
