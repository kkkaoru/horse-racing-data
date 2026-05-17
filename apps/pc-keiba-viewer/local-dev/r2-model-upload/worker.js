export default {
  async fetch(request, env) {
    const token = request.headers.get("x-upload-token");
    if (!env.UPLOAD_TOKEN || token !== env.UPLOAD_TOKEN) {
      return Response.json({ error: "unauthorized" }, { status: 401 });
    }

    const url = new URL(request.url);
    const key = url.searchParams.get("key");
    if (!key || !key.startsWith("models/gemma-4-e2b/")) {
      return Response.json({ error: "invalid key" }, { status: 400 });
    }

    if (url.pathname === "/create" && request.method === "POST") {
      const upload = await env.FINISH_POSITION_MODELS.createMultipartUpload(key, {
        httpMetadata: {
          contentType: "application/octet-stream",
        },
      });
      return Response.json({ key: upload.key, uploadId: upload.uploadId });
    }

    if (url.pathname === "/part" && request.method === "PUT") {
      const uploadId = url.searchParams.get("uploadId");
      const partNumber = Number(url.searchParams.get("partNumber"));
      if (!uploadId || !Number.isInteger(partNumber) || partNumber <= 0 || !request.body) {
        return Response.json({ error: "invalid part" }, { status: 400 });
      }
      const upload = env.FINISH_POSITION_MODELS.resumeMultipartUpload(key, uploadId);
      const part = await upload.uploadPart(partNumber, request.body);
      return Response.json(part);
    }

    if (url.pathname === "/complete" && request.method === "POST") {
      const { parts, uploadId } = await request.json();
      if (!uploadId || !Array.isArray(parts)) {
        return Response.json({ error: "invalid complete payload" }, { status: 400 });
      }
      const upload = env.FINISH_POSITION_MODELS.resumeMultipartUpload(key, uploadId);
      const object = await upload.complete(parts);
      return Response.json({ key: object.key, size: object.size });
    }

    if (url.pathname === "/abort" && request.method === "POST") {
      const { uploadId } = await request.json();
      if (!uploadId) {
        return Response.json({ error: "invalid abort payload" }, { status: 400 });
      }
      const upload = env.FINISH_POSITION_MODELS.resumeMultipartUpload(key, uploadId);
      await upload.abort();
      return Response.json({ ok: true });
    }

    return Response.json({ error: "not_found" }, { status: 404 });
  },
};
