// Run with bun. Lightweight JSON response helper.

const JSON_CONTENT_TYPE = "application/json; charset=utf-8";

const mergeHeaders = (init?: ResponseInit): Headers => {
  const headers = new Headers(init?.headers);
  headers.set("content-type", JSON_CONTENT_TYPE);
  return headers;
};

export const jsonResponse = (payload: unknown, init?: ResponseInit): Response =>
  new Response(JSON.stringify(payload), { ...init, headers: mergeHeaders(init) });
