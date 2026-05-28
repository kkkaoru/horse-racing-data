const JSON_CONTENT_TYPE = "application/json; charset=utf-8";

export const mergeJsonHeaders = (init?: ResponseInit): Headers => {
  const headers = new Headers({ "content-type": JSON_CONTENT_TYPE });
  if (!init?.headers) {
    return headers;
  }
  const inputHeaders = new Headers(init.headers);
  inputHeaders.forEach((value, key) => {
    headers.set(key, value);
  });
  return headers;
};

export const jsonResponse = (body: unknown, init?: ResponseInit): Response =>
  new Response(JSON.stringify(body), {
    headers: mergeJsonHeaders(init),
    status: init?.status ?? 200,
  });
