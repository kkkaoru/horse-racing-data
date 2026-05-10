export const mergeJsonHeaders = (init?: ResponseInit): Headers => {
  const headers = new Headers({
    "content-type": "application/json; charset=utf-8",
  });

  if (!init?.headers) {
    return headers;
  }

  const inputHeaders = new Headers(init.headers);
  inputHeaders.forEach((value, key) => {
    headers.set(key, value);
  });
  return headers;
};
