// Run with bun. Drain internal service-binding responses without buffering them in memory.

const MAX_INTERNAL_RESPONSE_BYTES = 1024 * 1024;

const drainResponseReader = async (
  reader: ReadableStreamDefaultReader<Uint8Array>,
  bytesRead: number,
): Promise<void> => {
  const chunk = await reader.read();
  if (chunk.done) {
    return;
  }
  const nextBytesRead = bytesRead + chunk.value.byteLength;
  if (nextBytesRead > MAX_INTERNAL_RESPONSE_BYTES) {
    await reader.cancel("internal response exceeded byte limit");
    throw new Error("internal response exceeded byte limit");
  }
  await drainResponseReader(reader, nextBytesRead);
};

export const drainResponseBody = async (response: Response): Promise<Response> => {
  if (response.body) {
    await drainResponseReader(response.body.getReader(), 0);
  }
  return response;
};
