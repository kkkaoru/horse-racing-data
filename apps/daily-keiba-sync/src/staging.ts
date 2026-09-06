const MULTIPART_PART_BYTES = 5 * 1024 * 1024;

const joinChunks = (chunks: readonly Uint8Array[], bytes: number): Uint8Array => {
  const joined = new Uint8Array(bytes);
  let offset = 0;
  for (const chunk of chunks) {
    joined.set(chunk, offset);
    offset += chunk.length;
  }
  return joined;
};

export const putStream = async (
  bucket: R2Bucket,
  key: string,
  stream: ReadableStream<Uint8Array>,
  options: R2MultipartOptions,
  partBytes = MULTIPART_PART_BYTES,
): Promise<void> => {
  if (!Number.isInteger(partBytes) || partBytes <= 0)
    throw new Error("Invalid multipart part size");
  const upload = await bucket.createMultipartUpload(key, options);
  const uploaded: R2UploadedPart[] = [];
  const reader = stream.getReader();
  let buffered: Uint8Array[] = [];
  let bufferedBytes = 0;
  try {
    while (true) {
      const result = await reader.read();
      if (result.done) break;
      buffered.push(result.value);
      bufferedBytes += result.value.length;
      if (bufferedBytes < partBytes) continue;
      const joined = joinChunks(buffered, bufferedBytes);
      let offset = 0;
      while (joined.length - offset >= partBytes) {
        const part = joined.slice(offset, offset + partBytes);
        uploaded.push(await upload.uploadPart(uploaded.length + 1, part));
        offset += partBytes;
      }
      buffered = offset === joined.length ? [] : [joined.slice(offset)];
      bufferedBytes = joined.length - offset;
    }
    if (bufferedBytes > 0) {
      uploaded.push(
        await upload.uploadPart(uploaded.length + 1, joinChunks(buffered, bufferedBytes)),
      );
    }
    if (uploaded.length === 0) throw new Error("Refusing to stage an empty provider stream");
    await upload.complete(uploaded);
  } catch (error: unknown) {
    await upload.abort();
    throw error;
  } finally {
    reader.releaseLock();
  }
};
