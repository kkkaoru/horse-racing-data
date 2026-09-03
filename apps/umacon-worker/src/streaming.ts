// Run with bun. Stream every NVOpen file and record without buffering the complete result set.

import {
  downloadFile,
  openAcquisition,
  splitRecords,
  type AcquisitionQuery,
  type NvFetch,
} from "./acquisition";

const ENCODER: TextEncoder = new TextEncoder();
const BASE64_CHUNK_BYTES: number = 8192;

export const encodeBase64 = (bytes: Uint8Array): string => {
  let binary: string = "";
  for (let offset: number = 0; offset < bytes.length; offset += BASE64_CHUNK_BYTES)
    binary += String.fromCharCode(...bytes.subarray(offset, offset + BASE64_CHUNK_BYTES));
  return btoa(binary);
};

const line = (value: object): Uint8Array => ENCODER.encode(`${JSON.stringify(value)}\n`);

export const createDataStream = async (
  config: string,
  query: AcquisitionQuery,
  fetcher: NvFetch = fetch,
): Promise<ReadableStream<Uint8Array>> => {
  const opened = await openAcquisition(config, query, fetcher);
  return new ReadableStream<Uint8Array>({
    async start(controller): Promise<void> {
      try {
        controller.enqueue(
          line({
            event: "open",
            files: opened.entries.length,
            transitions: opened.transitions,
          }),
        );
        let recordCount: number = 0;
        for (const entry of opened.entries) {
          const file = await downloadFile(entry, fetcher);
          const records: Uint8Array[] = splitRecords(file.decoded);
          controller.enqueue(
            line({
              archiveBytes: file.archiveBytes,
              decodedBytes: file.decoded.length,
              event: "file",
              filename: file.filename,
              records: records.length,
            }),
          );
          for (const record of records) {
            controller.enqueue(
              line({
                bytes: record.length,
                data: encodeBase64(record),
                encoding: "base64",
                event: "record",
                filename: file.filename,
              }),
            );
            recordCount += 1;
          }
        }
        controller.enqueue(
          line({
            event: "close",
            files: opened.entries.length,
            records: recordCount,
            transitions: [...opened.transitions, "payload-decoded"],
          }),
        );
        controller.close();
      } catch (error: unknown) {
        controller.error(error);
      }
    },
  });
};
