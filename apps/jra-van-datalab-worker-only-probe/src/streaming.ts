// Run with bun. Stream every JVOpen file and record without buffering the complete result set.

import {
  downloadJvFile,
  openJvData,
  splitJvRecords,
  type AcquisitionQuery,
  type JvFetch,
} from "./acquisition";
import type { CoreConfig } from "./protocol";

const ENCODER: TextEncoder = new TextEncoder();
const BASE64_CHUNK_BYTES: number = 8192;

export const encodeBase64 = (bytes: Uint8Array): string => {
  let binary = "";
  for (let offset = 0; offset < bytes.length; offset += BASE64_CHUNK_BYTES)
    binary += String.fromCharCode(...bytes.subarray(offset, offset + BASE64_CHUNK_BYTES));
  return btoa(binary);
};

const line = (value: object): Uint8Array => ENCODER.encode(`${JSON.stringify(value)}\n`);

export const createJvDataStream = async (
  config: CoreConfig,
  query: AcquisitionQuery,
  fetcher: JvFetch = fetch,
): Promise<ReadableStream<Uint8Array>> => {
  const opened = await openJvData(config, query, fetcher);
  return new ReadableStream<Uint8Array>({
    async start(controller): Promise<void> {
      try {
        controller.enqueue(
          line({
            event: "open",
            lastFileTimestamp: opened.lastFileTimestamp,
            readCount: opened.readCount,
            transitions: opened.transitions,
          }),
        );
        let recordCount = 0;
        for (const entry of opened.entries) {
          const file = await downloadJvFile(entry, fetcher);
          const records = splitJvRecords(file.decoded);
          controller.enqueue(
            line({
              decodedBytes: file.decoded.length,
              event: "file",
              fileBytes: file.fileBytes,
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
      } catch (error) {
        controller.error(error);
      }
    },
  });
};
