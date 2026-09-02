// Run with bun. Worker-native JVCourseFile/JVCourseFile2 equivalent.

import type { JvFetch } from "./acquisition";
import type { CoreConfig } from "./protocol";
import { acquireRealtimeData } from "./realtime";

export interface CourseResult {
  explanation: Uint8Array;
  image: Uint8Array;
  path: string;
}

const COURSE_DATA_SPEC: string = "0B72";
const COURSE_KEY_PATTERN: RegExp = /^\d{16}$/;
const COURSE_RECORD_BYTES: number = 6994;
const COURSE_PATH_BYTES: number = 192;
const MAX_IMAGE_BYTES: number = 2 * 1024 * 1024;
const DATA_ORIGIN: string = "http://datalab.cdn.jra-van.ne.jp";
const ASCII_DECODER: TextDecoder = new TextDecoder("ascii", { fatal: true, ignoreBOM: false });

export const validateCourseKey = (key: string): string => {
  if (!COURSE_KEY_PATTERN.test(key)) throw new Error("course key must contain sixteen digits");
  return key;
};

export const acquireCourse = async (
  config: CoreConfig,
  key: string,
  fetcher: JvFetch = fetch,
): Promise<CourseResult> => {
  const realtime = await acquireRealtimeData(
    config,
    { dataSpec: COURSE_DATA_SPEC, key: validateCourseKey(key) },
    fetcher,
  );
  if (realtime.records.length !== 1)
    throw new Error("JV course response has invalid record framing");
  const record = realtime.records[0]!;
  if (record.length !== COURSE_RECORD_BYTES)
    throw new Error("JV course response has invalid record framing");
  const path = ASCII_DECODER.decode(record.subarray(0, COURSE_PATH_BYTES)).trimEnd();
  if (!/^\/datalab\/crsimg\/[A-Za-z0-9._-]+\.gif$/.test(path))
    throw new Error("JV course image path is unsafe");
  const explanation = record.slice(COURSE_PATH_BYTES, -2);
  const response = await fetcher(`${DATA_ORIGIN}${path}`, { method: "GET", redirect: "manual" });
  if (!response.ok) throw new Error("JV course image endpoint returned an HTTP error");
  const image = new Uint8Array(await response.arrayBuffer());
  if (image.length < 6 || image.length > MAX_IMAGE_BYTES)
    throw new Error("JV course image has an invalid size");
  const signature = ASCII_DECODER.decode(image.subarray(0, 6));
  if (signature !== "GIF87a" && signature !== "GIF89a")
    throw new Error("JV course image is not GIF data");
  return { explanation, image, path };
};
