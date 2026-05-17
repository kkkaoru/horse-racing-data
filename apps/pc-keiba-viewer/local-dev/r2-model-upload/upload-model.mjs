const endpoint = process.env.R2_UPLOAD_ENDPOINT ?? "http://localhost:8788";
const token = process.env.R2_UPLOAD_TOKEN;
const filePath = process.env.R2_UPLOAD_FILE;
const key = process.env.R2_UPLOAD_KEY;
const partSize = Number(process.env.R2_UPLOAD_PART_SIZE ?? 5 * 1024 * 1024);

if (!token || !filePath || !key) {
  throw new Error("R2_UPLOAD_TOKEN, R2_UPLOAD_FILE, and R2_UPLOAD_KEY are required.");
}

const headers = { "x-upload-token": token };
const file = Bun.file(filePath);
const size = file.size;

const createResponse = await fetch(`${endpoint}/create?key=${encodeURIComponent(key)}`, {
  headers,
  method: "POST",
});
if (!createResponse.ok) {
  throw new Error(`create failed: ${createResponse.status} ${await createResponse.text()}`);
}
const { uploadId } = await createResponse.json();
const parts = [];

try {
  let partNumber = 1;
  for (let offset = 0; offset < size; offset += partSize) {
    const end = Math.min(offset + partSize, size);
    const body = file.slice(offset, end);
    // eslint-disable-next-line no-await-in-loop -- multipart parts are uploaded sequentially to avoid remote dev timeouts.
    const response = await fetch(
      `${endpoint}/part?key=${encodeURIComponent(key)}&uploadId=${encodeURIComponent(
        uploadId,
      )}&partNumber=${partNumber}`,
      {
        body,
        headers,
        method: "PUT",
      },
    );
    if (!response.ok) {
      // eslint-disable-next-line no-await-in-loop -- the error body belongs to the failed part response.
      throw new Error(`part ${partNumber} failed: ${response.status} ${await response.text()}`);
    }
    // eslint-disable-next-line no-await-in-loop -- part order is preserved for R2 multipart completion.
    const part = await response.json();
    parts.push(part);
    const progress = Math.round((end / size) * 1000) / 10;
    console.log(`uploaded part ${partNumber} (${progress}%)`);
    partNumber += 1;
  }

  const completeResponse = await fetch(`${endpoint}/complete?key=${encodeURIComponent(key)}`, {
    body: JSON.stringify({ parts, uploadId }),
    headers: { ...headers, "content-type": "application/json" },
    method: "POST",
  });
  if (!completeResponse.ok) {
    throw new Error(`complete failed: ${completeResponse.status} ${await completeResponse.text()}`);
  }
  console.log(JSON.stringify(await completeResponse.json(), null, 2));
} catch (error) {
  await fetch(`${endpoint}/abort?key=${encodeURIComponent(key)}`, {
    body: JSON.stringify({ uploadId }),
    headers: { ...headers, "content-type": "application/json" },
    method: "POST",
  }).catch(() => {});
  throw error;
}
