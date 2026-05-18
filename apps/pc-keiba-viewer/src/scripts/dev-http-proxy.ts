import http, { type IncomingMessage, type ServerResponse } from "node:http";
import https from "node:https";
import type { Socket } from "node:net";
import tls from "node:tls";

const LISTEN_HOST = process.env.PC_KEIBA_DEV_PROXY_HOST ?? "127.0.0.1";
const LISTEN_PORT = Number(process.env.PC_KEIBA_DEV_PROXY_PORT ?? "3000");
const TARGET_ORIGIN = process.env.PC_KEIBA_DEV_PROXY_TARGET ?? "https://localhost";
const TEXT_REWRITE_MAX_BYTES = Number(
  process.env.PC_KEIBA_DEV_PROXY_REWRITE_MAX_BYTES ?? "5242880",
);
const target = new URL(TARGET_ORIGIN);
const targetPort = Number(target.port || (target.protocol === "https:" ? 443 : 80));

if (target.protocol !== "https:") {
  throw new Error("PC_KEIBA_DEV_PROXY_TARGET must be an https:// URL.");
}

const getPublicOrigins = (
  req: IncomingMessage,
): {
  http: string;
  ws: string;
} => {
  const host = req.headers.host ?? `${LISTEN_HOST}:${LISTEN_PORT}`;
  return {
    http: `http://${host}`,
    ws: `ws://${host}`,
  };
};

const rewriteProxyText = (text: string, req: IncomingMessage): string => {
  const publicOrigins = getPublicOrigins(req);
  return text
    .replaceAll(target.origin, publicOrigins.http)
    .replaceAll(`https://${target.host}`, publicOrigins.http)
    .replaceAll(`wss://${target.host}`, publicOrigins.ws);
};

const rewriteLocationHeader = (
  location: string | undefined,
  req: IncomingMessage,
): string | undefined => {
  return typeof location === "string" ? rewriteProxyText(location, req) : location;
};

const contentTypeSupportsRewrite = (contentType: string | string[] | undefined): boolean => {
  const value = Array.isArray(contentType) ? contentType.join(",") : (contentType ?? "");
  return /(?:text\/html|text\/css|application\/(?:javascript|json)|text\/javascript)/iu.test(value);
};

const shouldRewriteBody = (req: IncomingMessage, headers: http.IncomingHttpHeaders): boolean => {
  if ((req.url ?? "").startsWith("/api/models/")) {
    return false;
  }
  if (!contentTypeSupportsRewrite(headers["content-type"])) {
    return false;
  }
  const contentLength = Number(headers["content-length"] ?? "0");
  return !contentLength || contentLength <= TEXT_REWRITE_MAX_BYTES;
};

const proxyHttpRequest = (req: IncomingMessage, res: ServerResponse): void => {
  const headers = { ...req.headers };
  headers["x-forwarded-host"] = req.headers.host ?? `${LISTEN_HOST}:${LISTEN_PORT}`;
  headers["x-forwarded-proto"] = "http";
  delete headers["accept-encoding"];

  const proxyReq = https.request(
    {
      headers,
      hostname: target.hostname,
      method: req.method,
      path: req.url ?? "/",
      port: targetPort,
      rejectUnauthorized: false,
      servername: target.hostname,
    },
    (proxyRes) => {
      const responseHeaders = { ...proxyRes.headers };
      const location = rewriteLocationHeader(responseHeaders.location, req);
      if (location) {
        responseHeaders.location = location;
      } else {
        delete responseHeaders.location;
      }
      if (!shouldRewriteBody(req, responseHeaders)) {
        res.writeHead(proxyRes.statusCode ?? 502, proxyRes.statusMessage, responseHeaders);
        proxyRes.pipe(res);
        return;
      }

      const chunks: Buffer[] = [];
      let receivedBytes = 0;
      proxyRes.on("data", (chunk: Buffer) => {
        receivedBytes += chunk.byteLength;
        chunks.push(chunk);
      });
      proxyRes.on("end", () => {
        if (receivedBytes > TEXT_REWRITE_MAX_BYTES) {
          delete responseHeaders["content-length"];
          res.writeHead(proxyRes.statusCode ?? 502, proxyRes.statusMessage, responseHeaders);
          res.end(Buffer.concat(chunks));
          return;
        }
        const rewritten = rewriteProxyText(Buffer.concat(chunks).toString("utf8"), req);
        const body = Buffer.from(rewritten);
        delete responseHeaders["content-encoding"];
        delete responseHeaders["transfer-encoding"];
        responseHeaders["content-length"] = String(body.byteLength);
        res.writeHead(proxyRes.statusCode ?? 502, proxyRes.statusMessage, responseHeaders);
        res.end(body);
      });
    },
  );

  proxyReq.on("error", (error) => {
    if (!res.headersSent) {
      res.writeHead(502, { "content-type": "text/plain; charset=utf-8" });
    }
    res.end(`dev proxy upstream error: ${error.message}`);
  });

  req.pipe(proxyReq);
};

const writeUpgradeRequest = (req: IncomingMessage, upstream: tls.TLSSocket, head: Buffer): void => {
  const path = req.url ?? "/";
  upstream.write(`${req.method ?? "GET"} ${path} HTTP/${req.httpVersion}\r\n`);
  for (const [name, value] of Object.entries(req.headers)) {
    if (value === undefined) {
      continue;
    }
    if (Array.isArray(value)) {
      for (const item of value) {
        upstream.write(`${name}: ${item}\r\n`);
      }
      continue;
    }
    upstream.write(`${name}: ${value}\r\n`);
  }
  upstream.write(`x-forwarded-host: ${req.headers.host ?? `${LISTEN_HOST}:${LISTEN_PORT}`}\r\n`);
  upstream.write("x-forwarded-proto: http\r\n");
  upstream.write("\r\n");
  if (head.length > 0) {
    upstream.write(head);
  }
};

const proxyWebSocketUpgrade = (req: IncomingMessage, socket: Socket, head: Buffer): void => {
  const upstream = tls.connect({
    host: target.hostname,
    port: targetPort,
    rejectUnauthorized: false,
    servername: target.hostname,
  });

  upstream.once("secureConnect", () => {
    writeUpgradeRequest(req, upstream, head);
    upstream.pipe(socket);
    socket.pipe(upstream);
  });

  upstream.on("error", () => {
    socket.destroy();
  });
  socket.on("error", () => {
    upstream.destroy();
  });
};

const server = http.createServer(proxyHttpRequest);
server.on("upgrade", proxyWebSocketUpgrade);
server.listen(LISTEN_PORT, LISTEN_HOST, () => {
  console.info(
    `pc-keiba dev HTTP proxy ready: http://${LISTEN_HOST}:${LISTEN_PORT} -> ${target.origin}`,
  );
});
