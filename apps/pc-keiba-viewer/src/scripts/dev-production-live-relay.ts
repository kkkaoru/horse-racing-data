const LISTEN_HOST = process.env.PC_KEIBA_PRODUCTION_LIVE_RELAY_HOST ?? "127.0.0.1";
const LISTEN_PORT = Number(process.env.PC_KEIBA_PRODUCTION_LIVE_RELAY_PORT ?? "3010");
const PRODUCTION_ORIGIN = (
  process.env.PC_KEIBA_PRODUCTION_API_ORIGIN ?? "https://pc-keiba-viewer.kkk4oru.com"
).replace(/\/+$/u, "");
const CLIENT_ID = process.env.PC_KEIBA_ACCESS_CLIENT_ID?.trim();
const CLIENT_SECRET = process.env.PC_KEIBA_ACCESS_CLIENT_SECRET?.trim();

if (!CLIENT_ID || !CLIENT_SECRET) {
  throw new Error(
    "PC_KEIBA_ACCESS_CLIENT_ID and PC_KEIBA_ACCESS_CLIENT_SECRET are required for the live relay.",
  );
}

const production = new URL(PRODUCTION_ORIGIN);

interface RelaySocketData {
  path: string;
  upstream: WebSocket | null;
}

const getAccessHeaders = (): Record<string, string> => ({
  "CF-Access-Client-Id": CLIENT_ID,
  "CF-Access-Client-Secret": CLIENT_SECRET,
});

const isLiveRelayRunning = async (): Promise<boolean> => {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 500);
    await fetch(`http://${LISTEN_HOST}:${LISTEN_PORT}/`, { signal: controller.signal });
    clearTimeout(timeout);
    return true;
  } catch {
    return false;
  }
};

if (await isLiveRelayRunning()) {
  console.log(
    `[production-live-relay] already listening on ws://${LISTEN_HOST}:${LISTEN_PORT}. Skipping start.`,
  );
  process.exit(0);
}

Bun.serve<RelaySocketData>({
  hostname: LISTEN_HOST,
  port: LISTEN_PORT,
  fetch(req, server) {
    const url = new URL(req.url);
    const path = `${url.pathname}${url.search}`;
    if (server.upgrade(req, { data: { path, upstream: null } })) {
      return undefined;
    }
    return new Response("Upgrade Required", { status: 426 });
  },
  websocket: {
    close(ws) {
      ws.data.upstream?.close();
    },
    message(ws, message) {
      ws.data.upstream?.send(message);
    },
    open(ws) {
      const upstream = new WebSocket(`wss://${production.host}${ws.data.path}`, {
        headers: getAccessHeaders(),
      });
      ws.data.upstream = upstream;
      upstream.addEventListener("message", (event) => {
        ws.send(event.data);
      });
      upstream.addEventListener("close", () => {
        ws.close();
      });
      upstream.addEventListener("error", () => {
        ws.close();
      });
    },
  },
});

console.log(
  `[production-live-relay] ws://${LISTEN_HOST}:${LISTEN_PORT} -> ${production.origin} (Access service token)`,
);

await new Promise(() => {
  // Keep the relay alive until the parent process exits.
});
