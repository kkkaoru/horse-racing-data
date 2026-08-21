// Run with bun. stdio entry for Agent Plugins mcp.json.
import { stdin, stdout } from "node:process";

import {
  encodeStdioMessage,
  extractStdioMessages,
  forwardMcpJsonRpc,
  loadMcpProxyConfig,
} from "./mcp-stdio-proxy";

const config = loadMcpProxyConfig(process.env);
if (typeof config === "string") {
  process.stderr.write(`${config}\n`);
  process.exit(1);
}

let buffer = "";
stdin.setEncoding("utf8");
stdin.on("data", (chunk: string) => {
  buffer += chunk;
  const extracted = extractStdioMessages(buffer);
  buffer = extracted.rest;
  const replies = extracted.messages.map((message) =>
    forwardMcpJsonRpc(config, message, fetch).then((reply) => {
      if (reply !== null) {
        stdout.write(encodeStdioMessage(reply));
      }
    }),
  );
  void Promise.all(replies);
});
