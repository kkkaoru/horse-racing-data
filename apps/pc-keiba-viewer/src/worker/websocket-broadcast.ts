// Run with bun (vitest) / Cloudflare Workers runtime.
// Shared helpers for the WebSocket Hibernation API used by RaceTrendRoom and
// PaddockRoom. The hibernation runtime auto-removes closed sockets, so a failed
// send is recovered by closing the socket; an already-closing socket close may
// throw, which is swallowed because nothing further can be done.

export const closeSocket = (socket: WebSocket): void => {
  try {
    socket.close();
  } catch {
    // Socket already closing/closed: nothing further to do.
    return;
  }
};

export const trySend = (socket: WebSocket, message: string): void => {
  try {
    socket.send(message);
  } catch {
    closeSocket(socket);
  }
};
