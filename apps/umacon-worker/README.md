# UmaConn Cloudflare Worker

Credential-protected Cloudflare Workers implementation of the verified UmaConn NV-Link stored-data path. It runs without Windows, COM, Wine, or Containers. The official Windows component remains the differential oracle; this is not an official or supported UmaConn runtime.

## API

- `GET /health` — public liveness check
- `GET /compatibility` — Bearer-protected compatibility and R2 artifact attestation
- `POST /acquire/stream` — Bearer-protected NDJSON stream preserving file order, record order, CRLF bytes, and Shift_JIS bytes encoded as Base64

Example body:

```json
{ "dataSpec": "RACE", "fromTime": "20260902000000", "option": 1 }
```

## Secrets

- `NV_CORE_CONFIG_V1` — opaque private-core configuration
- `UMMACON_WORKER_API_TOKEN` — independent public API Bearer token

Do not log either secret, upstream request URLs, downloaded records, or decoded opaque configuration. Initial terms acceptance, software-ID issuance, and revocation remain user operations in the official Windows UI.

## Private core supply chain

Wasm is downloaded from the immutable private GitHub Release pinned by `private-core.lock.json`. SHA-256 is verified before Wrangler statically bundles it. A credential-free provenance copy and manifest are stored in R2 and checked before each acquisition. Workers never dynamically execute R2 bytes.

The host first uses Workers `fetch`; when the legacy origin connection cannot be established there, it falls back to a bounded TLS HTTP/1.1 client over `cloudflare:sockets`. Redirects, response sizes, HTTP framing, archive expansion, and destination schemes remain fail-closed.

```bash
bun run --filter umacon-worker verify:local
bun run --filter umacon-worker deploy
```

Only the observed stored-data lifecycle is declared deployable. Unverified realtime, betting, UI, movie, and watch-event behavior remains outside this Worker.
