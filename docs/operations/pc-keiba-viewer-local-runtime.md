# pc-keiba-viewer local Node runtime troubleshooting

Last updated: 2026-08-15 07:05 JST

This note records a local-only failure mode seen when running the viewer with
`bun run start:local` (`next start`). It is not a production incident and no
production deploy was performed while investigating it.

## Symptom cluster

The same local server emitted three apparently separate failures:

1. Wrangler warned that the internal `PaddockRoom` and `RaceTrendRoom` Durable
   Object classes were not exported.
2. workerd terminated with `SQLITE_BUSY_RECOVERY` / `database is locked` for a
   Durable Object SQLite database.
3. Next.js repeatedly threw
   `TypeError: Failed to parse URL from [object Request]`.

Treat these as one failure until the process tree disproves it. In the observed
case all three had one root cause.

## Confirmed root cause

The affected process was `bun run start:local`, not `next dev`. The running
process was a production Node `next-server` (PID 28147 during the investigation).

`safeGetCloudflareRuntime()` called
`getCloudflareContext({ async: true })`. In a Node runtime without an existing
OpenNext global context, OpenNext's asynchronous path calls Wrangler's
`getPlatformProxy()`. Because no explicit dev config is passed at that point,
Wrangler loads the production `wrangler.jsonc`.

That caused the full chain:

- the sourceless platform proxy exposed the production internal Durable Object
  bindings without the application Durable Object exports, producing the class
  warnings;
- several proxy/workerd instances opened the same local persistent SQLite
  state, producing `SQLITE_BUSY_RECOVERY`;
- the resulting `PADDOCK_ROOM` binding was a platform-proxy stub rather than the
  production application Durable Object, and `room.fetch(Request)` failed while
  converting the request to a URL. The source map resolved the failure to the
  `room.fetch(getRoomRequest(...))` call in `src/lib/paddock-server.ts`.

The fix is in `src/lib/cloudflare-context.server.ts`: read the already-installed
Cloudflare global context synchronously first. A production Node server must not
start an implicit Wrangler proxy when that global is missing. Development may
still fall back to the asynchronous Wrangler path, preserving `next dev`
bindings.

## Why the production Worker behavior is unchanged

The helper is shared with the production OpenNext bundle, so this boundary was
checked rather than assumed safe.

The generated OpenNext Worker entry calls
`runWithCloudflareRequestContext(request, env, ctx, handler)`. The generated
`.open-next/cloudflare/init.js` defines
`Symbol.for("__cloudflare-context__")` as a getter over an
`AsyncLocalStorage`, then executes the complete handler (including the dynamic
server-handler import) inside:

```text
cloudflareContextALS.run({ env, ctx, cf: request.cf }, handler)
```

Therefore request-time production calls can retrieve the real context
synchronously. An AST scan of all non-test TypeScript/TSX sources found:

```text
calls=64
top_level=0
```

The 64 calls include the two wrapper-to-wrapper calls inside
`cloudflare-context.server.ts`; no helper call occurs during module top-level
initialization.

OpenNext's previous asynchronous implementation also reads this same global
first. If it is missing in a Worker, it does not wait for a later Worker
context: `getPlatformProxy()` is only the Node.js/SSG fallback, and the Worker
path otherwise throws. The safe wrapper then returns null. Consequently both
success behavior (the real request context) and failure behavior (null) are
unchanged in production. Code invoked outside the handler's async context would
have returned null before and after this fix.

## Isolated validation

The fix was validated without stopping or rebuilding the existing local
server:

1. Create a detached temporary worktree.
2. Apply only the implementation and test diff.
3. Run the production Next.js build.
4. Start it on port 3107 with `bun run start:local`.
5. Request both the reproducing paddock API endpoint and paddock page.
6. Compare workerd process counts and scan the server log.

Observed output:

```text
paddock_api_status=200 paddock_page_status=200 workerd_before=5 workerd_after=5
```

The isolated server log contained zero matches for:

```text
SQLITE_BUSY
database is locked
Failed to parse URL
no such Durable Object class
Durable Objects.*will not work
```

Quality checks after the final runtime-global-first implementation:

```text
Test Files  179 passed (179)
Tests       4061 passed (4061)
Statements  99.31% (3477/3501)
Branches    97.33% (2157/2216)
Functions   99.16% (836/843)
Lines       99.38% (3252/3272)
```

`oxfmt`, `oxlint`, CSS lint, and `tsc --noEmit` also exited successfully. Python
validation completed with 4,747 passing tests and 97.52% total coverage.

## Operational notes

- Do not delete Wrangler state as the first response. Confirm which parent
  process created each workerd instance first.
- Do not remove production Durable Object bindings from `wrangler.jsonc` to
  silence local warnings.
- Do not change `room.fetch(Request)` solely to accommodate a sourceless
  platform-proxy stub; the production Durable Object accepts the Request.
- A currently running `next start` process keeps the old `.next` build. Apply
  the fix with a coordinated stop, `bun run build`, and restart. Notify other
  local agents before stopping a shared viewer process.
