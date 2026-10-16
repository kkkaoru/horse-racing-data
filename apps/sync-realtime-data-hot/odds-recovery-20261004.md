# A8/05 odds recovery (2026-10-04)

- Target: `/races/2026/10/04/A8/05` (`jra:2026:1004:A8:05`).
- Before: production hot D1 had no fetch-state row or fetch log for this race.
- Official card verified by HTTP: https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0110520260101051/9D; post time 23:05 JST.
- Registered this race using the existing authenticated odds-fetch-state endpoint. Added its official card to the existing known overseas URL fallback; no new fetching architecture.
- Deployed hot Worker version `2bbec474-4f78-4da3-8c2d-56883b8df738`.
- Existing run-fetch-odds endpoint returned success. D1 recorded `last_odds_fetch_at=2026-10-04T19:06:26+09:00` and the correct card URL/post time.
- Hot canonical API verified seven bet types: win 16, place 16, quinella 120, wide 120, exacta 240, trio 560, trifecta 3360 (`/tmp/a8-hot-verified.json`).
- Viewer realtime API returned 16 win odds at `2026-10-04T10:07:53.395Z`; production HTML included win odds in its initial payload. This verifies the HTTP/server-rendered display path, not an interactive browser check or every bet type.
- Verification: TypeScript and lint passed; 836 tests passed. Coverage: statements 98.84%, branches 96.20%, functions 97.77%, lines 99.16%. Thresholds unchanged.
- Evidence: `/tmp/a8-hot-state-before.json`, `/tmp/a8-hot-state-after.json`, `/tmp/a8-fetch-result.json`, `/tmp/a8-realtime-verified.json`, `/tmp/a8-page-after.html`, `/tmp/a8-hot-coverage.log`, `/tmp/a8-hot-deploy.log`.
