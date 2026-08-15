# DAY_BASE_SPLIT and the 2026-08-12 outage (read-only)

Investigated 2026-08-16 06:54–07:00 JST. No secret value printed. No
deploy. Answers the three questions from advisor 06:53.

## 1. What changed in the 08-12 deploy

Handoff (`docs/race-day-handoff-2026-08-15.md` §1): after the
**2026-08-12 17:29 JST** deploy, finish-position output was absent for
three days (08-13 8 races, 08-14 19, 08-15 all 68).

`wrangler deployments list` (read-only, 06:50 JST) last **Upload**
before that window:

`aab128a1-9b7a-468c-b976-11b540a61a55` at **2026-08-12T08:29:40Z**
(= 17:29 JST). Matches the handoff clock.

Git commits that were already on `main` by the night of 08-11 and
would have been in a 08-12 image (not an exhaustive image bill of
materials; Cloudflare does not list the git SHA on that row):

| commit     | time JST    | what                                                                   |
| ---------- | ----------- | ---------------------------------------------------------------------- |
| `ac1a9ea2` | 08-11 21:42 | per-race-only generation; day-base PREWARM stays a non-generation path |
| `3d75c0d1` | 08-11 21:49 | R2 watermark metadata so shards can reuse a prewarmed day-base         |

`0f13c58a` (startup connect timeout) is recorded in the same handoff
as **intentionally not deployed** that night.

Immediate cause in the handoff, not inferred here:

> Cloudflare Queue `finish-position-predict-queue` had
> `delivery_paused=true`. Producers still accepted enqueue. The
> consumer received nothing.

The actor who paused delivery is **not known**. Audit Logs Read is
missing. Do not attribute the pause to deploy, split, or a person.

## 2. Did that deploy enable `DAY_BASE_SPLIT_ENABLED`?

What is recorded:

- Split code landed **2026-07-12** (`58cb8b93` / `e9ca8843` /
  `fae4ad82`). Default is **off**. `wrangler.jsonc` has no `vars`
  entry. Enablement is `wrangler secret put` only.
- `wrangler secret list` today: the **key** `DAY_BASE_SPLIT_ENABLED`
  is present. The **value is not readable**. Empty and `jra` look the
  same from here.
- 2026-07-17 (`0ecc0c39`, `9da2f5eb`): secret already existed; value
  already unreadable; flipping it as-is was judged **zero upside**
  because `ensure_day_base()` returned `None` for `r2-catalog://`
  (`e6111ca6`).
- 2026-08-11 `3d75c0d1` is the first commit that lets catalog sources
  trust a watermarked day-base. An 08-12 image is therefore the first
  production image where a non-empty allowlist **could** change
  behavior.
- No git commit between 08-11 and 08-15 says the secret was set to a
  category allowlist. No probe note records a `secret put` that night.

Conclusion that is allowed: **there is no record that the 08-12
deploy enabled the split.** The key existing does not mean the
allowlist is non-empty. Whether the 08-12 image ran with a live
allowlist is **unknown**.

## 3. Why did 08-14 restore keep split off?

Recorded messages, not interpretation:

- 08-14 14:45 UTC: rollback to `953d086b`
  ("Rollback FP generation regression; no predictions after
  2026-08-12 deploy").
- 08-14 15:00 UTC: restore `0c76062e`
  ("Restore current image after identifying paused queue root cause;
  **keep split off**").

So: they identified the paused queue, put the 08-14 image back, and
wrote down that split stays off. There is **no longer document** that
says split caused the 08-12 gap, or that it was ruled out. "Keep
split off" is an operational choice on the restore row. The reason
beyond those six words is **not recorded**.

## What this is not

- Not proof that split caused 08-12. The written cause is the paused
  queue.
- Not proof that split is safe to turn on. The 08-12 image is the
  first that could make the flag do work, and the restore
  deliberately left it off.
- Not a reason to `secret put` tonight.

Before any proposal says "enable split", re-read this file. The next
step is a dedicated, off-card investigation: secret value via a
channel that can read it, plus whether 08-12's pause coincided with
any allowlist change. That is not tonight's work.
