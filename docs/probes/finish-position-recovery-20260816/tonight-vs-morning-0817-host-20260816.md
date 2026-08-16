# Tonight vs morning for the 0817 NAR gaps

Opinion, from today’s clocks. Not a deploy.

**Prefer morning.** Do not start a host one-shot tonight.

Why “14h-old vs 30-min-old weights” is the wrong split:
today weights arrived 30–50 min before post. A 22:00 write and
an 08:15 write are both **weightless** for 35/01 11:45. The
rescore path is what should overlay bataiju, and today that
path did **not** land before post. Generating at 22:00 does
not buy a fresher weight. It only writes another market-free
row earlier.

Why tonight does **not** remove the sleep risk:
if nobody is up at 08:00, a 22:00 file still sits at 8/32+gaps
with a dead rescore. First post is still 11:45. The 08:00
count is still required. Tonight’s generate is extra work, not
a substitute alarm.

Why morning is enough:
cold NAR budget **90 min**. 08:15 start → ~09:45. 11:45 post
has margin. 08-16 leftover 8 rows: UPSERT, do not DELETE.
46 is empty; do that venue.

Cost of tonight: same Mac, same FORCE 8/4, no parallel
categories, and we are still in racing hours until 20:50.
A 22:00 start is allowed by the deploy ban but competes with
A8’s oversea window on the same machine. Not worth it.

## Is 90 min enough? (margin, not a promise)

Tonight NAR **27 min** was after JRA layers existed. Do not use 27.
Cold NAR = Iceberg base + 10 layers + score. 46 is a new venue in
the same `nar` category (one `PREDICT_CATEGORIES=nar` run, not a
third category). 8 leftover rows do not skip the base rebuild.

90 min is a **budget**, like tonight’s JRA resume+flush ~80 min
plus slack. If it slips to **150 min** (OOM once, resume, no
parallel): 08:15 → 10:45. Still **60 min** before 11:45.
If it slips to **180 min**: 11:15. Tight, still before post.

Do **not** start before 08:15 to “buy” the 27-min number. Buy
the 08:00 count instead. If 08:00 is already 32, skip.
If someone can start at **08:00** after a 2-minute count, that
is the only useful pull-forward (~15 min). Not 07:00.

User still picks. This is the recommendation: **sleep; 08:00
count; host if <32.**
