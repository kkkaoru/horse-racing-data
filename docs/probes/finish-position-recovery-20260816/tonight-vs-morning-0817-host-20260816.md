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

User still picks. This is the recommendation: **sleep; 08:00
count; host if <32.**
