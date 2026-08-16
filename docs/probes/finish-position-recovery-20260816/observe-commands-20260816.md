# Weight-rescore observe commands (copy-paste)

Read-only. No deploy. No PUT. No POST `/run`.
Primary clock is D1 `fetch-weights` status=ok, not JV/`nvd_se`.
Confirm landing, then take `containers list` in the **same minute**.
Do not infer HIT/MISS from Last-Modified.

Repo root. `source .env` once (never print it).

```sh
cd /Users/kkk4oru/ghq/github.com/kkkaoru/horse-racing-data
set -a; source .env; set +a
DATE=0816          # tomorrow: 0817
YMD=20260816       # tomorrow: 20260817
```

## 1. D1 — did weights / trigger fire?

```sh
cd apps/sync-realtime-data
bunx wrangler d1 execute sync-realtime-data --remote --command \
  "select race_key, status, created_at
   from fetch_logs
   where job_type='weight-rescore-trigger'
     and created_at >= '2026-08-16 09:00:00+09:00'
   order by created_at"
```

Tomorrow: change the timestamp to that morning.

First `fetch-weights` ok per race (landing):

```sh
bunx wrangler d1 execute sync-realtime-data --remote --command \
  "select race_key, min(created_at) as first_ok
   from fetch_logs
   where job_type='fetch-weights' and status='ok'
     and created_at >= '2026-08-16 09:00:00+09:00'
     and (race_key like 'jra:2026:0816:%' or race_key like 'nar:2026:0816:%')
   group by 1 order by 2"
```

Still empty? Look for `skip:weights-empty`. That is **not** a write.

```sh
bunx wrangler d1 execute sync-realtime-data --remote --command \
  "select race_key, status, created_at
   from fetch_logs
   where job_type='fetch-weights'
     and created_at >= '2026-08-16 09:00:00+09:00'
     and status in ('ok','skip:weights-empty','queued:weights-empty-retry')
   order by created_at desc limit 20"
```

Ban-ei keys are `nar:2026:0816:83:*`.

## 2. Neon — did `prediction_generated_at` move?

```sh
cd /Users/kkk4oru/ghq/github.com/kkkaoru/horse-racing-data
NEON_DATABASE_URL="$NEON_PRIMARY_URL" uv run --quiet --with 'psycopg[binary]' python - <<'PY'
import os,psycopg
DATE="0816"
with psycopg.connect(os.environ["NEON_DATABASE_URL"],connect_timeout=15) as c,c.cursor() as cur:
    for code,bango in [("04","01"),("07","01"),("01","01"),("01","02"),
                       ("35","01"),("44","01"),("55","01"),("83","01")]:
        cur.execute("""
          select count(*), min(prediction_generated_at)::text
          from race_finish_position_model_predictions
          where kaisai_nen='2026' and kaisai_tsukihi=%s
            and keibajo_code=%s and race_bango=%s
        """, (DATE, code, bango))
        print(f"{code}/{bango}", cur.fetchone())
    cur.execute("""
      select count(distinct (keibajo_code, race_bango)), count(*),
             max(prediction_generated_at)::text
      from race_finish_position_model_predictions
      where kaisai_nen='2026' and kaisai_tsukihi=%s
    """, (DATE,))
    print("all", cur.fetchone())
PY
```

Coverage today must stay **80 / 940** unless a new card appears.

## 3. JV / NAR weight columns (auxiliary)

D1 wins if these disagree.

```sh
NEON_DATABASE_URL="$NEON_PRIMARY_URL" uv run --quiet --with 'psycopg[binary]' python - <<'PY'
import os,psycopg
DATE="0816"
sql = """
  select keibajo_code, count(*) as n,
         count(*) filter (
           where nullif(trim(bataiju),'') is not null
             and trim(bataiju) not in ('','000')
         ) as with_w
  from {table}
  where kaisai_nen='2026' and kaisai_tsukihi=%s
    and keibajo_code in ({codes})
  group by 1 order by 1
"""
with psycopg.connect(os.environ["NEON_DATABASE_URL"],connect_timeout=15) as c,c.cursor() as cur:
    print("jvd_se")
    cur.execute(sql.format(table="jvd_se", codes="'01','04','07'"), (DATE,))
    for r in cur.fetchall(): print(r)
    print("nvd_se")
    cur.execute(sql.format(table="nvd_se", codes="'35','44','55','83'"), (DATE,))
    for r in cur.fetchall(): print(r)
PY
```

## 4. Containers — same minute as landing

Only after D1 `fetch-weights` ok. Baseline 08-16 11:20 / 12:01: LIVE **9**.

```sh
cd apps/finish-position-cron
bunx wrangler containers list
```

Read LIVE INSTANCES, STATE, LAST MODIFIED for
`finish-position-cron-finishpositionpredictcontainer`. Cap is 10.
Do not stop or start.

Optional names:

```sh
bunx wrangler containers instances a0348266-3050-47d4-9bad-b04086c1a02b
```

## 5. R2 Last-Modified (PUT only)

GET does not change this. Still 07:49 = no overwrite, not unread.

```sh
cd /Users/kkk4oru/ghq/github.com/kkkaoru/horse-racing-data
uv run --quiet --with boto3 python - <<'PY'
import os, boto3
from botocore.config import Config
acct = os.environ.get("R2_ACCOUNT_ID") or os.environ.get("CLOUDFLARE_ACCOUNT_ID")
s3 = boto3.client(
    "s3",
    endpoint_url=os.environ.get("R2_ENDPOINT_URL") or f"https://{acct}.r2.cloudflarestorage.com",
    aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    region_name="auto",
    config=Config(signature_version="s3v4"),
)
bucket = os.environ.get("R2_BUCKET", "pc-keiba-features-archive")
ymd = "20260816"
for cat, kei, race in [
    ("jra","04","01"),("jra","07","01"),("jra","01","01"),("jra","01","02"),
    ("nar","35","01"),("nar","44","01"),("nar","55","01"),
    ("ban-ei","83","01"),
]:
    key = f"feat-cache/catalog-v1/{cat}/{ymd}/{kei}/{race}/features.parquet"
    try:
        r = s3.head_object(Bucket=bucket, Key=key)
        print(key, "size", r["ContentLength"], "lm", r["LastModified"].isoformat())
    except Exception as e:
        print(key, type(e).__name__, str(e)[:120])
PY
```

## 6. Optional

Queue depth:

```sh
cd apps/finish-position-cron
bunx wrangler queues list
```

Retry / DLQ (`recorded_at`, not `created_at`):

```sh
bunx wrangler d1 execute finish-position-cron-db --remote --command \
  "select keibajo_code, race_bango, mode, error_name,
          substr(error_message,1,160) as err, recorded_at
   from finish_position_predict_retry_errors
   where run_ymd='20260816' order by recorded_at"
bunx wrangler d1 execute finish-position-cron-db --remote --command \
  "select keibajo_code, race_bango, mode, error_name, recorded_at
   from finish_position_predict_dlq_events
   where run_ymd='20260816' order by recorded_at"
```

Missing retry row ≠ never invoked.

## Tomorrow 09:10

1. D1 §1 until first `ok`.
2. Same minute: §4 list.
3. Neon §2 vs that morning’s baseline.
4. Pass = new `generated_at` **before post**. Fail = unchanged at post.
5. A fail after shipping tonight’s nine is the **wrong object**, not
   a bad patch (`tomorrow-morning-runbook-20260817.md`).
