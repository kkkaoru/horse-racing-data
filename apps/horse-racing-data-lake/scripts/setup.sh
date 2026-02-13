#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
APP_ENV_FILE="$APP_DIR/.env"
ROOT_ENV_FILE="$APP_DIR/../../.env"

# 共通 .env を読み込み（export して子プロセスに渡す）
set -a
# shellcheck source=/dev/null
source "$ROOT_ENV_FILE"

# アプリ固有 .env があれば読み込み
if [[ -f "$APP_ENV_FILE" ]]; then
  # shellcheck source=/dev/null
  source "$APP_ENV_FILE"
fi
set +a

# .env にキーを追記/更新するヘルパー関数
update_env() {
  local file="$1" key="$2" value="$3"
  if grep -q "^${key}=" "$file" 2>/dev/null; then
    sed -i '' "s|^${key}=.*|${key}=\"${value}\"|" "$file"
  else
    echo "${key}=\"${value}\"" >> "$file"
  fi
  echo "  ${key}=${value}"
}

# wrangler 出力から Stream ID を抽出
extract_stream_id() {
  grep -oE "[a-f0-9]{32}" | head -1 || true
}

# wrangler 出力からエンドポイント URL を抽出
extract_endpoint() {
  grep -i "endpoint" | grep -oE "https://[^ ]+" | head -1 || true
}

BUCKET_NAME="${HORSE_RACING_BUCKET_NAME:-horse-racing-data}"
NAMESPACE="horse_racing"

# テーブル定義
TABLES=(
  "race_records"
  "horse_info"
  "race_info"
  "trainer_info"
  "jockey_info"
  "owner_info"
  "breeder_info"
)

echo "=== Step 1: R2 バケット作成 ==="
bunx wrangler r2 bucket create "$BUCKET_NAME" || echo "Bucket may already exist"

echo "=== Step 2: R2 Data Catalog 有効化 ==="
CATALOG_OUTPUT=$(bunx wrangler r2 bucket catalog enable "$BUCKET_NAME" 2>&1 || echo "Catalog may already be enabled")
echo "$CATALOG_OUTPUT"

WAREHOUSE=$(echo "$CATALOG_OUTPUT" | grep -i "warehouse" | sed 's/.*: *//' | tr -d "[:space:]'" || true)
if [[ -n "$WAREHOUSE" ]]; then
  update_env "$APP_ENV_FILE" "HORSE_RACING_R2_SQL_WAREHOUSE" "$WAREHOUSE"
fi

# 各テーブルに対して Stream, Sink, Pipeline を作成
for TABLE in "${TABLES[@]}"; do
  STREAM_NAME="${TABLE}_stream"
  SINK_NAME="${TABLE}_sink"
  PIPELINE_NAME="${TABLE}_pipeline"
  SCHEMA_FILE="$APP_DIR/node_modules/horse-racing-schema/src/schemas/${TABLE}.json"
  ENV_PREFIX="$(echo "$TABLE" | tr '[:lower:]' '[:upper:]')"

  echo ""
  echo "=== テーブル: $TABLE ==="

  echo "--- Stream 作成: $STREAM_NAME ---"
  STREAM_OUTPUT=$(bunx wrangler pipelines streams create "$STREAM_NAME" \
    --schema-file "$SCHEMA_FILE" \
    --http-enabled true \
    --http-auth true 2>&1)
  echo "$STREAM_OUTPUT"

  STREAM_ID=$(echo "$STREAM_OUTPUT" | extract_stream_id)
  ENDPOINT=$(echo "$STREAM_OUTPUT" | extract_endpoint)

  if [[ -n "$STREAM_ID" ]]; then
    update_env "$APP_ENV_FILE" "${ENV_PREFIX}_STREAM_ID" "$STREAM_ID"
  fi
  if [[ -n "$ENDPOINT" ]]; then
    update_env "$APP_ENV_FILE" "${ENV_PREFIX}_STREAM_ENDPOINT" "$ENDPOINT"
  fi

  # テーブル固有のトークンがあればそれを使用、なければ共通トークンを使用
  TOKEN_VAR="${ENV_PREFIX}_API_TOKEN"
  CATALOG_TOKEN="${!TOKEN_VAR:-$CLOUDFLARE_API_TOKEN}"

  echo "--- Sink 作成: $SINK_NAME ---"
  bunx wrangler pipelines sinks create "$SINK_NAME" \
    --type r2-data-catalog \
    --bucket "$BUCKET_NAME" \
    --namespace "$NAMESPACE" \
    --table "$TABLE" \
    --catalog-token "$CATALOG_TOKEN" \
    --roll-interval 60

  echo "--- Pipeline 作成: $PIPELINE_NAME ---"
  bunx wrangler pipelines create "$PIPELINE_NAME" \
    --sql "INSERT INTO $SINK_NAME SELECT * FROM $STREAM_NAME"
done

echo ""
echo "=== 完了 ==="
echo "以下の値が $APP_ENV_FILE に保存されました:"
grep -E "^(RACE_RECORDS_|HORSE_INFO_|RACE_INFO_|TRAINER_INFO_|JOCKEY_INFO_|OWNER_INFO_|BREEDER_INFO_|HORSE_RACING_)" "$APP_ENV_FILE" || true
